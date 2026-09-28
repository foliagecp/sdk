package cache

// Taking back what a delete leaves in the bucket.
//
// The cache is the source of truth and KV is where it is exported, through the
// WAL: every write and every delete travels as a transaction and is applied to
// the bucket by the committer. A delete applied to a NATS KV bucket is not a
// removal, though — it is a message, a marker that becomes the key's last
// message while the bucket's one-per-subject limit drops the value it
// replaces. Nothing drops the marker. Every load reads the bucket with
// IgnoreDeletes, so nobody ever consumes it, and the stream keeps one message
// per key ever deleted for as long as the key stays deleted — under churn,
// without bound.
//
// The marker is ours: the committer published it, and the broker's ack says
// exactly where it landed. So the committer hands the marker's key and
// sequence here, and the purger deletes that one message from the stream by
// its sequence — the marker and nothing written after it. That is
// proportional to what was deleted and touches nothing else: it never needs
// to look at the bucket to find out what to remove, and the broker finds the
// message without walking the stream either (see KVPurgeTombstone — the
// runtime's leases live in the same stream, and a removal that held the
// stream up held them up too).
//
// Markers left from before — a version without this, or a shutdown that cut
// the purger off with work queued — are met by the one walk over the bucket
// that happens anyway: the load. It sees them, skips them as data, and hands
// them here too.

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	customNatsKv "github.com/foliagecp/sdk/embedded/nats/kv"
	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// tombstonePurgeWorkers is how many removal requests are in flight at once:
	// each is one round trip to the broker, and a delete cascade queues
	// thousands.
	tombstonePurgeWorkers = 4
	// tombstonePurgeAttempts is how many times a marker is tried before it is
	// given up on and left to the next load.
	tombstonePurgeAttempts = 3
	// tombstonePurgeRetryDelay is the pause before failed markers are tried
	// again, so a broker that is struggling is not hammered.
	tombstonePurgeRetryDelay = time.Second
	// tombstoneDropWarnInterval rate-limits the warning about markers given
	// up on: one line per interval, with the count.
	tombstoneDropWarnInterval = time.Minute
)

// kvTombstone is one delete marker in the bucket's stream: the store key it
// was published under and the stream sequence the broker gave it.
type kvTombstone struct {
	key      string
	seq      uint64
	attempts int
}

// tombstonePurger removes delete markers from the bucket's stream as they are
// reported, a bounded number at a time, retrying what fails.
type tombstonePurger struct {
	js nats.JetStreamContext
	kv nats.KeyValue
	id string // cache id, the metrics label

	mu    sync.Mutex
	queue []kvTombstone
	wake  chan struct{}
	// inFlight is the size of the batch being purged right now: taken out of
	// the queue, not yet removed. pending counts it, or a caller waiting for a
	// clean bucket would see zero while the requests are still out.
	inFlight atomic.Int64

	purged  atomic.Int64
	dropped atomic.Int64

	warnMu       sync.Mutex
	lastDropWarn time.Time
	dropsSince   int
}

func newTombstonePurger(js nats.JetStreamContext, kv nats.KeyValue, id string) *tombstonePurger {
	return &tombstonePurger{js: js, kv: kv, id: id, wake: make(chan struct{}, 1)}
}

// note queues the marker at seq under key for removal. Never blocks: the
// caller is the write pipeline or the loader, and neither waits on the broker
// for this.
func (p *tombstonePurger) note(key string, seq uint64) {
	p.mu.Lock()
	p.queue = append(p.queue, kvTombstone{key: key, seq: seq})
	p.mu.Unlock()
	select {
	case p.wake <- struct{}{}:
	default: // already awake
	}
}

// pending is how many markers are reported and not yet removed.
func (p *tombstonePurger) pending() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.queue) + int(p.inFlight.Load())
}

func (p *tombstonePurger) take() []kvTombstone {
	p.mu.Lock()
	batch := p.queue
	p.queue = nil
	p.mu.Unlock()
	return batch
}

func (p *tombstonePurger) requeue(ts []kvTombstone) {
	if len(ts) == 0 {
		return
	}
	p.mu.Lock()
	p.queue = append(ts, p.queue...)
	p.mu.Unlock()
}

// run drains the queue until ctx ends. Markers that fail are tried again after
// a pause, up to tombstonePurgeAttempts, then dropped with a warning — the
// next load will meet them and try once more.
func (p *tombstonePurger) run(ctx context.Context) {
	system.GlobalPrometrics.GetRoutinesCounter().Started("cache.tombstonePurger")
	defer system.GlobalPrometrics.GetRoutinesCounter().Stopped("cache.tombstonePurger")

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.wake:
		}
		for {
			batch := p.take()
			if len(batch) == 0 {
				break
			}
			p.inFlight.Store(int64(len(batch)))
			retry := p.purge(ctx, batch)
			p.requeue(retry)
			p.inFlight.Store(0)
			p.publishGauges()
			if len(retry) == 0 {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(tombstonePurgeRetryDelay):
			}
		}
	}
}

// purge removes the batch with tombstonePurgeWorkers requests in flight and
// returns the markers that failed and still have attempts left.
func (p *tombstonePurger) purge(ctx context.Context, batch []kvTombstone) []kvTombstone {
	var (
		mu    sync.Mutex
		retry []kvTombstone
		wg    sync.WaitGroup
	)
	work := make(chan kvTombstone)
	for w := 0; w < tombstonePurgeWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range work {
				if ctx.Err() != nil {
					mu.Lock()
					retry = append(retry, t)
					mu.Unlock()
					continue
				}
				if err := customNatsKv.KVPurgeTombstone(p.js, p.kv, t.key, t.seq); err != nil {
					t.attempts++
					if t.attempts < tombstonePurgeAttempts {
						mu.Lock()
						retry = append(retry, t)
						mu.Unlock()
					} else {
						p.dropped.Add(1)
						p.warnDropped(t, err)
					}
					continue
				}
				p.purged.Add(1)
			}
		}()
	}
	for _, t := range batch {
		work <- t
	}
	close(work)
	wg.Wait()
	return retry
}

// warnDropped logs a marker given up on, at most once per interval, with how
// many were given up on since the last line.
func (p *tombstonePurger) warnDropped(t kvTombstone, err error) {
	p.warnMu.Lock()
	defer p.warnMu.Unlock()
	p.dropsSince++
	if time.Since(p.lastDropWarn) < tombstoneDropWarnInterval {
		return
	}
	lg.Logf(lg.WarnLevel, "cache %q: gave up purging %d KV delete marker(s) after %d attempts, last: key=%s seq=%d: %v (left for the next load)",
		p.id, p.dropsSince, tombstonePurgeAttempts, t.key, t.seq, err)
	p.lastDropWarn = time.Now()
	p.dropsSince = 0
}

func (p *tombstonePurger) publishGauges() {
	set := func(name, help string, value float64) {
		gv, err := system.GlobalPrometrics.EnsureGaugeVecSimple(name, help, []string{"id"})
		if err != nil {
			return
		}
		gv.With(prometheus.Labels{"id": p.id}).Set(value)
	}
	set("kv_tombstones_pending", "KV delete markers queued for removal and not yet removed", float64(p.pending()))
	set("kv_tombstones_purged_total", "KV delete markers removed from the bucket's stream since start", float64(p.purged.Load()))
	set("kv_tombstones_dropped_total", "KV delete markers given up on after repeated purge failures since start", float64(p.dropped.Load()))
}

// NoteKVTombstone reports that the delete marker for the store key sits at
// stream sequence seq in the bucket, so the purger can remove it. The
// committer calls it with what the broker's ack says; the loader calls it with
// what it meets. No-op without a KV.
func (cs *Store) NoteKVTombstone(key string, seq uint64) {
	if cs.tombstones == nil {
		return
	}
	cs.tombstones.note(key, seq)
}

// KVTombstonesPendingForTest is how many reported markers the purger has not
// removed yet, so a test can wait for the bucket to be clean instead of
// sleeping.
func (cs *Store) KVTombstonesPendingForTest() int {
	if cs.tombstones == nil {
		return 0
	}
	return cs.tombstones.pending()
}
