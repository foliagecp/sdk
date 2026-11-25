package kv

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

var (
	// walkQuietInterval is how long the walk waits for the next message before
	// it asks the consumer whether there is one to wait for.
	walkQuietInterval = 250 * time.Millisecond
	// walkStallTimeout is how long the walk goes without a message and without
	// the consumer confirming the end before it reports a stalled read.
	walkStallTimeout = 30 * time.Second
)

// ErrWalkStalled is returned by KVWalkLatest when the read neither advances
// nor can be shown complete for walkStallTimeout.
var ErrWalkStalled = errors.New("kv walk stalled")

// KVWalkLatest calls visit with the latest message of every key matching keys,
// a value or a delete marker, in stream order, and returns once everything the
// bucket held has been visited.
//
// It reads what nats.go's Watch reads up to its end-of-initial-values marker,
// and differs in how the end is found. Watch learns it from the pending count
// each message carries: the last message must say zero. A message removed from
// the stream before its delivery, as a delete marker purged by its sequence
// is, never arrives, the counts already sent stay above zero, and Watch waits
// for a message that will not come. The walk takes the zero count when it
// sees one and otherwise, once the stream goes quiet, asks the consumer
// itself: nothing pending and nothing delivered beyond what was visited is
// the end.
func KVWalkLatest(ctx context.Context, js nats.JetStreamContext, kv nats.KeyValue, keys string,
	visit func(key string, value []byte, revision uint64, deleted bool)) error {
	pre := KVStoredSubject(kv, "")
	// The handler hands messages over and blocks while the walk is busy: that
	// is what paces the delivery, through the consumer's flow control. The
	// client side is therefore left without a limit of its own.
	updates := make(chan *nats.Msg, 256)
	done := make(chan struct{})
	defer close(done)
	sub, err := js.Subscribe(pre+keys, func(m *nats.Msg) {
		select {
		case updates <- m:
		case <-done:
		}
	}, nats.BindStream(KVStreamName(kv)), nats.OrderedConsumer(), nats.DeliverLastPerSubject())
	if err != nil {
		return fmt.Errorf("kv walk subscribe: %w", err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := sub.SetPendingLimits(-1, -1); err != nil {
		return fmt.Errorf("kv walk subscribe: %w", err)
	}

	// The consumer that delivered the last visited message and its sequence
	// there. An ordered consumer is recreated under a new name after a gap, so
	// the name is part of the position.
	var consumer string
	var seen uint64
	atEnd := func() bool {
		ci, err := sub.ConsumerInfo()
		if err != nil || ci.NumPending != 0 {
			return false
		}
		return ci.Delivered.Consumer == 0 || (ci.Name == consumer && ci.Delivered.Consumer == seen)
	}
	// An empty bucket is the common case in tests and on a first start: do not
	// make it wait out a quiet interval.
	if atEnd() {
		return nil
	}

	lastProgress := time.Now()
	quiet := time.NewTimer(walkQuietInterval)
	defer quiet.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-quiet.C:
			if atEnd() {
				return nil
			}
			if time.Since(lastProgress) > walkStallTimeout {
				return fmt.Errorf("%w: no message for %s after %d from consumer %q, and the consumer does not confirm the end",
					ErrWalkStalled, walkStallTimeout, seen, consumer)
			}
			quiet.Reset(walkQuietInterval)
		case msg := <-updates:
			meta, err := msg.Metadata()
			if err != nil || len(msg.Subject) <= len(pre) {
				continue
			}
			lastProgress = time.Now()
			consumer, seen = meta.Consumer, meta.Sequence.Consumer
			op := msg.Header.Get(kvop)
			visit(msg.Subject[len(pre):], msg.Data, meta.Sequence.Stream, op == kvdel || op == kvpurge)
			if meta.NumPending == 0 {
				return nil
			}
			if !quiet.Stop() {
				select {
				case <-quiet.C:
				default:
				}
			}
			quiet.Reset(walkQuietInterval)
		}
	}
}
