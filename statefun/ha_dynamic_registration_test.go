package statefun

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func dynamicHAPair(t *testing.T, prepare ...func(*Runtime)) (active, passive *Runtime) {
	t.Helper()
	if system.GlobalPrometrics == nil {
		system.GlobalPrometrics = system.NewPrometrics("", "")
	}
	url := newHABroker(t)
	a, err := NewRuntime(*NewRuntimeConfigSimple(url, "hadomain").SetActivePassiveMode(true).SetKVMutexLifeTimeSec(4))
	require.NoError(t, err)
	b, err := NewRuntime(*NewRuntimeConfigSimple(url, "hadomain").SetActivePassiveMode(true).SetKVMutexLifeTimeSec(4))
	require.NoError(t, err)
	for _, setup := range prepare {
		setup(a)
		setup(b)
	}
	requireHARuntimesReady(t, launchHATestRuntime(t, a), launchHATestRuntime(t, b))
	require.Eventually(t, func() bool { return a.IsActiveInstance() != b.IsActiveInstance() }, 20*time.Second, 20*time.Millisecond)
	if a.IsActiveInstance() {
		return a, b
	}
	return b, a
}

func dynamicHAHandler(hits *atomic.Int64) FunctionLogicHandler {
	return func(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
		hits.Add(1)
		if ctx.Reply != nil {
			ctx.Reply.With(easyjson.NewJSONObjectWithKeyValue("status", easyjson.NewJSON("ok")).GetPtr())
		}
	}
}

func requireDynamicHAHandles(t *testing.T, active, passive *Runtime, fn string, activeHits, passiveHits *atomic.Int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		if !active.IsActiveInstance() || passive.IsActiveInstance() {
			return false
		}
		_ = active.Signal(sfPlugins.JetstreamGlobalSignal, fn, "x", easyjson.NewJSONObject().GetPtr(), nil)
		return activeHits.Load() > 0
	}, 10*time.Second, 50*time.Millisecond, "dynamic function must serve without an HA promotion")
	require.True(t, active.IsActiveInstance())
	require.False(t, passive.IsActiveInstance())
	require.Zero(t, passiveHits.Load(), "standby must never execute the function")
}

func Test_HA_DynamicRegistration_PassiveFirstServesWithoutPromotion(t *testing.T) {
	active, passive := dynamicHAPair(t)
	const fn = "fn.dynamic.passive.first"
	var activeHits, passiveHits atomic.Int64
	_, err := RegisterDynamicFunctionType(passive, fn, dynamicHAHandler(&passiveHits), *NewFunctionTypeConfig())
	require.NoError(t, err)
	_, err = RegisterDynamicFunctionType(active, fn, dynamicHAHandler(&activeHits), *NewFunctionTypeConfig())
	require.NoError(t, err)
	requireDynamicHAHandles(t, active, passive, fn, &activeHits, &passiveHits)
}

func Test_HA_DynamicRegistration_PassiveDoesNotTakeFunctionLock(t *testing.T) {
	active, passive := dynamicHAPair(t)
	const fn = "fn.dynamic.passive.no.lock"
	var hits atomic.Int64
	ft, err := RegisterDynamicFunctionType(passive, fn, dynamicHAHandler(&hits), *NewFunctionTypeConfig())
	require.NoError(t, err)
	_, err = active.Domain.kv.Get(system.GetHashStr(fn) + ".mutex")
	require.ErrorIs(t, err, nats.ErrKeyNotFound, "passive registration must not create a function lease")
	info, err := active.js.StreamInfo(ft.getStreamName())
	require.NoError(t, err)
	require.Zero(t, info.State.Consumers)
	require.Zero(t, hits.Load())
}

func Test_HA_DynamicRegistration_RetriesBusyLockWithoutPromotion(t *testing.T) {
	active, passive := dynamicHAPair(t)
	const fn = "fn.dynamic.busy.lock"
	var activeHits, passiveHits atomic.Int64
	// A lease acquired independently of registration models an earlier owner.
	key := system.GetHashStr(fn)
	revision, err := KeyMutexLock(context.Background(), active, key, true)
	require.NoError(t, err)
	_, err = RegisterDynamicFunctionType(active, fn, dynamicHAHandler(&activeHits), *NewFunctionTypeConfig())
	require.NoError(t, err)
	_, err = RegisterDynamicFunctionType(passive, fn, dynamicHAHandler(&passiveHits), *NewFunctionTypeConfig())
	require.NoError(t, err)
	require.NoError(t, KeyMutexUnlock(context.Background(), active, key, revision))
	requireDynamicHAHandles(t, active, passive, fn, &activeHits, &passiveHits)
}

func Test_HA_DynamicRegistration_RenewsAcquiredFunctionLock(t *testing.T) {
	active, passive := dynamicHAPair(t)
	const fn = "fn.dynamic.renew.lock"
	var hits atomic.Int64
	_, err := RegisterDynamicFunctionType(active, fn, dynamicHAHandler(&hits), *NewFunctionTypeConfig())
	require.NoError(t, err)
	key := system.GetHashStr(fn)
	first, err := active.Domain.kv.Get(key + ".mutex")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		entry, err := active.Domain.kv.Get(key + ".mutex")
		return err == nil && entry.Revision() > first.Revision() &&
			system.GetCurrentTimeNs() > system.BytesToInt64(first.Value())+int64(4*time.Second)
	}, 7*time.Second, 20*time.Millisecond, "dynamically acquired function lease must remain owned past its original TTL")
	_, err = KeyMutexLock(context.Background(), passive, key, true)
	require.ErrorIs(t, err, ErrMutexLocked)
	require.True(t, active.IsActiveInstance())
	require.False(t, passive.IsActiveInstance())
}

func Test_HA_DynamicRegistration_ExpiredLeaseIsRetriedWithoutPromotion(t *testing.T) {
	active, passive := dynamicHAPair(t)
	const fn = "fn.dynamic.expired.lease"
	var activeHits, passiveHits atomic.Int64
	_, err := KeyMutexLock(context.Background(), active, system.GetHashStr(fn), true)
	require.NoError(t, err)
	_, err = RegisterDynamicFunctionType(passive, fn, dynamicHAHandler(&passiveHits), *NewFunctionTypeConfig())
	require.NoError(t, err)
	_, err = RegisterDynamicFunctionType(active, fn, dynamicHAHandler(&activeHits), *NewFunctionTypeConfig())
	require.NoError(t, err)
	// No explicit unlock and no role change: the lifecycle must acquire the
	// expired lease itself, then create the missing consumer and subscription.
	requireDynamicHAHandles(t, active, passive, fn, &activeHits, &passiveHits)
}

func Test_HA_DynamicRegistration_PassiveFirstFailsOver(t *testing.T) {
	active, passive := dynamicHAPair(t)
	const fn = "fn.dynamic.passive.first.failover"
	var activeHits, passiveHits atomic.Int64
	_, err := RegisterDynamicFunctionType(passive, fn, dynamicHAHandler(&passiveHits), *NewFunctionTypeConfig())
	require.NoError(t, err)
	_, err = RegisterDynamicFunctionType(active, fn, dynamicHAHandler(&activeHits), *NewFunctionTypeConfig())
	require.NoError(t, err)
	requireDynamicHAHandles(t, active, passive, fn, &activeHits, &passiveHits)
	active.Shutdown(true)
	require.Eventually(t, func() bool {
		_ = passive.Signal(sfPlugins.JetstreamGlobalSignal, fn, "after-failover", easyjson.NewJSONObject().GetPtr(), nil)
		return passive.IsActiveInstance() && passiveHits.Load() > 0
	}, 20*time.Second, 50*time.Millisecond)
}

func Test_HA_DynamicRegistration_ConcurrentFunctionsRemainExclusive(t *testing.T) {
	active, passive := dynamicHAPair(t)
	const count = 8
	var activeHits, passiveHits [count]atomic.Int64
	var wg sync.WaitGroup
	failures := make(chan error, 2*count)
	for i := 0; i < count; i++ {
		for _, owner := range []struct {
			rt   *Runtime
			hits *atomic.Int64
		}{{passive, &passiveHits[i]}, {active, &activeHits[i]}} {
			wg.Add(1)
			go func(i int, rt *Runtime, hits *atomic.Int64) {
				defer wg.Done()
				_, err := RegisterDynamicFunctionType(rt, fmt.Sprintf("fn.dynamic.concurrent.%d", i), dynamicHAHandler(hits), *NewFunctionTypeConfig())
				failures <- err
			}(i, owner.rt, owner.hits)
		}
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	for i := 0; i < count; i++ {
		fn := fmt.Sprintf("fn.dynamic.concurrent.%d", i)
		requireDynamicHAHandles(t, active, passive, fn, &activeHits[i], &passiveHits[i])
		active.ftMu.RLock()
		ft := active.registeredFunctionTypes[fn]
		active.ftMu.RUnlock()
		info, err := active.js.StreamInfo(ft.getStreamName())
		require.NoError(t, err)
		require.Equal(t, 1, info.State.Consumers)
	}
}

var errDynamicSubscription = errors.New("injected dynamic subscription failure")

type dynamicSubscriptionFailure struct {
	nats.JetStreamContext
	fail atomic.Bool
}

func (j *dynamicSubscriptionFailure) QueueSubscribe(subject, queue string, cb nats.MsgHandler, opts ...nats.SubOpt) (*nats.Subscription, error) {
	if j.fail.CompareAndSwap(true, false) {
		return nil, errDynamicSubscription
	}
	return j.JetStreamContext.QueueSubscribe(subject, queue, cb, opts...)
}

func dynamicFailurePair(t *testing.T) (*Runtime, *Runtime, *dynamicSubscriptionFailure) {
	t.Helper()
	wrappers := make(map[*Runtime]*dynamicSubscriptionFailure)
	active, passive := dynamicHAPair(t, func(rt *Runtime) {
		wrapper := &dynamicSubscriptionFailure{JetStreamContext: rt.js}
		rt.js = wrapper
		wrappers[rt] = wrapper
	})
	return active, passive, wrappers[active]
}

func Test_HA_DynamicRegistration_FailedRegistrationReleasesLease(t *testing.T) {
	active, passive, failure := dynamicFailurePair(t)
	const fn = "fn.dynamic.registration.rollback"
	var activeHits, passiveHits atomic.Int64
	failure.fail.Store(true)
	_, err := RegisterDynamicFunctionType(active, fn, dynamicHAHandler(&activeHits), *NewFunctionTypeConfig())
	require.ErrorIs(t, err, errDynamicSubscription)
	active.ftMu.RLock()
	_, exists := active.registeredFunctionTypes[fn]
	active.ftMu.RUnlock()
	require.False(t, exists, "failed registration must not remain visible")
	key := system.GetHashStr(fn)
	entry, err := active.Domain.kv.Get(key + ".mutex")
	require.NoError(t, err)
	require.Zero(t, system.BytesToInt64(entry.Value()), "failed registration must release its acquired lease")
	_, err = RegisterDynamicFunctionType(active, fn, dynamicHAHandler(&activeHits), *NewFunctionTypeConfig())
	require.NoError(t, err)
	requireDynamicHAHandles(t, active, passive, fn, &activeHits, &passiveHits)
}

func Test_HA_DynamicRegistration_RetriesFailedSubscriptionAfterLeaseAcquired(t *testing.T) {
	active, passive, failure := dynamicFailurePair(t)
	const fn = "fn.dynamic.subscription.retry"
	var activeHits, passiveHits atomic.Int64
	key := system.GetHashStr(fn)
	revision, err := KeyMutexLock(context.Background(), active, key, true)
	require.NoError(t, err)
	_, err = RegisterDynamicFunctionType(active, fn, dynamicHAHandler(&activeHits), *NewFunctionTypeConfig())
	require.NoError(t, err)
	failure.fail.Store(true)
	require.NoError(t, KeyMutexUnlock(context.Background(), active, key, revision))
	requireDynamicHAHandles(t, active, passive, fn, &activeHits, &passiveHits)
	require.False(t, failure.fail.Load(), "the subscription failure must actually be exercised")
}

func Test_HA_DynamicRegistration_RetryDoesNotDuplicateExistingSubscriptions(t *testing.T) {
	active, passive := dynamicHAPair(t)
	const serving = "fn.dynamic.existing.responder"
	const waiting = "fn.dynamic.waiting.responder"
	var servingHits, waitingHits, standbyHits atomic.Int64
	cfg := *NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.NatsCoreGlobalRequest)
	servingFT, err := RegisterDynamicFunctionType(active, serving, dynamicHAHandler(&servingHits), cfg)
	require.NoError(t, err)
	require.NoError(t, active.nc.Flush())
	active.ftLifecycleMu.Lock()
	signalSubscription, requestSubscription := servingFT.signalSubscription, servingFT.requestSubscription
	active.ftLifecycleMu.Unlock()
	key := system.GetHashStr(waiting)
	revision, err := KeyMutexLock(context.Background(), active, key, true)
	require.NoError(t, err)
	_, err = RegisterDynamicFunctionType(active, waiting, dynamicHAHandler(&waitingHits), *NewFunctionTypeConfig())
	require.NoError(t, err)
	require.NoError(t, KeyMutexUnlock(context.Background(), active, key, revision))
	requireDynamicHAHandles(t, active, passive, waiting, &waitingHits, &standbyHits)
	active.ftLifecycleMu.Lock()
	sameSignal := signalSubscription == servingFT.signalSubscription
	sameRequest := requestSubscription == servingFT.requestSubscription
	active.ftLifecycleMu.Unlock()
	require.True(t, sameSignal, "starting a new function must preserve an existing signal subscription")
	require.True(t, sameRequest, "starting a new function must preserve an existing request subscription")
	reply, err := active.Request(sfPlugins.NatsCoreGlobalRequest, serving, "request", easyjson.NewJSONObject().GetPtr(), nil, time.Second)
	require.NoError(t, err)
	require.Equal(t, "ok", reply.GetByPath("status").AsStringDefault(""))
	require.EqualValues(t, 1, servingHits.Load())
}

type dynamicStreamGate struct {
	nats.JetStreamContext
	target      string
	enabled     atomic.Bool
	entered     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
	once        sync.Once
}

func (j *dynamicStreamGate) resume() { j.releaseOnce.Do(func() { close(j.release) }) }

func (j *dynamicStreamGate) AddStream(cfg *nats.StreamConfig, opts ...nats.JSOpt) (*nats.StreamInfo, error) {
	if cfg.Name == j.target && j.enabled.Load() {
		j.once.Do(func() { close(j.entered) })
		<-j.release
	}
	return j.JetStreamContext.AddStream(cfg, opts...)
}

func dynamicGatedPair(t *testing.T, name string) (*Runtime, *Runtime, *dynamicStreamGate) {
	t.Helper()
	gates := make(map[*Runtime]*dynamicStreamGate)
	active, passive := dynamicHAPair(t, func(rt *Runtime) {
		subject := fmt.Sprintf(DomainIngressSubjectsTmpl, rt.Domain.name, fmt.Sprintf("%s.%s.%s.*", SignalPrefix, rt.Domain.name, name))
		gate := &dynamicStreamGate{
			JetStreamContext: rt.js, target: system.GetHashStr(subject) + "_stream",
			entered: make(chan struct{}), release: make(chan struct{}),
		}
		rt.js = gate
		gates[rt] = gate
	})
	gate := gates[passive]
	gate.enabled.Store(true)
	t.Cleanup(gate.resume)
	return active, passive, gate
}

func Test_HA_DynamicRegistration_PromotionDuringStreamCreation(t *testing.T) {
	const fn = "fn.dynamic.promote.during.registration"
	active, passive, gate := dynamicGatedPair(t, fn)
	var hits atomic.Int64
	done := make(chan error, 1)
	go func() {
		_, err := RegisterDynamicFunctionType(passive, fn, dynamicHAHandler(&hits), *NewFunctionTypeConfig())
		done <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("stream creation was not reached")
	}
	passive.ftMu.RLock()
	_, visible := passive.registeredFunctionTypes[fn]
	passive.ftMu.RUnlock()
	require.False(t, visible, "promotion must not see a partially initialized function")
	active.Shutdown(true)
	require.Eventually(t, passive.IsActiveInstance, 15*time.Second, 20*time.Millisecond,
		"slow stream creation must not block HA promotion")
	gate.enabled.Store(false)
	gate.resume()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("registration did not finish")
	}
	require.Eventually(t, func() bool {
		_ = passive.Signal(sfPlugins.JetstreamGlobalSignal, fn, "x", easyjson.NewJSONObject().GetPtr(), nil)
		return hits.Load() > 0
	}, 5*time.Second, 20*time.Millisecond)
}

func Test_HA_DynamicRegistration_ShutdownDuringStreamCreation(t *testing.T) {
	const fn = "fn.dynamic.shutdown.during.registration"
	_, passive, gate := dynamicGatedPair(t, fn)
	var hits atomic.Int64
	done := make(chan error, 1)
	go func() {
		_, err := RegisterDynamicFunctionType(passive, fn, dynamicHAHandler(&hits), *NewFunctionTypeConfig())
		done <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("stream creation was not reached")
	}
	passive.Shutdown(true)
	gate.enabled.Store(false)
	gate.resume()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("registration outlived shutdown")
	}
	passive.ftMu.RLock()
	_, visible := passive.registeredFunctionTypes[fn]
	passive.ftMu.RUnlock()
	require.False(t, visible)
	require.Zero(t, hits.Load())
}

func Test_HA_DynamicRegistration_LocalCallsDuringRegistration(t *testing.T) {
	if system.GlobalPrometrics == nil {
		system.GlobalPrometrics = system.NewPrometrics("", "")
	}
	rt, err := NewRuntime(*NewRuntimeConfigSimple(newHABroker(t), "hadomain").SetActivePassiveMode(false))
	require.NoError(t, err)
	requireHARuntimesReady(t, launchHATestRuntime(t, rt))
	const stable = "fn.dynamic.local.stable"
	var hits atomic.Int64
	cfg := *NewFunctionTypeConfig().SetMultipleInstancesAllowance(true).SetAllowedSignalProviders().SetAllowedRequestProviders(sfPlugins.GolangLocalRequest)
	_, err = RegisterDynamicFunctionType(rt, stable, dynamicHAHandler(&hits), cfg)
	require.NoError(t, err)
	started, stop, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	failures := make(chan error, 1)
	go func() {
		defer close(done)
		var once sync.Once
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, err := rt.Request(sfPlugins.GolangLocalRequest, stable, "x", easyjson.NewJSONObject().GetPtr(), nil, time.Second)
			once.Do(func() { close(started) })
			if err != nil {
				failures <- err
				return
			}
		}
	}()
	t.Cleanup(func() {
		select {
		case <-stop:
		default:
			close(stop)
		}
		<-done
	})
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("local request did not start")
	}
	for i := 0; i < 32; i++ {
		_, err := RegisterDynamicFunctionType(rt, fmt.Sprintf("fn.dynamic.local.concurrent.%d", i), dynamicHAHandler(&hits), cfg)
		require.NoError(t, err)
	}
	close(stop)
	<-done
	select {
	case err := <-failures:
		require.NoError(t, err)
	default:
	}
	require.Greater(t, hits.Load(), int64(0))
	_, err = rt.Domain.kv.Get(system.GetHashStr(stable) + ".mutex")
	require.ErrorIs(t, err, nats.ErrKeyNotFound, "multi-instance registration must not acquire a lease")
	_, err = RegisterDynamicFunctionType(rt, stable, dynamicHAHandler(&hits), cfg)
	require.ErrorContains(t, err, "already registered")
}

type dynamicSubscribeGate struct {
	nats.JetStreamContext
	target  string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (j *dynamicSubscribeGate) QueueSubscribe(subject, queue string, cb nats.MsgHandler, opts ...nats.SubOpt) (*nats.Subscription, error) {
	if subject == j.target {
		close(j.entered)
		<-j.release
	}
	return j.JetStreamContext.QueueSubscribe(subject, queue, cb, opts...)
}

func (j *dynamicSubscribeGate) resume() { j.once.Do(func() { close(j.release) }) }

func Test_HA_DynamicRegistration_DemotionDoesNotWaitForRegistration(t *testing.T) {
	const fn = "fn.dynamic.demote.during.subscription"
	gates := make(map[*Runtime]*dynamicSubscribeGate)
	active, passive := dynamicHAPair(t, func(rt *Runtime) {
		gate := &dynamicSubscribeGate{
			JetStreamContext: rt.js,
			target:           fmt.Sprintf(DomainIngressSubjectsTmpl, rt.Domain.name, fmt.Sprintf("%s.%s.%s.*", SignalPrefix, rt.Domain.name, fn)),
			entered:          make(chan struct{}), release: make(chan struct{}),
		}
		gates[rt] = gate
		rt.js = gate
	})
	gate := gates[active]
	t.Cleanup(gate.resume)
	var hits atomic.Int64
	done := make(chan error, 1)
	go func() {
		_, err := RegisterDynamicFunctionType(active, fn, dynamicHAHandler(&hits), *NewFunctionTypeConfig())
		done <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("subscription creation was not reached")
	}
	runtimeKey := system.GetHashStr(RuntimeName)
	require.Eventually(t, func() bool {
		entry, err := active.Domain.kv.Get(runtimeKey + ".mutex")
		return err == nil && KeyMutexUnlock(context.Background(), active, runtimeKey, entry.Revision()) == nil
	}, 5*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool { return !active.IsActiveInstance() }, 5*time.Second, 20*time.Millisecond,
		"a pending registration must not postpone revoking a lost active role")
	require.Eventually(t, passive.IsActiveInstance, 15*time.Second, 20*time.Millisecond)
	gate.resume()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("registration did not finish")
	}
	active.ftLifecycleMu.Lock()
	ft := active.registeredFunctionTypes[fn]
	subscribed := ft.signalSubscription != nil && ft.signalSubscription.IsValid()
	active.ftLifecycleMu.Unlock()
	require.False(t, subscribed, "a registration which loses its active role must not leave a live subscription")
	require.Zero(t, hits.Load())
}

func Test_HA_DynamicRegistration_PromotionRetriesDeferredSubscriptions(t *testing.T) {
	active, passive := dynamicHAPair(t)
	const fn = "fn.dynamic.promotion.deferred"
	var hits atomic.Int64
	_, err := RegisterDynamicFunctionType(passive, fn, dynamicHAHandler(&hits),
		*NewFunctionTypeConfig().SetMultipleInstancesAllowance(true))
	require.NoError(t, err)
	// Hold the lifecycle gate across promotion to model in-flight registration.
	// A multi-instance function has no pending lease acquisition to mask a lost
	// subscription request by triggering subscriptions again on a later tick.
	passive.ftLifecycleMu.Lock()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(passive.ftLifecycleMu.Unlock) }
	t.Cleanup(release)
	active.Shutdown(true)
	require.Eventually(t, passive.IsActiveInstance, 15*time.Second, 20*time.Millisecond)
	revision := passive.getActiveRevID()
	require.Eventually(t, func() bool { return passive.getActiveRevID() != revision },
		5*time.Second, 20*time.Millisecond, "runtime lease must refresh while subscription work is deferred")
	release()
	require.Eventually(t, func() bool {
		_ = passive.Signal(sfPlugins.JetstreamGlobalSignal, fn, "after-promotion", easyjson.NewJSONObject().GetPtr(), nil)
		return hits.Load() > 0
	}, 5*time.Second, 20*time.Millisecond, "promotion must retain deferred subscriptions until they can start")
}
