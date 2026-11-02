package statefun

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

// Change real broker resources at a precise inventory boundary. Delegating
// every other call preserves real listing, consumer and domain checks.
type changingDomainInventory struct {
	nats.JetStreamContext
	calls     int
	afterRead func(int, *nats.AccountInfo) error
}

func (js *changingDomainInventory) AccountInfo(opts ...nats.JSOpt) (*nats.AccountInfo, error) {
	info, err := js.JetStreamContext.AccountInfo(opts...)
	if err != nil {
		return nil, err
	}
	js.calls++
	if err := js.afterRead(js.calls, info); err != nil {
		return nil, err
	}
	return info, nil
}

func TestDomainState_ConcurrentCompatibleStreamCreationRetries(t *testing.T) {
	_, js, srv := domainTestNode(t, "domain_new")
	rt := domainTestRuntime(t, srv)
	changing := &changingDomainInventory{JetStreamContext: js, afterRead: func(call int, _ *nats.AccountInfo) error {
		if call != 1 {
			return nil
		}
		_, err := js.AddStream(&nats.StreamConfig{Name: domainEgressStreamName, Subjects: []string{"$SE.domain_new.>"}, Storage: nats.MemoryStorage})
		return err
	}}
	rt.js = changing
	require.NoError(t, rt.validateDomainState(context.Background(), "domain-test"))
	require.GreaterOrEqual(t, changing.calls, 4, "a changed listing must be read again, never accepted as complete")
}

func TestDomainState_ConcurrentForeignStreamCreationStillRefused(t *testing.T) {
	_, js, srv := domainTestNode(t, "domain_new")
	rt := domainTestRuntime(t, srv)
	changing := &changingDomainInventory{JetStreamContext: js, afterRead: func(call int, _ *nats.AccountInfo) error {
		if call != 1 {
			return nil
		}
		_, err := js.AddStream(&nats.StreamConfig{Name: domainEgressStreamName, Subjects: []string{"$SE.domain_old.>"}, Storage: nats.MemoryStorage})
		return err
	}}
	rt.js = changing
	require.ErrorIs(t, rt.validateDomainState(context.Background(), "domain-test"), ErrDomainStateMismatch)
	require.GreaterOrEqual(t, changing.calls, 4)
	info, err := js.StreamInfo(domainEgressStreamName)
	require.NoError(t, err)
	require.Equal(t, []string{"$SE.domain_old.>"}, info.Config.Subjects, "validation must not rewrite conflicting state")
}

func TestDomainState_ConcurrentConsumerCreationRetries(t *testing.T) {
	_, js, srv := domainTestNode(t, "domain_new")
	addDomainTestStream(t, js, domainEgressStreamName, "$SE.domain_new.>")
	rt := domainTestRuntime(t, srv)
	changing := &changingDomainInventory{JetStreamContext: js, afterRead: func(call int, _ *nats.AccountInfo) error {
		if call != 2 {
			return nil
		}
		// StreamInfo has already been listed with zero consumers. A compatible
		// auxiliary reader appears before the consumer list is fetched.
		_, err := js.AddConsumer(domainEgressStreamName, &nats.ConsumerConfig{Durable: "auxiliary-reader", AckPolicy: nats.AckExplicitPolicy})
		return err
	}}
	rt.js = changing
	require.NoError(t, rt.validateDomainState(context.Background(), "domain-test"))
	require.GreaterOrEqual(t, changing.calls, 4, "consumer counts must come from a refreshed stream snapshot")
}

func TestDomainState_IncompleteInventoryRemainsBoundedAndFailClosed(t *testing.T) {
	_, js, srv := domainTestNode(t, "domain_new")
	rt := domainTestRuntime(t, srv)
	rt.js = &changingDomainInventory{JetStreamContext: js, afterRead: func(_ int, info *nats.AccountInfo) error {
		// Model a listing that repeatedly omits one stream. No partial snapshot
		// may prove the absence of conflicting domain state.
		info.Streams++
		return nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	err := rt.validateDomainState(ctx, "domain-test")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, err, errDomainInventoryChanged)
}

func TestDomainState_InventoryReadErrorsAreNotCountChangeRetries(t *testing.T) {
	_, js, srv := domainTestNode(t, "domain_new")
	rt := domainTestRuntime(t, srv)
	denied := errors.New("inventory access denied")
	changing := &changingDomainInventory{JetStreamContext: js, afterRead: func(int, *nats.AccountInfo) error { return denied }}
	rt.js = changing
	require.ErrorIs(t, rt.validateDomainState(context.Background(), "domain-test"), denied)
	require.Equal(t, 1, changing.calls)
}
