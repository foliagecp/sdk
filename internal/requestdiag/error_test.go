package requestdiag

import (
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func TestWrapPreservesTransportCauseAndExplainsAddress(t *testing.T) {
	a := Address{LocalDomain: "local", DefaultDomain: "hub", TargetDomain: "remote", Function: "functions.test", ObjectID: "remote/object", Transport: "NATS Request/Reply", DomainSource: "object_id_prefix", Timeout: time.Second}
	for _, cause := range []error{nats.ErrNoResponders, nats.ErrTimeout, nats.ErrConnectionClosed} {
		t.Run(cause.Error(), func(t *testing.T) {
			err := Wrap(cause, a)
			require.ErrorIs(t, err, cause)
			for _, part := range []string{cause.Error(), a.Function, a.ObjectID, `target_domain="remote"`, `transport="NATS Request/Reply"`, `timeout=1s`} {
				require.Contains(t, err.Error(), part)
			}
			// Legacy clients match these phrases to detect missing graph entities.
			for _, phrase := range []string{"does not exist", "not found"} {
				require.False(t, strings.Contains(err.Error(), phrase))
			}
		})
	}
	require.NoError(t, Wrap(nil, a))
}
