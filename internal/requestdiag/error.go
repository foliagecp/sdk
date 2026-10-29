// Package requestdiag provides SDK-internal call diagnostics without changing
// public call signatures or wire envelopes. Transport causes remain unwrap-able.
package requestdiag

import (
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

type Address struct {
	LocalDomain, DefaultDomain, TargetDomain, Function, ObjectID, Transport, DomainSource string
	Timeout                                                                               time.Duration
}

func Wrap(err error, a Address) error {
	if err == nil {
		return nil
	}
	reason := "call failed"
	switch {
	case errors.Is(err, nats.ErrNoResponders):
		reason = "no reachable request responder; check the domain prefix in the object ID, application readiness and Request/Reply support; domain validity and object existence are undetermined"
	case errors.Is(err, nats.ErrTimeout):
		reason = "reply deadline exceeded; the operation may already have started; verify service health and workload before retrying"
	}
	return fmt.Errorf("SDK call: function=%q object=%q target_domain=%q domain_source=%q local_domain=%q default_domain=%q transport=%q timeout=%s: %s: %w", a.Function, a.ObjectID, a.TargetDomain, a.DomainSource, a.LocalDomain, a.DefaultDomain, a.Transport, a.Timeout, reason, err)
}
