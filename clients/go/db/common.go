package db

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/foliagecp/easyjson"
	sf "github.com/foliagecp/sdk/statefun"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfp "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/nats-io/nats.go"
	"golang.org/x/sync/singleflight"
)

// readResult is the payload returned by every Read wrapper through its
// singleflight group. We carry the (data, err) tuple verbatim so that
// the calling Read method can surface them exactly as the underlying
// SDK request did — IDLE/ErrNotFound included.
//
// IMPORTANT: callers of singleflight-deduplicated reads MUST clone the
// returned data before mutating it. Several waiters arriving on the
// same in-flight call share the same JSON pointer; without a Clone()
// at the surface boundary, one waiter's SetByPath could be observed
// by another. Read wrappers in this package handle the clone for the
// caller — see ObjectRead / VertexRead / etc.
type readResult struct {
	data easyjson.JSON
	err  error
}

// doRead runs fn under the given singleflight group keyed by `key` and
// returns a fresh (cloned) data + the original error, isolating
// concurrent waiters from each other's mutations.
//
// The inner fn must return readResult inside the any-typed first
// return; the error second return is reserved for unrecoverable
// singleflight-level failures, which the SDK request path does not
// produce — application-level errors travel inside readResult.err so
// they propagate to every waiter on the in-flight call.
//
// When group is nil (client constructed without the New... factory,
// so readFlight was never initialized) doRead falls back to a direct
// fn invocation — no deduplication, but correctness preserved.
func doRead(group *singleflight.Group, key string, fn func() (any, error)) (easyjson.JSON, error) {
	if group == nil {
		v, _ := fn()
		r, ok := v.(readResult)
		if !ok {
			return easyjson.NewJSONNull(), fmt.Errorf("db_client: read inner produced unexpected type %T", v)
		}
		return r.data, r.err
	}
	v, _, _ := group.Do(key, fn)
	r, ok := v.(readResult)
	if !ok {
		return easyjson.NewJSONNull(), fmt.Errorf("db_client: singleflight inner produced unexpected type %T", v)
	}
	return r.data.Clone(), r.err
}

// ErrNotFound is returned by Read-flavoured client calls when the underlying
// CMDB / graph entity does not exist. It wraps the "idle" status produced by
// the SDK's OpMediator for read paths where IDLE semantically means "not
// found" rather than "no-op success".
//
// Callers should prefer `errors.Is(err, db.ErrNotFound)` to distinguish a
// missing entity from other failures. The original status code and details
// from the SDK are still accessible by unwrapping into *OpError.
var ErrNotFound = errors.New("db_client: entity not found")

type OpError struct {
	StatusCode int
	Details    string
}

func (oe *OpError) Error() string {
	return fmt.Sprintf("db_client operation failed with status %d: %s", oe.StatusCode, oe.Details)
}

// OpErrorFromOpMsg is the LENIENT mapping used by Create / Update / Delete
// client wrappers. It treats both OK and IDLE as success (returns nil),
// because for those operations IDLE means "the requested state is already
// satisfied" — e.g. deleting an already-missing entity is idempotent.
//
// DO NOT use this from Read paths: for reads, IDLE means "not found" and
// must not be swallowed silently. Use OpErrorFromOpMsgStrict instead.
func OpErrorFromOpMsg(om sfMediators.OpMsg) error {
	if om.Status == sfMediators.SYNC_OP_STATUS_OK || om.Status == sfMediators.SYNC_OP_STATUS_IDLE {
		return nil
	}
	return &OpError{om.Status, om.Details}
}

// OpErrorFromOpMsgStrict is the STRICT mapping used by Read-flavoured client
// wrappers. It returns nil only for OK; IDLE is surfaced as ErrNotFound
// (wrapped via %w so callers can `errors.Is(err, ErrNotFound)`); all other
// statuses become an *OpError exactly as in OpErrorFromOpMsg.
//
// Rationale: the lenient mapping (OK || IDLE -> nil) is correct for write
// paths where IDLE = "no-op, already satisfied". For reads however, IDLE is
// emitted by HL/LL when the target entity does not exist, and returning nil
// would force every caller to also inspect the returned JSON for emptiness
// to distinguish "found, empty body" from "not found". Surfacing IDLE as a
// typed error fixes that footgun while keeping the original details visible
// for any caller still relying on substring matching against the message
// (e.g. legacy `strings.Contains(err.Error(), "does not exist")` checks).
func OpErrorFromOpMsgStrict(om sfMediators.OpMsg) error {
	if om.Status == sfMediators.SYNC_OP_STATUS_OK {
		return nil
	}
	if om.Status == sfMediators.SYNC_OP_STATUS_IDLE {
		details := om.Details
		if details == "" {
			details = "idle"
		}
		return fmt.Errorf("%w: %s", ErrNotFound, details)
	}
	return &OpError{om.Status, om.Details}
}

func buildNatsData(callerTypename string, callerID string, payload *easyjson.JSON, options *easyjson.JSON) []byte {
	data := easyjson.NewJSONObject()
	data.SetByPath("caller_typename", easyjson.NewJSON(callerTypename))
	data.SetByPath("caller_id", easyjson.NewJSON(callerID))
	if payload != nil {
		data.SetByPath("payload", *payload)
	}
	if options != nil {
		data.SetByPath("options", *options)
	}
	return data.ToBytes()
}

func getRequestFunc(nc *nats.Conn, NatsRequestTimeoutSec int, HubDomainName string) sfp.SFRequestFunc {
	return func(r sfp.RequestProvider, targetTypename string, targetID string, payload *easyjson.JSON, options *easyjson.JSON, timeout ...time.Duration) (*easyjson.JSON, error) {
		targetDomain := HubDomainName
		tokens := strings.Split(targetID, sf.ObjectIDDomainSeparator)
		if len(tokens) == 2 {
			targetDomain = tokens[0]
		}
		requestTimeout := time.Duration(NatsRequestTimeoutSec) * time.Second
		if len(timeout) > 0 && timeout[0] > 0 {
			requestTimeout = timeout[0]
		}

		resp, err := nc.Request(
			fmt.Sprintf("%s.%s.%s.%s", sf.RequestPrefix, targetDomain, targetTypename, targetID),
			buildNatsData("cli", "cli", payload, options),
			requestTimeout,
		)
		if err == nil {
			if j, ok := easyjson.JSONFromBytes(resp.Data); ok {
				return &j, err
			}
			return nil, fmt.Errorf("response from function typename \"%s\" with id \"%s\" is not a json", targetTypename, targetID)
		}
		return nil, err
	}
}

func seqFree(name string) string {
	//return name + "===" + system.GetUniqueStrID()
	return name
}
