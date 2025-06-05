package db

import (
	"fmt"
	"strings"
	"time"

	"github.com/foliagecp/easyjson"
	sf "github.com/foliagecp/sdk/statefun"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfp "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats.go"
)

type OpError struct {
	StatusCode int
	Details    string
}

func (oe *OpError) Error() string {
	return fmt.Sprintf("db_client operation failed with status %d: %s", oe.StatusCode, oe.Details)
}

func OpErrorFromOpMsg(om sfMediators.OpMsg) error {
	if om.Status == sfMediators.SYNC_OP_STATUS_OK || om.Status == sfMediators.SYNC_OP_STATUS_IDLE {
		return nil
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

		resp, err := nc.Request(
			fmt.Sprintf("%s.%s.%s.%s", sf.RequestPrefix, targetDomain, targetTypename, targetID),
			buildNatsData("cli", "cli", payload, options),
			time.Duration(NatsRequestTimeoutSec)*time.Second,
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
	return name + "===" + system.GetUniqueStrID()
}
