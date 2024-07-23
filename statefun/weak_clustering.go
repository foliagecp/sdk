package statefun

import (
	"fmt"
	"strings"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats.go"
)

const (
	ObjectIDWeakClusteringDomainSeparator = "#"
)

func (r *Runtime) getShadowObjectDomainAndID(shadowObjectId string) (domainName, objectIdWithoutDomain string, err error) {
	err = nil

	idWithoutDomain := r.Domain.GetObjectIDWithoutDomain(shadowObjectId)

	tokens := strings.Split(idWithoutDomain, ObjectIDWeakClusteringDomainSeparator)
	if !(len(tokens) > 1 && len(tokens[0]) > 0 && len(tokens[1]) > 0) {
		return "", "", fmt.Errorf("id=%s is not a shadow object's id", shadowObjectId)
	}

	domainName = tokens[0]
	objectIdWithoutDomain = tokens[1]

	return
}

func (r *Runtime) isShadowObject(idWithDomain string) bool {
	idWithoutDomain := r.Domain.GetObjectIDWithoutDomain(idWithDomain)

	if tokens := strings.Split(idWithoutDomain, ObjectIDWeakClusteringDomainSeparator); len(tokens) > 1 && len(tokens[0]) > 0 && len(tokens[1]) > 0 {
		return true
	}

	return false
}

func (r *Runtime) signalShadowObject(callerTypename string, callerID string, targetTypename string, targetID string, payload *easyjson.JSON, options *easyjson.JSON) error {
	tDomainName, tObjectIdWithoutDomain, err := r.getShadowObjectDomainAndID(targetID)
	if err != nil {
		return err
	}
	objectIdInRemoteDomain := fmt.Sprintf("%s%s%s", tDomainName, ObjectIDDomainSeparator, tObjectIdWithoutDomain)

	shadowCallerID := fmt.Sprintf(
		"%s%s%s%s%s",
		tDomainName,
		ObjectIDDomainSeparator,
		r.Domain.GetDomainFromObjectID(callerID),
		ObjectIDWeakClusteringDomainSeparator,
		r.Domain.GetObjectIDWithoutDomain(callerID),
	)
	system.MsgOnErrorReturn(r.nc.Publish(
		fmt.Sprintf(DomainIngressSubjectsTmpl, tDomainName, fmt.Sprintf("%s.%s.%s.%s", SignalPrefix, tDomainName, targetTypename, objectIdInRemoteDomain)),
		buildNatsData(r.Domain.name, callerTypename, shadowCallerID, payload, options),
	))

	return nil
}

func (r *Runtime) requestShadowObject(callerTypename string, callerID string, targetTypename string, targetID string, payload *easyjson.JSON, options *easyjson.JSON) (*nats.Msg, error) {
	tDomainName, tObjectIdWithoutDomain, err := r.getShadowObjectDomainAndID(targetID)
	if err != nil {
		return nil, err
	}
	objectIdInRemoteDomain := fmt.Sprintf("%s%s%s", tDomainName, ObjectIDDomainSeparator, tObjectIdWithoutDomain)

	shadowCallerID := fmt.Sprintf(
		"%s%s%s%s%s",
		tDomainName,
		ObjectIDDomainSeparator,
		r.Domain.GetDomainFromObjectID(callerID),
		ObjectIDWeakClusteringDomainSeparator,
		r.Domain.GetObjectIDWithoutDomain(callerID),
	)
	resp, err := r.nc.Request(
		fmt.Sprintf("%s.%s.%s.%s", RequestPrefix, tDomainName, targetTypename, objectIdInRemoteDomain),
		buildNatsData(r.Domain.name, callerTypename, shadowCallerID, payload, options),
		time.Duration(r.config.requestTimeoutSec)*time.Second,
	)

	return resp, err
}
