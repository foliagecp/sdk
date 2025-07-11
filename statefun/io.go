package statefun

import (
	"fmt"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/nats-io/nats.go"

	"github.com/foliagecp/sdk/statefun/logger"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

const (
	ShadowObjectCallParamOptionPath string = "shadow_object.can_receive"
)

func buildNatsData(callerDomain string, callerTypename string, callerID string, payload *easyjson.JSON, options *easyjson.JSON) []byte {
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

func (r *Runtime) signalShadowObject(callerTypename string, callerID string, targetTypename string, targetID string, payload *easyjson.JSON, options *easyjson.JSON) error {
	tDomainName, tObjectIdWithoutDomain, err := r.Domain.GetShadowObjectDomainAndID(targetID)
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
	tDomainName, tObjectIdWithoutDomain, err := r.Domain.GetShadowObjectDomainAndID(targetID)
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

func (r *Runtime) egress(egressProvider sfPlugins.EgressProvider, callerTypename string, callerID string, payload *easyjson.JSON) error {
	natsCoreEgress := func() error {
		go func() {
			system.GlobalPrometrics.GetRoutinesCounter().Started("ingress-jetstreamGlobalSignal-gofunc")
			defer system.GlobalPrometrics.GetRoutinesCounter().Stopped("ingress-jetstreamGlobalSignal-gofunc")

			system.MsgOnErrorReturn(r.nc.Publish(
				fmt.Sprintf("%s.%s.%s", "egress", callerTypename, callerID),
				payload.ToBytes(),
			))
		}()
		return nil
	}

	switch egressProvider {
	case sfPlugins.NatsCoreEgress:
		return natsCoreEgress()
	default:
		return fmt.Errorf("unknown egress provider: %d", egressProvider)
	}
}

/* return
* 0 - ok
* 1 - domain differs
* 2 - function is not registered
* 3 - function does not support this communication type
 */
func (r *Runtime) functionTypeIsReadyForGoLangCommunication(targetFunctionTypeName string, isRequest bool, targetID string) int {
	var targetFT *FunctionType
	if r.Domain.GetDomainFromObjectID(targetID) == r.Domain.name {
		if ft, ok := r.registeredFunctionTypes[targetFunctionTypeName]; ok {
			targetFT = ft
		} else {
			return 2
		}
	} else {
		return 1
	}
	supportsCommunicationType := false
	if isRequest {
		if targetFT.config.IsRequestProviderAllowed(sfPlugins.GolangLocalRequest) {
			supportsCommunicationType = true
		}
	} else {
		if targetFT.config.IsSignalProviderAllowed(sfPlugins.GolangLocalSignal) {
			supportsCommunicationType = true
		}
	}
	if !supportsCommunicationType {
		return 3
	}
	return 0
}

func (r *Runtime) signal(signalProvider sfPlugins.SignalProvider, callerTypename string, callerID string, targetTypename string, targetID string, payload *easyjson.JSON, options *easyjson.JSON) error {
	shadowObjectCanBeReceiver := false
	if options != nil {
		shadowObjectCanBeReceiver = options.GetByPath(ShadowObjectCallParamOptionPath).AsBoolDefault(false)
	}
	jetstreamGlobalSignal := func() error {
		go func() {
			system.GlobalPrometrics.GetRoutinesCounter().Started("ingress-jetstreamGlobalSignal-gofunc")
			defer system.GlobalPrometrics.GetRoutinesCounter().Stopped("ingress-jetstreamGlobalSignal-gofunc")

			if !shadowObjectCanBeReceiver && r.Domain.IsShadowObject(targetID) {
				system.MsgOnErrorReturn(r.signalShadowObject(callerTypename, callerID, targetTypename, targetID, payload, options))
			} else {
				// If publishing signal to the same domain
				if r.Domain.name == r.Domain.GetDomainFromObjectID(targetID) {
					system.MsgOnErrorReturn(r.nc.Publish( // Publish directly into function's topic bypassing egress router
						fmt.Sprintf(DomainIngressSubjectsTmpl, r.Domain.name, fmt.Sprintf("%s.%s.%s.%s", SignalPrefix, r.Domain.name, targetTypename, targetID)),
						buildNatsData(r.Domain.name, callerTypename, callerID, payload, options),
					))
				} else { // Publish into egress router
					system.MsgOnErrorReturn(r.nc.Publish(
						fmt.Sprintf(DomainEgressSubjectsTmpl, r.Domain.name, fmt.Sprintf("%s.%s.%s.%s", SignalPrefix, r.Domain.GetDomainFromObjectID(targetID), targetTypename, targetID)),
						buildNatsData(r.Domain.name, callerTypename, callerID, payload, options),
					))
				}
			}
		}()
		return nil
	}
	// TODO: This implementation is weak, cause we always should wait one functions ends its execution before next can start
	goLangLocalSignal := func() error {
		switch r.functionTypeIsReadyForGoLangCommunication(targetTypename, false, targetID) {
		case 0:
			func() {
				targetFT := r.registeredFunctionTypes[targetTypename]

				// Do not send original data, prevents same data concurrent access from different functions
				var payloadCopy *easyjson.JSON = nil
				var optionsCopy *easyjson.JSON = nil
				if payload != nil {
					payloadCopy = payload.Clone().GetPtr()
				}
				if options != nil {
					optionsCopy = options.Clone().GetPtr()
				}
				// ----------------------------------------------------------------------------------------
				functionMsg := FunctionTypeMsg{
					Caller:  &sfPlugins.StatefunAddress{Typename: callerTypename, ID: callerID},
					Payload: payloadCopy,
					Options: optionsCopy,
				}

				ackChannel := make(chan struct{})
				nackChannel := make(chan struct{})

				functionMsg.AckCallback = func(ack bool) {
					if ack {
						ackChannel <- struct{}{}
					} else {
						functionMsg.RefusalCallback(false)
					}
				}
				functionMsg.RefusalCallback = func(_ bool) {
					logger.Logf(logger.WarnLevel, "goLangLocalSignal: receiver typename=%s called on id=%s refused from msg, for safety reasons msg is being redirected to NATS Jetstream", targetTypename, targetID)
					system.MsgOnErrorReturn(r.signal(sfPlugins.JetstreamGlobalSignal, callerTypename, callerID, targetTypename, targetID, payload, options))
					nackChannel <- struct{}{}
				}

				targetFT.sendMsg(targetID, functionMsg)

				select {
				case _, ok := <-ackChannel:
					if !ok {
						logger.Logln(logger.ErrorLevel, "goLangLocalSignal: ackChannel was unexpectedly closed")
					}
					// if ok - whether signal is delivered
				case _, ok := <-nackChannel:
					if !ok {
						logger.Logln(logger.ErrorLevel, "goLangLocalSignal: nackChannel was unexpectedly closed")
					}
					// if ok - whether signal is redirected to NATS Jetstream due to nack command
				case <-time.After(time.Duration(targetFT.config.msgAckWaitMs) * time.Millisecond):
					logger.Logf(logger.WarnLevel, "goLangLocalSignal: receiver typename=%s called on id=%s did not ack msg in time, for safety reasons msg is being redirected to NATS Jetstream", targetTypename, targetID)
					system.MsgOnErrorReturn(r.signal(sfPlugins.JetstreamGlobalSignal, callerTypename, callerID, targetTypename, targetID, payload, options))
				}
			}()
			return nil
		case 1:
			return fmt.Errorf("goLangLocalSignal: cannot request function with the typename %s via golang, domain differs: %s(runtime) != %s(id)", callerTypename, r.Domain.name, r.Domain.GetDomainFromObjectID(targetID))
		case 2:
			return fmt.Errorf("goLangLocalSignal: cannot request function with the typename %s via golang, not registered", callerTypename)
		case 3:
			fallthrough
		default:
			logger.Logf(logger.WarnLevel, "goLangLocalSignal: receiver typename=%s does not support golang signals, for safety reasons msg is being redirected to NATS Jetstream", targetTypename)
			system.MsgOnErrorReturn(r.signal(sfPlugins.JetstreamGlobalSignal, callerTypename, callerID, targetTypename, targetID, payload, options))
			return nil
		}
	}

	switch signalProvider {
	case sfPlugins.JetstreamGlobalSignal:
		return jetstreamGlobalSignal()
	case sfPlugins.GolangLocalSignal:
		return goLangLocalSignal()
	case sfPlugins.AutoSignalSelect:
		return jetstreamGlobalSignal() // TODO: Find a way to fix weak solution of the goLangLocalSignal
		/*selection := sfPlugins.JetstreamGlobalSignal
		if r.isShadowObject(targetID) {
			if r.functionTypeIsReadyForGoLangCommunication(targetTypename, false, targetID) == 0 {
				selection = sfPlugins.GolangLocalSignal
			}
		}
		return r.signal(selection, callerTypename, callerID, targetTypename, targetID, payload, options)*/
	default:
		return fmt.Errorf("unknown signal provider: %d", signalProvider)
	}
}

func (r *Runtime) request(requestProvider sfPlugins.RequestProvider, callerTypename string, callerID string, targetTypename string, targetID string, payload *easyjson.JSON, options *easyjson.JSON, timeout ...time.Duration) (*easyjson.JSON, error) {
	shadowObjectCanBeReceiver := false
	if options != nil {
		shadowObjectCanBeReceiver = options.GetByPath(ShadowObjectCallParamOptionPath).AsBoolDefault(false)
	}
	requestTimeoutDuration := time.Duration(r.config.requestTimeoutSec) * time.Second
	if len(timeout) > 0 {
		requestTimeoutDuration = timeout[0]
	}
	natsCoreGlobalRequest := func() (*easyjson.JSON, error) {
		var (
			resp *nats.Msg
			err  error
		)

		if !shadowObjectCanBeReceiver && r.Domain.IsShadowObject(targetID) {
			resp, err = r.requestShadowObject(callerTypename, callerID, targetTypename, targetID, payload, options)
		} else {
			resp, err = r.nc.Request(
				fmt.Sprintf("%s.%s.%s.%s", RequestPrefix, r.Domain.GetDomainFromObjectID(targetID), targetTypename, targetID),
				buildNatsData(r.Domain.name, callerTypename, callerID, payload, options),
				requestTimeoutDuration,
			)
		}

		if err == nil {
			if j, ok := easyjson.JSONFromBytes(resp.Data); ok {
				return &j, nil
			}
			return nil, fmt.Errorf("response from function typename \"%s\" with id \"%s\" is not a json", targetTypename, targetID)
		}
		return nil, err
	}
	goLangLocalRequest := func() (*easyjson.JSON, error) {
		switch r.functionTypeIsReadyForGoLangCommunication(targetTypename, true, targetID) {
		case 0:
			targetFT := r.registeredFunctionTypes[targetTypename]
			resultJSONChannel := make(chan *easyjson.JSON, 1)

			// Do not send original data, prevents same data concurrent access from different functions
			var payloadCopy *easyjson.JSON = nil
			var optionsCopy *easyjson.JSON = nil
			if payload != nil {
				payloadCopy = payload.Clone().GetPtr()
			}
			if options != nil {
				optionsCopy = options.Clone().GetPtr()
			}
			// ----------------------------------------------------------------------------------------
			functionMsg := FunctionTypeMsg{
				Caller:  &sfPlugins.StatefunAddress{Typename: callerTypename, ID: callerID},
				Payload: payloadCopy,
				Options: optionsCopy,
			}

			/*functionMsg.RequestCallback = func(data *easyjson.JSON) {
				go func() {
					resultJSONChannel <- data.Clone().GetPtr() // Clone().GetPtr() prevents data to contain custom Golang types
				}()
			}
			functionMsg.RefusalCallback = func(_ bool) {
				go func() {
					close(resultJSONChannel)
				}()
			}

			targetFT.prometricsMeasureMsgDeliver(GolangReq)
			targetFT.sendMsg(targetID, functionMsg)*/

			functionMsg.RequestCallback = func(data *easyjson.JSON) {
				select {
				case resultJSONChannel <- data.Clone().GetPtr():
				default: // channel is ether closed or already has a value
				}
			}
			functionMsg.RefusalCallback = func(_ bool) {
				defer func() {
					if r := recover(); r != nil { // in case channel is already closed
						logger.Logf(logger.ErrorLevel, "panic in request functionMsg.RefusalCallback close(resultJSONChannel) caller:%s:%s target:%s:%s: %v", callerTypename, callerID, targetTypename, targetID, r)
					}
				}()
				close(resultJSONChannel)
			}

			targetFT.prometricsMeasureMsgDeliver(GolangReq)
			targetFT.workerTaskExecutor(targetID, functionMsg)

			select {
			case resultJSON, ok := <-resultJSONChannel:
				if ok {
					return resultJSON, nil
				}
				return nil, fmt.Errorf("goLangLocalRequest: target function with typename \"%s\" with id \"%s\" resufes to handle request", targetTypename, targetID)
			case <-time.After(requestTimeoutDuration):
				return nil, fmt.Errorf("goLangLocalRequest: timeout occured while requesting function typename \"%s\" with id \"%s\"", targetTypename, targetID)
			}
		case 1:
			return nil, fmt.Errorf("goLangLocalRequest: cannot request function with the typename %s via golang, domain differs: %s(runtime) != %s(id)", callerTypename, r.Domain.name, r.Domain.GetDomainFromObjectID(targetID))
		case 2:
			return nil, fmt.Errorf("goLangLocalRequest: cannot request function with the typename %s via golang, not registered", callerTypename)
		case 3:
			fallthrough
		default:
			return nil, fmt.Errorf("goLangLocalRequest: function with the typename %s does not support request-reply via golang", callerTypename)
		}
	}

	switch requestProvider {
	case sfPlugins.NatsCoreGlobalRequest:
		return natsCoreGlobalRequest()
	case sfPlugins.GolangLocalRequest:
		return goLangLocalRequest()
	case sfPlugins.AutoRequestSelect:
		selection := sfPlugins.NatsCoreGlobalRequest
		if shadowObjectCanBeReceiver || !r.Domain.IsShadowObject(targetID) {
			if r.functionTypeIsReadyForGoLangCommunication(targetTypename, true, targetID) == 0 {
				selection = sfPlugins.GolangLocalRequest
			}
		}
		return r.request(selection, callerTypename, callerID, targetTypename, targetID, payload, options)
	default:
		return nil, fmt.Errorf("unknown request provider: %d", requestProvider)
	}
}

func (r *Runtime) Signal(signalProvider sfPlugins.SignalProvider, typename string, id string, payload *easyjson.JSON, options *easyjson.JSON) error {
	return r.signal(signalProvider, "ingress", "signal", typename, r.Domain.GetValidObjectId(id), payload, options)
}

func (r *Runtime) Request(requestProvider sfPlugins.RequestProvider, typename string, id string, payload *easyjson.JSON, options *easyjson.JSON, timeout ...time.Duration) (*easyjson.JSON, error) {
	return r.request(requestProvider, "ingress", "request", typename, r.Domain.GetValidObjectId(id), payload, options, timeout...)
}

// ------------------------------------------------------------------------------------------------
