package statefun

import (
	"fmt"
	"time"

	"github.com/foliagecp/easyjson"

	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

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

func (r *Runtime) signal(signalProvider sfPlugins.SignalProvider, callerTypename string, callerID string, targetTypename string, targetID string, payload *easyjson.JSON, options *easyjson.JSON) error {
	jetstreamGlobalSignal := func() error {
		go func() {
			system.MsgOnErrorReturn(r.nc.Publish(fmt.Sprintf("%s.%s", targetTypename, targetID), buildNatsData(callerTypename, callerID, payload, options)))
		}()
		return nil
	}

	switch signalProvider {
	case sfPlugins.JetstreamGlobalSignal:
		return jetstreamGlobalSignal()
	default:
		return fmt.Errorf("unknown signal provider: %d", signalProvider)
	}
}

func (r *Runtime) Signal(signalProvider sfPlugins.SignalProvider, typename string, id string, payload *easyjson.JSON, options *easyjson.JSON) error {
	return r.signal(signalProvider, "ingress", "nats", typename, id, payload, options)
}

func (r *Runtime) request(requestProvider sfPlugins.RequestProvider, callerTypename string, callerID string, targetTypename string, targetID string, payload *easyjson.JSON, options *easyjson.JSON) (*easyjson.JSON, error) {
	natsCoreGlobalRequest := func() (*easyjson.JSON, error) {
		resp, err := r.nc.Request(
			fmt.Sprintf("service.%s.%s", targetTypename, targetID),
			buildNatsData(callerTypename, callerID, payload, options),
			time.Duration(r.config.requestTimeoutSec)*time.Second,
		)
		if err == nil {
			if j, ok := easyjson.JSONFromBytes(resp.Data); ok {
				return &j, nil
			}
			return nil, fmt.Errorf("response from function typename \"%s\" with id \"%s\" is not a json", targetTypename, targetID)
		}
		return nil, err
	}

	goLangLocalRequest := func() (*easyjson.JSON, error) {
		if targetFT, ok := r.registeredFunctionTypes[targetTypename]; ok {
			resultJSONChannel := make(chan *easyjson.JSON)

			functionMsg := FunctionTypeMsg{
				Caller:  &sfPlugins.StatefunAddress{Typename: callerTypename, ID: callerID},
				Payload: payload,
				Options: options,
			}

			functionMsg.RequestCallback = func(data *easyjson.JSON) {
				resultJSONChannel <- data
			}
			functionMsg.RefusalCallback = func() {
				close(resultJSONChannel)
			}

			targetFT.sendMsg(targetID, functionMsg)

			select {
			case resultJSON, ok := <-resultJSONChannel:
				if ok {
					return resultJSON, nil
				}
				return nil, fmt.Errorf("target function typename \"%s\" with id \"%s\" resufes to handle request", targetTypename, targetID)
			case <-time.After(time.Duration(r.config.requestTimeoutSec) * time.Second):
				return nil, fmt.Errorf("timeout occured while requesting function typename \"%s\" with id \"%s\"", targetTypename, targetID)
			}
		} else {
			return nil, fmt.Errorf("callFunctionGolangSync cannot call function with the typename %s, not registered", callerTypename)
		}
	}

	switch requestProvider {
	case sfPlugins.NatsCoreGlobalRequest:
		return natsCoreGlobalRequest()
	case sfPlugins.GolangLocalRequest:
		return goLangLocalRequest()
	default:
		return nil, fmt.Errorf("unknown request provider: %d", requestProvider)
	}
}

func (r *Runtime) Request(requestProvider sfPlugins.RequestProvider, typename string, id string, payload *easyjson.JSON, options *easyjson.JSON) (*easyjson.JSON, error) {
	return r.request(requestProvider, "ingress", "go", typename, id, payload, options)
}
