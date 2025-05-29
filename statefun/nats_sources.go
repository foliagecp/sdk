package statefun

import (
	"fmt"
	"strings"
	"time"

	lg "github.com/foliagecp/sdk/statefun/logger"

	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"

	"github.com/nats-io/nats.go"
)

func AddRequestSourceNatsCore(ft *FunctionType) error {
	_, err := ft.runtime.nc.Subscribe(RequestPrefix+"."+ft.runtime.Domain.name+"."+ft.name+".*", func(msg *nats.Msg) {
		system.MsgOnErrorReturn(handleNatsMsg(ft, msg, true))
	})

	if err != nil {
		lg.Logf(lg.ErrorLevel, "Invalid request reply subscription for function type %s: %s", ft.name, err)
		return err
	}

	return nil
}

func AddSignalSourceJetstreamQueuePushConsumer(ft *FunctionType) error {
	consumerName := ft.runtime.Domain.name + "-" + strings.ReplaceAll(ft.name, ".", "")
	consumerGroup := consumerName + "-group"
	lg.Logf(lg.TraceLevel, "Handling function type %s", ft.name)

	// Create stream consumer if does not exist ---------------------
	consumerExists := false
	for info := range ft.runtime.js.Consumers(ft.getStreamName(), nats.MaxWait(10*time.Second)) {
		if info.Name == consumerName {
			consumerExists = true
		}
	}
	if !consumerExists {
		_, err := ft.runtime.js.AddConsumer(ft.getStreamName(), &nats.ConsumerConfig{
			Name:           consumerName,
			Durable:        consumerName,
			DeliverSubject: consumerName,
			DeliverGroup:   consumerGroup,
			FilterSubject:  ft.subject,
			AckPolicy:      nats.AckExplicitPolicy,
			AckWait:        time.Duration(ft.config.msgAckWaitMs) * time.Millisecond, // AckWait should be long due to async message Ack
			MaxDeliver:     ft.config.msgMaxDeliver,
		})
		system.MsgOnErrorReturn(err)
	}
	// --------------------------------------------------------------

	_, err := ft.runtime.js.QueueSubscribe(
		ft.subject,
		consumerGroup,
		func(msg *nats.Msg) {
			system.MsgOnErrorReturn(handleNatsMsg(ft, msg, false))
		},
		nats.Bind(ft.getStreamName(), consumerName),
		nats.ManualAck(),
	)
	if err != nil {
		lg.Logf(lg.ErrorLevel, "Invalid signal subscription for function type %s: %s", ft.name, err)
		return err
	}
	return nil
}

func handleNatsMsg(ft *FunctionType, msg *nats.Msg, requestReply bool) (err error) {
	tokens := strings.Split(msg.Subject, ".")
	id := tokens[len(tokens)-1]

	data, ok := easyjson.JSONFromBytes(msg.Data)
	if !ok {
		system.MsgOnErrorReturn(msg.Ack())
		return fmt.Errorf("nats.Msg for function %s with id=%s is not a JSON", ft.name, id)
	}

	var payload *easyjson.JSON
	if data.GetByPath("payload").IsObject() {
		j := data.GetByPath("payload")
		payload = &j
	} else {
		j := easyjson.NewJSONObject()
		payload = &j
	}

	var msgOptions *easyjson.JSON
	if data.GetByPath("options").IsObject() {
		msgOptions = data.GetByPath("options").GetPtr()
	} else {
		msgOptions = easyjson.NewJSONObject().GetPtr()
	}

	caller := sfPlugins.StatefunAddress{}
	if data.GetByPath("caller_typename").IsString() {
		caller.Typename, _ = data.GetByPath("caller_typename").AsString()
	}
	if data.GetByPath("caller_id").IsString() {
		caller.ID, _ = data.GetByPath("caller_id").AsString()
	}

	// Create function message ------------------------
	functionMsg := FunctionTypeMsg{
		Caller:  &caller,
		Payload: payload,
		Options: msgOptions,
	}
	if requestReply {
		functionMsg.RequestCallback = func(data *easyjson.JSON) {
			system.MsgOnErrorReturn(msg.Respond(data.ToBytes()))
		}
		functionMsg.RefusalCallback = func(_ bool) {
			system.MsgOnErrorReturn(msg.Respond([]byte{}))
		}
	} else {
		functionMsg.AckCallback = func(ack bool) {
			if ack {
				system.MsgOnErrorReturn(msg.Ack())
			} else {
				system.MsgOnErrorReturn(msg.Nak())
			}
		}
		functionMsg.RefusalCallback = func(skipForever bool) {
			if skipForever {
				system.MsgOnErrorReturn(msg.Ack())
			} else {
				system.MsgOnErrorReturn(msg.Nak())
			}

		}
	}
	// ------------------------------------------------

	ft.sendMsg(id, functionMsg)

	return
}
