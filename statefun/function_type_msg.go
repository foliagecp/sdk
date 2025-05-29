package statefun

import (
	"github.com/foliagecp/easyjson"

	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

type HandlerMsgRefusalType int

type RefuseCallbackAction = func(skipForever bool)
type RequestCallbackAction = func(data *easyjson.JSON)
type AckCallbackAction = func(ack bool)

type FunctionTypeMsg struct {
	Caller          *sfPlugins.StatefunAddress
	Payload         *easyjson.JSON
	Options         *easyjson.JSON
	RefusalCallback RefuseCallbackAction
	RequestCallback RequestCallbackAction
	AckCallback     AckCallbackAction
}
