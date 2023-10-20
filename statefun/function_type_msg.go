package statefun

import (
	"github.com/foliagecp/easyjson"

	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

type HandlerMsgRefusalType int

type RefusalCallbackAction = func()
type RequestCallbackAction = func(data *easyjson.JSON)
type SignalCallbackAction = func(ack bool)

type FunctionTypeMsg struct {
	Caller          *sfPlugins.StatefunAddress
	Payload         *easyjson.JSON
	Options         *easyjson.JSON
	RefusalCallback RefusalCallbackAction
	RequestCallback RequestCallbackAction
	AckCallback     SignalCallbackAction
}
