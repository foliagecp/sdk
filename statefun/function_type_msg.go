package statefun

import (
	"github.com/foliagecp/easyjson"

	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

type HandlerMsgRefusalType int

type MereCallbackAction = func()
type RequestCallbackAction = func(data *easyjson.JSON)
type AckCallbackAction = func(ack bool)

type FunctionTypeMsg struct {
	Caller          *sfPlugins.StatefunAddress
	Payload         *easyjson.JSON
	Options         *easyjson.JSON
	RefusalCallback MereCallbackAction
	RequestCallback RequestCallbackAction
	AckCallback     AckCallbackAction
	SkipCallback    MereCallbackAction
}
