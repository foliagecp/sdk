

package statefun

import "github.com/foliagecp/easyjson"

const (
	MsgAckWaitTimeoutMs      = 10000
	MsgChannelSize           = 64
	MsgAckChannelSize        = 64
	BalanceNeeded            = true
	MutexLifetimeSec         = 120
	MultipleInstancesAllowed = false
	PrometricsEnabled        = false
)

type FunctionTypeConfig struct {
	msgAckWaitMs             int
	msgChannelSize           int
	msgAckChannelSize        int
	balanceNeeded            bool
	balanced                 bool
	serviceActive            bool
	mutexLifeTimeSec         int
	options                  *easyjson.JSON
	multipleInstancesAllowed bool
	prometricsEnabled        bool
}

func NewFunctionTypeConfig() *FunctionTypeConfig {
	return &FunctionTypeConfig{
		msgAckWaitMs:             MsgAckWaitTimeoutMs,
		msgChannelSize:           MsgChannelSize,
		msgAckChannelSize:        MsgAckChannelSize,
		balanceNeeded:            BalanceNeeded,
		mutexLifeTimeSec:         MutexLifetimeSec,
		options:                  easyjson.NewJSONObject().GetPtr(),
		multipleInstancesAllowed: MultipleInstancesAllowed,
		prometricsEnabled:        PrometricsEnabled,
	}
}

func (ftc *FunctionTypeConfig) SetMsgAckWaitMs(msgAckWaitMs int) *FunctionTypeConfig {
	ftc.msgAckWaitMs = msgAckWaitMs
	return ftc
}

func (ftc *FunctionTypeConfig) SeMsgChannelSize(msgChannelSize int) *FunctionTypeConfig {
	ftc.msgChannelSize = msgChannelSize
	return ftc
}

func (ftc *FunctionTypeConfig) SetMsgAckChannelSize(msgAckChannelSize int) *FunctionTypeConfig {
	ftc.msgAckChannelSize = msgAckChannelSize
	return ftc
}

func (ftc *FunctionTypeConfig) SetBalanceNeeded(balanceNeeded bool) *FunctionTypeConfig {
	ftc.balanceNeeded = balanceNeeded
	return ftc
}

// TODO: if serviceActive == false GOLANG local call should also be not possible!
// TODO: SetAccessebility([]string = "golang local request" | "golang local signal" | "jetstream signal" | "nats core request")
func (ftc *FunctionTypeConfig) SetServiceState(active bool) *FunctionTypeConfig {
	ftc.serviceActive = active
	return ftc
}

func (ftc *FunctionTypeConfig) SetMultipleInstancesAllowance(allowed bool) *FunctionTypeConfig {
	ftc.multipleInstancesAllowed = allowed
	return ftc
}

func (ftc *FunctionTypeConfig) SetMutexLifeTimeSec(mutexLifeTimeSec int) *FunctionTypeConfig {
	ftc.mutexLifeTimeSec = mutexLifeTimeSec
	return ftc
}

func (ftc *FunctionTypeConfig) SetOptions(options *easyjson.JSON) *FunctionTypeConfig {
	ftc.options = options
	return ftc
}

func (ftc *FunctionTypeConfig) SetPrometricsEnabled(allowed bool) *FunctionTypeConfig {
	ftc.prometricsEnabled = allowed
	return ftc
}
