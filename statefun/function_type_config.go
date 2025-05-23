package statefun

import (
	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

const (
	MsgAckWaitTimeoutMs      = 10000
	msgMaxDeliver            = 5
	IdChannelSize            = 10
	BalanceNeeded            = true
	MutexLifetimeSec         = 120
	MultipleInstancesAllowed = false
	MaxIdHandlers            = 20
)

type FunctionTypeConfig struct {
	msgAckWaitMs             int
	msgMaxDeliver            int
	idChannelSize            int
	balanceNeeded            bool
	mutexLifeTimeSec         int
	options                  *easyjson.JSON
	multipleInstancesAllowed bool
	allowedSignalProviders   map[sfPlugins.SignalProvider]struct{}
	allowedRequestProviders  map[sfPlugins.RequestProvider]struct{}
	functionWorkerPoolConfig SFWorkerPoolConfig
}

func NewFunctionTypeConfig() *FunctionTypeConfig {
	ft := &FunctionTypeConfig{
		msgAckWaitMs:             MsgAckWaitTimeoutMs,
		msgMaxDeliver:            msgMaxDeliver,
		idChannelSize:            system.GetEnvMustProceed[int]("DEFAULT_FT_ID_CHANNEL_SIZE", IdChannelSize),
		balanceNeeded:            BalanceNeeded,
		mutexLifeTimeSec:         MutexLifetimeSec,
		options:                  easyjson.NewJSONObject().GetPtr(),
		multipleInstancesAllowed: MultipleInstancesAllowed,
		allowedSignalProviders:   map[sfPlugins.SignalProvider]struct{}{},
		allowedRequestProviders:  map[sfPlugins.RequestProvider]struct{}{},
		functionWorkerPoolConfig: NewSFWorkerPoolConfig(WPLoadDefault),
	}
	ft.allowedSignalProviders[sfPlugins.AutoSignalSelect] = struct{}{}
	return ft
}

func (ftc *FunctionTypeConfig) SetMsgAckWaitMs(msgAckWaitMs int) *FunctionTypeConfig {
	ftc.msgAckWaitMs = msgAckWaitMs
	return ftc
}

func (ftc *FunctionTypeConfig) SetMsgMaxDeliver(msgMaxDeliver int) *FunctionTypeConfig {
	ftc.msgMaxDeliver = msgMaxDeliver
	return ftc
}

// Deprecated
func (ftc *FunctionTypeConfig) SetMsgChannelSize(msgChannelSize int) *FunctionTypeConfig {
	return ftc
}

func (ftc *FunctionTypeConfig) SetIdChannelSize(idChannelSize int) *FunctionTypeConfig {
	ftc.idChannelSize = idChannelSize
	return ftc
}

// Deprecated
func (ftc *FunctionTypeConfig) SetMsgAckChannelSize(msgAckChannelSize int) *FunctionTypeConfig {
	return ftc
}

func (ftc *FunctionTypeConfig) SetBalanceNeeded(balanceNeeded bool) *FunctionTypeConfig {
	ftc.balanceNeeded = balanceNeeded
	return ftc
}

func (ftc *FunctionTypeConfig) SetAllowedSignalProviders(allowedSignalProviders ...sfPlugins.SignalProvider) *FunctionTypeConfig {
	ftc.allowedSignalProviders = map[sfPlugins.SignalProvider]struct{}{}
	for _, asp := range allowedSignalProviders {
		ftc.allowedSignalProviders[asp] = struct{}{}
	}
	return ftc
}

func (ftc *FunctionTypeConfig) SetAllowedRequestProviders(allowedRequestProviders ...sfPlugins.RequestProvider) *FunctionTypeConfig {
	ftc.allowedRequestProviders = map[sfPlugins.RequestProvider]struct{}{}
	for _, asp := range allowedRequestProviders {
		ftc.allowedRequestProviders[asp] = struct{}{}
	}
	return ftc
}

func (ftc *FunctionTypeConfig) IsSignalProviderAllowed(signalProvider sfPlugins.SignalProvider) bool {
	if _, ok := ftc.allowedSignalProviders[sfPlugins.AutoSignalSelect]; ok {
		return true
	}
	if _, ok := ftc.allowedSignalProviders[signalProvider]; ok {
		return true
	}
	return false
}

func (ftc *FunctionTypeConfig) IsRequestProviderAllowed(requestProvider sfPlugins.RequestProvider) bool {
	if _, ok := ftc.allowedRequestProviders[sfPlugins.AutoRequestSelect]; ok {
		return true
	}
	if _, ok := ftc.allowedRequestProviders[requestProvider]; ok {
		return true
	}
	return false
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
	ftc.options = options.Clone().GetPtr()
	return ftc
}

// Deprecated
func (ftc *FunctionTypeConfig) SetMaxIdHandlers(maxIdHandlers int) *FunctionTypeConfig {
	return ftc
}

func (ftc *FunctionTypeConfig) SetWorkerPoolConfig(functionWorkerPoolConfig SFWorkerPoolConfig) *FunctionTypeConfig {
	ftc.functionWorkerPoolConfig = functionWorkerPoolConfig
	return ftc
}

func (ftc *FunctionTypeConfig) SetWorkerPoolLoadType(wpLoadType WPLoadType) *FunctionTypeConfig {
	ftc.functionWorkerPoolConfig = NewSFWorkerPoolConfig(wpLoadType)
	return ftc
}
