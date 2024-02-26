// Copyright 2023 NJWS Inc.

package statefun

import (
	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

const (
	MsgAckWaitTimeoutMs      = 10000
	MsgChannelSize           = 64
	MsgAckChannelSize        = 64
	BalanceNeeded            = true
	MutexLifetimeSec         = 120
	MultipleInstancesAllowed = false
	MaxIdHandlers            = 20
)

type FunctionTypeConfig struct {
	msgAckWaitMs             int
	msgChannelSize           int
	msgAckChannelSize        int
	balanceNeeded            bool
	mutexLifeTimeSec         int
	options                  *easyjson.JSON
	multipleInstancesAllowed bool
	maxIdHandlers            int
	allowedSignalProviders   map[sfPlugins.SignalProvider]struct{}
	allowedRequestProviders  map[sfPlugins.RequestProvider]struct{}
}

func NewFunctionTypeConfig() *FunctionTypeConfig {
	ft := &FunctionTypeConfig{
		msgAckWaitMs:             MsgAckWaitTimeoutMs,
		msgChannelSize:           MsgChannelSize,
		msgAckChannelSize:        MsgAckChannelSize,
		balanceNeeded:            BalanceNeeded,
		mutexLifeTimeSec:         MutexLifetimeSec,
		options:                  easyjson.NewJSONObject().GetPtr(),
		multipleInstancesAllowed: MultipleInstancesAllowed,
		maxIdHandlers:            MaxIdHandlers,
		allowedSignalProviders:   map[sfPlugins.SignalProvider]struct{}{},
		allowedRequestProviders:  map[sfPlugins.RequestProvider]struct{}{},
	}
	ft.allowedSignalProviders[sfPlugins.AutoSignalSelect] = struct{}{}
	return ft
}

func (ftc *FunctionTypeConfig) SetMsgAckWaitMs(msgAckWaitMs int) *FunctionTypeConfig {
	ftc.msgAckWaitMs = msgAckWaitMs
	return ftc
}

func (ftc *FunctionTypeConfig) SetMsgChannelSize(msgChannelSize int) *FunctionTypeConfig {
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

func (ftc *FunctionTypeConfig) SetMaxIdHandlers(maxIdHandlers int) *FunctionTypeConfig {
	ftc.maxIdHandlers = maxIdHandlers
	return ftc
}
