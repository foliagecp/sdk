// Copyright 2023 NJWS Inc.

package statefun

import "json_easy"

const (
	MAS_ACK_WAIT_TIMEOUT_MS = 10000
	MSG_CHANNEL_SIZE        = 64
	MSG_ACK_CHANNEL_SIZE    = 64
	BALANCE_NEEDED          = true
	MUTEX_LIFETIME_SEC      = 120
)

type FunctionTypeConfig struct {
	msgAckWaitMs      int
	msgChannelSize    int
	msgAckChannelSize int
	balanceNeeded     bool
	balanced          bool
	mutexLifeTimeSec  int
	options           *json_easy.JSON
}

func NewFunctionTypeConfig() *FunctionTypeConfig {
	return &FunctionTypeConfig{
		msgAckWaitMs:      MAS_ACK_WAIT_TIMEOUT_MS,
		msgChannelSize:    MSG_CHANNEL_SIZE,
		msgAckChannelSize: MSG_ACK_CHANNEL_SIZE,
		balanceNeeded:     BALANCE_NEEDED,
		mutexLifeTimeSec:  MUTEX_LIFETIME_SEC,
		options:           json_easy.NewJSONObject().GetPtr(),
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

func (ftc *FunctionTypeConfig) SetMutexLifeTimeSec(mutexLifeTimeSec int) *FunctionTypeConfig {
	ftc.mutexLifeTimeSec = mutexLifeTimeSec
	return ftc
}

func (ftc *FunctionTypeConfig) SetOptions(options *json_easy.JSON) *FunctionTypeConfig {
	ftc.options = options
	return ftc
}
