// Copyright 2023 NJWS Inc.

package statefun

const (
	NatsURL                     = "nats://nats:foliage@nats:4222"
	RuntimeName                 = "foliage_runtime"
	KVMutexLifetimeSec          = 120
	KVMutexIsOldPollingInterval = 10
	FunctionTypeIDLifetimeMs    = 5000
	RequestTimeoutSec           = 60
	HubDomainName               = "hub"
)

type RuntimeConfig struct {
	natsURL                        string
	kvMutexLifeTimeSec             int
	kvMutexIsOldPollingIntervalSec int
	functionTypeIDLifetimeMs       int
	requestTimeoutSec              int
	hubDomainName                  string
}

func NewRuntimeConfig() *RuntimeConfig {
	return &RuntimeConfig{
		natsURL:                        NatsURL,
		kvMutexLifeTimeSec:             KVMutexLifetimeSec,
		kvMutexIsOldPollingIntervalSec: KVMutexIsOldPollingInterval,
		functionTypeIDLifetimeMs:       FunctionTypeIDLifetimeMs,
		requestTimeoutSec:              RequestTimeoutSec,
		hubDomainName:                  HubDomainName,
	}
}

func NewRuntimeConfigSimple(natsURL string, runtimeName string) *RuntimeConfig {
	ro := NewRuntimeConfig()
	return ro.SetNatsURL(natsURL)
}

func (ro *RuntimeConfig) SetHubDomainName(hubDomainName string) *RuntimeConfig {
	ro.hubDomainName = hubDomainName
	return ro
}

func (ro *RuntimeConfig) SetNatsURL(natsURL string) *RuntimeConfig {
	ro.natsURL = natsURL
	return ro
}

func (ro *RuntimeConfig) SetKVMutexIsOldPollingIntervalSec(kvMutexIsOldPollingIntervalSec int) *RuntimeConfig {
	ro.kvMutexIsOldPollingIntervalSec = kvMutexIsOldPollingIntervalSec
	return ro
}

func (ro *RuntimeConfig) SetKVMutexLifeTimeSec(kvMutexLifeTimeSec int) *RuntimeConfig {
	ro.kvMutexLifeTimeSec = kvMutexLifeTimeSec
	return ro
}

func (ro *RuntimeConfig) SetFunctionTypeIDLifetimeMs(functionTypeIDLifetimeMs int) *RuntimeConfig {
	ro.functionTypeIDLifetimeMs = functionTypeIDLifetimeMs
	return ro
}

func (ro *RuntimeConfig) SetRequestTimeoutSec(requestTimeoutSec int) *RuntimeConfig {
	ro.requestTimeoutSec = requestTimeoutSec
	return ro
}
