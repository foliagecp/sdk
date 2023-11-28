// Copyright 2023 NJWS Inc.

package statefun

const (
	NatsURL                     = "nats://nats:foliage@nats:4222"
	RuntimeName                 = "foliage_runtime"
	KeyValueStoreBucketName     = RuntimeName + "_kv_store"
	KVMutexLifetimeSec          = 120
	KVMutexIsOldPollingInterval = 10
	FunctionTypeIDLifetimeMs    = 5000
	RequestTimeoutSec           = 60
)

type RuntimeConfig struct {
	natsURL                        string
	keyValueStoreBucketName        string
	kvMutexLifeTimeSec             int
	kvMutexIsOldPollingIntervalSec int
	functionTypeIDLifetimeMs       int
	requestTimeoutSec              int
}

func NewRuntimeConfig() *RuntimeConfig {
	return &RuntimeConfig{
		natsURL:                        NatsURL,
		keyValueStoreBucketName:        KeyValueStoreBucketName,
		kvMutexLifeTimeSec:             KVMutexLifetimeSec,
		kvMutexIsOldPollingIntervalSec: KVMutexIsOldPollingInterval,
		functionTypeIDLifetimeMs:       FunctionTypeIDLifetimeMs,
		requestTimeoutSec:              RequestTimeoutSec,
	}
}

func NewRuntimeConfigSimple(natsURL string, runtimeName string) *RuntimeConfig {
	ro := NewRuntimeConfig()
	return ro.SetNatsURL(natsURL).SeKeyValueStoreBucketName("common_kv_store")
}

func (ro *RuntimeConfig) SetNatsURL(natsURL string) *RuntimeConfig {
	ro.natsURL = natsURL
	return ro
}

func (ro *RuntimeConfig) SeKeyValueStoreBucketName(keyValueStoreBucketName string) *RuntimeConfig {
	ro.keyValueStoreBucketName = keyValueStoreBucketName
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
