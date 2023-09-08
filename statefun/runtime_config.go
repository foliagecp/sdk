

package statefun

import (
	"fmt"
)

const (
	NatsURL                      = "nats://nats:foliage@nats:4222"
	RuntimeName                  = "foliage_runtime"
	KeyValueStoreBucketName      = RuntimeName + "_kv_store"
	FunctionTypesStreamName      = RuntimeName + "_stream"
	KVMutexLifetimeSec           = 120
	KVMutexIsOldPollingInterval  = 10
	FunctionTypeIDLifetimeMs     = 5000
	IngressCallGolangSyncTimeout = 60
)

type RuntimeConfig struct {
	natsURL                         string
	keyValueStoreBucketName         string
	functionTypesStreamName         string
	kvMutexLifeTimeSec              int
	kvMutexIsOldPollingIntervalSec  int
	functionTypeIDLifetimeMs        int
	ingressCallGoLangSyncTimeoutSec int
}

func NewRuntimeConfig() *RuntimeConfig {
	return &RuntimeConfig{
		natsURL:                         NatsURL,
		keyValueStoreBucketName:         KeyValueStoreBucketName,
		functionTypesStreamName:         FunctionTypesStreamName,
		kvMutexLifeTimeSec:              KVMutexLifetimeSec,
		kvMutexIsOldPollingIntervalSec:  KVMutexIsOldPollingInterval,
		functionTypeIDLifetimeMs:        FunctionTypeIDLifetimeMs,
		ingressCallGoLangSyncTimeoutSec: IngressCallGolangSyncTimeout,
	}
}

func NewRuntimeConfigSimple(natsURL string, runtimeName string) *RuntimeConfig {
	ro := NewRuntimeConfig()
	return ro.SetNatsURL(natsURL).SeKeyValueStoreBucketName(fmt.Sprintf("%s_kv_store", runtimeName)).SetFunctionTypesStreamName(fmt.Sprintf("%s_stream", runtimeName))
}

func (ro *RuntimeConfig) SetNatsURL(natsURL string) *RuntimeConfig {
	ro.natsURL = natsURL
	return ro
}

func (ro *RuntimeConfig) SeKeyValueStoreBucketName(keyValueStoreBucketName string) *RuntimeConfig {
	ro.keyValueStoreBucketName = keyValueStoreBucketName
	return ro
}

func (ro *RuntimeConfig) SetFunctionTypesStreamName(functionTypesStreamName string) *RuntimeConfig {
	ro.functionTypesStreamName = functionTypesStreamName
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

func (ro *RuntimeConfig) SetIngressCallGoLangSyncTimeoutSec(ingressCallGoLangSyncTimeoutSec int) *RuntimeConfig {
	ro.ingressCallGoLangSyncTimeoutSec = ingressCallGoLangSyncTimeoutSec
	return ro
}
