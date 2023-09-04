// Copyright 2023 NJWS Inc.

package statefun

import (
	"fmt"
)

const (
	NATS_URL                             = "nats://nats:foliage@nats:4222"
	RUNTIME_NAME                         = "foliage_runtime"
	KEY_VALUE_STORE_BUCKET_NAME          = RUNTIME_NAME + "_kv_store"
	FUNCTION_TYPES_STREAM_NAME           = RUNTIME_NAME + "_stream"
	KV_MUTEX_LIFETIME_SEC                = 120
	KV_MUTEX_IS_OLD_POLLING_INTERVAL     = 10
	FUNCTION_TYPE_ID_LIFETIME_MS         = 5000
	INGRESS_CALL_GOLANG_SYNC_TIMEOUT_SEC = 60
)

type RuntimeConfig struct {
	natsURL                         string
	keyValueStoreBucketName         string
	functionTypesStreamName         string
	kvMutexLifeTimeSec              int
	kvMutexIsOldPollingIntervalSec  int
	functionTypeIdLifetimeMs        int
	ingressCallGoLangSyncTimeoutSec int
}

func NewRuntimeConfig() *RuntimeConfig {
	return &RuntimeConfig{
		natsURL:                         NATS_URL,
		keyValueStoreBucketName:         KEY_VALUE_STORE_BUCKET_NAME,
		functionTypesStreamName:         FUNCTION_TYPES_STREAM_NAME,
		kvMutexLifeTimeSec:              KV_MUTEX_LIFETIME_SEC,
		kvMutexIsOldPollingIntervalSec:  KV_MUTEX_IS_OLD_POLLING_INTERVAL,
		functionTypeIdLifetimeMs:        FUNCTION_TYPE_ID_LIFETIME_MS,
		ingressCallGoLangSyncTimeoutSec: INGRESS_CALL_GOLANG_SYNC_TIMEOUT_SEC,
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

func (ro *RuntimeConfig) SetFunctionTypeIdLifetimeMs(functionTypeIdLifetimeMs int) *RuntimeConfig {
	ro.functionTypeIdLifetimeMs = functionTypeIdLifetimeMs
	return ro
}

func (ro *RuntimeConfig) SetIngressCallGoLangSyncTimeoutSec(ingressCallGoLangSyncTimeoutSec int) *RuntimeConfig {
	ro.ingressCallGoLangSyncTimeoutSec = ingressCallGoLangSyncTimeoutSec
	return ro
}
