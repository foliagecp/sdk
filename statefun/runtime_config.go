package statefun

import "time"

const (
	RuntimeName                 = "runtime"
	NatsURL                     = "nats://nats:foliage@nats:4222"
	KVMutexLifetimeSec          = 10
	KVMutexIsOldPollingInterval = 10
	FunctionTypeIDLifetimeMs    = 5000
	RequestTimeoutSec           = 60
	GCIntervalSec               = 5
	DefaultHubDomainName        = "hub"
	HandlesDomainRouters        = true
	EnableTLS                   = false
	EnableNatsClusterMode       = false
	NatsReplicasCount           = 1
	FtStreamMaxMsgs             = 10000
	FtStreamMaxBytes            = 1024 * 1024 * 256
	FtStreamMaxAge              = 24 * time.Hour
	SysStreamMaxMsgs            = 80000
	SysStreamMaxBytes           = 1024 * 1024 * 512
	SysStreamMaxAge             = 12 * time.Hour
	KVStreamMaxMsgs             = -1 //unlimited
	KVStreamMaxBytes            = -1 //unlimited
	KVStreamMaxAge              = -1 //unlimited
	activePassiveMode           = true
)

type RuntimeConfig struct {
	name                  string
	natsURL               string
	enableNatsClusterMode bool
	StreamParams
	kvMutexLifeTimeSec             int
	kvMutexIsOldPollingIntervalSec int
	functionTypeIDLifetimeMs       int
	requestTimeoutSec              int
	gcIntervalSec                  int
	desiredHUBDomainName           string
	handlesDomainRouters           bool
	activePassiveMode              bool
	isActiveInstance               bool
	activeRevID                    uint64
	enableTLS                      bool
}

type StreamParams struct {
	natsReplicasCount int
	ftStreamMaxMsgs   int64
	ftStreamMaxBytes  int64
	ftStreamMaxAge    time.Duration
	sysStreamMaxMsgs  int64
	sysStreamMaxBytes int64
	sysStreamMaxAge   time.Duration
	kvStreamMaxMsgs   int64
	kvStreamMaxBytes  int64
	kvStreamMaxAge    time.Duration
}

func NewRuntimeConfig() *RuntimeConfig {
	streamParams := StreamParams{
		natsReplicasCount: NatsReplicasCount,
		ftStreamMaxMsgs:   FtStreamMaxMsgs,
		ftStreamMaxBytes:  FtStreamMaxBytes,
		ftStreamMaxAge:    FtStreamMaxAge,
		sysStreamMaxMsgs:  SysStreamMaxMsgs,
		sysStreamMaxBytes: SysStreamMaxBytes,
		sysStreamMaxAge:   SysStreamMaxAge,
		kvStreamMaxMsgs:   KVStreamMaxMsgs,
		kvStreamMaxBytes:  KVStreamMaxBytes,
		kvStreamMaxAge:    KVStreamMaxAge,
	}

	return &RuntimeConfig{
		name:                           RuntimeName,
		natsURL:                        NatsURL,
		enableNatsClusterMode:          EnableNatsClusterMode,
		StreamParams:                   streamParams,
		kvMutexLifeTimeSec:             KVMutexLifetimeSec,
		kvMutexIsOldPollingIntervalSec: KVMutexIsOldPollingInterval,
		functionTypeIDLifetimeMs:       FunctionTypeIDLifetimeMs,
		requestTimeoutSec:              RequestTimeoutSec,
		gcIntervalSec:                  GCIntervalSec,
		desiredHUBDomainName:           DefaultHubDomainName,
		handlesDomainRouters:           HandlesDomainRouters,
		enableTLS:                      EnableTLS,
		activePassiveMode:              activePassiveMode,
		isActiveInstance:               true,
	}
}

func NewRuntimeConfigSimple(natsURL string, runtimeName string) *RuntimeConfig {
	ro := NewRuntimeConfig()
	return ro.SetNatsURL(natsURL)
}

func (ro *RuntimeConfig) SetHubDomainName(hubDomainName string) *RuntimeConfig {
	ro.desiredHUBDomainName = hubDomainName
	return ro
}

func (ro *RuntimeConfig) UseJSDomainAsHubDomainName() *RuntimeConfig {
	ro.desiredHUBDomainName = "" // empty string means auto fill with current domain name from nats
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

func (ro *RuntimeConfig) SetGCIntervalSec(gcIntervalSec int) *RuntimeConfig {
	ro.gcIntervalSec = gcIntervalSec
	return ro
}

func (ro *RuntimeConfig) SetDomainRoutersHandling(handlesDomainRouters bool) *RuntimeConfig {
	ro.handlesDomainRouters = handlesDomainRouters
	return ro
}

func (ro *RuntimeConfig) SetTLS(enableTLS bool) *RuntimeConfig {
	ro.enableTLS = enableTLS
	return ro
}

func (ro *RuntimeConfig) EnableNatsCluster(enableCluster bool) *RuntimeConfig {
	ro.enableNatsClusterMode = enableCluster
	return ro
}

func (ro *RuntimeConfig) SetNatsReplicas(replicasCount int) *RuntimeConfig {
	ro.natsReplicasCount = replicasCount
	return ro
}

func (ro *RuntimeConfig) ConfigureNatsCluster(replicasCount int) *RuntimeConfig {
	return ro.EnableNatsCluster(true).SetNatsReplicas(replicasCount)
}

func (ro *RuntimeConfig) SetActivePassiveMode(activePassiveMode bool) *RuntimeConfig {
	ro.activePassiveMode = activePassiveMode
	return ro
}

type StreamType int

const (
	StreamTypeFunction StreamType = iota
	StreamTypeSystem
	StreamTypeKV
)

func (ro *RuntimeConfig) SetStreamMaxMessages(streamType StreamType, maxMessages int64) *RuntimeConfig {
	switch streamType {
	case StreamTypeFunction:
		ro.ftStreamMaxMsgs = maxMessages
	case StreamTypeSystem:
		ro.sysStreamMaxMsgs = maxMessages
	case StreamTypeKV:
		ro.kvStreamMaxMsgs = maxMessages
	}

	return ro
}

func (ro *RuntimeConfig) SetStreamMaxBytes(streamType StreamType, maxBytes int64) *RuntimeConfig {
	switch streamType {
	case StreamTypeFunction:
		ro.ftStreamMaxBytes = maxBytes
	case StreamTypeSystem:
		ro.sysStreamMaxBytes = maxBytes
	case StreamTypeKV:
		ro.kvStreamMaxBytes = maxBytes
	}

	return ro
}

func (ro *RuntimeConfig) SetStreamMaxAge(streamType StreamType, maxAge time.Duration) *RuntimeConfig {
	switch streamType {
	case StreamTypeFunction:
		ro.ftStreamMaxAge = maxAge
	case StreamTypeSystem:
		ro.sysStreamMaxAge = maxAge
	case StreamTypeKV:
		ro.kvStreamMaxAge = maxAge
	}

	return ro
}
