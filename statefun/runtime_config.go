package statefun

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
	EnableNatsClusterMode       = false
	NatsReplicasCount           = 1
	activePassiveMode           = true
)

type RuntimeConfig struct {
	name                           string
	natsURL                        string
	enableNatsClusterMode          bool
	natsReplicasCount              int
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
}

func NewRuntimeConfig() *RuntimeConfig {
	return &RuntimeConfig{
		name:                           RuntimeName,
		natsURL:                        NatsURL,
		enableNatsClusterMode:          EnableNatsClusterMode,
		natsReplicasCount:              NatsReplicasCount,
		kvMutexLifeTimeSec:             KVMutexLifetimeSec,
		kvMutexIsOldPollingIntervalSec: KVMutexIsOldPollingInterval,
		functionTypeIDLifetimeMs:       FunctionTypeIDLifetimeMs,
		requestTimeoutSec:              RequestTimeoutSec,
		gcIntervalSec:                  GCIntervalSec,
		desiredHUBDomainName:           DefaultHubDomainName,
		handlesDomainRouters:           HandlesDomainRouters,
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
