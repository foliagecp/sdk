

package statefun

const (
	RuntimeName                 = "runtime"
	NatsURL                     = "nats://nats:foliage@nats:4222"
	KVMutexLifetimeSec          = 120
	KVMutexIsOldPollingInterval = 10
	FunctionTypeIDLifetimeMs    = 5000
	RequestTimeoutSec           = 60
	GCIntervalSec               = 5
	DefaultHubDomainName        = "hub"
	HandlesDomainRouters        = true
)

type RuntimeConfig struct {
	name                           string
	natsURL                        string
	kvMutexLifeTimeSec             int
	kvMutexIsOldPollingIntervalSec int
	functionTypeIDLifetimeMs       int
	requestTimeoutSec              int
	gcIntervalSec                  int
	hubDomainName                  string
	handlesDomainRouters           bool
}

func NewRuntimeConfig() *RuntimeConfig {
	return &RuntimeConfig{
		name:                           RuntimeName,
		natsURL:                        NatsURL,
		kvMutexLifeTimeSec:             KVMutexLifetimeSec,
		kvMutexIsOldPollingIntervalSec: KVMutexIsOldPollingInterval,
		functionTypeIDLifetimeMs:       FunctionTypeIDLifetimeMs,
		requestTimeoutSec:              RequestTimeoutSec,
		gcIntervalSec:                  GCIntervalSec,
		hubDomainName:                  DefaultHubDomainName,
		handlesDomainRouters:           HandlesDomainRouters,
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

func (ro *RuntimeConfig) UseJSDomainAsHubDomainName() *RuntimeConfig {
	ro.hubDomainName = "" // empty string means auto fill
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
