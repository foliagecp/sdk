package statefun

import (
	"time"

	"github.com/foliagecp/sdk/statefun/system"
)

const (
	RuntimeName                 = "runtime"
	NatsURL                     = "nats://nats:foliage@nats:4222"
	KVMutexLifetimeSec          = 10
	KVMutexIsOldPollingInterval = 10
	FunctionTypeIDLifetimeMs    = 5000
	RequestTimeoutSec           = 60
	// NatsAPITimeoutSec is the JetStream API request timeout (nats.MaxWait) for
	// the runtime's JS context. 0 means "leave the nats.go default" (~5s),
	// preserving the historical behaviour. Tests raise it so KV-mutex Put/Update
	// calls do not spuriously time out when a single embedded NATS server is
	// saturated under the full -race suite (the failures are "context deadline
	// exceeded" on kv.Put/Update, not a logic bug).
	NatsAPITimeoutSec = 0
	// FunctionPoolDrainTimeoutSec bounds how long a worker pool waits — in the
	// background, off the lifecycle critical path — for in-flight handlers to
	// finish after the function is torn down (passive transition / shutdown)
	// before cancelling their contexts as a last resort. Defaults to
	// RequestTimeoutSec so it tracks the longest a single handler call may
	// legitimately take; recovery is NOT delayed by it (the drain is async).
	FunctionPoolDrainTimeoutSec = RequestTimeoutSec
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
	TraceStreamMaxMsgs          = 100000
	TraceStreamMaxBytes         = 1024 * 1024 * 64
	TraceStreamMaxAge           = 24 //hours
	KVStreamMaxMsgs             = -1 //unlimited
	KVStreamMaxBytes            = -1 //unlimited
	KVStreamMaxAge              = 0  //unlimited
	activePassiveMode           = true
)

// Export stream defaults
const (
	DefaultExportEnabled        = false
	DefaultExportStreamMaxMsgs  = int64(100000)
	DefaultExportStreamMaxBytes = int64(1024 * 1024 * 512) // 512MB
	DefaultExportStreamMaxAge   = 72 * time.Hour
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
	natsAPITimeoutSec              int
	functionPoolDrainTimeoutSec    int
	gcIntervalSec                  int
	desiredHUBDomainName           string
	handlesDomainRouters           bool
	activePassiveMode              bool
	isActiveInstance               bool
	activeRevID                    uint64
	enableTLS                      bool
	exportEnabled                  bool
	exportStreamMaxMsgs            int64
	exportStreamMaxBytes           int64
	exportStreamMaxAge             time.Duration
}

type StreamParams struct {
	natsReplicasCount   int
	ftStreamMaxMsgs     int64
	ftStreamMaxBytes    int64
	ftStreamMaxAge      time.Duration
	sysStreamMaxMsgs    int64
	sysStreamMaxBytes   int64
	sysStreamMaxAge     time.Duration
	kvStreamMaxMsgs     int64
	kvStreamMaxBytes    int64
	kvStreamMaxAge      time.Duration
	traceStreamMaxMsgs  int64
	traceStreamMaxBytes int64
	traceStreamMaxAge   time.Duration
}

func NewRuntimeConfig() *RuntimeConfig {
	streamParams := StreamParams{
		natsReplicasCount:   NatsReplicasCount,
		ftStreamMaxMsgs:     FtStreamMaxMsgs,
		ftStreamMaxBytes:    FtStreamMaxBytes,
		ftStreamMaxAge:      FtStreamMaxAge,
		sysStreamMaxMsgs:    SysStreamMaxMsgs,
		sysStreamMaxBytes:   SysStreamMaxBytes,
		sysStreamMaxAge:     SysStreamMaxAge,
		kvStreamMaxMsgs:     KVStreamMaxMsgs,
		kvStreamMaxBytes:    KVStreamMaxBytes,
		kvStreamMaxAge:      KVStreamMaxAge,
		traceStreamMaxMsgs:  int64(system.GetEnvMustProceed("TRACE_STREAM_MAX_MSG", TraceStreamMaxMsgs)),
		traceStreamMaxBytes: int64(system.GetEnvMustProceed("TRACE_STREAM_MAX_BYTES", TraceStreamMaxBytes)),
		traceStreamMaxAge:   time.Duration(system.GetEnvMustProceed("TRACE_STREAM_MAX_AGE_HOURS", TraceStreamMaxAge)) * time.Hour,
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
		natsAPITimeoutSec:              NatsAPITimeoutSec,
		functionPoolDrainTimeoutSec:    FunctionPoolDrainTimeoutSec,
		gcIntervalSec:                  GCIntervalSec,
		desiredHUBDomainName:           DefaultHubDomainName,
		handlesDomainRouters:           HandlesDomainRouters,
		enableTLS:                      EnableTLS,
		activePassiveMode:              activePassiveMode,
		isActiveInstance:               true,
		exportEnabled:                  DefaultExportEnabled,
		exportStreamMaxMsgs:            DefaultExportStreamMaxMsgs,
		exportStreamMaxBytes:           DefaultExportStreamMaxBytes,
		exportStreamMaxAge:             DefaultExportStreamMaxAge,
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

// SetNatsAPITimeoutSec sets the JetStream API request timeout (nats.MaxWait)
// for the runtime's JS context. 0 keeps the nats.go default. Mainly used by
// tests to give a saturated embedded NATS server enough headroom under -race.
func (ro *RuntimeConfig) SetNatsAPITimeoutSec(sec int) *RuntimeConfig {
	ro.natsAPITimeoutSec = sec
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

// SetFunctionPoolDrainTimeoutSec sets how long a function's worker pool waits
// (in the background, off the recovery path) for in-flight handlers to finish
// after teardown before cancelling their contexts as a last resort.
func (ro *RuntimeConfig) SetFunctionPoolDrainTimeoutSec(sec int) *RuntimeConfig {
	ro.functionPoolDrainTimeoutSec = sec
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
	StreamTypeTrace
)

func (ro *RuntimeConfig) SetStreamMaxMessages(streamType StreamType, maxMessages int64) *RuntimeConfig {
	switch streamType {
	case StreamTypeFunction:
		ro.ftStreamMaxMsgs = maxMessages
	case StreamTypeSystem:
		ro.sysStreamMaxMsgs = maxMessages
	case StreamTypeKV:
		ro.kvStreamMaxMsgs = maxMessages
	case StreamTypeTrace:
		ro.traceStreamMaxBytes = maxMessages
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
	case StreamTypeTrace:
		ro.traceStreamMaxBytes = maxBytes
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
	case StreamTypeTrace:
		ro.traceStreamMaxAge = maxAge
	}

	return ro
}

func (ro *RuntimeConfig) SetExportEnabled(enabled bool) *RuntimeConfig {
	ro.exportEnabled = enabled
	return ro
}

func (ro *RuntimeConfig) SetExportStreamMaxMessages(maxMsgs int64) *RuntimeConfig {
	ro.exportStreamMaxMsgs = maxMsgs
	return ro
}

func (ro *RuntimeConfig) SetExportStreamMaxBytes(maxBytes int64) *RuntimeConfig {
	ro.exportStreamMaxBytes = maxBytes
	return ro
}

func (ro *RuntimeConfig) SetExportStreamMaxAge(maxAge time.Duration) *RuntimeConfig {
	ro.exportStreamMaxAge = maxAge
	return ro
}
