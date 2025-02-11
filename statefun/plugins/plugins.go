

// Foliage statefun plugins package.
// Provides unified interfaces for stateful functions plugins
package plugins

import (
	"sync"
	"time"

	"github.com/foliagecp/sdk/statefun/cache"
	lg "github.com/foliagecp/sdk/statefun/logger"

	"github.com/foliagecp/easyjson"
)

type PluginError interface {
	Error() string
	GetLocation() string
	GetStackTrace() string
}

type StatefunAddress struct {
	Typename string
	ID       string
}

type SignalProvider int

const (
	AutoSignalSelect SignalProvider = iota
	JetstreamGlobalSignal
	GolangLocalSignal
)

type RequestProvider int

const (
	AutoRequestSelect RequestProvider = iota
	NatsCoreGlobalRequest
	GolangLocalRequest
)

type EgressProvider int

type SFSignalFunc func(SignalProvider, string, string, *easyjson.JSON, *easyjson.JSON) error
type SFRequestFunc func(RequestProvider, string, string, *easyjson.JSON, *easyjson.JSON, ...time.Duration) (*easyjson.JSON, error)
type SFEgressFunc func(EgressProvider, *easyjson.JSON, ...string) error

const (
	NatsCoreEgress EgressProvider = iota
)

type SyncReply struct {
	With                    func(*easyjson.JSON)
	CancelDefaultReply      func()
	OverrideRequestCallback func() *SyncReply
}

type Domain interface {
	HubDomainName() string
	Name() string
	Cache() *cache.Store
	GetDomainFromObjectID(objectID string) string
	GetObjectIDWithoutDomain(objectID string) string
	CreateObjectIDWithDomain(domain string, objectID string, domainReplace bool) string
	CreateObjectIDWithThisDomain(objectID string, domainReplace bool) string
	CreateObjectIDWithHubDomain(objectID string, domainReplace bool) string
	// Get all domains in weak cluster including this one
	GetWeakClusterDomains() []string
	// Set all domains in weak cluster (this domain name will also be included automatically if not defined)
	SetWeakClusterDomains(weakClusterDomains []string)
	/*
	* otherDomainName/ObjectId -> thisDomainName/otherDomainName#ObjectId
	* thisDomainName/ObjectId -> thisDomainName/ObjectId
	 */
	GetShadowObjectShadowId(objectIdWithAnyDomainName string) string
	/*
	* domainName1/domainName2#ObjectId -> domainName2, ObjectId
	 */
	GetShadowObjectDomainAndID(shadowObjectId string) (domainName, objectIdWithoutDomain string, err error)
	/*
	* domainName1/domainName2#ObjectId -> true
	* domainName1/ObjectId  -> false
	 */
	IsShadowObject(idWithDomain string) bool

	GetValidObjectId(objectId string) string
}

type StatefunContextProcessor struct {
	GetFunctionContext        func() *easyjson.JSON
	SetFunctionContext        func(*easyjson.JSON)
	SetContextExpirationAfter func(time.Duration)
	GetObjectContext          func() *easyjson.JSON
	SetObjectContext          func(*easyjson.JSON)
	ObjectMutexLock           func(objectId string, errorOnLocked bool) error
	ObjectMutexUnlock         func(objectId string) error
	Domain                    Domain
	// TODO: DownstreamSignal(<function type>, <links filters>, <payload>, <options>)
	Signal  SFSignalFunc
	Request SFRequestFunc
	Egress  SFEgressFunc
	Self    StatefunAddress
	Caller  StatefunAddress
	Payload *easyjson.JSON
	Options *easyjson.JSON
	Reply   *SyncReply // when requested in function: nil - function was signaled, !nil - function was requested
}

type StatefunExecutor interface {
	Run(ctx *StatefunContextProcessor) error
	BuildError() error
}

type StatefunExecutorConstructor func(alias string, source string) StatefunExecutor

type TypenameExecutorPlugin struct {
	alias                      string
	source                     string
	idExecutors                sync.Map
	executorContructorFunction StatefunExecutorConstructor
}

func NewTypenameExecutor(alias string, source string, executorContructorFunction StatefunExecutorConstructor) *TypenameExecutorPlugin {
	tnex := TypenameExecutorPlugin{alias: alias, source: source, executorContructorFunction: executorContructorFunction}
	return &tnex
}

func (tnex *TypenameExecutorPlugin) AddForID(id string) {
	if tnex.executorContructorFunction == nil {
		lg.Logf(lg.ErrorLevel, "Cannot create new StatefunExecutor for id=%s: missing newExecutor function", id)
		tnex.idExecutors.Store(id, nil)
	} else {
		lg.Logf(lg.TraceLevel, "______________ Created StatefunExecutor for id=%s", id)
		executor := tnex.executorContructorFunction(tnex.alias, tnex.source)
		tnex.idExecutors.Store(id, executor)
	}
}

func (tnex *TypenameExecutorPlugin) RemoveForID(id string) {
	tnex.idExecutors.Delete(id)
}

func (tnex *TypenameExecutorPlugin) GetForID(id string) StatefunExecutor {
	value, _ := tnex.idExecutors.Load(id)
	return value.(StatefunExecutor)
}
