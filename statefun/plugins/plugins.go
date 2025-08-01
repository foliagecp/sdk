// Foliage statefun plugins package.
// Provides unified interfaces for stateful functions plugins
package plugins

import (
	"fmt"
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

type SFSignalFunc func(signalProvider SignalProvider, typename string, id string, payload *easyjson.JSON, options *easyjson.JSON) error
type SFRequestFunc func(requestProvider RequestProvider, typename string, id string, payload *easyjson.JSON, options *easyjson.JSON, timeout ...time.Duration) (*easyjson.JSON, error)
type ObjectSignalFunc func(signalProvider SignalProvider, query LinkQuery, typename string, id string, payload *easyjson.JSON, options *easyjson.JSON) (map[string]error, error)
type ObjectRequestFunc func(requestProvider RequestProvider, query LinkQuery, typename string, id string, payload *easyjson.JSON, options *easyjson.JSON, timeout ...time.Duration) (map[string]*ObjectRequestReply, error)
type SFEgressFunc func(egressProvider EgressProvider, payload *easyjson.JSON, customId ...string) error

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

	CreateCustomShadowId(storeDomain, targetDomain, uuid string) string
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
	Signal                    SFSignalFunc
	Request                   SFRequestFunc
	ObjectSignal              ObjectSignalFunc
	ObjectRequest             ObjectRequestFunc
	Egress                    SFEgressFunc
	Self                      StatefunAddress
	Caller                    StatefunAddress
	Payload                   *easyjson.JSON
	Options                   *easyjson.JSON
	Reply                     *SyncReply // when requested in function: nil - function was signaled, !nil - function was requested
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
		if _, ok := tnex.idExecutors.Load(id); !ok {
			lg.Logf(lg.TraceLevel, "______________ Created StatefunExecutor for id=%s", id)
			executor := tnex.executorContructorFunction(tnex.alias, tnex.source)
			tnex.idExecutors.Store(id, executor)
		}
	}
}

func (tnex *TypenameExecutorPlugin) RemoveForID(id string) {
	tnex.idExecutors.Delete(id)
}

func (tnex *TypenameExecutorPlugin) GetForID(id string) StatefunExecutor {
	value, _ := tnex.idExecutors.Load(id)
	return value.(StatefunExecutor)
}

type ObjectRequestReply struct {
	ReqReply *easyjson.JSON
	ReqError error
}

type LinkQuery struct {
	linkType string                 //link type
	name     string                 //link name
	tags     []string               //link tags
	filter   map[string]interface{} //objects filter
	custom   string                 //custom query
}

func NewLinkQuery(lt string) LinkQuery {
	return LinkQuery{
		linkType: lt,
	}
}

func (lq *LinkQuery) WithName(name string) {
	lq.name = name
}

func (lq *LinkQuery) WithTags(tags ...string) {
	lq.tags = tags
}

func (lq *LinkQuery) WithCustom(custom string) {
	lq.custom = custom
}

func (lq *LinkQuery) WithFilter(filter map[string]interface{}) {
	lq.filter = filter
}

func (lq *LinkQuery) GetType() string {
	return lq.linkType
}

func (lq *LinkQuery) GetName() string {
	if lq.name == "" {
		return "*"
	}
	return lq.name
}

func (lq *LinkQuery) GetTags() []string {
	if lq.tags == nil || len(lq.tags) == 0 {
		return []string{}
	}
	return lq.tags
}

func (lq *LinkQuery) GetFilter() map[string]interface{} {
	if lq.filter == nil && len(lq.filter) == 0 {
		return map[string]interface{}{}
	}
	return lq.filter
}

func (lq *LinkQuery) GetCustom() string {
	return lq.custom
}

func (lq *LinkQuery) Validate() error {
	if lq.linkType == "" {
		return fmt.Errorf("link type is empty")
	}
	return nil
}
