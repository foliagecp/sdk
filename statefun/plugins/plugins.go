

package plugins

import (
	"fmt"
	"json_easy"
	"sync"

	"github.com/foliagecp/sdk/statefun/cache"
)

type StatefunAddress struct {
	Typename string
	ID       string
}

type StatefunContextProcessor struct {
	GlobalCache        *cache.CacheStore
	GetFunctionContext func() *json_easy.JSON
	SetFunctionContext func(*json_easy.JSON)
	GetObjectContext   func() *json_easy.JSON
	SetObjectContext   func(*json_easy.JSON)
	Call               func(string, string, *json_easy.JSON, *json_easy.JSON)
	// TODO: DownstreamCall(<function type>, <links filters>, <payload>, <options>)
	GolangCallSync func(string, string, *json_easy.JSON, *json_easy.JSON) *json_easy.JSON
	Egress         func(string, *json_easy.JSON)
	Self           StatefunAddress
	Caller         StatefunAddress
	Payload        *json_easy.JSON
	Options        *json_easy.JSON
}

type StatefunExecutor interface {
	Run(contextProcessor *StatefunContextProcessor) error
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

func (tnex *TypenameExecutorPlugin) AddForId(id string) {
	if tnex.executorContructorFunction == nil {
		fmt.Printf("Cannot create new StatefunExecutor for id=%s: missing newExecutor function\n", id)
		tnex.idExecutors.Store(id, nil)
	} else {
		fmt.Printf("______________ Created StatefunExecutor for id=%s\n", id)
		executor := tnex.executorContructorFunction(tnex.alias, tnex.source)
		tnex.idExecutors.Store(id, executor)
	}
}

func (tnex *TypenameExecutorPlugin) RemoveForId(id string) {
	tnex.idExecutors.Delete(id)
}

func (tnex *TypenameExecutorPlugin) GetForId(id string) StatefunExecutor {
	value, _ := tnex.idExecutors.Load(id)
	return value.(StatefunExecutor)
}
