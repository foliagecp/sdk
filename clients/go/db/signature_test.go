package db

// Compile-time signature guard for the ENTIRE public sync-client surface.
//
// Downstream applications pin these methods in their OWN interfaces, so every
// signature below is FROZEN: adding a parameter — even a variadic one —
// changes the method type and silently breaks interface satisfaction in every
// consumer while all existing calls keep compiling (learned the hard way when
// VertexReadDetailsV2 briefly grew a variadic). New capabilities go into NEW
// methods (e.g. VertexReadDetailsV2Full).
//
// These assertions fail to compile the moment any pinned signature drifts.
// When ADDING a method, add it here too; existing entries must never change.

import (
	"time"

	"github.com/foliagecp/easyjson"
)

var _ interface {
	VertexCreate(id string, body ...easyjson.JSON) error
	VertexDelete(id string) error
	VertexRead(id string, details ...bool) (easyjson.JSON, error)
	VertexReadDetailsV2(id string) (easyjson.JSON, error)
	VertexReadDetailsV2Full(id string, linkContent ...bool) (easyjson.JSON, error)
	VertexUpdate(id string, body easyjson.JSON, replace bool, upsert ...bool) error
	VerticesLinkCreate(from, to, linkName, linkType string, tags []string, body ...easyjson.JSON) error
	VerticesLinkDelete(from, linkName string) error
	VerticesLinkDeleteByToAndType(from, to, linkType string) error
	VerticesLinkRead(from, linkName string, details ...bool) (easyjson.JSON, error)
	VerticesLinkReadByToAndType(from, to, linkType string, details ...bool) (easyjson.JSON, error)
	VerticesLinkUpdate(from, linkName string, tags []string, body easyjson.JSON, replace bool, toAndType4Upsert ...string) error
	VerticesLinkUpdateByToAndType(from, to, linkType string, tags []string, body easyjson.JSON, replace bool, name4Upsert ...string) error
} = GraphSyncClient{}

var _ interface {
	MetaTriggerObjectDelete(triggerType TriggerType, statefunName ...string) error
	MetaTriggerObjectDrop(triggerType TriggerType) error
	MetaTriggerObjectLinkDelete(triggerType TriggerType, statefunName ...string) error
	MetaTriggerObjectLinkDrop(triggerType TriggerType) error
	MetaTriggerObjectLinkSet(triggerType TriggerType, statefunName ...string) error
	MetaTriggerObjectSet(triggerType TriggerType, statefunName ...string) error
	MetaTriggerTypeDelete(triggerType TriggerType, statefunName ...string) error
	MetaTriggerTypeDrop(triggerType TriggerType) error
	MetaTriggerTypeSet(triggerType TriggerType, statefunName ...string) error
	MetaTriggerTypesLinkDelete(triggerType TriggerType, statefunName ...string) error
	MetaTriggerTypesLinkDrop(triggerType TriggerType) error
	MetaTriggerTypesLinkSet(triggerType TriggerType, statefunName ...string) error
	ObjectCreate(objectID, originType string, body ...easyjson.JSON) error
	ObjectDelete(id string) error
	ObjectDeleteWithDetails(id string) (easyjson.JSON, error)
	ObjectRead(name string) (easyjson.JSON, error)
	ObjectReadV2(name string) (easyjson.JSON, error)
	ObjectUpdate(objectID string, body easyjson.JSON, replace bool, originType4Upsert ...string) error
	ObjectUpdateWithDetails(objectID string, body easyjson.JSON, replace bool, originType4Upsert ...string) (easyjson.JSON, error)
	ObjectsLinkCreate(from, to, name string, tags []string, body ...easyjson.JSON) error
	ObjectsLinkDelete(from, to string) error
	ObjectsLinkDeleteWithDetails(from, to string) (easyjson.JSON, error)
	ObjectsLinkRead(from, to string) (easyjson.JSON, error)
	ObjectsLinkSuperTypeCreate(from, to, fromClaimType, toClaimType, name string, tags []string, body ...easyjson.JSON) error
	ObjectsLinkSuperTypeDelete(from, to, fromClaimType, toClaimType string) error
	ObjectsLinkSuperTypeUpdate(from, to, fromClaimType, toClaimType, name string, tags []string, body easyjson.JSON, replace bool) error
	ObjectsLinkUpdate(from, to string, tags []string, body easyjson.JSON, replace bool, name4Upsert ...string) error
	ObjectsLinkUpdateWithDetails(from, to string, tags []string, body easyjson.JSON, replace bool, name4Upsert ...string) (easyjson.JSON, error)
	TriggerLinkDrop(fromTypeName, toTypeName string, triggerType TriggerType) error
	TriggerLinkRemove(fromTypeName, toTypeName string, triggerType TriggerType, statefunName ...string) error
	TriggerLinkSet(fromTypeName, toTypeName string, triggerType TriggerType, statefunName ...string) error
	TriggerObjectDelete(typeName string, triggerType TriggerType, statefunName ...string) error
	TriggerObjectDrop(typeName string, triggerType TriggerType) error
	TriggerObjectSet(typeName string, triggerType TriggerType, statefunName ...string) error
	TypeCreate(name string, body ...easyjson.JSON) error
	TypeDelete(name string) error
	TypeRead(name string) (easyjson.JSON, error)
	TypeRemoveSubType(baseType, childType string) error
	TypeSetSubType(baseType, childType string) error
	TypeUpdate(name string, body easyjson.JSON, replace bool, upsert ...bool) error
	TypesLinkCreate(from, to, objectLinkType string, tags []string, body ...easyjson.JSON) error
	TypesLinkDelete(from, to string) error
	TypesLinkRead(from, to string) (easyjson.JSON, error)
	TypesLinkUpdate(from, to string, tags []string, body easyjson.JSON, replace bool, objectLinkType4Upsert ...string) error
} = CMDBSyncClient{}

var _ interface {
	FPLQuery(id, queryStringOfJSON string) (easyjson.JSON, error)
	JPGQLCtraQuery(id, query string) ([]string, error)
} = QuerySyncClient{}

var _ interface {
	BatchCreate(batchID ...string) *Batch
} = DBSyncClient{}

var _ interface {
	Commit() ([]BatchResult, error)
	Len() int
	Operation(typename, id string, payload easyjson.JSON, options ...easyjson.JSON) *Batch
	Parallel() *Batch
	StopOnError() *Batch
	SubBatchSize(n int) *Batch
	Timeout(d time.Duration) *Batch
} = (*Batch)(nil)
