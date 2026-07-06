package db

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfp "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats.go"
	"golang.org/x/sync/singleflight"
)

type TriggerType = string

const (
	CreateTrigger TriggerType = "create"
	UpdateTrigger TriggerType = "update"
	DeleteTrigger TriggerType = "delete"
	ReadTrigger   TriggerType = "read"
)

type CMDBSyncClient struct {
	request                   sfp.SFRequestFunc
	ShadowObjectCanBeRecevier bool

	// readFlight deduplicates concurrent identical Read calls in this
	// client instance. Multiple goroutines issuing the same ObjectRead
	// (etc.) collapse into a single NATS round-trip; remaining waiters
	// receive a Clone of the result so they can mutate it independently.
	//
	// The field is a *singleflight.Group rather than a value because
	// CMDBSyncClient methods are value-receivers — copying the struct
	// (which happens on every method call) would otherwise duplicate
	// the in-flight map and defeat the deduplication. With a pointer,
	// every copy shares the same Group.
	//
	// IMPORTANT: deduplication is in-flight only. Once Do returns the
	// entry is removed and the next Read starts a fresh call — so a
	// stale read is impossible. The group is per-client-instance, so
	// two clients in the same process do not share each other's
	// in-flight reads — which matches existing expectations: clients
	// are independent abstractions.
	//
	// When nil (e.g. struct constructed directly without the New...
	// constructor) the Read wrappers fall back to direct request calls
	// and no deduplication happens — correctness is preserved.
	readFlight *singleflight.Group
}

func NewCMDBSyncClient(NatsURL string, NatsRequestTimeoutSec int, HubDomainName string) (CMDBSyncClient, error) {
	var err error
	nc, err := nats.Connect(NatsURL)
	if err != nil {
		return CMDBSyncClient{}, err
	}
	request := getRequestFunc(nc, NatsRequestTimeoutSec, HubDomainName)
	return NewCMDBSyncClientFromRequestFunction(request)
}

/*
ctx.Request
// or
runtime.Request
*/
func NewCMDBSyncClientFromRequestFunction(request sfp.SFRequestFunc) (CMDBSyncClient, error) {
	if request == nil {
		return CMDBSyncClient{}, fmt.Errorf("request must not be nil")
	}
	return CMDBSyncClient{request: request, readFlight: &singleflight.Group{}}, nil
}

// ------------------------------------------------------------------------------------------------

func (cmdb CMDBSyncClient) commonTriggerDelete(body easyjson.JSON, triggerType TriggerType, statefunName ...string) easyjson.JSON {
	triggerPath := fmt.Sprintf("triggers.%s", triggerType)
	var bodyTriggers easyjson.JSON
	if body.GetByPath(triggerPath).IsNonEmptyObject() {
		newTriggers := []string{}
		if arr, ok := body.GetByPath(triggerPath).AsArrayString(); ok {
			for _, sf := range arr {
				toRemove := false
				for _, sf2Remove := range statefunName {
					if sf == sf2Remove {
						toRemove = true
					}
				}
				if !toRemove {
					newTriggers = append(newTriggers, sf)
				}
			}
		}
		bodyTriggers = easyjson.NewJSONObjectWithKeyValue(triggerPath, easyjson.NewJSON(newTriggers))
	} else {
		bodyTriggers = easyjson.NewJSONObjectWithKeyValue(triggerPath, easyjson.NewJSONArray())
	}

	body.SetByPath(triggerPath, bodyTriggers)
	newBody := body.GetByPath("body")
	if newBody.IsNull() {
		newBody = easyjson.NewJSONObject()
	}

	return newBody
}

func (cmdb CMDBSyncClient) commonTriggersDrop(body easyjson.JSON, triggerType TriggerType) easyjson.JSON {
	triggerPath := fmt.Sprintf("triggers.%s", triggerType)

	body.SetByPath(triggerPath, easyjson.NewJSONArray())
	newBody := body.GetByPath("body")
	if newBody.IsNull() {
		newBody = easyjson.NewJSONObject()
	}

	return newBody
}

func (cmdb CMDBSyncClient) TriggerObjectSet(typeName string, triggerType TriggerType, statefunName ...string) error {
	if len(statefunName) == 0 {
		return fmt.Errorf("at least 1 statefun name is required")
	}

	body := easyjson.NewJSONObject()
	body.SetByPath(fmt.Sprintf("triggers.%s", triggerType), easyjson.NewJSON(statefunName))
	return cmdb.TypeUpdate(
		typeName,
		body,
		false,
		true,
	)
}

func (cmdb CMDBSyncClient) TriggerObjectDelete(typeName string, triggerType TriggerType, statefunName ...string) error {
	if len(statefunName) == 0 {
		return fmt.Errorf("at least 1 statefun name is required")
	}

	data, err := cmdb.TypeRead(typeName)
	if err != nil {
		return err
	}

	body := data.GetByPath("body")
	if !body.IsNull() {
		body = cmdb.commonTriggerDelete(body, triggerType, statefunName...)
	}

	return cmdb.TypeUpdate(
		typeName,
		body,
		true,
	)
}

func (cmdb CMDBSyncClient) TriggerObjectDrop(typeName string, triggerType TriggerType) error {
	data, err := cmdb.TypeRead(typeName)
	if err != nil {
		return err
	}

	body := data.GetByPath("body")
	if !body.IsNull() {
		body = cmdb.commonTriggersDrop(body, triggerType)
	}

	return cmdb.TypeUpdate(
		typeName,
		body,
		true,
	)
}

func (cmdb CMDBSyncClient) TriggerLinkSet(fromTypeName, toTypeName string, triggerType TriggerType, statefunName ...string) error {
	if len(statefunName) == 0 {
		return fmt.Errorf("at least 1 statefun name is required")
	}
	body := easyjson.NewJSONObject()
	body.SetByPath(fmt.Sprintf("triggers.%s", triggerType), easyjson.NewJSON(statefunName))
	return cmdb.TypesLinkUpdate(
		fromTypeName,
		toTypeName,
		nil,
		body,
		false,
		"",
	)
}

func (cmdb CMDBSyncClient) TriggerLinkRemove(fromTypeName, toTypeName string, triggerType TriggerType, statefunName ...string) error {
	if len(statefunName) == 0 {
		return fmt.Errorf("at least 1 statefun name is required")
	}

	data, err := cmdb.TypesLinkRead(fromTypeName, toTypeName)
	if err != nil {
		return err
	}

	tags := []string{}
	if arr, ok := data.GetByPath("tags").AsArrayString(); ok {
		tags = arr
	}

	body := data.GetByPath("body")
	if !body.IsNull() {
		body = cmdb.commonTriggerDelete(body, triggerType, statefunName...)
	}

	return cmdb.TypesLinkUpdate(
		fromTypeName,
		toTypeName,
		tags,
		body,
		true,
	)
}

func (cmdb CMDBSyncClient) TriggerLinkDrop(fromTypeName, toTypeName string, triggerType TriggerType) error {
	data, err := cmdb.TypesLinkRead(fromTypeName, toTypeName)
	if err != nil {
		return err
	}

	tags := []string{}
	if arr, ok := data.GetByPath("tags").AsArrayString(); ok {
		tags = arr
	}

	body := data.GetByPath("body")
	if !body.IsNull() {
		body = cmdb.commonTriggersDrop(body, triggerType)
	}

	return cmdb.TypesLinkUpdate(
		fromTypeName,
		toTypeName,
		tags,
		body,
		true,
	)
}

// ------------------------------------------------------------------------------------------------
// Meta lifecycle triggers — fire on the lifecycle of the TYPES and type-to-type
// links (stored in the `types` root vertex body) or, globally, of ALL OBJECTS
// and object-to-object links (stored in the `objects` root vertex body), under
// body.meta_triggers.<section>.<event>. section ∈ {type, types_link, object,
// object_link}; event ∈ {create, update, delete, read}.
// ------------------------------------------------------------------------------------------------

const (
	metaRootTypes   = "types"   // BUILT_IN_TYPES root vertex
	metaRootObjects = "objects" // BUILT_IN_OBJECTS root vertex
)

// metaRootRead reads a system root vertex (types / objects).
func (cmdb CMDBSyncClient) metaRootRead(rootName string) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	om := sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.graph.api.vertex.read", rootName, &payload, nil))
	if om.Status != sfMediators.SYNC_OP_STATUS_OK {
		return easyjson.NewJSONNull(), OpErrorFromOpMsgStrict(om)
	}
	return om.Data, nil
}

// metaTriggerWrite merges meta_triggers.<section>.<triggerType> = fns into the
// root vertex body with replace=false (a deep merge that replaces only that
// array, leaving body.version / cache.parent_types and the other sections
// untouched).
func (cmdb CMDBSyncClient) metaTriggerWrite(rootName, section string, triggerType TriggerType, fns []string) error {
	body := easyjson.NewJSONObject()
	body.SetByPath(fmt.Sprintf("meta_triggers.%s.%s", section, triggerType), easyjson.NewJSON(fns))

	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	payload.SetByPath("replace", easyjson.NewJSON(false))
	payload.SetByPath("body", body)
	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.graph.api.vertex.update", rootName, &payload, &options)))
}

// metaTriggerPrune returns the current statefun list for (section, triggerType)
// in a root body with the named statefuns removed.
func metaTriggerPrune(rootBody easyjson.JSON, section string, triggerType TriggerType, remove ...string) []string {
	path := fmt.Sprintf("meta_triggers.%s.%s", section, triggerType)
	kept := []string{}
	if arr, ok := rootBody.GetByPath(path).AsArrayString(); ok {
		for _, sf := range arr {
			drop := false
			for _, r := range remove {
				if sf == r {
					drop = true
					break
				}
			}
			if !drop {
				kept = append(kept, sf)
			}
		}
	}
	return kept
}

func (cmdb CMDBSyncClient) metaTriggerSet(rootName, section string, triggerType TriggerType, statefunName ...string) error {
	if len(statefunName) == 0 {
		return fmt.Errorf("at least 1 statefun name is required")
	}
	return cmdb.metaTriggerWrite(rootName, section, triggerType, statefunName)
}

// metaRootReplace writes the FULL root body back with replace=true. Removal MUST
// use replace: the body merge (replace=false) unions arrays (easyjson DeepMerge
// append-unique) and so can only ADD trigger fns, never remove them. Reading and
// rewriting the whole body preserves the types-root `version` marker that lives
// alongside meta_triggers.
func (cmdb CMDBSyncClient) metaRootReplace(rootName string, body easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	payload.SetByPath("replace", easyjson.NewJSON(true))
	payload.SetByPath("body", body)
	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.graph.api.vertex.update", rootName, &payload, &options)))
}

func (cmdb CMDBSyncClient) metaTriggerDelete(rootName, section string, triggerType TriggerType, statefunName ...string) error {
	if len(statefunName) == 0 {
		return fmt.Errorf("at least 1 statefun name is required")
	}
	data, err := cmdb.metaRootRead(rootName)
	if err != nil {
		return err
	}
	body := data.GetByPath("body")
	if body.IsNull() {
		body = easyjson.NewJSONObject()
	}
	body.SetByPath(fmt.Sprintf("meta_triggers.%s.%s", section, triggerType), easyjson.NewJSON(metaTriggerPrune(body, section, triggerType, statefunName...)))
	return cmdb.metaRootReplace(rootName, body)
}

func (cmdb CMDBSyncClient) metaTriggerDrop(rootName, section string, triggerType TriggerType) error {
	data, err := cmdb.metaRootRead(rootName)
	if err != nil {
		return err
	}
	body := data.GetByPath("body")
	if body.IsNull() {
		body = easyjson.NewJSONObject()
	}
	body.SetByPath(fmt.Sprintf("meta_triggers.%s.%s", section, triggerType), easyjson.NewJSONArray())
	return cmdb.metaRootReplace(rootName, body)
}

// Type lifecycle triggers (stored in the `types` root).
func (cmdb CMDBSyncClient) MetaTriggerTypeSet(triggerType TriggerType, statefunName ...string) error {
	return cmdb.metaTriggerSet(metaRootTypes, "type", triggerType, statefunName...)
}
func (cmdb CMDBSyncClient) MetaTriggerTypeDelete(triggerType TriggerType, statefunName ...string) error {
	return cmdb.metaTriggerDelete(metaRootTypes, "type", triggerType, statefunName...)
}
func (cmdb CMDBSyncClient) MetaTriggerTypeDrop(triggerType TriggerType) error {
	return cmdb.metaTriggerDrop(metaRootTypes, "type", triggerType)
}

// Type-to-type link lifecycle triggers (stored in the `types` root).
func (cmdb CMDBSyncClient) MetaTriggerTypesLinkSet(triggerType TriggerType, statefunName ...string) error {
	return cmdb.metaTriggerSet(metaRootTypes, "types_link", triggerType, statefunName...)
}
func (cmdb CMDBSyncClient) MetaTriggerTypesLinkDelete(triggerType TriggerType, statefunName ...string) error {
	return cmdb.metaTriggerDelete(metaRootTypes, "types_link", triggerType, statefunName...)
}
func (cmdb CMDBSyncClient) MetaTriggerTypesLinkDrop(triggerType TriggerType) error {
	return cmdb.metaTriggerDrop(metaRootTypes, "types_link", triggerType)
}

// Global object lifecycle triggers (stored in the `objects` root).
func (cmdb CMDBSyncClient) MetaTriggerObjectSet(triggerType TriggerType, statefunName ...string) error {
	return cmdb.metaTriggerSet(metaRootObjects, "object", triggerType, statefunName...)
}
func (cmdb CMDBSyncClient) MetaTriggerObjectDelete(triggerType TriggerType, statefunName ...string) error {
	return cmdb.metaTriggerDelete(metaRootObjects, "object", triggerType, statefunName...)
}
func (cmdb CMDBSyncClient) MetaTriggerObjectDrop(triggerType TriggerType) error {
	return cmdb.metaTriggerDrop(metaRootObjects, "object", triggerType)
}

// Global object-to-object link lifecycle triggers (stored in the `objects` root).
func (cmdb CMDBSyncClient) MetaTriggerObjectLinkSet(triggerType TriggerType, statefunName ...string) error {
	return cmdb.metaTriggerSet(metaRootObjects, "object_link", triggerType, statefunName...)
}
func (cmdb CMDBSyncClient) MetaTriggerObjectLinkDelete(triggerType TriggerType, statefunName ...string) error {
	return cmdb.metaTriggerDelete(metaRootObjects, "object_link", triggerType, statefunName...)
}
func (cmdb CMDBSyncClient) MetaTriggerObjectLinkDrop(triggerType TriggerType) error {
	return cmdb.metaTriggerDrop(metaRootObjects, "object_link", triggerType)
}

func (cmdb CMDBSyncClient) TypeCreate(name string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	payload.SetByPath("body", easyjson.NewJSONObject())
	if len(body) > 0 {
		payload.SetByPath("body", body[0])
	}

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.type.create", name, &payload, &options)))
}

func (cmdb CMDBSyncClient) TypeUpdate(name string, body easyjson.JSON, replace bool, upsert ...bool) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	if len(upsert) > 0 {
		payload.SetByPath("upsert", easyjson.NewJSON(upsert[0]))
	}
	payload.SetByPath("replace", easyjson.NewJSON(replace))
	payload.SetByPath("body", body)

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.type.update", name, &payload, &options)))
}

func (cmdb CMDBSyncClient) TypeDelete(name string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.type.delete", name, &payload, &options)))
}

func (cmdb CMDBSyncClient) TypeRead(name string) (easyjson.JSON, error) {
	return doRead(cmdb.readFlight, "TypeRead:"+name, func() (any, error) {
		options := easyjson.NewJSONObject()
		options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
		om := sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.type.read", name, nil, &options))
		return readResult{data: om.Data, err: OpErrorFromOpMsgStrict(om)}, nil
	})
}

func (cmdb CMDBSyncClient) ObjectCreate(objectID, originType string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	payload.SetByPath("origin_type", easyjson.NewJSON(originType))
	payload.SetByPath("body", easyjson.NewJSONObject())
	if len(body) > 0 {
		payload.SetByPath("body", body[0])
	}

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.object.create", objectID, &payload, &options)))
}

func (cmdb CMDBSyncClient) ObjectUpdate(objectID string, body easyjson.JSON, replace bool, originType4Upsert ...string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	if len(originType4Upsert) > 0 {
		payload.SetByPath("upsert", easyjson.NewJSON(true))
		payload.SetByPath("origin_type", easyjson.NewJSON(originType4Upsert[0]))
	}
	payload.SetByPath("replace", easyjson.NewJSON(replace))
	payload.SetByPath("body", body)

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.object.update", objectID, &payload, &options)))
}

func (cmdb CMDBSyncClient) ObjectUpdateWithDetails(objectID string, body easyjson.JSON, replace bool, originType4Upsert ...string) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	if len(originType4Upsert) > 0 {
		payload.SetByPath("upsert", easyjson.NewJSON(true))
		payload.SetByPath("origin_type", easyjson.NewJSON(originType4Upsert[0]))
	}
	payload.SetByPath("replace", easyjson.NewJSON(replace))
	payload.SetByPath("body", body)

	options := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	msg := sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.object.update", objectID, &payload, &options))
	return msg.Data, OpErrorFromOpMsg(msg)
}

func (cmdb CMDBSyncClient) ObjectDelete(id string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.object.delete", id, &payload, &options)))
}

func (cmdb CMDBSyncClient) ObjectDeleteWithDetails(id string) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	options := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	msg := sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.object.delete", id, &payload, &options))
	return msg.Data, OpErrorFromOpMsg(msg)
}

func (cmdb CMDBSyncClient) ObjectRead(name string) (easyjson.JSON, error) {
	// Key shape includes ":v1" so a concurrent ObjectReadV2 on the
	// same id does not collapse into this call — the two endpoints
	// produce different response shapes.
	return doRead(cmdb.readFlight, "ObjectRead:v1:"+name, func() (any, error) {
		payload := easyjson.NewJSONObject()
		payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
		options := easyjson.NewJSONObject()
		options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
		om := sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.object.read", name, &payload, &options))
		return readResult{data: om.Data, err: OpErrorFromOpMsgStrict(om)}, nil
	})
}

func (cmdb CMDBSyncClient) ObjectReadV2(name string) (easyjson.JSON, error) {
	return doRead(cmdb.readFlight, "ObjectRead:v2:"+name, func() (any, error) {
		payload := easyjson.NewJSONObject()
		payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
		payload.SetByPath("details_v2", easyjson.NewJSON(true))
		options := easyjson.NewJSONObject()
		options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
		om := sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.object.read", name, &payload, &options))
		return readResult{data: om.Data, err: OpErrorFromOpMsgStrict(om)}, nil
	})
}

// ------------------------------------------------------------------------------------------------

func (cmdb CMDBSyncClient) TypesLinkCreate(from, to, objectLinkType string, tags []string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("body", easyjson.NewJSONObject())
	if len(body) > 0 {
		payload.SetByPath("body", body[0])
	}
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.NewJSON(tags))
	}
	payload.SetByPath("object_type", easyjson.NewJSON(objectLinkType))

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.types.link.create", SeqFree(from), &payload, &options)))
}

func (cmdb CMDBSyncClient) TypesLinkUpdate(from, to string, tags []string, body easyjson.JSON, replace bool, objectLinkType4Upsert ...string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	if len(objectLinkType4Upsert) > 0 {
		payload.SetByPath("upsert", easyjson.NewJSON(true))
		payload.SetByPath("object_type", easyjson.NewJSON(objectLinkType4Upsert[0]))
	}
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("body", body)
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.NewJSON(tags))
	}
	payload.SetByPath("replace", easyjson.NewJSON(replace))

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.types.link.update", SeqFree(from), &payload, &options)))
}

func (cmdb CMDBSyncClient) TypesLinkDelete(from, to string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	payload.SetByPath("to", easyjson.NewJSON(to))

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.types.link.delete", SeqFree(from), &payload, &options)))
}

func (cmdb CMDBSyncClient) TypesLinkRead(from, to string) (easyjson.JSON, error) {
	return doRead(cmdb.readFlight, "TypesLinkRead:"+from+"|"+to, func() (any, error) {
		payload := easyjson.NewJSONObject()
		payload.SetByPath("to", easyjson.NewJSON(to))

		options := easyjson.NewJSONObject()
		options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
		om := sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.types.link.read", SeqFree(from), &payload, &options))
		return readResult{data: om.Data, err: OpErrorFromOpMsgStrict(om)}, nil
	})
}

func (cmdb CMDBSyncClient) ObjectsLinkCreate(from, to, name string, tags []string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("name", easyjson.NewJSON(name))
	payload.SetByPath("body", easyjson.NewJSONObject())
	if len(body) > 0 {
		payload.SetByPath("body", body[0])
	}
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.NewJSON(tags))
	}

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.create", SeqFree(from), &payload, &options)))
}

func (cmdb CMDBSyncClient) ObjectsLinkUpdate(from, to string, tags []string, body easyjson.JSON, replace bool, name4Upsert ...string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	if len(name4Upsert) > 0 {
		payload.SetByPath("upsert", easyjson.NewJSON(true))
		payload.SetByPath("name", easyjson.NewJSON(name4Upsert[0]))
	}
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("body", body)
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.NewJSON(tags))
	}
	payload.SetByPath("replace", easyjson.NewJSON(replace))

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.update", SeqFree(from), &payload, &options)))
}

func (cmdb CMDBSyncClient) ObjectsLinkUpdateWithDetails(from, to string, tags []string, body easyjson.JSON, replace bool, name4Upsert ...string) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	if len(name4Upsert) > 0 {
		payload.SetByPath("upsert", easyjson.NewJSON(true))
		payload.SetByPath("name", easyjson.NewJSON(name4Upsert[0]))
	}
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("body", body)
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.NewJSON(tags))
	}
	payload.SetByPath("replace", easyjson.NewJSON(replace))

	options := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	msg := sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.update", SeqFree(from), &payload, &options))
	return msg.Data, OpErrorFromOpMsg(msg)
}

func (cmdb CMDBSyncClient) ObjectsLinkDelete(from, to string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	payload.SetByPath("to", easyjson.NewJSON(to))

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.delete", SeqFree(from), &payload, &options)))
}

func (cmdb CMDBSyncClient) ObjectsLinkDeleteWithDetails(from, to string) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	payload.SetByPath("to", easyjson.NewJSON(to))

	options := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	msg := sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.delete", SeqFree(from), &payload, &options))
	return msg.Data, OpErrorFromOpMsg(msg)
}

func (cmdb CMDBSyncClient) ObjectsLinkRead(from, to string) (easyjson.JSON, error) {
	return doRead(cmdb.readFlight, "ObjectsLinkRead:"+from+"|"+to, func() (any, error) {
		payload := easyjson.NewJSONObject()
		payload.SetByPath("to", easyjson.NewJSON(to))

		options := easyjson.NewJSONObject()
		options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
		om := sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.read", SeqFree(from), &payload, &options))
		return readResult{data: om.Data, err: OpErrorFromOpMsgStrict(om)}, nil
	})
}

// ------------------------------------------------------------------------------------------------
