package crud

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

func getOpStackFromOptions(options *easyjson.JSON) *easyjson.JSON {
	returnOpStack := false
	if options != nil {
		returnOpStack = options.GetByPath("op_stack").AsBoolDefault(false)
	}
	var opStack *easyjson.JSON = nil
	if returnOpStack {
		opStack = easyjson.NewJSONArray().GetPtr()
	}
	return opStack
}

// getOpTimeFromPayloadIfExist return operation time from payload or current time if operation time does not exist
func getOpTimeFromPayloadIfExist(payload *easyjson.JSON) int64 {
	if payload != nil {
		if opTime := int64(payload.GetByPath("op_time").AsInt64Default(-1)); opTime > 0 {
			return opTime
		}
	}
	return system.GetCurrentTimeNs()
}

func addVertexOpToOpStack(opStack *easyjson.JSON, opName string, vertexId string, oldBody *easyjson.JSON, newBody *easyjson.JSON) bool {
	if opStack != nil && opStack.IsArray() {
		op := easyjson.NewJSONObjectWithKeyValue("op", easyjson.NewJSON(opName))
		op.SetByPath("id", easyjson.NewJSON(vertexId))
		if oldBody != nil {
			op.SetByPath("old_body", *oldBody)
		}
		if newBody != nil {
			op.SetByPath("new_body", *newBody)
		}
		opStack.AddToArray(op)
		return true
	}
	return false
}

func addLinkOpToOpStack(opStack *easyjson.JSON, opName string, fromVertexId string, toVertexId string, linkName string, linkType string, oldBody *easyjson.JSON, newBody *easyjson.JSON) bool {
	if opStack != nil && opStack.IsArray() {
		op := easyjson.NewJSONObjectWithKeyValue("op", easyjson.NewJSON(opName))
		op.SetByPath("from", easyjson.NewJSON(fromVertexId))
		op.SetByPath("to", easyjson.NewJSON(toVertexId))
		op.SetByPath("name", easyjson.NewJSON(linkName))
		op.SetByPath("type", easyjson.NewJSON(linkType))
		if oldBody != nil {
			op.SetByPath("old_body", *oldBody)
		}
		if newBody != nil {
			op.SetByPath("new_body", *newBody)
		}
		opStack.AddToArray(op)
		return true
	}
	return false
}

// rollbackOpStack walks an op_stack in reverse order and best-effort inverts
// each recorded LL operation by calling the matching LL counter-operation.
// Used by HL CRUD wrappers (e.g. CreateObject) to undo partial work when a
// multi-step pipeline fails mid-flight. Errors from inverse calls are
// intentionally swallowed — the goal is to bring the graph as close as
// possible to its pre-call state, not to add a second failure mode on top
// of the original one.
//
// Currently supported inverses:
//   functions.graph.api.vertex.create -> functions.graph.api.vertex.delete
//   functions.graph.api.link.create   -> functions.graph.api.link.delete
//
// Update/delete inverses are intentionally not handled here yet — only HL
// wrappers that emit the two ops above use this helper today. Extend the
// switch when more inverse pairs are needed.
func rollbackOpStack(ctx *sfPlugins.StatefunContextProcessor, opStack *easyjson.JSON) {
	if opStack == nil || !opStack.IsArray() {
		return
	}
	n := opStack.ArraySize()
	for i := n - 1; i >= 0; i-- {
		entry := opStack.ArrayElement(i)
		op := entry.GetByPath("op").AsStringDefault("")
		switch op {
		case "functions.graph.api.vertex.create":
			id := entry.GetByPath("id").AsStringDefault("")
			if id == "" {
				continue
			}
			payload := easyjson.NewJSONObject()
			_, _ = ctx.Request(sfPlugins.AutoRequestSelect,
				"functions.graph.api.vertex.delete",
				makeSequenceFreeParentBasedID(ctx, id),
				injectParentHoldsLocks(ctx, &payload), nil)
		case "functions.graph.api.link.create":
			from := entry.GetByPath("from").AsStringDefault("")
			name := entry.GetByPath("name").AsStringDefault("")
			if from == "" || name == "" {
				continue
			}
			payload := easyjson.NewJSONObjectWithKeyValue("name", easyjson.NewJSON(name))
			_, _ = ctx.Request(sfPlugins.AutoRequestSelect,
				"functions.graph.api.link.delete",
				makeSequenceFreeParentBasedID(ctx, from),
				injectParentHoldsLocks(ctx, &payload), nil)
		}
	}
}

func mergeOpStack(opStackRecepient *easyjson.JSON, opStackDonor *easyjson.JSON) bool {
	if opStackRecepient != nil && opStackRecepient.IsArray() && opStackDonor != nil && opStackDonor.IsArray() {
		for i := 0; i < opStackDonor.ArraySize(); i++ {
			opStackRecepient.AddToArray(opStackDonor.ArrayElement(i))
		}
	}
	return false
}

func resultWithOpStack(existingResult *easyjson.JSON, opStack *easyjson.JSON) easyjson.JSON {
	if existingResult == nil {
		if opStack == nil {
			return easyjson.NewJSONNull()
		}
		return easyjson.NewJSONObjectWithKeyValue("op_stack", *opStack)
	} else {
		if opStack == nil {
			return *existingResult
		}
		existingResult.SetByPath("op_stack", *opStack)
		return *existingResult
	}
}

func getFullLinkInfoFromSpecifiedIdentifier(ctx *sfPlugins.StatefunContextProcessor) (linkType, linkName, toId string, linkExists bool) {
	selfID := getOriginalID(ctx.Self.ID)
	if linkName, ok := ctx.Payload.GetByPath("name").AsString(); ok {
		linkName = ctx.Domain.GetObjectIDWithoutDomain(linkName)

		linkTargetBytes, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, selfID, linkName))
		if err != nil {
			return "", "", "", false
		}

		linkTargetStr := string(linkTargetBytes)
		linkTargetTokens := strings.Split(linkTargetStr, ".")
		linkType := linkTargetTokens[0]
		toId := linkTargetTokens[1]

		return ctx.Domain.GetObjectIDWithoutDomain(linkType), linkName, toId, true
	} else {
		if toVertexId, ok := ctx.Payload.GetByPath("to").AsString(); ok {
			toVertexId = ctx.Domain.CreateObjectIDWithThisDomain(toVertexId, false)
			if lt, ok := ctx.Payload.GetByPath("type").AsString(); ok {
				linkNameBytes, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, selfID, lt, toVertexId))
				if err == nil {
					return ctx.Domain.GetObjectIDWithoutDomain(lt), ctx.Domain.GetObjectIDWithoutDomain(string(linkNameBytes)), toVertexId, true
				}
			}
		}
	}
	return "", "", "", false
}

func indexRemoveVertexBody(ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)

	// Remove all indices -----------------------------
	indexKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(VertexBodyValueIndexPrefPattern+KeySuff1Pattern, selfID, ">"))
	for _, indexKey := range indexKeys {
		ctx.Domain.Cache().DeleteValue(indexKey, true, opTime, "")
	}
	// ------------------------------------------------
}

// indexVertexBody builds the body-value index for a freshly created vertex
// (the create path, where there is no previous body to diff against).
// Updates use reindexVertexBody for an incremental diff instead.
func indexVertexBody(ctx *sfPlugins.StatefunContextProcessor, vertexBody easyjson.JSON, opTime int64) {
	selfID := getOriginalID(ctx.Self.ID)
	// Index body keys ------------------------------------
	for _, bodyKey := range vertexBody.ObjectKeys() {
		value := vertexBody.GetByPath(bodyKey)
		bytesVal := []byte{}

		typeStr := ""
		if value.IsBool() {
			typeStr = "b"
			bytesVal = system.BoolToBytes(value.AsBoolDefault(false))
		}
		if value.IsNumeric() {
			typeStr = "n"
			bytesVal = system.Float64ToBytes(value.AsNumericDefault(0))
		}
		if value.IsString() {
			typeStr = "s"
			bytesVal = []byte(value.AsStringDefault(""))
		}

		if len(bytesVal) > 0 {
			ctx.Domain.Cache().SetValue(fmt.Sprintf(VertexBodyValueIndexPrefPattern+KeySuff2Pattern, selfID, typeStr, bodyKey), bytesVal, true, opTime, "")
		}
	}
	// ----------------------------------------------------
}

// indexableScalar reports whether a JSON value participates in the body-value
// secondary index, returning its type token ("b"/"n"/"s") and encoded bytes.
// Mirrors the inline logic in indexVertexBody / indexVertexLinkBody: only
// non-empty scalars are indexed; objects, arrays, null and empty strings are
// not. (BoolToBytes always yields 1 byte and Float64ToBytes 8 bytes, so bool
// and numeric are always indexed; only the empty string is excluded.)
func indexableScalar(value easyjson.JSON) (typeStr string, bytesVal []byte, ok bool) {
	switch {
	case value.IsBool():
		return "b", system.BoolToBytes(value.AsBoolDefault(false)), true
	case value.IsNumeric():
		return "n", system.Float64ToBytes(value.AsNumericDefault(0)), true
	case value.IsString():
		b := []byte(value.AsStringDefault(""))
		if len(b) == 0 {
			return "", nil, false
		}
		return "s", b, true
	}
	return "", nil, false
}

// reindexVertexBody updates the vertex body-value index INCREMENTALLY by
// diffing the previous body against the new one. Compared to the old
// reindex path (indexVertexBody with reindex=true), this avoids the
// GetKeysByPattern subtree scan entirely and only writes index keys that
// actually changed:
//
//   - field unchanged (same type + bytes) → no KV write, no WAL op
//   - field added / value changed         → SetValue
//   - field type changed (e.g. n→s)       → DeleteValue(old type key) + SetValue
//   - field removed or became non-scalar  → DeleteValue
//
// Correctness note: this trusts that the existing index reflects oldBody,
// which holds because the index is only ever mutated through these helpers
// together with the body write. JPGQL's index lookups fall back to the body
// when an index key is MISSING, but a STALE key would yield a wrong answer —
// hence deletions (removed fields and type changes) are handled explicitly.
func reindexVertexBody(ctx *sfPlugins.StatefunContextProcessor, oldBody, newBody *easyjson.JSON, opTime int64) {
	selfID := getOriginalID(ctx.Self.ID)

	newScalarKeys := map[string]struct{}{}
	for _, bodyKey := range newBody.ObjectKeys() {
		typeStr, bytesVal, ok := indexableScalar(newBody.GetByPath(bodyKey))
		if !ok {
			continue
		}
		newScalarKeys[bodyKey] = struct{}{}

		oTypeStr, oBytes, oOk := indexableScalar(oldBody.GetByPath(bodyKey))
		if oOk && oTypeStr == typeStr && bytes.Equal(oBytes, bytesVal) {
			continue // unchanged → skip the write entirely
		}
		if oOk && oTypeStr != typeStr {
			// Type changed: the old value lived under a different type token,
			// so its index key differs and must be removed to avoid a stale entry.
			ctx.Domain.Cache().DeleteValue(fmt.Sprintf(VertexBodyValueIndexPrefPattern+KeySuff2Pattern, selfID, oTypeStr, bodyKey), true, opTime, "")
		}
		ctx.Domain.Cache().SetValue(fmt.Sprintf(VertexBodyValueIndexPrefPattern+KeySuff2Pattern, selfID, typeStr, bodyKey), bytesVal, true, opTime, "")
	}

	// Remove index keys for fields that disappeared or stopped being scalar.
	for _, bodyKey := range oldBody.ObjectKeys() {
		oTypeStr, _, oOk := indexableScalar(oldBody.GetByPath(bodyKey))
		if !oOk {
			continue
		}
		if _, stillScalar := newScalarKeys[bodyKey]; stillScalar {
			continue
		}
		ctx.Domain.Cache().DeleteValue(fmt.Sprintf(VertexBodyValueIndexPrefPattern+KeySuff2Pattern, selfID, oTypeStr, bodyKey), true, opTime, "")
	}
}

// reindexVertexLinkBody is the link-body counterpart of reindexVertexBody:
// same incremental diff strategy against the link's previous body, avoiding
// the GetKeysByPattern scan.
func reindexVertexLinkBody(ctx *sfPlugins.StatefunContextProcessor, linkName string, oldBody, newBody *easyjson.JSON, opTime int64) {
	selfID := getOriginalID(ctx.Self.ID)

	newScalarKeys := map[string]struct{}{}
	for _, bodyKey := range newBody.ObjectKeys() {
		typeStr, bytesVal, ok := indexableScalar(newBody.GetByPath(bodyKey))
		if !ok {
			continue
		}
		newScalarKeys[bodyKey] = struct{}{}

		oTypeStr, oBytes, oOk := indexableScalar(oldBody.GetByPath(bodyKey))
		if oOk && oTypeStr == typeStr && bytes.Equal(oBytes, bytesVal) {
			continue
		}
		if oOk && oTypeStr != typeStr {
			ctx.Domain.Cache().DeleteValue(fmt.Sprintf(LinkBodyValueIndexPrefPattern+KeySuff3Pattern, selfID, linkName, oTypeStr, bodyKey), true, opTime, "")
		}
		ctx.Domain.Cache().SetValue(fmt.Sprintf(LinkBodyValueIndexPrefPattern+KeySuff3Pattern, selfID, linkName, typeStr, bodyKey), bytesVal, true, opTime, "")
	}

	for _, bodyKey := range oldBody.ObjectKeys() {
		oTypeStr, _, oOk := indexableScalar(oldBody.GetByPath(bodyKey))
		if !oOk {
			continue
		}
		if _, stillScalar := newScalarKeys[bodyKey]; stillScalar {
			continue
		}
		ctx.Domain.Cache().DeleteValue(fmt.Sprintf(LinkBodyValueIndexPrefPattern+KeySuff3Pattern, selfID, linkName, oTypeStr, bodyKey), true, opTime, "")
	}
}

func indexRemoveVertexLinkBody(ctx *sfPlugins.StatefunContextProcessor, linkName string) {
	selfID := getOriginalID(ctx.Self.ID)
	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)

	// Remove all indices -----------------------------
	indexKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(LinkBodyValueIndexPrefPattern+KeySuff2Pattern, selfID, linkName, ">"))
	for _, indexKey := range indexKeys {
		ctx.Domain.Cache().DeleteValue(indexKey, true, opTime, "")
	}
	// ------------------------------------------------
}

// indexVertexLinkBody builds the body-value index for a freshly created link.
// Updates use reindexVertexLinkBody for an incremental diff instead.
func indexVertexLinkBody(ctx *sfPlugins.StatefunContextProcessor, linkName string, linkBody easyjson.JSON, opTime int64) {
	selfID := getOriginalID(ctx.Self.ID)
	// Index body keys ------------------------------------
	for _, bodyKey := range linkBody.ObjectKeys() {
		value := linkBody.GetByPath(bodyKey)
		bytesVal := []byte{}

		typeStr := ""
		if value.IsBool() {
			typeStr = "b"
			bytesVal = system.BoolToBytes(value.AsBoolDefault(false))
		}
		if value.IsNumeric() {
			typeStr = "n"
			bytesVal = system.Float64ToBytes(value.AsNumericDefault(0))
		}
		if value.IsString() {
			typeStr = "s"
			bytesVal = []byte(value.AsStringDefault(""))
		}

		if len(bytesVal) > 0 {
			ctx.Domain.Cache().SetValue(fmt.Sprintf(LinkBodyValueIndexPrefPattern+KeySuff3Pattern, selfID, linkName, typeStr, bodyKey), bytesVal, true, opTime, "")
		}
	}
	// ----------------------------------------------------
}
