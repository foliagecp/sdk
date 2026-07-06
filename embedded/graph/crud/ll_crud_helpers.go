package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	lg "github.com/foliagecp/sdk/statefun/logger"
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
//
//	functions.graph.api.vertex.create -> functions.graph.api.vertex.delete
//	functions.graph.api.link.create   -> functions.graph.api.link.delete
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

// getFullLinkInfoFromSpecifiedIdentifier resolves the out-link a payload
// addresses, trying BOTH identities a link has:
//
//  1. by "name" via the out.to.<name> index;
//  2. by "type"+"to" via the from.ltype.<type>.<to> index — a link's semantic
//     identity for the uniqueness invariant (at most one link of a type per
//     direction).
//
// A name miss falls through to (2) when type+to are present: the payload may
// address an existing (type, to) edge under a different name, and reporting it
// absent would send an upsert into link.create only to be rejected by the very
// index this fallback consults. The type is normalized with
// GetObjectIDWithoutDomain exactly like LLAPILinkCreate normalizes it before
// writing the ltype index — a raw domain-prefixed type can never match a key
// that was written stripped.
func getFullLinkInfoFromSpecifiedIdentifier(ctx *sfPlugins.StatefunContextProcessor) (linkType, linkName, toId string, linkExists bool) {
	selfID := getOriginalID(ctx.Self.ID)
	requestedName := "" // set to the payload "name" ONLY when it named a link that does not exist (name miss)
	if name, ok := ctx.Payload.GetByPath("name").AsString(); ok {
		name = ctx.Domain.GetObjectIDWithoutDomain(name)

		linkTargetBytes, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, selfID, name))
		if err == nil {
			linkTargetStr := string(linkTargetBytes)
			linkTargetTokens := strings.Split(linkTargetStr, ".")
			linkType := linkTargetTokens[0]
			toId := linkTargetTokens[1]

			return ctx.Domain.GetObjectIDWithoutDomain(linkType), name, toId, true
		}
		requestedName = name
		// name miss — fall through to the type+to identity
	}
	if toVertexId, ok := ctx.Payload.GetByPath("to").AsString(); ok {
		toVertexId = ctx.Domain.CreateObjectIDWithThisDomain(toVertexId, false)
		if lt, ok := ctx.Payload.GetByPath("type").AsString(); ok {
			lt = ctx.Domain.GetObjectIDWithoutDomain(lt)
			linkNameBytes, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, selfID, lt, toVertexId))
			if err == nil {
				actualName := ctx.Domain.GetObjectIDWithoutDomain(string(linkNameBytes))
				// The caller named a link that does not exist, yet its (type, to)
				// resolves an existing edge under a DIFFERENT name. A link name is
				// its identifier and cannot be renamed via update — the mismatched
				// name is ignored and the edge is updated under its real name. Warn
				// loudly: the caller's notion of this link's name is broken.
				if requestedName != "" && requestedName != actualName {
					lg.Logf(lg.WarnLevel,
						"getFullLinkInfoFromSpecifiedIdentifier: broken link name on vertex %s — addressed with name=%q which does not exist, but edge (type=%s, to=%s) actually exists under name=%q; a link name is an identifier and cannot be renamed via update, so resolving to %q and ignoring the requested name (use delete+create to change a link name)",
						selfID, requestedName, lt, toVertexId, actualName, actualName)
				}
				return lt, actualName, toVertexId, true
			}
		}
	}
	return "", "", "", false
}
