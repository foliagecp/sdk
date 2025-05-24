package crud

import (
	"fmt"

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

func getLinkNameFromSpecifiedIdentifier(ctx *sfPlugins.StatefunContextProcessor) (string, bool) {
	selfID := getOriginalID(ctx.Self.ID)
	if linkName, ok := ctx.Payload.GetByPath("name").AsString(); ok {
		return linkName, true
	} else {
		if toVertexId, ok := ctx.Payload.GetByPath("to").AsString(); ok {
			toVertexId = ctx.Domain.CreateObjectIDWithThisDomain(toVertexId, false)
			if lt, ok := ctx.Payload.GetByPath("type").AsString(); ok {
				linkNameBytes, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, selfID, lt, toVertexId))
				if err == nil {
					return string(linkNameBytes), true
				}
			}
		}
	}
	return "", false
}

func indexRemoveVertexBody(ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	// Remove all indices -----------------------------
	indexKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(VertexBodyValueIndexPrefPattern+KeySuff1Pattern, selfID, ">"))
	for _, indexKey := range indexKeys {
		ctx.Domain.Cache().DeleteValue(indexKey, true, -1, "")
	}
	// ------------------------------------------------
}

func indexVertexBody(ctx *sfPlugins.StatefunContextProcessor, vertexBody easyjson.JSON, opTime int64, reindex bool) {
	selfID := getOriginalID(ctx.Self.ID)
	if reindex {
		indexRemoveVertexBody(ctx)
	}
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

func indexRemoveVertexLinkBody(ctx *sfPlugins.StatefunContextProcessor, linkName string) {
	selfID := getOriginalID(ctx.Self.ID)
	// Remove all indices -----------------------------
	indexKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(LinkBodyValueIndexPrefPattern+KeySuff2Pattern, selfID, linkName, ">"))
	for _, indexKey := range indexKeys {
		ctx.Domain.Cache().DeleteValue(indexKey, true, -1, "")
	}
	// ------------------------------------------------
}

func indexVertexLinkBody(ctx *sfPlugins.StatefunContextProcessor, linkName string, linkBody easyjson.JSON, opTime int64, reindex bool) {
	selfID := getOriginalID(ctx.Self.ID)
	if reindex {
		indexRemoveVertexLinkBody(ctx, linkName)
	}
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
