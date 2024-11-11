package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

func GraphVertexLinkRead(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	opStack := getOperationStackFromOptions(ctx.Options)

	var linkName string
	var linkTarget string
	var linkType string
	if lname, ltype, ltarget, err := getVertexLinkNameTypeTargetFromVariousIdentifiers(ctx, data); err == nil {
		linkName = lname
		linkType = ltype
		linkTarget = ltarget
	} else {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
		return
	}
	if !validLinkName.MatchString(linkName) {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("invalid link name")).Reply()
		return
	}

	linkBody, err1 := ctx.Domain.Cache().GetValueAsJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
	if err1 != nil { // Link's body does not exist
		linkBody = easyjson.NewJSONObject().GetPtr()
	}

	result := easyjson.NewJSONObjectWithKeyValue("body", *linkBody)

	if data.GetByPath("details").AsBoolDefault(false) {
		result.SetByPath("name", easyjson.NewJSON(linkName))
		result.SetByPath("type", easyjson.NewJSON(linkType))
		result.SetByPath("vertex.from", easyjson.NewJSON(ctx.Self.ID))
		result.SetByPath("vertex.to", easyjson.NewJSON(linkTarget))

		tags := []string{}
		tagKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff3Pattern, ctx.Self.ID, linkName, "tag", ">"))
		for _, tagKey := range tagKeys {
			tagKeyTokens := strings.Split(tagKey, ".")
			tags = append(tags, tagKeyTokens[len(tagKeyTokens)-1])
		}
		result.SetByPath("tags", easyjson.JSONFromArray(tags))
	}

	addVertexLinkOperationToOpStack(opStack, ctx.Self.Typename, ctx.Self.ID, linkTarget, linkName, linkType, nil, nil)

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(result.GetPtr(), opStack))).Reply()
}

func GraphVertexLinkCreateFromVertex(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	opStack := getOperationStackFromOptions(ctx.Options)

	var linkBody easyjson.JSON
	if data.GetByPath("body").IsObject() {
		linkBody = data.GetByPath("body")
	} else {
		linkBody = easyjson.NewJSONObject()
	}

	var toId string
	if s, ok := data.GetByPath("to").AsString(); ok && len(s) > 0 {
		toId = s
	} else {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("to is not defined")).Reply()
		return
	}
	toId = ctx.Domain.CreateObjectIDWithThisDomain(toId, false)

	var linkName string
	if s, ok := data.GetByPath("name").AsString(); ok && len(s) > 0 {
		linkName = s
		if !validLinkName.MatchString(linkName) {
			om.AggregateOpMsg(sfMediators.OpMsgFailed("invalid link name")).Reply()
			return
		}
	} else {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("name is not defined")).Reply()
		return
	}
	var linkType string
	if s, ok := data.GetByPath("type").AsString(); ok && len(s) > 0 {
		linkType = s
	} else {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("type is not defined")).Reply()
		return
	}

	if !FixateOperationIdTime(ctx, fmt.Sprintf("link-%s", linkName), opTime) {
		om.AggregateOpMsg(sfMediators.OpMsgIdle("cannot be completed without losing consistency")).Reply()
		return
	}

	// Check if link with this name already exists --------------
	_, recordTime, err := ctx.Domain.Cache().GetValueWithRecordTime(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
	if opTime != recordTime && err == nil { // Only our time makes us go further (operation was not completed previously)
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link from=%s with name=%s already exists", ctx.Self.ID, linkName))).Reply()
		return
	}
	// ----------------------------------------------------------
	// Check if link with this type "type" to "to" already exists
	_, recordTime, err = ctx.Domain.Cache().GetValueWithRecordTime(fmt.Sprintf(OutLinkTypeKeyPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkType, toId))
	if opTime != recordTime && err == nil {
		om.AggregateOpMsg(
			sfMediators.OpMsgFailed(
				fmt.Sprintf("link from=%s with name=%s to=%s with type=%s already exists, two vertices can have a link with this type and direction only once", ctx.Self.ID, linkName, toId, linkType),
			),
		).Reply()
		return
	}
	// -----------------------------------------------------------

	// Create out link on this vertex -------------------------
	// Set link target ------------------
	ctx.Domain.Cache().SetValueKVSync(fmt.Sprintf(OutLinkTargetKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName), []byte(fmt.Sprintf("%s.%s", linkType, toId)), opTime) // Store link body in KV
	// ----------------------------------
	// Set link body --------------------
	ctx.Domain.Cache().SetValueKVSync(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName), linkBody.ToBytes(), opTime) // Store link body in KV
	// ----------------------------------
	// Set link type --------------------
	ctx.Domain.Cache().SetValueKVSync(fmt.Sprintf(OutLinkTypeKeyPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkType, toId), []byte(linkName), opTime) // Store link type
	// ----------------------------------
	// Index link type ------------------
	ctx.Domain.Cache().SetValueKVSync(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff3Pattern, ctx.Self.ID, linkName, "type", linkType), nil, opTime)
	// ----------------------------------
	// Index link tags ------------------
	if data.GetByPath("tags").IsNonEmptyArray() {
		if linkTags, ok := data.GetByPath("tags").AsArrayString(); ok {
			for _, linkTag := range linkTags {
				ctx.Domain.Cache().SetValueKVSync(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff3Pattern, ctx.Self.ID, linkName, "tag", linkTag), nil, opTime)
			}
		}
	}
	// ----------------------------------
	// --------------------------------------------------------

	// Create this vertex if does not exist ----------------------
	_, err = ctx.Domain.Cache().GetValue(ctx.Self.ID)
	if err != nil {
		createVertexPayload := easyjson.NewJSONObject()
		createVertexPayload.SetByPath("operation.type", easyjson.NewJSON("create"))
		createVertexPayload.SetByPath("operation.target", easyjson.NewJSON("vertex"))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, ctx.Caller.Typename, ctx.Self.ID, &createVertexPayload, ctx.Options)
	}
	// -----------------------------------------------------------
	// Create in link on descendant vertex --------------------
	inLinkPayload := easyjson.NewJSONObject()
	inLinkPayload.SetByPath("operation.type", easyjson.NewJSON("create"))
	inLinkPayload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
	inLinkPayload.SetByPath("data.in_name", easyjson.NewJSON(linkName))
	inLinkPayload.SetByPath("data.in_type", easyjson.NewJSON(linkType))
	om.SignalWithAggregation(sfPlugins.AutoSignalSelect, ctx.Self.Typename, toId, &inLinkPayload, ctx.Options)
	// --------------------------------------------------------

	addVertexLinkOperationToOpStack(opStack, "create", ctx.Self.ID, toId, linkName, linkType, nil, &linkBody)
	om.AddIntermediateResult(ctx, resultWithOpStack(nil, opStack).GetPtr())
}

func GraphVertexLinkCreateToVertex(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, linkFromVertexUUID, inLinkType string, inLinkName string, opTime int64, data *easyjson.JSON) {
	if !FixateOperationIdTime(ctx, fmt.Sprintf("inlink-%s", inLinkName), opTime) {
		om.AggregateOpMsg(sfMediators.OpMsgIdle("cannot be completed without losing consistency")).Reply()
		return
	}

	ctx.Domain.Cache().SetValueKVSync(fmt.Sprintf(InLinkKeyPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkFromVertexUUID, inLinkName), []byte(inLinkType), opTime)

	// Create this vertex if does not exist ----------------------
	_, err := ctx.Domain.Cache().GetValue(ctx.Self.ID)
	if err != nil {
		createVertexPayload := easyjson.NewJSONObject()
		createVertexPayload.SetByPath("operation.type", easyjson.NewJSON("create"))
		createVertexPayload.SetByPath("operation.target", easyjson.NewJSON("vertex"))
		om.SignalWithAggregation(sfPlugins.AutoSignalSelect, "functions.graph.api.crud", ctx.Self.ID, &createVertexPayload, ctx.Options)
		return
	}
	// -----------------------------------------------------------

	om.AggregateOpMsg(sfMediators.OpMsgOk(easyjson.NewJSONNull())).Reply()
}

func GraphVertexLinkUpdate(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	opStack := getOperationStackFromOptions(ctx.Options)

	upsert := data.GetByPath("upsert").AsBoolDefault(false)
	replace := data.GetByPath("replace").AsBoolDefault(false)

	var linkName string
	var linkTarget string
	var linkType string
	if lname, ltype, ltarget, err := getVertexLinkNameTypeTargetFromVariousIdentifiers(ctx, data); err == nil {
		linkName = lname
		linkType = ltype
		linkTarget = ltarget
	} else {
		if upsert {
			createLinkPayload := easyjson.NewJSONObject()
			createLinkPayload.SetByPath("operation.type", easyjson.NewJSON("create"))
			createLinkPayload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
			createLinkPayload.SetByPath("data.body", data.GetByPath("body"))
			createLinkPayload.SetByPath("data.to", data.GetByPath("to"))
			createLinkPayload.SetByPath("data.name", data.GetByPath("name"))
			createLinkPayload.SetByPath("data.type", data.GetByPath("type"))
			om.SignalWithAggregation(sfPlugins.AutoSignalSelect, ctx.Self.Typename, ctx.Self.ID, &createLinkPayload, ctx.Options)
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgIdle(err.Error())).Reply()
		}
		return
	}
	if !validLinkName.MatchString(linkName) {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("invalid link name")).Reply()
		return
	}
	if !FixateOperationIdTime(ctx, fmt.Sprintf("link-%s", linkName), opTime) {
		om.AggregateOpMsg(sfMediators.OpMsgIdle("cannot be completed without losing consistency")).Reply()
		return
	}

	oldLinkBody, err1 := ctx.Domain.Cache().GetValueAsJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
	if err1 != nil { // Link's body does not exist
		oldLinkBody = easyjson.NewJSONObject().GetPtr()
	}

	var linkBody easyjson.JSON
	if data.GetByPath("body").IsObject() {
		linkBody = data.GetByPath("body")
	} else {
		linkBody = easyjson.NewJSONObject()
	}

	if replace {
		// Remove all indices -----------------------------
		indexKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkName, ">"))
		for _, indexKey := range indexKeys {
			ctx.Domain.Cache().DeleteValueKVSync(indexKey, -1)
		}
		// ------------------------------------------------
	} else { // merge
		newBody := oldLinkBody.Clone().GetPtr()
		newBody.DeepMerge(linkBody)
		linkBody = *newBody
	}

	// Create out link on this vertex -------------------------
	// Set link body --------------------
	ctx.Domain.Cache().SetValueKVSync(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName), linkBody.ToBytes(), opTime) // Store link body in KV
	// ----------------------------------
	// Index link type ------------------
	ctx.Domain.Cache().SetValueKVSync(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff3Pattern, ctx.Self.ID, linkName, "type", linkType), nil, opTime)
	// ----------------------------------
	// Index link tags ------------------
	if data.GetByPath("tags").IsNonEmptyArray() {
		if linkTags, ok := data.GetByPath("tags").AsArrayString(); ok {
			for _, linkTag := range linkTags {
				ctx.Domain.Cache().SetValueKVSync(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff3Pattern, ctx.Self.ID, linkName, "tag", linkTag), nil, opTime)
			}
		}
	}
	// ----------------------------------
	// --------------------------------------------------------
	addVertexLinkOperationToOpStack(opStack, "update", ctx.Self.ID, linkTarget, linkName, linkType, oldLinkBody, &linkBody)

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(nil, opStack))).Reply()
}

func GraphVertexLinkDeleteFromVertex(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, opTime int64, data *easyjson.JSON) {
	opStack := getOperationStackFromOptions(ctx.Options)

	var linkName string
	var linkTarget string
	var linkType string
	if lname, ltype, ltarget, err := getVertexLinkNameTypeTargetFromVariousIdentifiers(ctx, data); err == nil {
		linkName = lname
		linkType = ltype
		linkTarget = ltarget
	} else {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
		return
	}
	if !validLinkName.MatchString(linkName) {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("invalid link name")).Reply()
		return
	}
	if !FixateOperationIdTime(ctx, fmt.Sprintf("link-%s", linkName), opTime) {
		om.AggregateOpMsg(sfMediators.OpMsgIdle("cannot be completed without losing consistency")).Reply()
		return
	}

	oldLinkBody, err1 := ctx.Domain.Cache().GetValueAsJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
	if err1 != nil { // Link's body does not exist
		oldLinkBody = easyjson.NewJSONObject().GetPtr()
	}

	// Remove all indices -----------------------------
	indexKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkName, ">"))
	for _, indexKey := range indexKeys {
		ctx.Domain.Cache().DeleteValueKVSync(indexKey, -1)
	}
	// ------------------------------------------------

	// Delete link type -----------------
	ctx.Domain.Cache().DeleteValueKVSync(fmt.Sprintf(OutLinkTypeKeyPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkType, linkTarget), -1)
	// ----------------------------------
	// Delete link body -----------------
	ctx.Domain.Cache().DeleteValueKVSync(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName), -1)
	// ----------------------------------
	// Delete link target ---------------
	ctx.Domain.Cache().DeleteValueKVSync(fmt.Sprintf(OutLinkTargetKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName), -1)
	// ----------------------------------

	addVertexLinkOperationToOpStack(opStack, "delete", ctx.Self.ID, linkTarget, linkName, linkType, oldLinkBody, nil)

	// Delete in link on descendant vertex --------------------
	inLinkPayload := easyjson.NewJSONObject()
	inLinkPayload.SetByPath("operation.type", easyjson.NewJSON("delete"))
	inLinkPayload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
	inLinkPayload.SetByPath("data.in_name", easyjson.NewJSON(linkName))
	om.SignalWithAggregation(sfPlugins.AutoSignalSelect, ctx.Self.Typename, linkTarget, &inLinkPayload, ctx.Options)
	// --------------------------------------------------------

	om.AddIntermediateResult(ctx, resultWithOpStack(nil, opStack).GetPtr())
}

func GraphVertexLinkDeleteToVertex(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, linkFromVertexUUID, inLinkName string, opTime int64, data *easyjson.JSON) {
	if !FixateOperationIdTime(ctx, fmt.Sprintf("inlink-%s", inLinkName), opTime) {
		om.AggregateOpMsg(sfMediators.OpMsgIdle("cannot be completed without losing consistency")).Reply()
		return
	}
	ctx.Domain.Cache().DeleteValueKVSync(fmt.Sprintf(InLinkKeyPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkFromVertexUUID, inLinkName), -1)
	om.AggregateOpMsg(sfMediators.OpMsgOk(easyjson.NewJSONNull())).Reply()
}

func GraphVertexLinkCRUD_Dispatcher(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, operation string, opTime int64, data *easyjson.JSON) {
	switch operation {
	case "create":
		inType := data.GetByPath("in_type").AsStringDefault("")
		inName := data.GetByPath("in_name").AsStringDefault("")
		if len(ctx.Caller.ID) > 0 && len(inName) > 0 {
			GraphVertexLinkCreateToVertex(ctx, om, ctx.Caller.ID, inType, inName, opTime, data)
		} else {
			GraphVertexLinkCreateFromVertex(ctx, om, opTime, data)
		}
	case "update":
		GraphVertexLinkUpdate(ctx, om, opTime, data)
	case "delete":
		inName := data.GetByPath("in_name").AsStringDefault("")
		if len(ctx.Caller.ID) > 0 && len(inName) > 0 {
			GraphVertexLinkDeleteToVertex(ctx, om, ctx.Caller.ID, inName, opTime, data)
		} else {
			GraphVertexLinkDeleteFromVertex(ctx, om, opTime, data)
		}
	case "read":
		GraphVertexLinkRead(ctx, om, opTime, data)
	}
}
