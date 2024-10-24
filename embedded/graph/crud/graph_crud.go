package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

/*
GraphCUDGateway. Garanties sequential order for all graph api calls
This function works via signals and request-reply.

Request:

	payload: json - optional
		target: string - requred // supported values (case insensitive): "vertex", "link"
		operation: string - requred // supported values (case insensitive): "create", "update", "delete"
		data: json - required // operation data

	options: json - optional
		return_stack: bool - optional

Reply:

	payload: json
		status: string
		details: string - optional, if any exists
		data: json - optional, id any exists
			target: string - required // "vertex", "link"
			operation: string - required // "create", "update", "delete"
			op_stack: json array - optional
*/
func GraphCUDGateway(sfExec sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	meta := om.GetMeta(ctx)
	target := meta.GetByPath("target").AsStringDefault("")
	operation := meta.GetByPath("operation").AsStringDefault("")
	if len(target) == 0 {
		target = ctx.Payload.GetByPath("target").AsStringDefault("")
		operation = ctx.Payload.GetByPath("operation").AsStringDefault("")
		meta := easyjson.NewJSONObjectWithKeyValue("target", easyjson.NewJSON(target))
		meta.SetByPath("operation", easyjson.NewJSON(operation))
		om.SetMeta(ctx, meta)
	}
	switch strings.ToLower(target) {
	case "vertex":
		GraphVertexCUD(sfExec, ctx, om, operation)
	case "link":
		GraphLinkCUD(sfExec, ctx, om, operation)
	default:

	}
}

// Prevents execution of CRUD's block of intructions (when there is younger operation that already changed the same block) to remain data consistency
func FixateOperationIdTime(ctx *sfPlugins.StatefunContextProcessor, id string, opTime int64) bool {
	funcContext := ctx.GetFunctionContext()
	path := fmt.Sprintf("op.%s-%s-%s", system.GetHashStr(ctx.Self.Typename), ctx.Self.ID, id)
	alreadyFixatedTime := system.Str2Int(funcContext.GetByPath(path).AsStringDefault(""))
	if alreadyFixatedTime > opTime {
		return false
	}
	funcContext.SetByPath(path, easyjson.NewJSON(system.IntToStr(opTime)))
	ctx.SetFunctionContext(funcContext)
	return true
}

func GraphVertexRead(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)
	opStack := getOpStackFromOptions(ctx.Options)

	body, _, err := ctx.Domain.Cache().GetValueWithRecordTimeAsJSON(ctx.Self.ID)
	if err != nil { // If vertex does not exist
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("vertex with id=%s does not exist", ctx.Self.ID))).Reply()
		return
	}

	result := easyjson.NewJSONObjectWithKeyValue("body", *body)
	if ctx.Payload.GetByPath("details").AsBoolDefault(false) {
		outLinkNames := []string{}
		outLinkTypes := []string{}
		outLinkIds := []string{}
		outLinkKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkTargetKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, ">"))
		for _, outLinkKey := range outLinkKeys {
			linkKeyTokens := strings.Split(outLinkKey, ".")
			linkName := linkKeyTokens[len(linkKeyTokens)-1]
			outLinkNames = append(outLinkNames, linkName)

			linkTargetBytes, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
			if err == nil {
				tokens := strings.Split(string(linkTargetBytes), ".")
				if len(tokens) == 2 {
					outLinkTypes = append(outLinkTypes, tokens[0])
					outLinkIds = append(outLinkIds, tokens[1])
				}
			}
		}
		result.SetByPath("links.out.names", easyjson.JSONFromArray(outLinkNames))
		result.SetByPath("links.out.types", easyjson.JSONFromArray(outLinkTypes))
		result.SetByPath("links.out.ids", easyjson.JSONFromArray(outLinkIds))

		inLinkKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(InLinkKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, ">"))
		inLinks := easyjson.NewJSONArray()
		for _, inLinkKey := range inLinkKeys {
			linkKeyTokens := strings.Split(inLinkKey, ".")
			linkName := linkKeyTokens[len(linkKeyTokens)-1]
			linkFromVId := linkKeyTokens[len(linkKeyTokens)-2]
			inLinkJson := easyjson.NewJSONObjectWithKeyValue("from", easyjson.NewJSON(linkFromVId))
			inLinkJson.SetByPath("name", easyjson.NewJSON(linkName))
			inLinks.AddToArray(inLinkJson)
		}
		result.SetByPath("links.in", inLinks)
	}
	addVertexOpToOpStack(opStack, "read", ctx.Self.ID, nil, nil)

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(result.GetPtr(), opStack))).Reply()
}

func GraphLinkRead(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)
	opStack := getOpStackFromOptions(ctx.Options)

	var linkName string
	var linkTarget string
	var linkType string
	if lname, ltype, ltarget, err := getLinkNameTypeTargetFromVariousIdentifiers(ctx, ""); err == nil {
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

	if ctx.Payload.GetByPath("details").AsBoolDefault(false) {
		result.SetByPath("name", easyjson.NewJSON(linkName))
		result.SetByPath("type", easyjson.NewJSON(linkType))
		result.SetByPath("from", easyjson.NewJSON(ctx.Self.ID))
		result.SetByPath("to", easyjson.NewJSON(linkTarget))

		tags := []string{}
		tagKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff3Pattern, ctx.Self.ID, linkName, "tag", ">"))
		for _, tagKey := range tagKeys {
			tagKeyTokens := strings.Split(tagKey, ".")
			tags = append(tags, tagKeyTokens[len(tagKeyTokens)-1])
		}
		result.SetByPath("tags", easyjson.JSONFromArray(tags))
	}

	addLinkOpToOpStack(opStack, ctx.Self.Typename, ctx.Self.ID, linkTarget, linkName, linkType, nil, nil)

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(result.GetPtr(), opStack))).Reply()
}
