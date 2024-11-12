package crud

import "github.com/foliagecp/easyjson"

func getOperationStackFromOptions(options *easyjson.JSON) *easyjson.JSON {
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

func addVertexOperationToOpStack(opStack *easyjson.JSON, opType string, vertexId string, oldBody *easyjson.JSON, newBody *easyjson.JSON) bool {
	if opStack != nil && opStack.IsArray() {
		op := easyjson.NewJSONObject()
		op.SetByPath("operation.target", easyjson.NewJSON("vertex"))
		op.SetByPath("operation.type", easyjson.NewJSON(opType))
		op.SetByPath("id", easyjson.NewJSON(vertexId))
		if oldBody != nil {
			op.SetByPath("body.old", *oldBody)
		}
		if newBody != nil {
			op.SetByPath("body.new", *newBody)
		}
		opStack.AddToArray(op)
		return true
	}
	return false
}

func addVertexLinkOperationToOpStack(opStack *easyjson.JSON, opType string, fromVertexId string, toVertexId string, linkName string, linkType string, oldBody *easyjson.JSON, newBody *easyjson.JSON) bool {
	if opStack != nil && opStack.IsArray() {
		op := easyjson.NewJSONObject()
		op.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
		op.SetByPath("operation.type", easyjson.NewJSON(opType))

		op.SetByPath("link.from", easyjson.NewJSON(fromVertexId))
		op.SetByPath("link.to", easyjson.NewJSON(toVertexId))
		op.SetByPath("link.type", easyjson.NewJSON(linkType))
		op.SetByPath("name", easyjson.NewJSON(linkName))
		if oldBody != nil {
			op.SetByPath("body.old", *oldBody)
		}
		if newBody != nil {
			op.SetByPath("body.new", *newBody)
		}
		opStack.AddToArray(op)
		return true
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
