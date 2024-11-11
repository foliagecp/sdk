package crud

import (
	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
)

const (
	// crud_op.<op_target>.<op_id>
	opRegisterPrefixTemplate = "crud_op.%s.%s"
	// crud_op.<op_target>.<op_id>.<op_type>.<op_time>
	opRegisterTemplate = opRegisterPrefixTemplate + ".%s.%d"
)

var (
	CRUDValidTypes = map[string]struct{}{
		"create": {},
		"update": {},
		"delete": {},
		"read":   {},
	}
)

func unifiedCRUDDataAggregator(om *sfMediators.OpMediator) easyjson.JSON {
	aggregatedData := easyjson.NewJSONNull()
	for _, opMsg := range om.GetAggregatedOpMsgs() {
		if opMsg.Data.IsNonEmptyObject() {
			if aggregatedData.IsNull() {
				aggregatedData = opMsg.Data.Clone()
			} else {
				aggregatedData.DeepMerge(opMsg.Data)
			}
		}
	}
	return aggregatedData
}
