package crud

import (
	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
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
