

// Foliage graph store common package.
// Provides shared functions for other graph store packages
package common

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"

	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	sfSystem "github.com/foliagecp/sdk/statefun/system"
)

type SyncOpStatus = int

const (
	SYNC_OP_STATUS_OK SyncOpStatus = iota
	SYNC_OP_STATUS_IDLE
	SYNC_OP_STATUS_INCOMPLETE
	SYNC_OP_STATUS_FAILED
)

var (
	// SyncOpStatusMatrix[source_status][merging_status] = new_status
	SyncOpStatusMatrix = [][]SyncOpStatus{
		{SYNC_OP_STATUS_OK}, {SYNC_OP_STATUS_OK}, {SYNC_OP_STATUS_INCOMPLETE}, {SYNC_OP_STATUS_INCOMPLETE},
		{SYNC_OP_STATUS_OK}, {SYNC_OP_STATUS_IDLE}, {SYNC_OP_STATUS_INCOMPLETE}, {SYNC_OP_STATUS_FAILED},
		{SYNC_OP_STATUS_INCOMPLETE}, {SYNC_OP_STATUS_INCOMPLETE}, {SYNC_OP_STATUS_INCOMPLETE}, {SYNC_OP_STATUS_INCOMPLETE},
		{SYNC_OP_STATUS_INCOMPLETE}, {SYNC_OP_STATUS_FAILED}, {SYNC_OP_STATUS_INCOMPLETE}, {SYNC_OP_STATUS_FAILED},
	}

	SyncOpStatusNames = []string{"ok", "idle", "incomplete", "failed"}

	SyncOpStatusFromName = map[string]SyncOpStatus{
		"ok":         SYNC_OP_STATUS_OK,
		"idle":       SYNC_OP_STATUS_IDLE,
		"incomplete": SYNC_OP_STATUS_INCOMPLETE,
		"failed":     SYNC_OP_STATUS_FAILED,
	}

	defaultSyncOpStatusController SyncOpMsg = SyncOpIdle("empty controller")
)

type SyncOpStatusController struct {
	ctx  *sfPlugins.StatefunContextProcessor
	soms []SyncOpMsg
}

func NewSyncOpStatusController(ctx *sfPlugins.StatefunContextProcessor) *SyncOpStatusController {
	return &SyncOpStatusController{ctx, []SyncOpMsg{}}
}

func (sosc *SyncOpStatusController) Integreate(som SyncOpMsg) *SyncOpStatusController {
	sosc.soms = append(sosc.soms, som)
	return sosc
}

func (sosc *SyncOpStatusController) GetLastSyncOp() SyncOpMsg {
	if len(sosc.soms) > 0 {
		return sosc.soms[len(sosc.soms)-1]
	}
	return defaultSyncOpStatusController
}

func (sosc *SyncOpStatusController) GetStatus() SyncOpStatus {
	var status *SyncOpStatus = nil
	for _, som := range sosc.soms {
		if status == nil {
			*status = som.Status
		} else {
			*status = SyncOpStatusMatrix[*status][som.Status]
		}
	}
	if status != nil {
		return *status
	}
	return defaultSyncOpStatusController.Status
}

func (sosc *SyncOpStatusController) GetDetails() string {
	allDetails := []string{}
	for _, som := range sosc.soms {
		if len(som.Details) > 0 {
			allDetails = append(allDetails, som.Details)
		}
	}
	return strings.Join(allDetails, ";")
}

func (sosc *SyncOpStatusController) GetData() easyjson.JSON {
	if len(sosc.soms) > 0 {
		return sosc.soms[len(sosc.soms)-1].Data
	}
	return easyjson.NewJSONNull()
}

func (sosc *SyncOpStatusController) ReplyWithData(data *easyjson.JSON) {
	if sosc.ctx.Reply != nil {
		if data == nil {
			sosc.ctx.Reply.With(MakeSyncOpMsg(sosc.GetStatus(), sosc.GetDetails(), sosc.GetData()).ToJson())
		} else {
			sosc.ctx.Reply.With(MakeSyncOpMsg(sosc.GetStatus(), sosc.GetDetails(), *data).ToJson())
		}
	}
}

func (sosc *SyncOpStatusController) Reply() {
	sosc.ReplyWithData(nil)
}

type SyncOpMsg struct {
	Status  SyncOpStatus
	Details string
	Data    easyjson.JSON
}

func SyncOpOk(data easyjson.JSON) SyncOpMsg {
	return MakeSyncOpMsg(SYNC_OP_STATUS_OK, "", data)
}

func SyncOpIdle(details string) SyncOpMsg {
	return MakeSyncOpMsg(SYNC_OP_STATUS_OK, details, easyjson.NewJSONNull())
}

func SyncOpIncomplete(details string) SyncOpMsg {
	return MakeSyncOpMsg(SYNC_OP_STATUS_INCOMPLETE, details, easyjson.NewJSONNull())
}

func SyncOpFailed(details string) SyncOpMsg {
	return MakeSyncOpMsg(SYNC_OP_STATUS_FAILED, details, easyjson.NewJSONNull())
}

func MakeSyncOpMsg(status SyncOpStatus, details string, data easyjson.JSON) SyncOpMsg {
	return SyncOpMsg{status, details, data}
}

func SyncOpMsgFromJson(reply *easyjson.JSON) SyncOpMsg {
	if reply == nil {
		return SyncOpFailed("reply data is nil")
	}
	if !reply.GetByPath("status").IsString() {
		return SyncOpFailed("invalid reply data: status field is missing")
	}
	status := reply.GetByPath("status").AsStringDefault("")
	syncOpStatus, ok := SyncOpStatusFromName[status]
	if !ok {
		return SyncOpFailed(fmt.Sprintf("invalid reply data: status %s is unknown", status))
	}

	info := reply.GetByPath("details").AsStringDefault("")

	var data *easyjson.JSON = nil
	if reply.PathExists("data") {
		data = reply.GetByPath("data").GetPtr()
	}

	return MakeSyncOpMsg(syncOpStatus, info, *data)
}

func SyncOpMsgFromSfReply(reply *easyjson.JSON, err error) SyncOpMsg {
	if err == nil {
		return SyncOpMsgFromJson(reply)
	}
	return SyncOpFailed("statefun did not reply")
}

func (som SyncOpMsg) ToJson() *easyjson.JSON {
	reply := easyjson.NewJSONObjectWithKeyValue("status", easyjson.NewJSON(SyncOpStatusNames[som.Status]))
	if len(som.Details) > 0 {
		reply.SetByPath("details", easyjson.NewJSON(som.Details))
	}
	if !som.Data.IsNull() {
		reply.SetByPath("data", som.Data)
	}
	return &reply
}

func GetQueryID(contextProcessor *sfPlugins.StatefunContextProcessor) string {
	var queryID string
	if s, ok := contextProcessor.Payload.GetByPath("query_id").AsString(); ok {
		queryID = s
	} else {
		queryID = sfSystem.GetUniqueStrID()
	}
	return queryID
}
