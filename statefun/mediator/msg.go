package mediator

import (
	"fmt"

	"github.com/foliagecp/easyjson"
)

type OpStatus = int

const (
	SYNC_OP_STATUS_OK OpStatus = iota
	SYNC_OP_STATUS_IDLE
	SYNC_OP_STATUS_INCOMPLETE
	SYNC_OP_STATUS_FAILED
)

var (
	// OpStatusMatrix[source_status][merging_status] = new_status
	OpStatusMatrix = [][]OpStatus{
		{SYNC_OP_STATUS_OK, SYNC_OP_STATUS_OK, SYNC_OP_STATUS_INCOMPLETE, SYNC_OP_STATUS_INCOMPLETE},
		{SYNC_OP_STATUS_OK, SYNC_OP_STATUS_IDLE, SYNC_OP_STATUS_INCOMPLETE, SYNC_OP_STATUS_FAILED},
		{SYNC_OP_STATUS_INCOMPLETE, SYNC_OP_STATUS_INCOMPLETE, SYNC_OP_STATUS_INCOMPLETE, SYNC_OP_STATUS_INCOMPLETE},
		{SYNC_OP_STATUS_INCOMPLETE, SYNC_OP_STATUS_FAILED, SYNC_OP_STATUS_INCOMPLETE, SYNC_OP_STATUS_FAILED},
	}
)

var (
	OpStatusNames = []string{"ok", "idle", "incomplete", "failed"}

	OpStatusFromName = map[string]OpStatus{
		"ok":         SYNC_OP_STATUS_OK,
		"idle":       SYNC_OP_STATUS_IDLE,
		"incomplete": SYNC_OP_STATUS_INCOMPLETE,
		"failed":     SYNC_OP_STATUS_FAILED,
	}
)

type OpMsg struct {
	Status  OpStatus
	Details string
	Meta    string
	Data    easyjson.JSON
}

func OpMsgOk(data easyjson.JSON) OpMsg {
	return MakeOpMsg(SYNC_OP_STATUS_OK, "", "", data)
}

func OpMsgIdle(details string) OpMsg {
	return MakeOpMsg(SYNC_OP_STATUS_IDLE, details, "", easyjson.NewJSONNull())
}

func OpMsgIncomplete(details string) OpMsg {
	return MakeOpMsg(SYNC_OP_STATUS_INCOMPLETE, details, "", easyjson.NewJSONNull())
}

func OpMsgFailed(details string) OpMsg {
	return MakeOpMsg(SYNC_OP_STATUS_FAILED, details, "", easyjson.NewJSONNull())
}

func MakeOpMsg(status OpStatus, details string, meta string, data easyjson.JSON) OpMsg {
	return OpMsg{status, details, "", data}
}

func OpMsgFromJson(reply *easyjson.JSON) OpMsg {
	if reply == nil {
		return OpMsgFailed("reply data is nil")
	}
	if !reply.GetByPath("status").IsString() {
		return OpMsgFailed("invalid reply data: status field is missing")
	}
	status := reply.GetByPath("status").AsStringDefault("")
	syncOpStatus, ok := OpStatusFromName[status]
	if !ok {
		return OpMsgFailed(fmt.Sprintf("invalid reply data: status %s is unknown", status))
	}

	details := reply.GetByPath("details").AsStringDefault("")
	meta := reply.GetByPath("meta").AsStringDefault("")

	data := easyjson.NewJSONNull()
	if reply.PathExists("data") {
		data = reply.GetByPath("data")
	}

	return MakeOpMsg(syncOpStatus, details, meta, data)
}

func OpMsgFromSfReply(reply *easyjson.JSON, err error) OpMsg {
	if err == nil {
		return OpMsgFromJson(reply)
	}
	return OpMsgFailed(fmt.Sprintf("statefun reply error: %s", err.Error()))
}

func (som OpMsg) ToJson() *easyjson.JSON {
	reply := easyjson.NewJSONObjectWithKeyValue("status", easyjson.NewJSON(OpStatusNames[som.Status]))
	if len(som.Details) > 0 {
		reply.SetByPath("details", easyjson.NewJSON(som.Details))
	}
	if len(som.Meta) > 0 {
		reply.SetByPath("meta", easyjson.NewJSON(som.Meta))
	}
	if !som.Data.IsNull() {
		reply.SetByPath("data", som.Data)
	}
	return &reply
}

func GetSyncOpIntegratedStatusWithDefault(statuses []OpMsg, defaultStatus OpStatus) OpStatus {
	var status *OpStatus = nil
	for _, som := range statuses {
		if status == nil {
			status = &som.Status
		} else {
			*status = OpStatusMatrix[*status][som.Status]
		}
	}
	if status != nil {
		return *status
	}
	return defaultStatus
}
