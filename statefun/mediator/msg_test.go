package mediator

import (
	"fmt"
	"testing"
)

// A toolchain upgrade must not change the operation statuses sent to callers.
func TestOperationStatusUsesFinalReply(t *testing.T) {
	for length := 1; length <= 3; length++ {
		combinations := 1 << (2 * length)
		for encoded := 0; encoded < combinations; encoded++ {
			messages := make([]OpMsg, length)
			value := encoded
			for i := range messages {
				messages[i].Status = value % len(OpStatusNames)
				value /= len(OpStatusNames)
			}
			t.Run(fmt.Sprintf("length=%d/case=%d", length, encoded), func(t *testing.T) {
				want := messages[len(messages)-1].Status
				if got := GetSyncOpIntegratedStatusWithDefault(messages, SYNC_OP_STATUS_FAILED); got != want {
					t.Errorf("integrated status = %s, want final reply %s", OpStatusNames[got], OpStatusNames[want])
				}
				mediator := &OpMediator{opMsgs: messages}
				if got := mediator.GetStatus(); got != want {
					t.Errorf("mediator status = %s, want final reply %s", OpStatusNames[got], OpStatusNames[want])
				}
			})
		}
	}
}

func TestEmptyOperationStatusUsesDefault(t *testing.T) {
	for _, status := range []OpStatus{SYNC_OP_STATUS_OK, SYNC_OP_STATUS_IDLE, SYNC_OP_STATUS_INCOMPLETE, SYNC_OP_STATUS_FAILED} {
		if got := GetSyncOpIntegratedStatusWithDefault(nil, status); got != status {
			t.Errorf("empty integrated status = %d, want default %d", got, status)
		}
	}
	if got := (&OpMediator{}).GetStatus(); got != SYNC_OP_STATUS_IDLE {
		t.Errorf("empty mediator status = %d, want idle", got)
	}
}
