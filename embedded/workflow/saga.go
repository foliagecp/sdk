package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

// Stateful saga implementation that persists progress into sfctx.GetFunctionContext().
// - DSL-style builder: NewSaga(...).Step(...).WithCompensation(...).WithRetries(...).WithTimeout(...)
// - Execution is event-driven: each call to RunStateful executes at most one activity (or one compensation)
//   and then persists state and signals the function to continue. This guarantees progress survives
//   process restarts.
// - Uses function context path `saga.<id>.state` to store JSON-serialized sagaState.

// -----------------------------------------------------------------------------
// Builder types
// -----------------------------------------------------------------------------

type SagaStepDef struct {
	Name              string
	Activity          *WorkflowActivity
	ActivityInput     easyjson.JSON
	Compensation      *WorkflowActivity
	CompensationInput easyjson.JSON
	Retries           int
	Timeout           time.Duration
}

type SagaDefinition struct {
	ID    string
	Steps []SagaStepDef
}

func NewSaga(id string) *SagaDefinition {
	return &SagaDefinition{ID: id, Steps: []SagaStepDef{}}
}

func (s *SagaDefinition) Step(activity *WorkflowActivity, input easyjson.JSON, name string) *SagaDefinition {
	step := SagaStepDef{Activity: activity, ActivityInput: input, Name: name, Retries: 1}
	s.Steps = append(s.Steps, step)
	return s
}

func (s *SagaDefinition) WithCompensation(activity *WorkflowActivity, input easyjson.JSON) *SagaDefinition {
	if len(s.Steps) == 0 {
		panic("WithCompensation: no step to attach to")
	}
	s.Steps[len(s.Steps)-1].Compensation = activity
	s.Steps[len(s.Steps)-1].CompensationInput = input
	return s
}

func (s *SagaDefinition) WithRetries(n int) *SagaDefinition {
	if len(s.Steps) == 0 {
		panic("WithRetries: no step to attach to")
	}
	s.Steps[len(s.Steps)-1].Retries = n
	return s
}

func (s *SagaDefinition) WithTimeout(d time.Duration) *SagaDefinition {
	if len(s.Steps) == 0 {
		panic("WithTimeout: no step to attach to")
	}
	s.Steps[len(s.Steps)-1].Timeout = d
	return s
}

// -----------------------------------------------------------------------------
// Persistent state stored in FunctionContext
// -----------------------------------------------------------------------------

type sagaState struct {
	Current   int         `json:"current"`
	Completed []int       `json:"completed"`
	Rollback  bool        `json:"rollback"`
	Attempts  map[int]int `json:"attempts"` // attempts per step index
	LastError string      `json:"last_error"`
}

func statePath(id string) string {
	return fmt.Sprintf("saga.%s.state", id)
}

func loadSagaState(sfctx *sfPlugins.StatefunContextProcessor, sagaID string) (s sagaState) {
	fc := sfctx.GetFunctionContext()
	p := statePath(sagaID)
	if fc.PathExists(p) {
		raw := fc.GetByPath(p).AsStringDefault("")
		if raw != "" {
			_ = json.Unmarshal([]byte(raw), &s) // ignore error -> fallthrough to zero value
			if s.Attempts == nil {
				s.Attempts = map[int]int{}
			}
			return
		}
	}
	// default state
	s = sagaState{Current: 0, Completed: []int{}, Rollback: false, Attempts: map[int]int{}, LastError: ""}
	return
}

func saveSagaState(sfctx *sfPlugins.StatefunContextProcessor, sagaID string, s sagaState) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	fc := sfctx.GetFunctionContext()
	if ok := fc.SetByPath(statePath(sagaID), easyjson.NewJSON(string(b))); !ok {
		return fmt.Errorf("could not set saga state")
	}
	sfctx.SetFunctionContext(fc)
	return nil
}

func clearSagaState(sfctx *sfPlugins.StatefunContextProcessor, sagaID string) {
	fc := sfctx.GetFunctionContext()
	// overwrite with empty object
	_ = fc.SetByPath(statePath(sagaID), easyjson.NewJSONObject())
	sfctx.SetFunctionContext(fc)
}

// -----------------------------------------------------------------------------
// Execution: event-driven, step-by-step
// -----------------------------------------------------------------------------

// RunStateful executes (or continues) the saga using the provided tools and the
// current function context (sfctx). It performs at most one activity or one
// compensation per invocation and then persists state and signals the function
// to continue if needed.
func (sd *SagaDefinition) RunStateful(ctx context.Context, sfctx *sfPlugins.StatefunContextProcessor, tools WorkflowTools) {
	// load persisted state
	state := loadSagaState(sfctx, sd.ID)

	// helper: when we need to continue processing, signal self
	signalSelf := func() {
		// empty payload
		empty := easyjson.NewJSONObject().GetPtr()
		sfctx.Signal(sfPlugins.AutoSignalSelect, sfctx.Self.Typename, sfctx.Self.ID, empty, sfctx.Options)
	}

	// If we are in rollback mode -> execute one compensation step
	if state.Rollback {
		if len(state.Completed) == 0 {
			// nothing to compensate -> cleanup and finish
			clearSagaState(sfctx, sd.ID)
			return
		}

		// last executed step index
		lastIdx := state.Completed[len(state.Completed)-1]
		step := sd.Steps[lastIdx]

		if step.Compensation == nil {
			// nothing to do for this step -> remove it from completed and continue
			state.Completed = state.Completed[:len(state.Completed)-1]
			saveSagaState(sfctx, sd.ID, state)
			signalSelf()
			return
		}

		// save current state BEFORE calling activity to survive restarts
		saveSagaState(sfctx, sd.ID, state)

		// call compensation
		res := tools.ExecActivity(step.Compensation, step.CompensationInput)
		// if ExecActivity did a signal+panic, this invocation will stop here.
		// after callback the function will be re-invoked and ExecActivity will return the result.

		if res == nil {
			// defensive: should not happen, but just return
			return
		}

		// check result: by convention activity replies should contain {"ok": true} on success.
		if !res.GetByPath("ok").AsBoolDefault(true) {
			// compensation reported failure — keep rollback flag and record error
			state.LastError = res.GetByPath("error").AsStringDefault("compensation failed")
			saveSagaState(sfctx, sd.ID, state)
			// re-signal to retry compensation (could add backoff by external scheduler)
			signalSelf()
			return
		}

		// compensation success: drop last completed and persist
		state.Completed = state.Completed[:len(state.Completed)-1]
		state.LastError = ""
		saveSagaState(sfctx, sd.ID, state)
		// continue compensating if there are more
		signalSelf()
		return
	}

	// Normal forward mode
	if state.Current >= len(sd.Steps) {
		// finished successfully -> cleanup state
		clearSagaState(sfctx, sd.ID)
		return
	}

	step := sd.Steps[state.Current]

	// ensure attempts map exists
	if state.Attempts == nil {
		state.Attempts = map[int]int{}
	}

	// persist state before invoking the activity so that we survive crashes
	saveSagaState(sfctx, sd.ID, state)

	// execute activity (this will either return existing result or send signal+panic)
	res := tools.ExecActivity(step.Activity, step.ActivityInput)
	if res == nil {
		// defensive
		return
	}

	// by convention activity returns {"ok": true} when success
	if res.GetByPath("ok").AsBoolDefault(true) {
		// mark completed
		state.Completed = append(state.Completed, state.Current)
		state.Attempts[state.Current] = 0
		state.Current++
		state.LastError = ""
		saveSagaState(sfctx, sd.ID, state)
		// continue next step
		signalSelf()
		return
	}

	// failure case: accumulate attempt and decide to rollback or retry
	state.Attempts[state.Current] = state.Attempts[state.Current] + 1
	if state.Attempts[state.Current] >= maxInt(step.Retries, 1) {
		// trigger rollback
		state.Rollback = true
		state.LastError = res.GetByPath("error").AsStringDefault("step failed")
		saveSagaState(sfctx, sd.ID, state)
		// start compensation loop
		signalSelf()
		return
	}

	// else retry the same step (signal self)
	saveSagaState(sfctx, sd.ID, state)
	signalSelf()
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// -----------------------------------------------------------------------------
// Usage notes (register activities / engine)
// -----------------------------------------------------------------------------
// - Define WorkflowActivity instances (RegisterStatefun) for every activity and its compensation.
// - Build saga with NewSaga("id").Step(...).WithCompensation(...).WithRetries(...)
// - In your workflow logic handler call saga.RunStateful(ctx, tools.ctx, tools)
//   (tools.ctx is the StatefunContextProcessor stored inside WorkflowTools)
// - Activities should reply with JSON {"ok": true} on success or {"ok": false, "error": "..."} on failure.

// Example (in your workflow logic):
// func MyWorkflow(tools workflow.WorkflowTools) {
//     ctx := context.Background()
//     saga := NewSaga("firmware_update_001").
//         Step(workflowActivityBackup, easyjson.NewJSONObjectWithKeyValue("file", easyjson.NewJSON("/tmp/fw.bin")), "backup").
//         WithCompensation(workflowActivityRestore, easyjson.NewJSONObjectWithKeyValue("file", easyjson.NewJSON("/tmp/fw.bin"))).
//         WithRetries(3).
//         Step(workflowActivityInstall, easyjson.NewJSONObjectWithKeyValue("file", easyjson.NewJSON("/tmp/fw.bin")), "install").
//         WithRetries(1)
//
//     saga.RunStateful(ctx, tools.ctx, tools)
// }

// -----------------------------------------------------------------------------
