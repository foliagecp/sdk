// Package db — regression tests for the OpMsg → error mapping.
//
// These tests pin down the contract between the SDK's sync clients (CMDB,
// Graph) and the OpMediator status codes coming back from server-side
// CRUD functions. The two interesting failure modes are:
//
//   1. IDLE-as-success — the original OpErrorFromOpMsg returns nil for
//      SYNC_OP_STATUS_IDLE. That is correct for write paths (Create / Update /
//      Delete) where IDLE means "the requested state is already satisfied",
//      but WRONG for read paths where IDLE means "entity not found": callers
//      received (empty_data, nil) and silently processed the empty body.
//
//   2. The fix (OpErrorFromOpMsgStrict) must NOT regress idempotency for
//      Delete-flavoured calls. My earlier D2 fixes in
//      embedded/graph/crud/hl_crud.go and hl_polytype_crud.go rely on
//      DeleteObjectsLink returning OpMsgIdle for a missing edge, which
//      OpErrorFromOpMsg must continue to map to nil so user code keeps
//      seeing successful idempotent deletes.
//
// The tests below exercise:
//   - the lenient/strict mappings directly (table-driven, no runtime);
//   - every read-flavoured client wrapper end-to-end through a mock
//     SFRequestFunc, asserting:
//       (a) IDLE → wrapped ErrNotFound, errors.Is works;
//       (b) details from server-side ("does not exist") survive the wrap
//           so legacy strings.Contains checks keep working;
//       (c) OK → nil + data;
//       (d) FAILED/INCOMPLETE → *OpError preserves StatusCode and Details;
//   - every Delete-flavoured client wrapper end-to-end, asserting that
//     IDLE → nil (idempotency invariant; protects the D2 fixes);
//   - every Create/Update-flavoured client wrapper end-to-end, asserting
//     OK → nil and FAILED → *OpError.
package db

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfp "github.com/foliagecp/sdk/statefun/plugins"
)

// -----------------------------------------------------------------------------
// 1. Direct unit tests for the mapping functions.
// -----------------------------------------------------------------------------

func TestOpErrorFromOpMsg_Lenient(t *testing.T) {
	t.Run("OK returns nil", func(t *testing.T) {
		if err := OpErrorFromOpMsg(sfMediators.OpMsgOk(easyjson.NewJSONObject())); err != nil {
			t.Fatalf("expected nil for OK, got %v", err)
		}
	})

	t.Run("IDLE returns nil (idempotency invariant — protects D2 fixes)", func(t *testing.T) {
		err := OpErrorFromOpMsg(sfMediators.OpMsgIdle("edge does not exist"))
		if err != nil {
			t.Fatalf("LENIENT mapping must treat IDLE as success — broke idempotency for Delete callers. got %v", err)
		}
	})

	t.Run("FAILED returns *OpError", func(t *testing.T) {
		err := OpErrorFromOpMsg(sfMediators.OpMsgFailed("nope"))
		if err == nil {
			t.Fatal("expected error for FAILED")
		}
		var oe *OpError
		if !errors.As(err, &oe) {
			t.Fatalf("expected *OpError, got %T", err)
		}
		if oe.StatusCode != sfMediators.SYNC_OP_STATUS_FAILED {
			t.Fatalf("StatusCode=%d, want FAILED(%d)", oe.StatusCode, sfMediators.SYNC_OP_STATUS_FAILED)
		}
		if oe.Details != "nope" {
			t.Fatalf("Details=%q, want %q", oe.Details, "nope")
		}
	})

	t.Run("INCOMPLETE returns *OpError", func(t *testing.T) {
		err := OpErrorFromOpMsg(sfMediators.OpMsgIncomplete("half-done"))
		var oe *OpError
		if !errors.As(err, &oe) || oe.StatusCode != sfMediators.SYNC_OP_STATUS_INCOMPLETE {
			t.Fatalf("expected *OpError(INCOMPLETE), got %v", err)
		}
	})
}

func TestOpErrorFromOpMsgStrict(t *testing.T) {
	t.Run("OK returns nil", func(t *testing.T) {
		if err := OpErrorFromOpMsgStrict(sfMediators.OpMsgOk(easyjson.NewJSONObject())); err != nil {
			t.Fatalf("expected nil for OK, got %v", err)
		}
	})

	t.Run("IDLE returns ErrNotFound (the actual bug fix)", func(t *testing.T) {
		err := OpErrorFromOpMsgStrict(sfMediators.OpMsgIdle("object \"x\" does not exist"))
		if err == nil {
			t.Fatal("STRICT mapping must NOT swallow IDLE for read paths")
		}
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected errors.Is(err, ErrNotFound) to hold, got %v", err)
		}
	})

	t.Run("IDLE preserves server details for legacy strings.Contains callers", func(t *testing.T) {
		// Legacy code (e.g. skala-backend/common/hlmb/factory.go) checks
		// `strings.Contains(err.Error(), "does not exist")` to detect missing
		// entities. The fmt.Errorf("%w: %s", ...) wrap must keep that working.
		err := OpErrorFromOpMsgStrict(sfMediators.OpMsgIdle("object \"foo\" does not exist"))
		if !strings.Contains(err.Error(), "does not exist") {
			t.Fatalf("substring \"does not exist\" lost during wrap; got %q", err.Error())
		}
	})

	t.Run("IDLE with empty details still wraps ErrNotFound", func(t *testing.T) {
		err := OpErrorFromOpMsgStrict(sfMediators.OpMsgIdle(""))
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound for empty-details IDLE, got %v", err)
		}
	})

	t.Run("FAILED returns *OpError, NOT ErrNotFound", func(t *testing.T) {
		err := OpErrorFromOpMsgStrict(sfMediators.OpMsgFailed("server boom"))
		if errors.Is(err, ErrNotFound) {
			t.Fatal("FAILED must not be misclassified as ErrNotFound")
		}
		var oe *OpError
		if !errors.As(err, &oe) || oe.StatusCode != sfMediators.SYNC_OP_STATUS_FAILED {
			t.Fatalf("expected *OpError(FAILED), got %v", err)
		}
	})

	t.Run("INCOMPLETE returns *OpError, NOT ErrNotFound", func(t *testing.T) {
		err := OpErrorFromOpMsgStrict(sfMediators.OpMsgIncomplete("half-done"))
		if errors.Is(err, ErrNotFound) {
			t.Fatal("INCOMPLETE must not be misclassified as ErrNotFound")
		}
		var oe *OpError
		if !errors.As(err, &oe) || oe.StatusCode != sfMediators.SYNC_OP_STATUS_INCOMPLETE {
			t.Fatalf("expected *OpError(INCOMPLETE), got %v", err)
		}
	})
}

// -----------------------------------------------------------------------------
// 2. Mock request infrastructure — exercises every client wrapper without NATS.
// -----------------------------------------------------------------------------

// mockReply describes one canned response from the mock request function.
// `data` is optional; when nil an empty JSON object is sent in OK replies.
type mockReply struct {
	status  sfMediators.OpStatus
	details string
	data    *easyjson.JSON
}

// mockCall captures one invocation made by a client wrapper, useful when a
// test wants to assert which typename/id were targeted.
type mockCall struct {
	typename string
	id       string
}

// newMockRequest returns an SFRequestFunc that replies according to a
// typename → reply map. Unknown typenames fail the test loudly so we never
// silently exercise the wrong code path.
func newMockRequest(t *testing.T, replies map[string]mockReply, calls *[]mockCall) sfp.SFRequestFunc {
	t.Helper()
	return func(_ sfp.RequestProvider, typename, id string, _, _ *easyjson.JSON, _ ...time.Duration) (*easyjson.JSON, error) {
		if calls != nil {
			*calls = append(*calls, mockCall{typename: typename, id: id})
		}
		r, ok := replies[typename]
		if !ok {
			t.Fatalf("mockRequest: unexpected call to %q (id=%q)", typename, id)
		}
		reply := easyjson.NewJSONObjectWithKeyValue("status", easyjson.NewJSON(sfMediators.OpStatusNames[r.status]))
		if r.details != "" {
			reply.SetByPath("details", easyjson.NewJSON(r.details))
		}
		if r.data != nil {
			reply.SetByPath("data", *r.data)
		}
		return &reply, nil
	}
}

func newCMDB(t *testing.T, replies map[string]mockReply, calls *[]mockCall) CMDBSyncClient {
	t.Helper()
	c, err := NewCMDBSyncClientFromRequestFunction(newMockRequest(t, replies, calls))
	if err != nil {
		t.Fatalf("NewCMDBSyncClientFromRequestFunction: %v", err)
	}
	return c
}

func newGraph(t *testing.T, replies map[string]mockReply, calls *[]mockCall) GraphSyncClient {
	t.Helper()
	c, err := NewGraphSyncClientFromRequestFunction(newMockRequest(t, replies, calls))
	if err != nil {
		t.Fatalf("NewGraphSyncClientFromRequestFunction: %v", err)
	}
	return c
}

// -----------------------------------------------------------------------------
// 3. Read-flavoured wrappers: IDLE must surface as ErrNotFound.
// -----------------------------------------------------------------------------

// readCase describes one Read wrapper invocation; reusing it keeps the
// matrix below compact.
type readCase struct {
	name     string
	typename string                                            // expected server typename
	invoke   func(t *testing.T, replies map[string]mockReply) (easyjson.JSON, error)
}

func readCases() []readCase {
	return []readCase{
		{
			name:     "CMDB.TypeRead",
			typename: "functions.cmdb.api.type.read",
			invoke: func(t *testing.T, r map[string]mockReply) (easyjson.JSON, error) {
				return newCMDB(t, r, nil).TypeRead("T")
			},
		},
		{
			name:     "CMDB.ObjectRead",
			typename: "functions.cmdb.api.object.read",
			invoke: func(t *testing.T, r map[string]mockReply) (easyjson.JSON, error) {
				return newCMDB(t, r, nil).ObjectRead("o")
			},
		},
		{
			name:     "CMDB.ObjectReadV2",
			typename: "functions.cmdb.api.object.read",
			invoke: func(t *testing.T, r map[string]mockReply) (easyjson.JSON, error) {
				return newCMDB(t, r, nil).ObjectReadV2("o")
			},
		},
		{
			name:     "CMDB.TypesLinkRead",
			typename: "functions.cmdb.api.types.link.read",
			invoke: func(t *testing.T, r map[string]mockReply) (easyjson.JSON, error) {
				return newCMDB(t, r, nil).TypesLinkRead("A", "B")
			},
		},
		{
			name:     "CMDB.ObjectsLinkRead",
			typename: "functions.cmdb.api.objects.link.read",
			invoke: func(t *testing.T, r map[string]mockReply) (easyjson.JSON, error) {
				return newCMDB(t, r, nil).ObjectsLinkRead("a", "b")
			},
		},
		{
			name:     "Graph.VertexRead",
			typename: "functions.graph.api.vertex.read",
			invoke: func(t *testing.T, r map[string]mockReply) (easyjson.JSON, error) {
				return newGraph(t, r, nil).VertexRead("v")
			},
		},
		{
			name:     "Graph.VertexReadDetailsV2",
			typename: "functions.graph.api.vertex.read",
			invoke: func(t *testing.T, r map[string]mockReply) (easyjson.JSON, error) {
				return newGraph(t, r, nil).VertexReadDetailsV2("v")
			},
		},
		{
			name:     "Graph.VerticesLinkRead",
			typename: "functions.graph.api.link.read",
			invoke: func(t *testing.T, r map[string]mockReply) (easyjson.JSON, error) {
				return newGraph(t, r, nil).VerticesLinkRead("v", "ln")
			},
		},
		{
			name:     "Graph.VerticesLinkReadByToAndType",
			typename: "functions.graph.api.link.read",
			invoke: func(t *testing.T, r map[string]mockReply) (easyjson.JSON, error) {
				return newGraph(t, r, nil).VerticesLinkReadByToAndType("v", "to", "type")
			},
		},
	}
}

// TestRead_IDLE_SurfacesAsErrNotFound: for every read wrapper, when the
// server replies IDLE (entity does not exist), the client MUST return an
// error that satisfies `errors.Is(err, ErrNotFound)` and preserves the
// server-side "does not exist" substring.
func TestRead_IDLE_SurfacesAsErrNotFound(t *testing.T) {
	for _, tc := range readCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			replies := map[string]mockReply{
				tc.typename: {status: sfMediators.SYNC_OP_STATUS_IDLE, details: "object \"x\" does not exist"},
			}
			_, err := tc.invoke(t, replies)
			if err == nil {
				t.Fatalf("%s on IDLE: expected an error, got nil — regression of the IDLE-as-success bug", tc.name)
			}
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("%s on IDLE: errors.Is(err, ErrNotFound) must hold, got %v", tc.name, err)
			}
			if !strings.Contains(err.Error(), "does not exist") {
				t.Fatalf("%s on IDLE: server details lost during wrap; got %q", tc.name, err.Error())
			}
		})
	}
}

// TestRead_OK_ReturnsData: every read wrapper must return (data, nil)
// when the server replies OK. The data payload travels through the
// OpMsg.Data field, so this also pins down that the strict mapping does
// not accidentally discard data.
func TestRead_OK_ReturnsData(t *testing.T) {
	data := easyjson.NewJSONObjectWithKeyValue("hello", easyjson.NewJSON("world"))

	for _, tc := range readCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			replies := map[string]mockReply{
				tc.typename: {status: sfMediators.SYNC_OP_STATUS_OK, data: &data},
			}
			got, err := tc.invoke(t, replies)
			if err != nil {
				t.Fatalf("%s OK: unexpected error %v", tc.name, err)
			}
			if got.GetByPath("hello").AsStringDefault("") != "world" {
				t.Fatalf("%s OK: data payload lost; got %s", tc.name, got.ToString())
			}
		})
	}
}

// TestRead_FAILED_ReturnsOpError: FAILED must map to *OpError, NOT ErrNotFound.
func TestRead_FAILED_ReturnsOpError(t *testing.T) {
	for _, tc := range readCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			replies := map[string]mockReply{
				tc.typename: {status: sfMediators.SYNC_OP_STATUS_FAILED, details: "boom"},
			}
			_, err := tc.invoke(t, replies)
			if err == nil {
				t.Fatalf("%s FAILED: expected error", tc.name)
			}
			if errors.Is(err, ErrNotFound) {
				t.Fatalf("%s FAILED: must not be misclassified as ErrNotFound; got %v", tc.name, err)
			}
			var oe *OpError
			if !errors.As(err, &oe) {
				t.Fatalf("%s FAILED: expected *OpError, got %T", tc.name, err)
			}
			if oe.StatusCode != sfMediators.SYNC_OP_STATUS_FAILED {
				t.Fatalf("%s FAILED: StatusCode=%d, want FAILED(%d)", tc.name, oe.StatusCode, sfMediators.SYNC_OP_STATUS_FAILED)
			}
			if oe.Details != "boom" {
				t.Fatalf("%s FAILED: Details=%q, want %q", tc.name, oe.Details, "boom")
			}
		})
	}
}

// -----------------------------------------------------------------------------
// 4. Delete-flavoured wrappers — IDLE MUST stay nil (protects D2 idempotency).
// -----------------------------------------------------------------------------

type writeCase struct {
	name     string
	typename string
	invoke   func(t *testing.T, replies map[string]mockReply) error
}

// deleteCases enumerates every Delete entry point that user code currently
// relies on being idempotent. If any of these starts returning an error
// for IDLE, my D2 fixes in embedded/graph/crud/hl_crud.go and
// hl_polytype_crud.go (which return OpMsgIdle for missing edges) will
// silently break user code.
func deleteCases() []writeCase {
	return []writeCase{
		{
			name:     "CMDB.TypeDelete",
			typename: "functions.cmdb.api.type.delete",
			invoke: func(t *testing.T, r map[string]mockReply) error {
				return newCMDB(t, r, nil).TypeDelete("T")
			},
		},
		{
			name:     "CMDB.ObjectDelete",
			typename: "functions.cmdb.api.object.delete",
			invoke: func(t *testing.T, r map[string]mockReply) error {
				return newCMDB(t, r, nil).ObjectDelete("o")
			},
		},
		{
			name:     "CMDB.TypesLinkDelete",
			typename: "functions.cmdb.api.types.link.delete",
			invoke: func(t *testing.T, r map[string]mockReply) error {
				return newCMDB(t, r, nil).TypesLinkDelete("A", "B")
			},
		},
		{
			name:     "CMDB.ObjectsLinkDelete",
			typename: "functions.cmdb.api.objects.link.delete",
			invoke: func(t *testing.T, r map[string]mockReply) error {
				return newCMDB(t, r, nil).ObjectsLinkDelete("a", "b")
			},
		},
		{
			name:     "Graph.VertexDelete",
			typename: "functions.graph.api.vertex.delete",
			invoke: func(t *testing.T, r map[string]mockReply) error {
				return newGraph(t, r, nil).VertexDelete("v")
			},
		},
		{
			name:     "Graph.VerticesLinkDelete",
			typename: "functions.graph.api.link.delete",
			invoke: func(t *testing.T, r map[string]mockReply) error {
				return newGraph(t, r, nil).VerticesLinkDelete("v", "ln")
			},
		},
		{
			name:     "Graph.VerticesLinkDeleteByToAndType",
			typename: "functions.graph.api.link.delete",
			invoke: func(t *testing.T, r map[string]mockReply) error {
				return newGraph(t, r, nil).VerticesLinkDeleteByToAndType("v", "to", "type")
			},
		},
	}
}

// TestDelete_IDLE_StillReturnsNil — Delete idempotency invariant.
// HL/LL CRUD emits IDLE when there is nothing to delete; the client MUST
// surface that as a no-op (nil error). Otherwise my D2 fixes (DeleteObjectsLink
// and DeleteObjectsLinkFromSuperTypes returning OpMsgIdle for missing edges)
// will start surfacing as errors and break callers that rely on idempotent
// teardown semantics.
func TestDelete_IDLE_StillReturnsNil(t *testing.T) {
	for _, tc := range deleteCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			replies := map[string]mockReply{
				tc.typename: {status: sfMediators.SYNC_OP_STATUS_IDLE, details: "edge does not exist"},
			}
			if err := tc.invoke(t, replies); err != nil {
				t.Fatalf("%s on IDLE: must remain idempotent (nil error). Got %v — this regression silently breaks D2 fixes.", tc.name, err)
			}
		})
	}
}

// TestDelete_OK_ReturnsNil — sanity that the happy path is unchanged.
func TestDelete_OK_ReturnsNil(t *testing.T) {
	for _, tc := range deleteCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			replies := map[string]mockReply{
				tc.typename: {status: sfMediators.SYNC_OP_STATUS_OK},
			}
			if err := tc.invoke(t, replies); err != nil {
				t.Fatalf("%s on OK: expected nil, got %v", tc.name, err)
			}
		})
	}
}

// TestDelete_FAILED_ReturnsOpError — real failures still propagate.
func TestDelete_FAILED_ReturnsOpError(t *testing.T) {
	for _, tc := range deleteCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			replies := map[string]mockReply{
				tc.typename: {status: sfMediators.SYNC_OP_STATUS_FAILED, details: "permission denied"},
			}
			err := tc.invoke(t, replies)
			var oe *OpError
			if !errors.As(err, &oe) || oe.StatusCode != sfMediators.SYNC_OP_STATUS_FAILED {
				t.Fatalf("%s FAILED: expected *OpError(FAILED), got %v", tc.name, err)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// 5. Create / Update wrappers — IDLE must keep returning nil, FAILED must error.
// -----------------------------------------------------------------------------

func createUpdateCases() []writeCase {
	return []writeCase{
		{
			name:     "CMDB.TypeCreate",
			typename: "functions.cmdb.api.type.create",
			invoke: func(t *testing.T, r map[string]mockReply) error {
				return newCMDB(t, r, nil).TypeCreate("T")
			},
		},
		{
			name:     "CMDB.TypeUpdate",
			typename: "functions.cmdb.api.type.update",
			invoke: func(t *testing.T, r map[string]mockReply) error {
				return newCMDB(t, r, nil).TypeUpdate("T", easyjson.NewJSONObject(), false)
			},
		},
		{
			name:     "CMDB.ObjectCreate",
			typename: "functions.cmdb.api.object.create",
			invoke: func(t *testing.T, r map[string]mockReply) error {
				return newCMDB(t, r, nil).ObjectCreate("o", "T")
			},
		},
		{
			name:     "CMDB.ObjectUpdate",
			typename: "functions.cmdb.api.object.update",
			invoke: func(t *testing.T, r map[string]mockReply) error {
				return newCMDB(t, r, nil).ObjectUpdate("o", easyjson.NewJSONObject(), false)
			},
		},
		{
			name:     "CMDB.TypesLinkCreate",
			typename: "functions.cmdb.api.types.link.create",
			invoke: func(t *testing.T, r map[string]mockReply) error {
				return newCMDB(t, r, nil).TypesLinkCreate("A", "B", "rel", nil)
			},
		},
		{
			name:     "CMDB.TypesLinkUpdate",
			typename: "functions.cmdb.api.types.link.update",
			invoke: func(t *testing.T, r map[string]mockReply) error {
				return newCMDB(t, r, nil).TypesLinkUpdate("A", "B", nil, easyjson.NewJSONObject(), false)
			},
		},
		{
			name:     "CMDB.ObjectsLinkCreate",
			typename: "functions.cmdb.api.objects.link.create",
			invoke: func(t *testing.T, r map[string]mockReply) error {
				return newCMDB(t, r, nil).ObjectsLinkCreate("a", "b", "ln", nil)
			},
		},
		{
			name:     "CMDB.ObjectsLinkUpdate",
			typename: "functions.cmdb.api.objects.link.update",
			invoke: func(t *testing.T, r map[string]mockReply) error {
				return newCMDB(t, r, nil).ObjectsLinkUpdate("a", "b", nil, easyjson.NewJSONObject(), false)
			},
		},
		{
			name:     "Graph.VertexCreate",
			typename: "functions.graph.api.vertex.create",
			invoke: func(t *testing.T, r map[string]mockReply) error {
				return newGraph(t, r, nil).VertexCreate("v")
			},
		},
		{
			name:     "Graph.VertexUpdate",
			typename: "functions.graph.api.vertex.update",
			invoke: func(t *testing.T, r map[string]mockReply) error {
				return newGraph(t, r, nil).VertexUpdate("v", easyjson.NewJSONObject(), false)
			},
		},
		{
			name:     "Graph.VerticesLinkCreate",
			typename: "functions.graph.api.link.create",
			invoke: func(t *testing.T, r map[string]mockReply) error {
				return newGraph(t, r, nil).VerticesLinkCreate("a", "b", "ln", "lt", nil)
			},
		},
		{
			name:     "Graph.VerticesLinkUpdate",
			typename: "functions.graph.api.link.update",
			invoke: func(t *testing.T, r map[string]mockReply) error {
				return newGraph(t, r, nil).VerticesLinkUpdate("a", "ln", nil, easyjson.NewJSONObject(), false)
			},
		},
	}
}

func TestCreateUpdate_OK_ReturnsNil(t *testing.T) {
	for _, tc := range createUpdateCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.invoke(t, map[string]mockReply{tc.typename: {status: sfMediators.SYNC_OP_STATUS_OK}}); err != nil {
				t.Fatalf("%s on OK: expected nil, got %v", tc.name, err)
			}
		})
	}
}

// TestCreateUpdate_IDLE_StillReturnsNil — Create/Update are lenient too:
// IDLE here means "the requested state was already satisfied" (e.g. body
// is byte-identical to what's already stored), and existing callers MUST
// see nil. This is symmetric with deletes and was the pre-fix behavior;
// the test pins it down so future changes don't regress it.
func TestCreateUpdate_IDLE_StillReturnsNil(t *testing.T) {
	for _, tc := range createUpdateCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.invoke(t, map[string]mockReply{tc.typename: {status: sfMediators.SYNC_OP_STATUS_IDLE, details: "no change"}}); err != nil {
				t.Fatalf("%s on IDLE: expected nil (no-op success), got %v", tc.name, err)
			}
		})
	}
}

func TestCreateUpdate_FAILED_ReturnsOpError(t *testing.T) {
	for _, tc := range createUpdateCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := tc.invoke(t, map[string]mockReply{tc.typename: {status: sfMediators.SYNC_OP_STATUS_FAILED, details: "validation"}})
			var oe *OpError
			if !errors.As(err, &oe) || oe.StatusCode != sfMediators.SYNC_OP_STATUS_FAILED {
				t.Fatalf("%s FAILED: expected *OpError(FAILED), got %v", tc.name, err)
			}
			if oe.Details != "validation" {
				t.Fatalf("%s FAILED: Details=%q, want %q", tc.name, oe.Details, "validation")
			}
		})
	}
}

// -----------------------------------------------------------------------------
// 6. *WithDetails wrappers — same contract, different return signature.
// -----------------------------------------------------------------------------

// TestObjectUpdateWithDetails_StatusMatrix pins down the WithDetails variant
// which also returns the data payload. The mapping must follow OpErrorFromOpMsg
// (lenient), not the strict variant, because this is a write path.
func TestObjectUpdateWithDetails_StatusMatrix(t *testing.T) {
	cases := []struct {
		name   string
		status sfMediators.OpStatus
		want   func(err error) bool
	}{
		{"OK", sfMediators.SYNC_OP_STATUS_OK, func(err error) bool { return err == nil }},
		{"IDLE-is-nil-for-writes", sfMediators.SYNC_OP_STATUS_IDLE, func(err error) bool { return err == nil }},
		{"FAILED", sfMediators.SYNC_OP_STATUS_FAILED, func(err error) bool {
			var oe *OpError
			return errors.As(err, &oe) && oe.StatusCode == sfMediators.SYNC_OP_STATUS_FAILED
		}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			cmdb := newCMDB(t, map[string]mockReply{
				"functions.cmdb.api.object.update": {status: c.status},
			}, nil)
			_, err := cmdb.ObjectUpdateWithDetails("o", easyjson.NewJSONObject(), false)
			if !c.want(err) {
				t.Fatalf("ObjectUpdateWithDetails %s: unexpected err %v", c.name, err)
			}
		})
	}
}

// TestObjectDeleteWithDetails_IdempotentOnIDLE pins down idempotency for
// the with-details delete variant — same invariant as the plain Delete.
func TestObjectDeleteWithDetails_IdempotentOnIDLE(t *testing.T) {
	cmdb := newCMDB(t, map[string]mockReply{
		"functions.cmdb.api.object.delete": {status: sfMediators.SYNC_OP_STATUS_IDLE, details: "already gone"},
	}, nil)
	if _, err := cmdb.ObjectDeleteWithDetails("o"); err != nil {
		t.Fatalf("ObjectDeleteWithDetails on IDLE: expected nil (idempotency), got %v", err)
	}
}

// TestObjectsLinkDeleteWithDetails_IdempotentOnIDLE — the most important
// idempotency carrier for D2 user code (delete an edge whose target is
// already gone). Server-side this is exactly the OpMsgIdle path I added
// in DeleteObjectsLink.
func TestObjectsLinkDeleteWithDetails_IdempotentOnIDLE(t *testing.T) {
	cmdb := newCMDB(t, map[string]mockReply{
		"functions.cmdb.api.objects.link.delete": {status: sfMediators.SYNC_OP_STATUS_IDLE, details: "edge does not exist"},
	}, nil)
	if _, err := cmdb.ObjectsLinkDeleteWithDetails("a", "b"); err != nil {
		t.Fatalf("ObjectsLinkDeleteWithDetails on IDLE: expected nil (D2 idempotency), got %v", err)
	}
}

// -----------------------------------------------------------------------------
// 7. Routing sanity — make sure the wrappers still target the right typenames.
// -----------------------------------------------------------------------------

// Read methods build up their own payload (e.g. ObjectReadV2 sets
// details_v2=true). It is easy to introduce a copy-paste bug that sends a
// read against the wrong server typename. This guard test catches any such
// regression.
func TestReadWrappers_TargetExpectedTypename(t *testing.T) {
	for _, tc := range readCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var calls []mockCall
			replies := map[string]mockReply{
				tc.typename: {status: sfMediators.SYNC_OP_STATUS_OK},
			}
			c, err := NewCMDBSyncClientFromRequestFunction(newMockRequest(t, replies, &calls))
			if err != nil {
				t.Fatalf("client init: %v", err)
			}
			_ = c
			// Re-run the actual invocation with a request that records the call.
			// We piggyback via a fresh client; the typename routing is what
			// matters, not the surface type.
			if _, err := tc.invoke(t, replies); err != nil {
				// OK case must succeed — if it errors here it's a routing bug,
				// not a mapping bug.
				t.Fatalf("%s OK: unexpected err %v", tc.name, err)
			}
		})
	}
}
