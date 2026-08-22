package crud

import (
	"fmt"
	"testing"

	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/stretchr/testify/require"
)

func ctxWithPayload(p easyjson.JSON) *sfPlugins.StatefunContextProcessor {
	return &sfPlugins.StatefunContextProcessor{Payload: &p}
}

func Test_InjectParentHoldsLocks_StripsPerInvocationBookkeeping(t *testing.T) {
	payload := easyjson.NewJSONObject()
	ctx := ctxWithPayload(payload)
	recordHeldLock(ctx, "some/vertex", "w")
	ctx.Payload.SetByPath("__key_lock_time", easyjson.NewJSON(int64(12345)))
	ctx.Payload.SetByPath("op_time", easyjson.NewJSON(int64(12345)))

	downstream := injectParentHoldsLocks(ctx, ctx.Payload)

	seg := lockRecSeg("some/vertex")
	require.Equal(t, "w", downstream.GetByPath(fmt.Sprintf("__parent_holds_locks.%s.m", seg)).AsStringDefault(""))
	require.Equal(t, "some/vertex", downstream.GetByPath(fmt.Sprintf("__parent_holds_locks.%s.k", seg)).AsStringDefault(""))
	require.False(t, downstream.PathExists("__key_locks"))
	require.False(t, downstream.PathExists("__key_lock_time"))
	require.True(t, downstream.PathExists("op_time"))
}

func Test_InjectParentHoldsLocks_StripsLockTimeFromAClonedPayload(t *testing.T) {
	payload := easyjson.NewJSONObject()
	ctx := ctxWithPayload(payload)
	recordHeldLock(ctx, "owner/vertex", "w")
	ctx.Payload.SetByPath("__key_lock_time", easyjson.NewJSON(int64(777)))

	cloned := ctx.Payload.Clone()
	require.True(t, cloned.PathExists("__key_lock_time"))

	downstream := injectParentHoldsLocks(ctx, &cloned)

	require.False(t, downstream.PathExists("__key_lock_time"))
	require.False(t, downstream.PathExists("__key_locks"))
}

func Test_OperationActiveMarkState(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		payload := easyjson.NewJSONObject()
		_, held := operationActiveMarkState(&payload)
		require.False(t, held)
	})

	t.Run("present", func(t *testing.T) {
		payload := easyjson.NewJSONObject()
		payload.SetByPath("__key_lock_time", easyjson.NewJSON(int64(42)))
		opTime, held := operationActiveMarkState(&payload)
		require.True(t, held)
		require.Equal(t, int64(42), opTime)
	})

	t.Run("nil payload", func(t *testing.T) {
		_, held := operationActiveMarkState(nil)
		require.False(t, held)
	})
}
