//go:build leak

package leak

import (
	"fmt"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/triggerfunc"
	"github.com/foliagecp/sdk/statefun"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	sfPluginJS "github.com/foliagecp/sdk/statefun/plugins/js"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/stretchr/testify/suite"
)

// S4 — statefun function contexts (cache keys `<typename>.<id>`).
//
// (a) a context WITH an expiry is reclaimed by the runtime GC — and it
//     proves the observation channel the two regression probes rely on;
// (b) the production namegen trigger marks its context to expire even on the
//     executor error paths (finding L2);
// (c) contexts die with their object: vertex.delete drops every
//     `<typename>.<id>` key of the deleted vertex (finding L1).

// contextExpirationMark mirrors the unexported contextExpirationKey constant
// in statefun/function_type.go — the marker the GC requires before it will
// ever delete a function context.
const contextExpirationMark = "____ctx_expires_after_ms"

type S4Suite struct{ leakSuite }

func TestS4FunctionContexts(t *testing.T) { suite.Run(t, new(S4Suite)) }

// ctxWriter returns a request-driven statefun that stores a ~1KiB context on
// its id, optionally marked to expire.
func ctxWriter(ttl time.Duration) statefun.FunctionLogicHandler {
	return func(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
		body := leakBody(1024)
		ctx.SetFunctionContext(&body)
		if ttl > 0 {
			ctx.SetContextExpirationAfter(ttl)
		}
		sfMediators.NewOpMediator(ctx).AggregateOpMsg(sfMediators.OpMsgOk(easyjson.NewJSONObject())).Reply()
	}
}

func (s *S4Suite) registerCtxFn(name string, ttl time.Duration) func(rt *statefun.Runtime) {
	return func(rt *statefun.Runtime) {
		statefun.NewFunctionType(rt, name, ctxWriter(ttl),
			*statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	}
}

func (s *S4Suite) callFn(fn, id string) error {
	payload := easyjson.NewJSONObject()
	_, err := s.Request(sfPlugins.AutoRequestSelect, fn, id, &payload, nil)
	return err
}

// countCtxKeysWithoutTTL counts contexts of fn that carry NO expiration mark —
// exactly the population the GC can never reclaim.
func (s *S4Suite) countCtxKeysWithoutTTL(fn string) int {
	n := 0
	for _, key := range s.cacheStore().GetKeysByPattern(fn + ".>") {
		if j, err := s.cacheStore().GetValueJSON(key); err == nil && !j.PathExists(contextExpirationMark) {
			n++
		}
	}
	return n
}

// Test_ContextWithTTLIsReclaimed: contexts written with a 500ms expiry must
// disappear once the GC (1s cadence in this suite) has passed. Expected: PASS.
func (s *S4Suite) Test_ContextWithTTLIsReclaimed() {
	const fn = "test.leak.ctx.expiring"
	s.bootCRUD(s.registerCtxFn(fn, 500*time.Millisecond))
	k := scaled(30)

	cycle := func(c int) error {
		for i := 0; i < k; i++ {
			if err := s.callFn(fn, fmt.Sprintf("s4a-%d-%d", c, i)); err != nil {
				return err
			}
		}
		deadline := time.Now().Add(15 * time.Second)
		for s.kvCount(fn+".>") > 0 && time.Now().Before(deadline) {
			time.Sleep(100 * time.Millisecond)
		}
		if n := s.kvCount(fn + ".>"); n > 0 {
			return fmt.Errorf("%d expiring contexts not reclaimed within 15s", n)
		}
		return nil
	}

	collect := func(smp *Sample) {
		s.collectCore(smp)
		smp.Custom["fn_ctx_keys"] = float64(s.kvCount(fn + ".>"))
	}
	rep := s.newRunner("s4a_ctx_ttl", cycle, collect).Run(s.T())
	rep.AssertClean(s.T())
	rep.AssertStable(s.T(), "fn_ctx_keys")
}

// Test_NamegenBuildErrorLeaksContext — regression guard for finding L2.
//
// The PRODUCTION handler triggerfunc.ObjectNameGenerator stores the whole
// object body + type body as its function context; historically it only
// marked the context to expire at the very end, so the executor build-error
// path returned in between and left one permanent, GC-invisible context per
// touched object. The executor here is registered with a non-compiling source
// to make that branch fire for every id; the handler, flow and context write
// are the production ones. The probe requires every context the handler
// writes to carry the expiration mark even on the error path, and the
// GC-invisible population to stay at baseline.
func (s *S4Suite) Test_NamegenBuildErrorLeaksContext() {
	const fn = "functions.triggers.object.namegen"
	s.bootCRUD(func(rt *statefun.Runtime) {
		ft := statefun.NewFunctionType(rt, fn, triggerfunc.ObjectNameGenerator,
			*statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
		system.MsgOnErrorReturn(ft.SetExecutor("name_generator.js", "syntax error(", sfPluginJS.StatefunExecutorPluginJSContructor))
	})
	s.Require().NoError(s.dbc.CMDB.TypeCreate("t_s4b"))
	k := scaled(15)

	cycle := func(c int) error {
		ids := make([]string, 0, k)
		for i := 0; i < k; i++ {
			id := fmt.Sprintf("s4b-%d-%d", c, i)
			if err := s.dbc.CMDB.ObjectCreate(id, "t_s4b", leakBody(100)); err != nil {
				return fmt.Errorf("object.create %s: %w", id, err)
			}
			ids = append(ids, id)
		}
		for _, id := range ids {
			if err := s.Signal(sfPlugins.JetstreamGlobalSignal, fn, id, easyjson.NewJSONObject().GetPtr(), nil); err != nil {
				return fmt.Errorf("signal namegen %s: %w", id, err)
			}
		}
		// Wait until every invocation of THIS cycle has written its context
		// (they persist for the 1min production TTL, so presence is stable).
		deadline := time.Now().Add(20 * time.Second)
		for _, id := range ids {
			key := fn + "." + s.domainID(id)
			for !s.cacheStore().ExistsJson(key) {
				if time.Now().After(deadline) {
					return fmt.Errorf("namegen invocation for %s never wrote its context", id)
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
		// The L2 pin: even on the build-error path every stored context must
		// already carry the expiration mark.
		for _, id := range ids {
			key := fn + "." + s.domainID(id)
			if j, err := s.cacheStore().GetValueJSON(key); err == nil && !j.PathExists(contextExpirationMark) {
				return fmt.Errorf("namegen context for %s carries no expiration mark", id)
			}
		}
		for _, id := range ids {
			if err := s.dbc.CMDB.ObjectDelete(id); err != nil {
				return fmt.Errorf("object.delete %s: %w", id, err)
			}
		}
		return nil
	}

	collect := func(smp *Sample) {
		smp.Custom["namegen_ctx_no_ttl"] = float64(s.countCtxKeysWithoutTTL(fn))
	}
	rep := s.newRunner("s4b_namegen_builderr", cycle, collect).Run(s.T())
	rep.AssertStable(s.T(), "namegen_ctx_no_ttl")
}

// Test_ContextOfDeletedObjectSurvives — regression guard for finding L1.
//
// Historically a function context written without an expiry stayed in the
// cache forever after its object was deleted (no cleanup hook existed, the
// GC reclaims only expiry-marked contexts). vertex.delete now drops every
// `<typename>.<id>` context of the deleted vertex, so the population must
// return to baseline.
func (s *S4Suite) Test_ContextOfDeletedObjectSurvives() {
	const fn = "test.leak.ctx.noexpiry"
	s.bootCRUD(s.registerCtxFn(fn, 0))
	s.Require().NoError(s.dbc.CMDB.TypeCreate("t_s4c"))
	k := scaled(30)

	cycle := func(c int) error {
		ids := make([]string, 0, k)
		for i := 0; i < k; i++ {
			id := fmt.Sprintf("s4c-%d-%d", c, i)
			if err := s.dbc.CMDB.ObjectCreate(id, "t_s4c"); err != nil {
				return fmt.Errorf("object.create %s: %w", id, err)
			}
			ids = append(ids, id)
		}
		for _, id := range ids {
			if err := s.callFn(fn, id); err != nil {
				return fmt.Errorf("ctx write %s: %w", id, err)
			}
		}
		for _, id := range ids {
			if err := s.dbc.CMDB.ObjectDelete(id); err != nil {
				return fmt.Errorf("object.delete %s: %w", id, err)
			}
		}
		return nil
	}

	collect := func(smp *Sample) {
		smp.Custom["orphan_ctx_keys"] = float64(s.kvCount(fn + ".>"))
	}
	rep := s.newRunner("s4c_ctx_orphans", cycle, collect).Run(s.T())
	rep.AssertStable(s.T(), "orphan_ctx_keys")
}
