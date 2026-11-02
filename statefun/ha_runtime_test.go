package statefun

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/foliagecp/sdk/statefun/cache"
	"github.com/stretchr/testify/require"
)

// Start results must be observed: IsActiveInstance defaults to true before
// startup and cannot distinguish an owner from an instance that failed to boot.
type haTestRuntime struct {
	rt     *Runtime
	cancel context.CancelFunc
	done   chan struct{}
	err    error // published by closing done
}

func launchHATestRuntime(t *testing.T, rt *Runtime) *haTestRuntime {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h := &haTestRuntime{rt: rt, cancel: cancel, done: make(chan struct{})}
	t.Cleanup(func() {
		// Cleanup callbacks run before the earlier broker callback. Context
		// cancellation alone does not stop a runtime after Start is complete.
		ctx, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		require.NoError(t, h.stop(ctx), "HA runtime must stop before its broker")
	})
	go func() {
		defer close(h.done)
		h.err = rt.Start(ctx, cache.NewCacheConfig("hadomain_cache"))
	}()
	return h
}

func (h *haTestRuntime) waitReady(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-h.done:
			if h.err != nil {
				return fmt.Errorf("HA runtime startup failed: %w", h.err)
			}
			return fmt.Errorf("HA runtime exited before readiness")
		default:
		}
		if h.rt.IsReady() {
			return nil
		}
		select {
		case <-h.done:
		case <-ticker.C:
		case <-ctx.Done():
			return fmt.Errorf("HA runtime readiness: %w", ctx.Err())
		}
	}
}

func requireHARuntimesReady(t *testing.T, runtimes ...*haTestRuntime) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i, h := range runtimes {
		require.NoErrorf(t, h.waitReady(ctx), "HA instance %d must finish startup before checking ownership", i)
	}
}

func (h *haTestRuntime) stop(ctx context.Context) error {
	h.cancel() // bounds an incomplete startup
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		// IsReady publishes gs. Never race an emergency shutdown against its
		// initialization while a runtime is still starting.
		if h.rt.IsReady() {
			h.rt.Shutdown(true)
		}
		select {
		case <-h.done:
			h.rt.nc.Close() // validation can fail before Start installs its cleanup
			return nil
		case <-ticker.C:
		case <-ctx.Done():
			h.rt.nc.Close()
			return fmt.Errorf("HA runtime did not stop: %w", ctx.Err())
		}
	}
}
