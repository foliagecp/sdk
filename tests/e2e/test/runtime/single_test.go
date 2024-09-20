package statefun_test

import (
	"context"
	"fmt"
	"github.com/foliagecp/sdk/statefun/cache"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/logger"
	"github.com/foliagecp/sdk/tests/e2e/test/nats"
)

func TestKeyMutexLockUnlock(t *testing.T) {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Set up NATS cluster
	ct, err := nats.CreateCluster(ctx, "cluster", 3, 0)
	if err != nil {
		t.Fatalf("Error creating NATS cluster: %v", err)
	}
	ct.Start(ctx)

	// Create runtime configuration
	conn := strings.Join(ct.GetClusterConnection(ctx), ",")
	conf := statefun.NewRuntimeConfigSimple(conn, "basic")
	runtime, err := statefun.NewRuntime(*conf)
	if err != nil {
		t.Fatalf("Cannot create statefun runtime: %v", err)
	}

	// Register the test function to run after the runtime starts
	runtime.RegisterOnAfterStartFunction(func(ctx context.Context, runtime *statefun.Runtime) error {
		return runKeyMutexTests(ctx, t, runtime)
	}, true)
	go func() {
		// Start the runtime
		err = runtime.Start(ctx, cache.NewCacheConfig("main_cache"))
		if err != nil {
			t.Errorf("Cannot start runtime: %v", err)
			return
		}
	}()

	<-ctx.Done()

	runtime.Shutdown()
}

func runKeyMutexTests(ctx context.Context, t *testing.T, runtime *statefun.Runtime) error {
	tests := []struct {
		name            string
		workersCount    int
		workTimeMs      int
		afterWorkTimeMs int
	}{
		{"SingleWorker", 1, 100, 50},
		{"MultipleWorkers", 5, 100, 50},
		{"StressTest", 100, 5, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lg := logger.GetLogger()
			lg.Debug(ctx, ">>> Starting test: %s", tt.name)

			var wg sync.WaitGroup
			for i := 0; i < tt.workersCount; i++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					for j := 0; j < 5; j++ {
						lockKey := fmt.Sprintf("test.object.%d", j)
						lockRevID, err := statefun.KeyMutexLock(ctx, runtime, lockKey, false)
						if err != nil {
							t.Errorf("Worker %d: KeyMutexLock failed: %v", workerID, err)
							return
						}
						lg.Trace(ctx, "Worker %d acquired lock on %s", workerID, lockKey)

						time.Sleep(time.Duration(tt.workTimeMs) * time.Millisecond)

						err = statefun.KeyMutexUnlock(ctx, runtime, lockKey, lockRevID)
						if err != nil {
							t.Errorf("Worker %d: KeyMutexUnlock failed: %v", workerID, err)
							return
						}
						lg.Trace(ctx, "Worker %d released lock on %s", workerID, lockKey)

						time.Sleep(time.Duration(tt.afterWorkTimeMs) * time.Millisecond)
					}
				}(i)
			}
			wg.Wait()
			lg.Debug(ctx, "<<< Completed test: %s", tt.name)
		})
	}
	return nil
}
