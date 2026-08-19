package test

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	"github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats-server/v2/server"
	natsservertest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

var (
	defaultRuntimeName = "test_app"
	defaultCacheID     = "test_cache"
)

// RuntimeConfigMutatorForTest, when non-nil, is applied to every new test
// environment's RuntimeConfig right before the runtime is built. Set it from
// TestMain, never mid-suite — the environment is recreated per test method.
// Leak tests use it to shorten GC cadence (SetGCIntervalSec /
// SetFunctionTypeIDLifetimeMs) so idle per-id machinery is reclaimed within
// the test window.
var RuntimeConfigMutatorForTest func(*statefun.RuntimeConfig)

type statefunTestEnvironment struct {
	srv         *server.Server
	runtime     *statefun.Runtime
	runtimeCfg  *statefun.RuntimeConfig
	cacheCfg    *cache.Config
	nc          *nats.Conn
	jsStoreDir  string
	runtimeDone chan struct{} // closed when the Start() goroutine returns
}

func newStatefunTestEnvironment() *statefunTestEnvironment {
	srv, jsStoreDir := runServer()
	// Single-instance (always-active) for tests: unit/integration suites do not
	// exercise HA, and the active/passive KV-lock dance is what makes parallel
	// `go test ./...` flaky ("Lost runtime lock, becoming passive") when many
	// embedded-NATS runtimes contend. Disabling it makes the harness deterministic.
	runtimeConfig := statefun.NewRuntimeConfigSimple(srv.ClientURL(), defaultRuntimeName).
		SetActivePassiveMode(false).
		// Generous JetStream API timeout: under the full suite with -race a single
		// embedded NATS server is saturated, and KV-mutex Put/Update otherwise
		// time out at the ~5s nats default, making CMDB ops return "failed".
		SetNatsAPITimeoutSec(30)
	cacheConfig := cache.NewCacheConfig(defaultCacheID)

	if RuntimeConfigMutatorForTest != nil {
		RuntimeConfigMutatorForTest(runtimeConfig)
	}

	return &statefunTestEnvironment{
		srv:        srv,
		nc:         mustNatsConn(srv.ClientURL()),
		runtime:    mustNewRuntime(*runtimeConfig),
		runtimeCfg: runtimeConfig,
		cacheCfg:   cacheConfig,
		jsStoreDir: jsStoreDir,
	}
}

func (env *statefunTestEnvironment) StartRuntime() error {
	errChan := make(chan error, 1)
	env.runtimeDone = make(chan struct{})

	go func() {
		defer close(env.runtimeDone)
		if err := env.runtime.Start(context.TODO(), env.cacheCfg); err != nil {
			errChan <- err
		}
	}()

	// Wait until the runtime is actually READY, not a blind 1s sleep. Runtime
	// sets r.isReady (atomic) only AFTER Domain.start() has built and published
	// the cache store; observing IsReady()==true establishes a happens-before
	// edge with the Start goroutine, so the test's subsequent Domain.Cache()
	// reads are properly synchronized. The old time.After(1s) provided no such
	// edge, which raced Domain.start()'s `dm.cache = ...` write against the
	// test's Cache() read under heavy -race load.
	deadline := time.After(30 * time.Second)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-errChan:
			return err
		case <-deadline:
			return fmt.Errorf("runtime did not become ready within 30s")
		case <-ticker.C:
			if env.runtime.IsReady() {
				return nil
			}
		}
	}
}

func (env *statefunTestEnvironment) Runtime() *statefun.Runtime {
	return env.runtime
}

// NatsURL is the address of the embedded broker. Several applications sharing
// one domain is the normal shape of a Foliage deployment — one provides the
// CRUD layer, the others attach to the same graph — so a test must be able to
// start a SECOND runtime next to this environment's own.
func (env *statefunTestEnvironment) NatsURL() string {
	return env.srv.ClientURL()
}

func (env *statefunTestEnvironment) RegisterFunction(name string, handler statefun.FunctionLogicHandler, cfg statefun.FunctionTypeConfig) {
	statefun.NewFunctionType(env.runtime, name, handler, cfg)
}

func (env *statefunTestEnvironment) OnAfterStartFunction(f statefun.OnAfterStartFunction, async bool) {
	env.runtime.RegisterOnAfterStartFunction(f, async)
}

func (env *statefunTestEnvironment) Stop() {
	// Stop the RUNTIME first, then NATS. Start() runs in a goroutine, so without
	// an explicit shutdown the runtime's background goroutines (worker pools,
	// kvLazyWriter, backup-barrier refresh, lifecycle ticker, transaction
	// committer) outlive the test and keep spinning/retrying against the
	// about-to-die NATS. Across ~30 suites in a package those leaked runtimes
	// accumulate and starve the next suite's embedded NATS of CPU — the source
	// of the non-deterministic "context deadline exceeded" failures.
	//
	// Use the EMERGENCY shutdown: each test gets a throwaway runtime, so there
	// is nothing to drain cleanly, and the graceful path would otherwise cost
	// ~10s per test (it waits for the GC to declare functions idle: 5s function
	// lifetime + 5s GC interval). With 60+ tests in a package that blows past
	// the 10m `go test` package timeout. Emergency cancels all contexts and
	// releases the main loop at once; Start() then closes r.nc and returns,
	// which closes runtimeDone — usually well under a second.
	if env.runtimeDone != nil {
		env.runtime.Shutdown(true)
		select {
		case <-env.runtimeDone:
		case <-time.After(30 * time.Second):
		}
	}
	// Close the test's own NATS connection. Like the runtime's, it is opened
	// with MaxReconnect=-1, so if left open it reconnect-storms forever once the
	// embedded server is shut down, leaking its subscription goroutines.
	env.nc.Close()
	system.MsgOnErrorReturn(env.cleanJetStreamStorage())
	env.srv.Shutdown()
	env.srv.WaitForShutdown()
}

func (env *statefunTestEnvironment) Request(provider plugins.RequestProvider, typename, id string, payload, options *easyjson.JSON, timeout ...time.Duration) (*easyjson.JSON, error) {
	return env.runtime.Request(provider, typename, id, payload, options)
}

func (env *statefunTestEnvironment) Signal(provider plugins.SignalProvider, typename, id string, payload, options *easyjson.JSON) error {
	return env.runtime.Signal(provider, typename, id, payload, options)
}

func (env *statefunTestEnvironment) Publish(subj string, data []byte) error {
	return env.nc.Publish(subj, data)
}

func (env *statefunTestEnvironment) Subscribe(subj string, h nats.MsgHandler) (*nats.Subscription, error) {
	return env.nc.Subscribe(subj, h)
}

func (env *statefunTestEnvironment) SubscribeSync(subj string) (*nats.Subscription, error) {
	return env.nc.SubscribeSync(subj)
}

func (env *statefunTestEnvironment) SubscribeEgress(typename, id string) (*nats.Subscription, error) {
	subj := fmt.Sprintf(`egress.%s.%s`, typename, env.SetThisDomainPreffix(id))

	sub, err := env.nc.SubscribeSync(subj)
	if err != nil {
		return nil, err
	}
	return sub, nil
}

func (env *statefunTestEnvironment) CacheValue(key string) (*easyjson.JSON, error) {
	id := env.runtime.Domain.CreateObjectIDWithThisDomain(key, true)
	return env.runtime.Domain.Cache().GetValueJSON(id)
}

func (env *statefunTestEnvironment) SetThisDomainPreffix(id string) string {
	return env.runtime.Domain.CreateObjectIDWithThisDomain(id, true)
}

func (env *statefunTestEnvironment) cleanJetStreamStorage() error {
	// Remove the exact temp dir we created for this broker. Relying on
	// JetStreamConfig().StoreDir leaks the empty parent because nats reports the
	// "<dir>/jetstream" subdir, so RemoveAll on it leaves "<dir>" behind.
	if env.jsStoreDir != "" {
		if err := os.RemoveAll(env.jsStoreDir); err != nil {
			return fmt.Errorf("unable to clean storage: %w", err)
		}
		return nil
	}

	// Fallback (no tracked dir): clean whatever the server reports.
	var sd string
	if config := env.srv.JetStreamConfig(); config != nil {
		sd = config.StoreDir
	}
	if sd != "" {
		if err := os.RemoveAll(sd); err != nil {
			return fmt.Errorf("unable to clean storage: %w", err)
		}
	}

	return nil
}

// runServer starts an embedded NATS with JetStream and returns the server plus
// the unique store dir it owns (empty if a temp dir could not be created).
func runServer() (*server.Server, string) {
	opts := natsservertest.DefaultTestOptions
	opts.JetStream = true
	// random port
	opts.Port = -1
	// Unique JetStream store per broker instance. DefaultTestOptions uses a
	// FIXED shared dir ($TMPDIR/nats/jetstream); pointing two independent
	// embedded brokers at one dir — parallel `go test`, or a killed-but-lingering
	// broker plus a fresh one — makes nats-server nil-deref during JetStream
	// recovery (checkForOrphanMsgs/checkStateForInterestStream). Each broker is a
	// distinct server and, like a production NATS with its own volume, must own
	// its store. Single-broker restart-on-the-same-store recovery is unaffected;
	// Stop() removes this dir.
	storeDir := ""
	if dir, err := os.MkdirTemp("", "foliage-nats-js-"); err == nil {
		opts.StoreDir = dir
		storeDir = dir
	}

	return natsservertest.RunServer(&opts), storeDir
}

func mustNewRuntime(cfg statefun.RuntimeConfig) *statefun.Runtime {
	r, err := statefun.NewRuntime(cfg)
	if err != nil {
		panic(err)
	}
	return r
}

func mustNatsConn(url string) *nats.Conn {
	nc, err := nats.Connect(url)
	if err != nil {
		panic(err)
	}
	return nc
}
