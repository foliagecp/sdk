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

type statefunTestEnvironment struct {
	srv        *server.Server
	runtime    *statefun.Runtime
	runtimeCfg *statefun.RuntimeConfig
	cacheCfg   *cache.Config
	nc         *nats.Conn
	jsStoreDir string
}

func newStatefunTestEnvironment() *statefunTestEnvironment {
	srv, jsStoreDir := runServer()
	// Single-instance (always-active) for tests: unit/integration suites do not
	// exercise HA, and the active/passive KV-lock dance is what makes parallel
	// `go test ./...` flaky ("Lost runtime lock, becoming passive") when many
	// embedded-NATS runtimes contend. Disabling it makes the harness deterministic.
	runtimeConfig := statefun.NewRuntimeConfigSimple(srv.ClientURL(), defaultRuntimeName).SetActivePassiveMode(false)
	cacheConfig := cache.NewCacheConfig(defaultCacheID)

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

	go func() {
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

func (env *statefunTestEnvironment) RegisterFunction(name string, handler statefun.FunctionLogicHandler, cfg statefun.FunctionTypeConfig) {
	statefun.NewFunctionType(env.runtime, name, handler, cfg)
}

func (env *statefunTestEnvironment) OnAfterStartFunction(f statefun.OnAfterStartFunction, async bool) {
	env.runtime.RegisterOnAfterStartFunction(f, async)
}

func (env *statefunTestEnvironment) Stop() {
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
