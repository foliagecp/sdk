package test

import (
	"fmt"
	"os"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	"github.com/foliagecp/sdk/statefun/plugins"
	"github.com/nats-io/nats-server/v2/server"
	natsservertest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

var (
	defaultNATSPort    = 10000
	defaultRuntimeName = "test_app"
	defaultCacheID     = "test_cache"
)

type statefunTestEnvironment struct {
	srv        *server.Server
	runtime    *statefun.Runtime
	runtimeCfg *statefun.RuntimeConfig
	cacheCfg   *cache.Config
	nc         *nats.Conn
}

func newStatefunTestEnvironment() *statefunTestEnvironment {
	srv := runServer()
	runtimeConfig := statefun.NewRuntimeConfigSimple(srv.ClientURL(), defaultRuntimeName)
	cacheConfig := cache.NewCacheConfig(defaultCacheID)

	return &statefunTestEnvironment{
		srv:        srv,
		nc:         mustNatsConn(srv.ClientURL()),
		runtime:    mustNewRuntime(*runtimeConfig),
		runtimeCfg: runtimeConfig,
		cacheCfg:   cacheConfig,
	}
}

func (env *statefunTestEnvironment) StartRuntime() error {
	errChan := make(chan error, 1)

	go func() {
		if err := env.runtime.Start(env.cacheCfg); err != nil {
			errChan <- err
		}
	}()

	select {
	case err := <-errChan:
		return err
	case <-time.After(1 * time.Second):
	}

	return nil
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
	env.cleanJetStreamStorage()
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
	return env.runtime.Domain.Cache().GetValueAsJSON(id)
}

func (env *statefunTestEnvironment) SetThisDomainPreffix(id string) string {
	return env.runtime.Domain.CreateObjectIDWithThisDomain(id, true)
}

func (env *statefunTestEnvironment) cleanJetStreamStorage() error {
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

func runServer() *server.Server {
	opts := natsservertest.DefaultTestOptions
	opts.JetStream = true
	// random port
	opts.Port = -1

	return natsservertest.RunServer(&opts)
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
