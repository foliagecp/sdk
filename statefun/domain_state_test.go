package statefun

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun/cache"
	sfp "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats-server/v2/server"
	ntest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func domainTestNode(t *testing.T, domain string) (*nats.Conn, nats.JetStreamContext, *server.Server) {
	t.Helper()
	opts := ntest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.JetStreamDomain = domain
	opts.StoreDir = t.TempDir()
	srv := ntest.RunServer(&opts)
	t.Cleanup(srv.Shutdown)
	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	require.NoError(t, err)
	return nc, js, srv
}
func addDomainTestStream(t *testing.T, js nats.JetStreamContext, name string, subjects ...string) {
	t.Helper()
	_, err := js.AddStream(&nats.StreamConfig{Name: name, Subjects: subjects, Storage: nats.MemoryStorage})
	require.NoError(t, err)
}
func domainTestRuntime(t *testing.T, srv *server.Server) *Runtime {
	t.Helper()
	cfg := NewRuntimeConfigSimple(srv.ClientURL(), "domain-test").UseJSDomainAsHubDomainName().SetActivePassiveMode(false)
	rt, err := NewRuntime(*cfg)
	require.NoError(t, err)
	t.Cleanup(rt.nc.Close)
	return rt
}
func TestDomainState_FreshTargetStartsAndServes(t *testing.T) {
	if system.GlobalPrometrics == nil {
		system.GlobalPrometrics = system.NewPrometrics("", "")
	}
	_, js, srv := domainTestNode(t, "domain_new")
	rt := domainTestRuntime(t, srv)
	NewFunctionType(rt, "test.domain.echo", func(_ sfp.StatefunExecutor, ctx *sfp.StatefunContextProcessor) { ctx.Reply.With(ctx.Payload) }, *NewFunctionTypeConfig().SetAllowedRequestProviders(sfp.NatsCoreGlobalRequest))
	ready := make(chan struct{})
	rt.RegisterOnAfterStartFunction(func(context.Context, *Runtime) error { close(ready); return nil }, false)
	done := make(chan error, 1)
	go func() { done <- rt.Start(context.Background(), cache.NewCacheConfig("domain-test")) }()
	t.Cleanup(func() { rt.Shutdown(true) })
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("startup failed: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("startup timed out")
	}
	result, err := rt.Request(sfp.NatsCoreGlobalRequest, "test.domain.echo", "x", easyjson.NewJSONObjectWithKeyValue("value", easyjson.NewJSON("ok")).GetPtr(), nil, time.Second)
	require.NoError(t, err)
	require.Equal(t, "ok", result.GetByPath("value").AsStringDefault(""))
	require.NoError(t, rt.validateDomainState(context.Background(), "domain-test"))
	info, err := js.StreamInfo(domainEgressStreamName)
	require.NoError(t, err)
	require.Equal(t, []string{"$SE.domain_new.>"}, info.Config.Subjects)
}
func TestDomainState_RefusesStaleStateWithoutMutation(t *testing.T) {
	for _, routers := range []bool{true, false} {
		for _, kind := range []string{"egress", "ingress", "trace", "consumer", "consumer-group", "transform", "cache"} {
			t.Run(fmt.Sprintf("%s/routers=%v", kind, routers), func(t *testing.T) {
				_, js, srv := domainTestNode(t, "domain_new")
				switch kind {
				case "egress":
					addDomainTestStream(t, js, domainEgressStreamName, "$SE.domain_old.>")
				case "trace":
					addDomainTestStream(t, js, domainTraceStreamName, "trace.domain_old.events.>")
				case "ingress":
					addDomainTestStream(t, js, hubEventStreamName, "signal.>")
					_, err := js.AddStream(&nats.StreamConfig{Name: domainIngressStreamName, Sources: []*nats.StreamSource{{Name: hubEventStreamName, FilterSubject: "signal.domain_old.>"}}})
					require.NoError(t, err)
				case "transform":
					_, err := js.AddStream(&nats.StreamConfig{Name: domainEgressStreamName, Subjects: []string{"$SE.domain_new.>"}, SubjectTransform: &nats.SubjectTransformConfig{Source: "$SE.domain_new.>", Destination: "$SE.domain_old.>"}})
					require.NoError(t, err)
				case "consumer-group":
					addDomainTestStream(t, js, domainEgressStreamName, "$SE.domain_new.>")
					_, err := js.AddConsumer(domainEgressStreamName, &nats.ConsumerConfig{Durable: "domain_egress-domain_new-consumer", FilterSubject: "$SE.domain_new.>", DeliverSubject: "domain_old.old.delivery", DeliverGroup: "domain_old-old-group", AckPolicy: nats.AckExplicitPolicy})
					require.NoError(t, err)
				case "consumer":
					addDomainTestStream(t, js, domainEgressStreamName, "$SE.domain_new.>")
					_, err := js.AddConsumer(domainEgressStreamName, &nats.ConsumerConfig{Durable: "domain_egress-domain_old-consumer", FilterSubject: "$SE.domain_old.>", AckPolicy: nats.AckExplicitPolicy})
					require.NoError(t, err)
				case "cache":
					_, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "domain_old_domain-test_cache_bucket"})
					require.NoError(t, err)
				}
				before, err := domainInventory(context.Background(), js)
				require.NoError(t, err)
				rt := domainTestRuntime(t, srv)
				rt.config.handlesDomainRouters = routers
				err = rt.Start(context.Background(), cache.NewCacheConfig("domain-test"))
				require.ErrorIs(t, err, ErrDomainStateMismatch)
				require.Contains(t, err.Error(), "domain_new")
				if kind != "transform" {
					require.Contains(t, err.Error(), "domain_old")
				}
				after, err := domainInventory(context.Background(), js)
				require.NoError(t, err)
				require.Len(t, after, len(before))
				for i := range before {
					require.Equal(t, before[i].Config, after[i].Config)
					require.Equal(t, before[i].State.Msgs, after[i].State.Msgs)
				}
			})
		}
	}
}
func TestDomainState_RemoteHubAndLimitsAreNotLocalDomainMismatch(t *testing.T) {
	info := &nats.StreamInfo{Config: nats.StreamConfig{Name: domainIngressStreamName, Sources: []*nats.StreamSource{{Name: hubEventStreamName, FilterSubject: "signal.edge.>", External: &nats.ExternalStream{APIPrefix: "$JS.hub.API"}}}}}
	require.NoError(t, checkDomainStream(info, "edge", "hub"))
	info.Config.Sources[0].External.APIPrefix = "$JS.other.API"
	require.ErrorIs(t, checkDomainStream(info, "edge", "hub"), ErrDomainStateMismatch)
	require.NoError(t, checkDomainStream(&nats.StreamInfo{Config: nats.StreamConfig{Name: domainEgressStreamName, Subjects: []string{"$SE.edge.>"}, MaxMsgs: 1}}, "edge", "hub"))
}
func TestDomainState_CancelledReadDoesNotPass(t *testing.T) {
	_, _, srv := domainTestNode(t, "domain_new")
	rt := domainTestRuntime(t, srv)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, rt.validateDomainState(ctx, "test"))
}

func TestDomainState_CacheIDsWithCommonSuffixRemainCompatible(t *testing.T) {
	for _, domain := range []string{"domain_new", "domain_new_extra"} {
		t.Run(domain, func(t *testing.T) {
			_, js, srv := domainTestNode(t, domain)
			for _, id := range []string{"app", "extra_app"} {
				_, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: domain + "_" + id + "_cache_bucket"})
				require.NoError(t, err)
			}
			rt := domainTestRuntime(t, srv)
			require.NoError(t, rt.validateDomainState(context.Background(), "app"))
		})
	}
}
