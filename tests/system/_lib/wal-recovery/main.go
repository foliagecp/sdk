// Command wal-recovery is a disposable system-test fixture, not an SDK admin tool.
// It reproduces a real broker crash and round-trips the resulting stopped store
// through an archive. It never modifies a consumer state file or existing data.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/foliagecp/easyjson"
	customNatsKv "github.com/foliagecp/sdk/embedded/nats/kv"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	sfp "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats.go"
)

const (
	bucket   = "restoretest_recovery_cache_bucket"
	consumer = statefun.CommitterDurableName + "-" + bucket
	subject  = "wal.commits." + bucket
	image    = "nats:2.12.4"
	prefix   = "foliage-systest-wal-ack-recovery-"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("expected selftest or a fixture phase")
	}
	if os.Args[1] == "selftest" {
		return selftest()
	}
	if len(os.Args) < 3 {
		return fmt.Errorf("fixture phase needs its private NATS URL")
	}
	url := os.Args[2]
	nc, err := nats.Connect(url, nats.Timeout(5*time.Second))
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		return err
	}
	switch os.Args[1] {
	case "seed":
		return seed(nc, js)
	case "crash-sdk", "crash-legacy":
		if len(os.Args) < 4 {
			return fmt.Errorf("crash phase needs its private container")
		}
		return crash(nc, js, url, os.Args[3], os.Args[1] == "crash-legacy")
	case "verify":
		return verify(nc, js, url, false)
	case "verify-legacy":
		return verify(nc, js, url, true)
	}
	return fmt.Errorf("unknown fixture phase")
}
func consumerConfig() *nats.ConsumerConfig {
	return &nats.ConsumerConfig{Name: consumer, Durable: consumer, DeliverSubject: consumer, DeliverGroup: consumer + "-group", FilterSubject: subject, AckPolicy: nats.AckExplicitPolicy, AckWait: 5 * time.Second, MaxDeliver: 5, MaxAckPending: 1}
}
func put(key, value string) cache.WALOp {
	return cache.WALOp{OpType: cache.OpTypePUT, Key: "store.restoretest/" + key, Value: []byte(value)}
}
func seed(nc *nats.Conn, js nats.JetStreamContext) error {
	if _, err := js.AddStream(&nats.StreamConfig{Name: statefun.WALCommitsStreamName, Subjects: []string{statefun.WALCommitsSubject}, Retention: nats.WorkQueuePolicy, Storage: nats.FileStorage, MaxAge: 24 * time.Hour}); err != nil {
		return err
	}
	kv, err := customNatsKv.CreateKeyValue(nc, js, &nats.KeyValueConfig{Bucket: bucket})
	if err != nil {
		return err
	}
	// A second bucket's retained record keeps the stream's first sequence below
	// the orphan. This matters: a missing interior record is the stuck case.
	if _, err = js.Publish("wal.commits.other_bucket", []byte("another application's pending transaction")); err != nil {
		return err
	}
	transactions := [][]cache.WALOp{
		{put("baseline", "base"), put("value", "old"), put("deleted", "old")},
		{put("value", "final"), {OpType: cache.OpTypeDelete, Key: "store.restoretest/deleted"}},
		{put("tail", "present"), put("value.out.to.rel", "restoretest/target"), put("value.out.index.rel.type.kind", "")},
	}
	for i, ops := range transactions {
		msg := nats.NewMsg(subject)
		msg.Header.Set("tx_id", strconv.Itoa(i+2))
		msg.Data, err = json.Marshal(ops)
		if err != nil {
			return err
		}
		if _, err = js.PublishMsg(msg); err != nil {
			return err
		}
	}
	if _, err = js.AddConsumer(statefun.WALCommitsStreamName, consumerConfig()); err != nil {
		return err
	}
	sub, err := js.QueueSubscribeSync(subject, consumer+"-group", nats.Bind(statefun.WALCommitsStreamName, consumer), nats.ManualAck())
	if err != nil {
		return err
	}
	first, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		return err
	}
	if err = apply(kv, first); err != nil {
		return err
	}
	pending, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		return err
	}
	meta, err := pending.Metadata()
	if err != nil {
		return err
	}
	if meta.Sequence.Stream != 3 {
		return fmt.Errorf("seed did not leave sequence 3 pending")
	}
	fmt.Println("Seeded complete KV baseline; WAL sequence 3 delivered and unacknowledged")
	return nil
}
func apply(kv nats.KeyValue, msg *nats.Msg) error {
	var ops []cache.WALOp
	if err := json.Unmarshal(msg.Data, &ops); err != nil {
		return err
	}
	for _, op := range ops {
		if op.OpType == cache.OpTypeDelete {
			if err := kv.Delete(op.Key); err != nil {
				return err
			}
		} else {
			if _, err := kv.Put(op.Key, op.Value); err != nil {
				return err
			}
		}
	}
	return msg.AckSync()
}
func runtime(url string) (*statefun.Runtime, error) {
	system.GlobalPrometrics = system.NewPrometrics("", "")
	rt, err := statefun.NewRuntime(*statefun.NewRuntimeConfigSimple(url, "wal-recovery-test").UseJSDomainAsHubDomainName().SetActivePassiveMode(false))
	if err != nil {
		return nil, err
	}
	statefun.NewFunctionType(rt, "test.wal.read", func(_ sfp.StatefunExecutor, ctx *sfp.StatefunContextProcessor) {
		value, err := rt.Domain.Cache().GetValue("restoretest/value")
		if err != nil {
			ctx.Reply.With(easyjson.NewJSONObjectWithKeyValue("error", easyjson.NewJSON(err.Error())).GetPtr())
			return
		}
		ctx.Reply.With(easyjson.NewJSONObjectWithKeyValue("value", easyjson.NewJSON(string(value))).GetPtr())
	}, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfp.NatsCoreGlobalRequest).SetMultipleInstancesAllowance(true))
	return rt, nil
}
func crash(nc *nats.Conn, js nats.JetStreamContext, url, name string, legacy bool) error {
	if !strings.HasPrefix(name, prefix) {
		return fmt.Errorf("refusing to kill a non-test container")
	}
	endpoint, err := docker("context", "inspect", "--format", "{{.Endpoints.docker.Host}}")
	if err != nil {
		return err
	}
	if configured := os.Getenv("DOCKER_HOST"); configured != "" {
		endpoint = configured
	}
	if !strings.HasPrefix(endpoint, "unix://") {
		return fmt.Errorf("crash test requires a local Unix Docker endpoint")
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", strings.TrimPrefix(endpoint, "unix://"))
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	if legacy {
		sub, err := js.QueueSubscribeSync(subject, consumer+"-group", nats.Bind(statefun.WALCommitsStreamName, consumer), nats.ManualAck())
		if err != nil {
			return err
		}
		msg, err := sub.NextMsg(8 * time.Second)
		if err != nil {
			return err
		}
		time.Sleep(20 * time.Millisecond)
		kv, err := js.KeyValue(bucket)
		if err != nil {
			return err
		}
		if err = apply(kv, msg); err != nil {
			return err
		}
	} else {
		rt, err := runtime(url)
		if err != nil {
			return err
		}
		done := make(chan error, 1)
		go func() { done <- rt.Start(context.Background(), cache.NewCacheConfig("recovery")) }()
		deadline := time.Now().Add(15 * time.Second)
		for {
			_, err = js.GetMsg(statefun.WALCommitsStreamName, 3)
			if errors.Is(err, nats.ErrMsgNotFound) {
				break
			}
			if err != nil {
				return err
			}
			select {
			case err = <-done:
				return fmt.Errorf("runtime stopped during crash fixture: %v", err)
			default:
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("SDK never applied the fixture transaction")
			}
			time.Sleep(time.Millisecond)
		}
	}
	kv, err := js.KeyValue(bucket)
	if err != nil {
		return err
	}
	entry, err := kv.Get("store.restoretest/value")
	if err != nil {
		return err
	}
	if string(entry.Value()) != "final" {
		return fmt.Errorf("WAL disappeared before KV apply completed")
	}
	resp, err := client.Post("http://docker/containers/"+name+"/kill?signal=KILL", "", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("container kill returned %s", resp.Status)
	}
	fmt.Printf("KV apply and WAL acknowledgement completed; actual NATS SIGKILL (legacy=%v)\n", legacy)
	return nil
}
func verify(nc *nats.Conn, js nats.JetStreamContext, url string, legacy bool) error {
	rt, err := runtime(url)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- rt.Start(ctx, cache.NewCacheConfig("recovery")) }()
	for !rt.IsReady() {
		select {
		case err = <-done:
			if !legacy {
				return fmt.Errorf("recovery startup: %w", err)
			}
			if !errors.Is(err, statefun.ErrWALRecovery) {
				return fmt.Errorf("legacy backup did not return an explicit recovery error: %v", err)
			}
			info, e := js.ConsumerInfo(statefun.WALCommitsStreamName, consumer)
			if e != nil {
				return e
			}
			if info.NumAckPending != 1 {
				return fmt.Errorf("unproven pending ACK was altered")
			}
			fmt.Println("PASS: legacy orphan refused without acknowledging or deleting transaction data")
			return nil
		case <-ctx.Done():
			return fmt.Errorf("recovery startup exceeded its deadline: %w", ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
	if legacy {
		return fmt.Errorf("legacy orphan was silently accepted")
	}
	for key, value := range map[string]string{"baseline": "base", "value": "final", "tail": "present", "value.out.to.rel": "restoretest/target", "value.out.index.rel.type.kind": ""} {
		entry, e := rt.Domain.Cache().GetValue("restoretest/" + key)
		if e != nil {
			return fmt.Errorf("restored cache key %s: %w", key, e)
		}
		if string(entry) != value {
			return fmt.Errorf("restored cache key %s differs: %q", key, entry)
		}
		kv, e := js.KeyValue(bucket)
		if e != nil {
			return e
		}
		persisted, e := kv.Get("store.restoretest/" + key)
		if e != nil {
			return e
		}
		if string(persisted.Value()) != value {
			return fmt.Errorf("restored KV key %s differs", key)
		}
	}
	if cached, e := rt.Domain.Cache().GetValue("restoretest/deleted"); e == nil {
		kv, ke := js.KeyValue(bucket)
		if ke != nil {
			return ke
		}
		entry, pe := kv.Get("store.restoretest/deleted")
		var persisted string
		if entry != nil {
			persisted = string(entry.Value())
		}
		return fmt.Errorf("deleted value was resurrected: cache=%q KV=%q KV_error=%v", cached, persisted, pe)
	}
	response, err := rt.Request(sfp.NatsCoreGlobalRequest, "test.wal.read", "probe", nil, nil, 3*time.Second)
	if err != nil {
		return err
	}
	if response.GetByPath("value").AsStringDefault("") != "final" {
		return fmt.Errorf("restored runtime served a stale value")
	}
	opTime := system.GetCurrentTimeNs()
	store := rt.Domain.Cache()
	store.MarkOperationActive(opTime)
	ok := store.SetValue("restoretest/after-recovery", []byte("still-writable"), true, opTime)
	store.MarkOperationDone(opTime)
	if !ok {
		return fmt.Errorf("restored runtime refused a new write")
	}
	if err = rt.Domain.WaitForKVCaughtUp(ctx, 10*time.Second); err != nil {
		return err
	}
	kv, err := js.KeyValue(bucket)
	if err != nil {
		return err
	}
	entry, err := kv.Get("store.restoretest/after-recovery")
	if err != nil {
		return err
	}
	if string(entry.Value()) != "still-writable" {
		return fmt.Errorf("post-recovery transaction did not reach KV")
	}
	info, err := js.ConsumerInfo(statefun.WALCommitsStreamName, consumer)
	if err != nil {
		return err
	}
	if info.NumPending != 0 || info.NumAckPending != 0 {
		return fmt.Errorf("recovered WAL did not drain")
	}
	stopped := make(chan struct{})
	go func() { rt.Shutdown(true); close(stopped) }()
	select {
	case <-stopped:
	case <-ctx.Done():
		return fmt.Errorf("restored runtime did not shut down")
	}
	select {
	case err = <-done:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return fmt.Errorf("runtime Start did not return after shutdown")
	}
	fmt.Println("PASS: original consumer recovered; KV/cache/DELETE/empty values/request/new write/restart verified")
	return nil
}

func command(timeout time.Duration, name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("%s failed: %w\n%s", name, err, out)
	}
	return strings.TrimSpace(string(out)), nil
}
func docker(args ...string) (string, error) { return command(60*time.Second, "docker", args...) }
func child(args ...string) error {
	out, err := command(45*time.Second, os.Args[0], args...)
	fmt.Println(out)
	return err
}
func selftest() error {
	dir, err := os.MkdirTemp("", "foliage-wal-recovery-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	config := filepath.Join(dir, "nats.conf")
	if err = os.WriteFile(config, []byte("port: 4222\njetstream {\n store_dir: /data\n domain: restoretest\n}\n"), 0600); err != nil {
		return err
	}
	for _, legacy := range []bool{false, true} {
		found := false
		for trial := 1; trial <= 6; trial++ {
			fmt.Printf("Crash/backup/restore scenario: legacy=%v attempt=%d\n", legacy, trial)
			reproduced, e := trialCase(dir, config, legacy, trial)
			if e != nil {
				return e
			}
			if reproduced {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("broker crash window was not exercised in six attempts (legacy=%v); recovery not verified", legacy)
		}
	}
	fmt.Println("wal-ack-recovery: PASS")
	return nil
}
func create(name, config string) error {
	_, err := docker("create", "--name", name, "-p", "127.0.0.1::4222", "-v", config+":/etc/nats/nats.conf:ro", image, "-c", "/etc/nats/nats.conf")
	return err
}
func start(name string) (string, error) {
	if _, err := docker("start", name); err != nil {
		return "", err
	}
	address, err := docker("port", name, "4222/tcp")
	if err != nil {
		return "", err
	}
	url := "nats://" + address
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		nc, err := nats.Connect(url, nats.Timeout(200*time.Millisecond), nats.NoReconnect())
		if err == nil {
			nc.Close()
			return url, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "", fmt.Errorf("test NATS did not listen")
}
func ghost(url string) (bool, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return false, err
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		return false, err
	}
	info, err := js.ConsumerInfo(statefun.WALCommitsStreamName, consumer)
	if err != nil {
		return false, err
	}
	if info.NumAckPending != 1 {
		return false, nil
	}
	_, err = js.GetMsg(statefun.WALCommitsStreamName, info.Delivered.Stream)
	if !errors.Is(err, nats.ErrMsgNotFound) {
		return false, err
	}
	time.Sleep(150 * time.Millisecond)
	after, err := js.ConsumerInfo(statefun.WALCommitsStreamName, consumer)
	if err != nil {
		return false, err
	}
	if after.NumAckPending != 1 || after.Delivered.Stream != info.Delivered.Stream {
		return false, nil
	}
	fmt.Printf("Actual orphan reproduced: delivered=%d floor=%d pending=%d ack_pending=%d\n", after.Delivered.Stream, after.AckFloor.Stream, after.NumPending, after.NumAckPending)
	return true, nil
}
func trialCase(dir, config string, legacy bool, trial int) (bool, error) {
	name := fmt.Sprintf("%s%d-%v-%d", prefix, os.Getpid(), legacy, trial)
	restored := name + "-restored"
	defer func() { _, _ = docker("rm", "-fv", name); _, _ = docker("rm", "-fv", restored) }()
	if err := create(name, config); err != nil {
		return false, err
	}
	url, err := start(name)
	if err != nil {
		return false, err
	}
	if err = child("seed", url); err != nil {
		return false, err
	}
	if _, err = docker("stop", "-t", "10", name); err != nil {
		return false, err
	}
	url, err = start(name)
	if err != nil {
		return false, err
	}
	phase := "crash-sdk"
	if legacy {
		phase = "crash-legacy"
	}
	if err = child(phase, url, name); err != nil {
		return false, err
	}
	url, err = start(name)
	if err != nil {
		return false, err
	}
	found, err := ghost(url)
	if err != nil || !found {
		return found, err
	}
	if _, err = docker("stop", "-t", "30", name); err != nil {
		return false, err
	}
	copyDir := filepath.Join(dir, fmt.Sprintf("copy-%v-%d", legacy, trial))
	restoreDir := copyDir + "-restored"
	if err = os.MkdirAll(copyDir, 0700); err != nil {
		return false, err
	}
	if err = os.MkdirAll(restoreDir, 0700); err != nil {
		return false, err
	}
	if _, err = docker("cp", name+":/data/.", copyDir); err != nil {
		return false, err
	}
	archive := copyDir + ".tar.gz"
	if _, err = command(30*time.Second, "tar", "-czf", archive, "-C", copyDir, "."); err != nil {
		return false, err
	}
	if _, err = command(30*time.Second, "tar", "-xzf", archive, "-C", restoreDir); err != nil {
		return false, err
	}
	if err = create(restored, config); err != nil {
		return false, err
	}
	if _, err = docker("cp", restoreDir+"/.", restored+":/data"); err != nil {
		return false, err
	}
	url, err = start(restored)
	if err != nil {
		return false, err
	}
	restoredGhost, err := ghost(url)
	if err != nil {
		return false, err
	}
	if !restoredGhost {
		return false, fmt.Errorf("backup did not preserve the reproduced orphan")
	}
	phase = "verify"
	if legacy {
		phase = "verify-legacy"
	}
	if err = child(phase, url); err != nil {
		return false, err
	}
	if !legacy {
		if _, err = docker("stop", "-t", "30", restored); err != nil {
			return false, err
		}
		url, err = start(restored)
		if err != nil {
			return false, err
		}
		if err = child("verify", url); err != nil {
			return false, err
		}
	}
	return true, nil
}
