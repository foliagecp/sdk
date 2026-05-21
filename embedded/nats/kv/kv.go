// Copyright 2021-2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	kvBucketNamePre         = "KV_"
	kvBucketNameTmpl        = "KV_%s"
	kvSubjectsTmpl          = "$KV.%s.>"
	kvSubjectsPreTmpl       = "$KV.%s."
	kvSubjectsPreDomainTmpl = "%s.$KV.%s."
	kvNoPending             = "0"

	_EMPTY_          = ""
	kvLatestRevision = 0

	kvop    = "KV-Operation"
	kvdel   = "DEL"
	kvpurge = "PURGE"
)

var (
	validBucketRe = regexp.MustCompile(`\A[a-zA-Z0-9_-]+\z`)
	semVerRe      = regexp.MustCompile(`\Av?([0-9]+)\.?([0-9]+)?\.?([0-9]+)?`)
	validKeyRe    = regexp.MustCompile(`\A[-/_$#@%+=\.a-zA-Z0-9]+\z`)
)

func versionComponents(version string) (major, minor, patch int, err error) {
	m := semVerRe.FindStringSubmatch(version)
	if m == nil {
		return 0, 0, 0, errors.New("invalid semver")
	}
	major, err = strconv.Atoi(m[1])
	if err != nil {
		return -1, -1, -1, err
	}
	minor, err = strconv.Atoi(m[2])
	if err != nil {
		return -1, -1, -1, err
	}
	patch, err = strconv.Atoi(m[3])
	if err != nil {
		return -1, -1, -1, err
	}
	return major, minor, patch, err
}

func serverMinVersion(nc *nats.Conn, major, minor, patch int) bool {
	smajor, sminor, spatch, _ := versionComponents(nc.ConnectedServerVersion())
	if smajor < major || (smajor == major && sminor < minor) || (smajor == major && sminor == minor && spatch < patch) {
		return false
	}
	return true
}

// Helper for copying when we do not want to change user's version.
func streamSourceCopy(ss *nats.StreamSource) *nats.StreamSource {
	nss := *ss
	// Check pointers
	if ss.OptStartTime != nil {
		t := *ss.OptStartTime
		nss.OptStartTime = &t
	}
	if ss.External != nil {
		ext := *ss.External
		nss.External = &ext
	}
	return &nss
}

// CreateKeyValue will create a KeyValue store with the following configuration.
func CreateKeyValue(nc *nats.Conn, js nats.JetStreamContext, cfg *nats.KeyValueConfig) (nats.KeyValue, error) {
	nc.ConnectedServerVersion()
	if !serverMinVersion(nc, 2, 6, 2) {
		return nil, errors.New("nats: key-value requires at least server version 2.6.2")
	}
	if cfg == nil {
		return nil, nats.ErrKeyValueConfigRequired
	}
	if !validBucketRe.MatchString(cfg.Bucket) {
		return nil, nats.ErrInvalidBucketName
	}
	if _, err := js.AccountInfo(); err != nil {
		return nil, err
	}

	// Default to 1 for history. Max is 64 for now.
	history := int64(1)
	if cfg.History > 0 {
		if cfg.History > nats.KeyValueMaxHistory {
			return nil, nats.ErrHistoryToLarge
		}
		history = int64(cfg.History)
	}

	replicas := cfg.Replicas
	if replicas == 0 {
		replicas = 1
	}

	// We will set explicitly some values so that we can do comparison
	// if we get an "already in use" error and need to check if it is same.
	maxBytes := cfg.MaxBytes
	if maxBytes == 0 {
		maxBytes = -1
	}
	maxMsgSize := cfg.MaxValueSize
	if maxMsgSize == 0 {
		maxMsgSize = -1
	}
	// When stream's MaxAge is not set, server uses 2 minutes as the default
	// for the duplicate window. If MaxAge is set, and lower than 2 minutes,
	// then the duplicate window will be set to that. If MaxAge is greater,
	// we will cap the duplicate window to 2 minutes (to be consistent with
	// previous behavior).
	duplicateWindow := 2 * time.Minute
	if cfg.TTL > 0 && cfg.TTL < duplicateWindow {
		duplicateWindow = cfg.TTL
	}
	scfg := &nats.StreamConfig{
		Name:              fmt.Sprintf(kvBucketNameTmpl, cfg.Bucket),
		Description:       cfg.Description,
		MaxMsgsPerSubject: history,
		MaxBytes:          maxBytes,
		MaxAge:            cfg.TTL,
		MaxMsgSize:        maxMsgSize,
		Storage:           cfg.Storage,
		Replicas:          replicas,
		Placement:         cfg.Placement,
		AllowRollup:       true,
		DenyDelete:        false,
		Duplicates:        duplicateWindow,
		MaxMsgs:           -1,
		MaxConsumers:      -1,
		AllowDirect:       true,
		RePublish:         cfg.RePublish,
	}
	if cfg.Mirror != nil {
		// Copy in case we need to make changes so we do not change caller's version.
		m := streamSourceCopy(cfg.Mirror)
		if !strings.HasPrefix(m.Name, kvBucketNamePre) {
			m.Name = fmt.Sprintf(kvBucketNameTmpl, m.Name)
		}
		scfg.Mirror = m
		scfg.MirrorDirect = true
	} else if len(cfg.Sources) > 0 {
		// For now we do not allow direct subjects for sources. If that is desired a user could use stream API directly.
		for _, ss := range cfg.Sources {
			if !strings.HasPrefix(ss.Name, kvBucketNamePre) {
				ss = streamSourceCopy(ss)
				ss.Name = fmt.Sprintf(kvBucketNameTmpl, ss.Name)
			}
			scfg.Sources = append(scfg.Sources, ss)
		}
	} else {
		scfg.Subjects = []string{fmt.Sprintf(kvSubjectsTmpl, cfg.Bucket)}
	}

	// If we are at server version 2.7.2 or above use DiscardNew. We can not use DiscardNew for 2.7.1 or below.
	if serverMinVersion(nc, 2, 7, 2) {
		scfg.Discard = nats.DiscardNew
	}

	_, err := js.AddStream(scfg)
	if err != nil {
		// If we have a failure to add, it could be because we have
		// a config change if the KV was created against a pre 2.7.2
		// and we are now moving to a v2.7.2+. If that is the case
		// and the only difference is the discard policy, then update
		// the stream.
		// The same logic applies for KVs created pre 2.9.x and
		// the AllowDirect setting.
		if err == nats.ErrStreamNameAlreadyInUse {
			if si, _ := js.StreamInfo(scfg.Name); si != nil {
				// To compare, make the server's stream info discard
				// policy same than ours.
				si.Config.Discard = scfg.Discard
				// Also need to set allow direct for v2.9.x+
				si.Config.AllowDirect = scfg.AllowDirect
				if reflect.DeepEqual(&si.Config, scfg) {
					_, err = js.UpdateStream(scfg)
				}
			}
		}
		if err != nil {
			return nil, err
		}
	}

	return js.KeyValue(cfg.Bucket)
}

func KeyValid(key string) bool {
	if len(key) == 0 || key[0] == '.' || key[len(key)-1] == '.' {
		return false
	}
	return validKeyRe.MatchString(key)
}

func KVDelete(js nats.JetStreamContext, kv nats.KeyValue, key string) error {
	streamName := fmt.Sprintf(kvBucketNameTmpl, kv.Bucket())
	topicName := fmt.Sprintf("$KV.%s.%s", kv.Bucket(), key)
	rms, err := js.GetLastMsg(streamName, topicName)
	if err != nil {
		return err
	}
	return js.SecureDeleteMsg(fmt.Sprintf(kvBucketNameTmpl, kv.Bucket()), rms.Sequence)
}

// KVDeleteAsync is the pipeline-friendly sibling of KVDelete. Unlike
// KVDelete (which uses the JetStream admin RPCs GetLastMsg +
// SecureDeleteMsg — two synchronous round-trips per key, and no async
// API exists for them), KVDeleteAsync publishes a standard NATS KV
// "DEL" tombstone message via js.PublishAsync — one round-trip, async,
// pipelined just like KVPutAsync.
//
// Semantic differences vs KVDelete:
//
//   - KVDelete physically removes the original message from the
//     underlying $KV.<bucket>.<key> stream; storage is freed
//     immediately. Watchers (and passive replicas reading the stream)
//     receive no notification of the deletion — they just see the
//     message disappear, which most consumers cannot observe.
//
//   - KVDeleteAsync APPENDS a tombstone message with header
//     "KV-Operation: DEL". kv.Get returns nats.ErrKeyDeleted; watchers
//     receive a delete event (unless IgnoreDeletes is set on the
//     watcher). Storage of the original PUT message is reclaimed only
//     by JetStream history pruning (MaxHistory) or by an explicit
//     PurgeDeletes() pass.
//
// For WAL apply paths the tombstone style is preferred:
//   - replication-aware (passive caches see the delete),
//   - 1 RT instead of 2,
//   - pipelineable through PublishAsync.
//
// The returned PubAckFuture carries the same contract as KVPutAsync's:
// the message body MUST NOT be mutated until the ack arrives, and the
// JetStream context's pending-acks pool (default 4000) is shared with
// every other PublishAsync caller on the same connection.
func KVDeleteAsync(js nats.JetStreamContext, kv nats.KeyValue, key string) (nats.PubAckFuture, error) {
	if !KeyValid(key) {
		return nil, nats.ErrInvalidKey
	}

	subject, err := kvPutSubject(js, kv, key)
	if err != nil {
		return nil, err
	}

	m := nats.NewMsg(subject)
	m.Header.Set(kvop, kvdel)
	return js.PublishMsgAsync(m)
}

func KVPut(js nats.JetStreamContext, kv nats.KeyValue, key string, value []byte) (revision uint64, err error) {
	if !KeyValid(key) {
		return 0, nats.ErrInvalidKey
	}

	subject, err := kvPutSubject(js, kv, key)
	if err != nil {
		return 0, err
	}

	pa, err := js.Publish(subject, value)
	if err != nil {
		return 0, err
	}
	return pa.Sequence, err
}

// KVPutAsync is the non-blocking sibling of KVPut. It publishes the KV
// message via js.PublishAsync and returns a PubAckFuture immediately;
// the caller must drain f.Ok() / f.Err() before considering the write
// durable.
//
// Use this when you have a batch of writes and want to pipeline them
// instead of paying one RTT per key — typical wins are 5-25× depending
// on RTT vs batch size.
//
// IMPORTANT contract notes:
//
//   - The bytes in `value` MUST NOT be mutated until the returned
//     PubAckFuture has fired (Ok or Err). nats.go retains a reference
//     to the message body for the lifetime of the in-flight ack.
//
//   - All async publishes on a given JetStream context share a single
//     pending-acks pool, bounded by nats.PublishAsyncMaxPending
//     (default 4000 in nats.go v1.37). When the pool is full
//     PublishAsync stalls up to 200ms and then returns a "stalled"
//     error — callers issuing large batches must either chunk below
//     the cap or be ready to back off and retry.
//
//   - Server-side semantics are identical to KVPut: the message lands
//     in the underlying $KV.<bucket>.<key> stream with the same
//     sequence-assignment + durability guarantees. The difference is
//     purely client-side (sync request/reply vs registered future).
func KVPutAsync(js nats.JetStreamContext, kv nats.KeyValue, key string, value []byte) (nats.PubAckFuture, error) {
	if !KeyValid(key) {
		return nil, nats.ErrInvalidKey
	}

	subject, err := kvPutSubject(js, kv, key)
	if err != nil {
		return nil, err
	}

	return js.PublishAsync(subject, value)
}

// kvPutSubject reproduces the subject-resolution that the upstream
// nats.go kvs.put path uses (useJSPfx + putPre vs pre). Kept as a
// shared helper so KVPut and KVPutAsync cannot drift.
func kvPutSubject(js nats.JetStreamContext, kv nats.KeyValue, key string) (string, error) {
	var b strings.Builder

	if reflect.ValueOf(kv).Elem().FieldByName("useJSPfx").Bool() {
		opts := reflect.ValueOf(js).Elem().FieldByName("opts")
		if !opts.CanAddr() {
			return "", fmt.Errorf("KV put error: js.opts == nil, cannot obtain js.opts.pre")
		}
		js_opts_pre := opts.Elem().FieldByName("pre").String()
		b.WriteString(js_opts_pre)
	}

	kv_putPre := reflect.ValueOf(kv).Elem().FieldByName("putPre").String()
	if kv_putPre != _EMPTY_ {
		b.WriteString(kv_putPre)
	} else {
		b.WriteString(reflect.ValueOf(kv).Elem().FieldByName("pre").String())
	}
	b.WriteString(key)

	return b.String(), nil
}

func KVUpdate(js nats.JetStreamContext, kv nats.KeyValue, key string, value []byte, revision uint64) (uint64, error) {
	if !KeyValid(key) {
		return 0, nats.ErrInvalidKey
	}

	var b strings.Builder
	if reflect.ValueOf(kv).Elem().FieldByName("useJSPfx").Bool() {
		opts := reflect.ValueOf(js).Elem().FieldByName("opts")
		if !opts.CanAddr() {
			return 0, fmt.Errorf("KVUpdate error: js.opts == nil, cannot obtain js.opts.pre")
		}
		js_opts_pre := opts.Elem().FieldByName("pre").String()
		b.WriteString(js_opts_pre)
	}
	b.WriteString(reflect.ValueOf(kv).Elem().FieldByName("pre").String())
	b.WriteString(key)

	m := nats.Msg{Subject: b.String(), Header: nats.Header{}, Data: value}
	m.Header.Set(nats.ExpectedLastSubjSeqHdr, strconv.FormatUint(revision, 10))

	pa, err := js.PublishMsg(&m)
	if err != nil {
		return 0, err
	}
	return pa.Sequence, err
}

// Underlying entry.
type kve struct {
	bucket   string
	key      string
	value    []byte
	revision uint64
	delta    uint64
	created  time.Time
	op       nats.KeyValueOp
}

func (e *kve) Bucket() string             { return e.bucket }
func (e *kve) Key() string                { return e.key }
func (e *kve) Value() []byte              { return e.value }
func (e *kve) Revision() uint64           { return e.revision }
func (e *kve) Created() time.Time         { return e.created }
func (e *kve) Delta() uint64              { return e.delta }
func (e *kve) Operation() nats.KeyValueOp { return e.op }

// Get returns the latest value for the key.
func KVGet(js nats.JetStreamContext, kv nats.KeyValue, key string) (nats.KeyValueEntry, error) {
	e, err := kv_get(js, kv, key, kvLatestRevision)
	if err != nil {
		if errors.Is(err, nats.ErrKeyDeleted) {
			return nil, nats.ErrKeyNotFound
		}
		return nil, err
	}

	return e, nil
}

func kv_get(js nats.JetStreamContext, kv nats.KeyValue, key string, revision uint64) (nats.KeyValueEntry, error) {
	if !KeyValid(key) {
		return nil, nats.ErrInvalidKey
	}

	var b strings.Builder
	b.WriteString(reflect.ValueOf(kv).Elem().FieldByName("pre").String())
	b.WriteString(key)

	var m *nats.RawStreamMsg
	var err error
	var _opts [1]nats.JSOpt
	opts := _opts[:0]
	if reflect.ValueOf(kv).Elem().FieldByName("useDirect").Bool() {
		opts = append(opts, nats.DirectGet())
	}

	if revision == kvLatestRevision {
		m, err = js.GetLastMsg(reflect.ValueOf(kv).Elem().FieldByName("stream").String(), b.String(), opts...)
	} else {
		m, err = js.GetMsg(reflect.ValueOf(kv).Elem().FieldByName("stream").String(), revision, opts...)
		// If a sequence was provided, just make sure that the retrieved
		// message subject matches the request.
		if err == nil && m.Subject != b.String() {
			return nil, nats.ErrKeyNotFound
		}
	}
	if err != nil {
		if errors.Is(err, nats.ErrMsgNotFound) {
			err = nats.ErrKeyNotFound
		}
		return nil, err
	}

	entry := &kve{
		bucket:   reflect.ValueOf(kv).Elem().FieldByName("name").String(),
		key:      key,
		value:    m.Data,
		revision: m.Sequence,
		created:  m.Time,
	}

	// Double check here that this is not a DEL Operation marker.
	if len(m.Header) > 0 {
		switch m.Header.Get(kvop) {
		case kvdel:
			entry.op = nats.KeyValueDelete
			return entry, nats.ErrKeyDeleted
		case kvpurge:
			entry.op = nats.KeyValuePurge
			return entry, nats.ErrKeyDeleted
		}
	}

	return entry, nil
}
