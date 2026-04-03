package statefun

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/foliagecp/sdk/embedded/nats/kv"
	"github.com/foliagecp/sdk/statefun/cache"
	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/foliagecp/sdk/statefun/system"
)

type (
	targetSubjectCalculator func(msg *nats.Msg) (string, error)
)

const (
	SignalPrefix                          = "signal"
	RequestPrefix                         = "request"
	FromGlobalSignalTmpl                  = SignalPrefix + ".%s.%s"
	DomainSubjectsIngressPrefix           = "$SI"
	DomainSubjectsEgressPrefix            = "$SE"
	DomainIngressSubjectsTmpl             = DomainSubjectsIngressPrefix + ".%s.%s"
	DomainEgressSubjectsTmpl              = DomainSubjectsEgressPrefix + ".%s.%s"
	ObjectIDDomainSeparator               = "/"
	ObjectIDWeakClusteringDomainSeparator = "#"

	streamPrefix = "$JS.%s.API"

	hubEventStreamName        = "hub_events"
	domainIngressStreamName   = "domain_ingress"
	domainEgressStreamName    = "domain_egress"
	deadLetterQueueStreamName = "domain_dlq"
	domainTraceStreamName     = "domain_trace"
	domainTraceSubjectsTmpl   = "trace.%s.events.>"

	routerConsumerMaxAckWaitMs           = 2000
	lostConnectionSingleMsgProcessTimeMs = 700
	maxPendingMessages                   = routerConsumerMaxAckWaitMs / lostConnectionSingleMsgProcessTimeMs
)

type Domain struct {
	hubDomainName           string
	name                    string
	weakClusterDomains      map[string]struct{}
	weakClusterDomainsMutex sync.Mutex
	nc                      *nats.Conn
	js                      nats.JetStreamContext
	ftSC                    streamConfig
	sysSC                   streamConfig
	kvSC                    streamConfig
	traceSC                 streamConfig

	kv    nats.KeyValue
	cache *cache.Store

	shutdown        chan struct{}
	exportCommitter *ExportCommitter
	exportEnabled   bool
	exportSC        streamConfig
}

type streamConfig struct {
	replicasCount int
	maxMsgs       int64
	maxBytes      int64
	maxAge        time.Duration
}

func NewDomain(nc *nats.Conn, js nats.JetStreamContext, desiredHubDomainName string, ftSC, sysSC, kvSC, traceSC streamConfig) (dm *Domain, e error) {
	accInfo, err := js.AccountInfo()
	if err != nil {
		return nil, err
	}

	hubDomainName := desiredHubDomainName
	thisDomainName := accInfo.Domain
	if thisDomainName == "" {
		if hubDomainName == "" {
			thisDomainName = DefaultHubDomainName
			hubDomainName = DefaultHubDomainName
		} else {
			thisDomainName = hubDomainName
		}
	} else {
		if hubDomainName == "" {
			hubDomainName = thisDomainName
		}
	}

	domain := &Domain{
		hubDomainName:      hubDomainName,
		name:               thisDomainName,
		weakClusterDomains: map[string]struct{}{thisDomainName: {}},
		nc:                 nc,
		js:                 js,
		ftSC:               ftSC,
		sysSC:              sysSC,
		kvSC:               kvSC,
		traceSC:            traceSC,
	}

	return domain, nil
}

func (dm *Domain) HubDomainName() string {
	return dm.hubDomainName
}

func (dm *Domain) Name() string {
	return dm.name
}

func (dm *Domain) Cache() *cache.Store {
	return dm.cache
}

// Get all domains in weak cluster including this one
func (dm *Domain) GetWeakClusterDomains() []string {
	dm.weakClusterDomainsMutex.Lock()
	defer dm.weakClusterDomainsMutex.Unlock()

	weakClusterUniqueDomainNamesIncludingThis := []string{}
	for k := range dm.weakClusterDomains {
		weakClusterUniqueDomainNamesIncludingThis = append(weakClusterUniqueDomainNamesIncludingThis, k)
	}
	return weakClusterUniqueDomainNamesIncludingThis
}

// Set all domains in weak cluster (this domain name will also be included automatically if not defined)
func (dm *Domain) SetWeakClusterDomains(weakClusterDomains []string) {
	dm.weakClusterDomainsMutex.Lock()
	defer dm.weakClusterDomainsMutex.Unlock()

	dm.weakClusterDomains = map[string]struct{}{dm.name: {}}
	for _, dmn := range weakClusterDomains {
		dm.weakClusterDomains[dmn] = struct{}{}
	}
}

func (dm *Domain) CreateCustomShadowId(storeDomain, targetDomain, uuid string) string {
	return storeDomain + ObjectIDDomainSeparator + targetDomain + ObjectIDWeakClusteringDomainSeparator + uuid
}

/*
 * otherDomainName/ObjectId -> thisDomainName/otherDomainName#ObjectId
 * thisDomainName/ObjectId -> thisDomainName/ObjectId
 */
func (dm *Domain) GetShadowObjectShadowId(objectIdWithAnyDomainName string) string {
	objectDomain := dm.GetDomainFromObjectID(objectIdWithAnyDomainName)
	if objectDomain == dm.name {
		return objectIdWithAnyDomainName
	} else {
		return fmt.Sprintf(
			"%s%s%s%s%s",
			dm.name,
			ObjectIDDomainSeparator,
			objectDomain,
			ObjectIDWeakClusteringDomainSeparator,
			dm.GetObjectIDWithoutDomain(objectIdWithAnyDomainName),
		)
	}
}

/*
 * domainName1/domainName2#ObjectId -> domainName2, ObjectId
 */
func (dm *Domain) GetShadowObjectDomainAndID(shadowObjectId string) (domainName, objectIdWithoutDomain string, err error) {
	err = nil

	idWithoutDomain := dm.GetObjectIDWithoutDomain(shadowObjectId)

	tokens := strings.Split(idWithoutDomain, ObjectIDWeakClusteringDomainSeparator)
	if !(len(tokens) > 1 && len(tokens[0]) > 0 && len(tokens[1]) > 0) {
		return "", "", fmt.Errorf("id=%s is not a shadow object's id", shadowObjectId)
	}

	domainName = tokens[0]
	objectIdWithoutDomain = tokens[1]

	return
}

/*
* thisDomainName/otherDomainName#ObjectId -> thisDomainName/otherDomainName#ObjectId

* thisDomainName/thisDomainName#ObjectId -> thisDomainName/ObjectId
* otherDomainName/thisDomainName#ObjectId -> thisDomainName/ObjectId
* otherDomainName/otherDomainName#ObjectId -> thisDomainName/otherDomainName#ObjectId

* thisDomainName/ObjectId -> thisDomainName/ObjectId
* otherDomainName/ObjectId -> otherDomainName/ObjectId
 */
func (dm *Domain) GetValidObjectId(objectId string) string {
	if targetDomainName, objectIdWithoutDomain, err := dm.GetShadowObjectDomainAndID(objectId); err == nil {
		objectIdDomain := dm.GetDomainFromObjectID(objectId)
		if dm.name == targetDomainName {
			return dm.name + ObjectIDDomainSeparator + objectIdWithoutDomain
		}
		if objectIdDomain == targetDomainName {
			return dm.name + ObjectIDDomainSeparator + targetDomainName + ObjectIDWeakClusteringDomainSeparator + objectIdWithoutDomain
		}
		return objectId
	}
	return objectId
}

// GetObjectIDByShadowObjectID converts an input id to a "real" object id.
//
// Rules:
//
//	DomainA/DomainB#ObjectId -> DomainB/ObjectId
//	DomainA/ObjectId         -> DomainA/ObjectId
//	ObjectId                 -> ThisDomainName/ObjectId
func (dm *Domain) GetObjectIDByShadowObjectID(id string) string {
	// 1) Shadow object: DomainX/DomainY#ObjectId  => DomainY/ObjectId
	if dm.IsShadowObject(id) {
		targetDomain, objectID, err := dm.GetShadowObjectDomainAndID(id)
		if err == nil {
			return targetDomain + ObjectIDDomainSeparator + objectID
		}
		// If parsing failed for any reason — fall back to default rules below
	}

	// 2) Already has explicit domain: Domain/ObjectId => Domain/ObjectId
	// (also tolerates multiple "/" by taking first token as domain and last as id,
	//  same as your helpers do)
	if strings.Contains(id, ObjectIDDomainSeparator) {
		domain := dm.GetDomainFromObjectID(id)
		objectID := dm.GetObjectIDWithoutDomain(id)
		return domain + ObjectIDDomainSeparator + objectID
	}

	// 3) No domain at all: ObjectId => ThisDomain/ObjectId
	return dm.name + ObjectIDDomainSeparator + id
}

/*
 * domainName1/domainName2#ObjectId -> true
 * domainName1/ObjectId  -> false
 */
func (dm *Domain) IsShadowObject(idWithDomain string) bool {
	idWithoutDomain := dm.GetObjectIDWithoutDomain(idWithDomain)

	if tokens := strings.Split(idWithoutDomain, ObjectIDWeakClusteringDomainSeparator); len(tokens) > 1 && len(tokens[0]) > 0 && len(tokens[1]) > 0 {
		return true
	}

	return false
}

func (dm *Domain) GetDomainFromObjectID(objectID string) string {
	domain := dm.name
	if tokens := strings.Split(objectID, ObjectIDDomainSeparator); len(tokens) > 1 {
		if len(tokens) > 2 {
			lg.Logf(lg.WarnLevel, "GetDomainFromObjectID detected objectID=%s with multiple domains", objectID)
		}
		domain = tokens[0]
	}
	return domain
}

func (dm *Domain) GetObjectIDWithoutDomain(objectID string) string {
	id := objectID
	if tokens := strings.Split(objectID, ObjectIDDomainSeparator); len(tokens) > 1 {
		if len(tokens) > 2 {
			lg.Logf(lg.WarnLevel, "GetObjectIDWithoutDomain detected objectID=%s with multiple domains", objectID)
		}
		id = tokens[len(tokens)-1]
	}
	return id
}

func (dm *Domain) CreateObjectIDWithDomain(domain string, objectID string, domainReplace bool) (dmObjectID string) {
	dmObjectID = objectID
	if domainReplace || dm.GetObjectIDWithoutDomain(objectID) == objectID {
		dmObjectID = domain + ObjectIDDomainSeparator + dm.GetObjectIDWithoutDomain(objectID)
	}
	return
}

func (dm *Domain) CreateObjectIDWithThisDomain(objectID string, domainReplace bool) string {
	return dm.CreateObjectIDWithDomain(dm.name, objectID, domainReplace)
}

func (dm *Domain) CreateObjectIDWithHubDomain(objectID string, domainReplace bool) string {
	return dm.CreateObjectIDWithDomain(dm.hubDomainName, objectID, domainReplace)
}

func (dm *Domain) start(ctx context.Context, cacheConfig *cache.Config, createDomainRouters bool) error {
	bucketName := fmt.Sprintf("%s_%s_cache_bucket", dm.name, cacheConfig.GetId())

	// Create application key value store bucket if does not exist --
	if existingKV, err := dm.js.KeyValue(bucketName); err == nil {
		dm.kv = existingKV
	} else {
		var err error
		dm.kv, err = kv.CreateKeyValue(dm.nc, dm.js, &nats.KeyValueConfig{
			Bucket:   bucketName,
			Replicas: dm.kvSC.replicasCount,
			MaxBytes: dm.kvSC.maxBytes,
			TTL:      dm.kvSC.maxAge,
		})
		if err != nil {
			return err
		}
	}

	// --------------------------------------------------------------

	if createDomainRouters {
		if dm.hubDomainName == dm.name {
			if err := dm.createHubSignalStream(); err != nil {
				return err
			}
		}
		if err := dm.createDLQStream(); err != nil {
			return err
		}
		if err := dm.createIngresSignalStream(); err != nil {
			return err
		}
		if err := dm.createEgressSignalStream(); err != nil {
			return err
		}
		if err := dm.createIngressRouter(); err != nil {
			return err
		}
		if err := dm.createEgressRouter(); err != nil {
			return err
		}
		if err := dm.createTraceStream(); err != nil {
			return err
		}
		if err := dm.createWALOperationsStream(); err != nil {
			return err
		}
		if err := dm.createWALCommitsStream(); err != nil {
			return err
		}
	}

	// Create export stream if enabled
	if dm.exportEnabled {
		dm.exportCommitter = NewExportCommitter(dm.js, dm.name)
		if err := dm.exportCommitter.CreateExportStream(
			dm.exportSC.maxMsgs,
			dm.exportSC.maxBytes,
			dm.exportSC.maxAge,
			dm.exportSC.replicasCount,
		); err != nil {
			return fmt.Errorf("failed to create export stream: %w", err)
		}
		lg.Logf(lg.DebugLevel, "Export stream created: %s", dm.exportCommitter.StreamName())
	}

	le := lg.GetLogger()

	le.Trace(ctx, "Initializing the cache store...")
	dm.cache = cache.NewCacheStore(ctx, cacheConfig, dm.js, dm.kv)
	dm.cache.SetTransactionGenerator(dm)

	if err := dm.TransactionCommitter(ctx); err != nil {
		return err
	}
	if err := dm.checkKvConsistency(ctx); err != nil {
		return err
	}

	if dm.exportEnabled {
		if err := dm.publishStartupSnapshot(ctx); err != nil {
			lg.Logf(lg.WarnLevel, "Export startup snapshot failed (non-fatal): %s", err)
		}
	}

	le.Trace(ctx, "Cache store inited!")

	return nil
}

func (dm *Domain) checkKvConsistency(ctx context.Context) error {
	consumerName := CommitterDurableName + "-" + dm.kv.Bucket()

	const (
		checkInterval = 100 * time.Millisecond
		waitTimeout   = 30 * time.Second
	)

	timeout := time.NewTimer(waitTimeout)
	defer timeout.Stop()

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	lg.Logln(lg.TraceLevel, "Waiting for KV consistency ...")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-timeout.C:
			return fmt.Errorf("timeout waiting for KV consistency")

		case <-ticker.C:
			info, err := dm.js.ConsumerInfo(WALCommitsStreamName, consumerName)
			if err != nil {
				lg.Logf(lg.DebugLevel, "Failed to get consumer info: %s", err)
				continue
			}

			noPendingWork :=
				info.NumPending == 0 &&
					info.NumAckPending == 0

			consistent, err := dm.isKVConsistent()
			if err != nil {
				lg.Logf(lg.ErrorLevel, "Failed to check consistency: %s", err)
				continue
			}

			if !noPendingWork {
				lg.Logf(lg.TraceLevel, "Transactions in progress: pending=%d, ackPending=%d", info.NumPending, info.NumAckPending)

				if consistent {
					if err := dm.setKVConsistent(false); err != nil {
						return fmt.Errorf(
							"failed to set KV inconsistent: %w", err,
						)
					}
				}

				if !timeout.Stop() {
					select {
					case <-timeout.C:
					default:
					}
				}
				timeout.Reset(waitTimeout)
				continue
			}

			if !consistent {
				lg.Logln(lg.TraceLevel, "No pending work, setting KV consistent")
				if err := dm.setKVConsistent(true); err != nil {
					return fmt.Errorf(
						"failed to set KV consistent: %w", err,
					)
				}
			}

			lg.Logln(lg.TraceLevel, "KV is consistent")
			return nil
		}
	}
}

// publishStartupSnapshot reads all current KV entries and publishes them
// directly to the export stream as a single "startup" transaction.
// This allows the export dumper to rebuild its state after a restart without
// waiting for a user-triggered write to arrive for each pre-existing entity.
// The snapshot bypasses the WAL — KV is not modified.
func (dm *Domain) publishStartupSnapshot(ctx context.Context) error {
	storePrefix := dm.cache.GetStorePrefix()
	pattern := storePrefix + ".>"

	watchCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	watcher, err := dm.kv.Watch(pattern, nats.IgnoreDeletes(), nats.Context(watchCtx))
	if err != nil {
		return fmt.Errorf("startup snapshot watch: %w", err)
	}
	defer watcher.Stop()

	var ops []WALOp
	for entry := range watcher.Updates() {
		if entry == nil {
			break // nil = all historical entries delivered
		}
		ops = append(ops, WALOp{
			OpType: cache.OpTypePUT,
			Key:    entry.Key(),
			Value:  entry.Value(),
		})
	}

	if len(ops) == 0 {
		lg.Logln(lg.DebugLevel, "Export startup snapshot: KV is empty, nothing to flush")
		return nil
	}

	txID := fmt.Sprintf("startup-snapshot-%d", time.Now().UnixNano())
	lg.Logf(lg.InfoLevel, "Export startup snapshot: flushing %d KV entries as tx=%s", len(ops), txID)
	return dm.exportCommitter.ProcessTransaction(txID, ops, storePrefix)
}

func (dm *Domain) createIngressRouter() error {
	targetSubjectCalculator := func(msg *nats.Msg) (string, error) {
		return fmt.Sprintf(DomainIngressSubjectsTmpl, dm.name, msg.Subject), nil
	}
	return dm.createRouter(domainIngressStreamName, fmt.Sprintf(FromGlobalSignalTmpl, dm.name, ">"), targetSubjectCalculator)
}

func (dm *Domain) createEgressRouter() error {
	targetSubjectCalculator := func(msg *nats.Msg) (string, error) {
		tokens := strings.Split(msg.Subject, ".")
		if len(tokens) < 5 { // $SE.<domain_name>.signal.<signal_domain_name>.<function_name>
			return "", fmt.Errorf("not enough tokens in a signal's topic")
		}
		targetSubject := ""
		if tokens[1] == tokens[3] { // Signalling function is in the same domain
			tokens[0] = DomainSubjectsIngressPrefix
			targetSubject = strings.Join(tokens, ".")
		} else {
			targetSubject = strings.Join(tokens[2:], ".")
		}
		return targetSubject, nil
	}
	return dm.createRouter(domainEgressStreamName, fmt.Sprintf(DomainEgressSubjectsTmpl, dm.name, ">"), targetSubjectCalculator)
}

func (dm *Domain) createHubSignalStream() error {
	sc := &nats.StreamConfig{
		Name:      hubEventStreamName,
		Subjects:  []string{SignalPrefix + ".>"},
		Retention: nats.InterestPolicy,
		Replicas:  dm.sysSC.replicasCount,
	}
	return dm.createStreamIfNotExists(sc)
}

func (dm *Domain) createIngresSignalStream() error {
	var ss *nats.StreamSource
	if dm.hubDomainName == dm.name {
		ss = &nats.StreamSource{
			Name:          hubEventStreamName,
			FilterSubject: fmt.Sprintf(FromGlobalSignalTmpl, dm.name, ">"),
		}
	} else {
		ext := &nats.ExternalStream{
			APIPrefix: fmt.Sprintf(streamPrefix, dm.hubDomainName),
		}
		ss = &nats.StreamSource{
			Name:          hubEventStreamName,
			FilterSubject: fmt.Sprintf(FromGlobalSignalTmpl, dm.name, ">"),
			External:      ext,
		}
	}
	sc := &nats.StreamConfig{
		Name:      domainIngressStreamName,
		Sources:   []*nats.StreamSource{ss},
		Retention: nats.InterestPolicy,
		Replicas:  dm.sysSC.replicasCount,
		MaxBytes:  dm.sysSC.maxBytes,
		MaxMsgs:   dm.sysSC.maxMsgs,
		MaxAge:    dm.sysSC.maxAge,
	}
	return dm.createStreamIfNotExists(sc)
}

func (dm *Domain) createEgressSignalStream() error {
	sc := &nats.StreamConfig{
		Name:      domainEgressStreamName,
		Subjects:  []string{fmt.Sprintf(DomainEgressSubjectsTmpl, dm.name, ">")},
		Retention: nats.InterestPolicy,
		Replicas:  dm.sysSC.replicasCount,
		MaxBytes:  dm.sysSC.maxBytes,
		MaxMsgs:   dm.sysSC.maxMsgs,
		MaxAge:    dm.sysSC.maxAge,
	}
	return dm.createStreamIfNotExists(sc)
}

func (dm *Domain) createDLQStream() error {
	sc := &nats.StreamConfig{
		Name:      deadLetterQueueStreamName,
		Retention: nats.LimitsPolicy,
		Replicas:  dm.sysSC.replicasCount,
		MaxBytes:  dm.sysSC.maxBytes,
		MaxMsgs:   dm.sysSC.maxMsgs,
		MaxAge:    dm.sysSC.maxAge,
	}
	return dm.createStreamIfNotExists(sc)
}

func (dm *Domain) createTraceStream() error {
	sc := &nats.StreamConfig{
		Name:      domainTraceStreamName,
		Subjects:  []string{fmt.Sprintf(domainTraceSubjectsTmpl, dm.name)},
		Retention: nats.LimitsPolicy,
		Replicas:  dm.traceSC.replicasCount,
		MaxBytes:  dm.traceSC.maxBytes,
		MaxMsgs:   dm.traceSC.maxMsgs,
		MaxAge:    dm.traceSC.maxAge,
	}
	return dm.createStreamIfNotExists(sc)
}

func (dm *Domain) createWALOperationsStream() error {
	sc := &nats.StreamConfig{
		Name:      WALOperationsStreamName,
		Subjects:  []string{WALOperationsSubject},
		Retention: nats.WorkQueuePolicy,
		MaxAge:    24 * time.Hour,
		Replicas:  dm.sysSC.replicasCount,
	}
	return dm.createStreamIfNotExists(sc)
}

func (dm *Domain) createWALCommitsStream() error {
	sc := &nats.StreamConfig{
		Name:      WALCommitsStreamName,
		Subjects:  []string{WALCommitsSubject},
		Retention: nats.WorkQueuePolicy,
		MaxAge:    24 * time.Hour,
		Replicas:  dm.sysSC.replicasCount,
	}
	return dm.createStreamIfNotExists(sc)
}

func (dm *Domain) createStreamIfNotExists(sc *nats.StreamConfig) error {
	// Create streams if does not exist ------------------------------
	/* Each stream contains a single subject (topic).
	 * Differently named stream with overlapping subjects cannot exist!
	 */
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var existingStreams []string
	for info := range dm.js.StreamsInfo(nats.Context(ctx)) {
		existingStreams = append(existingStreams, info.Config.Name)
	}
	if !slices.Contains(existingStreams, sc.Name) {
		_, err := dm.js.AddStream(sc)
		return err
	}
	return nil
	// --------------------------------------------------------------
}

func (dm *Domain) createRouter(sourceStreamName string, subject string, tsc targetSubjectCalculator) error {
	consumerName := sourceStreamName + "-" + dm.name + "-consumer"
	consumerGroup := consumerName + "-group"
	lg.Logf(lg.TraceLevel, "Handling domain (domain=%s) router for sourceStreamName=%s", dm.name, sourceStreamName)

	// Create stream consumer if does not exist ---------------------
	consumerExists := false
	for info := range dm.js.Consumers(sourceStreamName, nats.MaxWait(10*time.Second)) {
		if info.Name == consumerName {
			consumerExists = true
		}
	}
	if !consumerExists {
		_, err := dm.js.AddConsumer(sourceStreamName, &nats.ConsumerConfig{
			Name:           consumerName,
			Durable:        consumerName,
			DeliverSubject: consumerName,
			DeliverGroup:   consumerGroup,
			FilterSubject:  subject,
			AckPolicy:      nats.AckExplicitPolicy,
			AckWait:        time.Duration(routerConsumerMaxAckWaitMs) * time.Millisecond,
			MaxAckPending:  maxPendingMessages,
		})
		system.MsgOnErrorReturn(err)
	}
	// --------------------------------------------------------------

	_, err := dm.js.QueueSubscribe(
		subject,
		consumerGroup,
		func(msg *nats.Msg) {
			targetSubject, err := tsc(msg)
			//lg.Logf(lg.TraceLevel, "Routing (from_domain=%s) %s:%s -> %s", dm.name, sourceStreamName, msg.Subject, targetSubject)
			if err == nil {
				pubAck, err := dm.js.Publish(targetSubject, msg.Data)
				if err == nil {
					lg.Logf(lg.TraceLevel, "Routed (from_domain=%s) %s:%s -> (to_domain=%s) %s:%s", dm.name, sourceStreamName, msg.Subject, pubAck.Domain, pubAck.Stream, targetSubject)
					system.MsgOnErrorReturn(msg.Ack())
					return
				} else {
					dlqMsg := dlqMsgBuilder(msg.Subject, sourceStreamName, dm.name, err.Error(), msg.Data)
					_, err := dm.js.PublishMsg(dlqMsg)
					switch sourceStreamName {
					case domainEgressStreamName:
						// Default logic - infinite republishing
					case domainIngressStreamName:
						// Send message to DLQ without retry
						if err == nil {
							lg.Logf(lg.DebugLevel, "Domain (domain=%s) router with sourceStreamName=%s republished message to DLQ", dm.name, sourceStreamName)
							system.MsgOnErrorReturn(msg.Ack())
							return
						}
					default:
					}

					lg.Logf(lg.ErrorLevel, "Domain (domain=%s) router with sourceStreamName=%s cannot republish message to subject %s: %s", dm.name, sourceStreamName, targetSubject, err)
					_, err = dm.js.Publish(msg.Subject, msg.Data)
					if err == nil {
						system.MsgOnErrorReturn(msg.Ack())
						return
					}
				}
			}
			system.MsgOnErrorReturn(msg.Nak())
		},
		nats.Bind(sourceStreamName, consumerName),
		nats.ManualAck(),
	)
	if err != nil {
		lg.Logf(lg.ErrorLevel, "Invalid subscription for domain (domain=%s) router with sourceStreamName=%s: %s", dm.name, sourceStreamName, err)
		return err
	}
	return nil
}

func dlqMsgBuilder(subject, stream, domain, errorMsg string, data []byte) *nats.Msg {
	dlqMsg := nats.NewMsg(deadLetterQueueStreamName)
	dlqMsg.Data = data
	dlqMsg.Header.Set("Original-Subject", subject)
	dlqMsg.Header.Set("Original-Stream", stream)
	dlqMsg.Header.Set("Domain", domain)
	dlqMsg.Header.Set("Error", errorMsg)
	dlqMsg.Header.Set("Timestamp", time.Now().UTC().String())

	return dlqMsg
}

func (dm *Domain) PublishOperation(txID string, opTime int64, opType cache.OpType, key string, value []byte) error {
	return dm.publishWALOperation(txID, opTime, opType, key, value)
}

func (dm *Domain) PublishCommit(txID string) error {
	return dm.publishWALCommit(txID)
}

func (dm *Domain) GenerateTransactionID() string {
	return generateTransactionID()
}

func (dm *Domain) isBackupBarrierActive() bool {
	if dm.cache == nil {
		return false
	}
	return dm.cache.IsBackupBarrierActive()
}
