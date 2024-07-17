package statefun

import (
	"context"
	"fmt"
	"slices"
	"strings"
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
	SignalPrefix                = "signal"
	RequestPrefix               = "request"
	FromGlobalSignalTmpl        = SignalPrefix + ".%s.%s"
	DomainSubjectsIngressPrefix = "$SI"
	DomainSubjectsEgressPrefix  = "$SE"
	DomainIngressSubjectsTmpl   = DomainSubjectsIngressPrefix + ".%s.%s"
	DomainEgressSubjectsTmpl    = DomainSubjectsEgressPrefix + ".%s.%s"
	ObjectIDDomainSeparator     = "/"

	streamPrefix = "$JS.%s.API"

	hubEventStreamName      = "hub_events"
	domainIngressStreamName = "domain_ingress"
	domainEgressStreamName  = "domain_egress"

	routerConsumerMaxAckWaitMs           = 2000
	lostConnectionSingleMsgProcessTimeMs = 700
	maxPendingMessages                   = routerConsumerMaxAckWaitMs / lostConnectionSingleMsgProcessTimeMs
)

type Domain struct {
	hubDomainName string
	name          string
	nc            *nats.Conn
	js            nats.JetStreamContext
	kv            nats.KeyValue
	cache         *cache.Store
}

func NewDomain(nc *nats.Conn, js nats.JetStreamContext, desiredHubDomainName string) (dm *Domain, e error) {
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
		hubDomainName: hubDomainName,
		name:          thisDomainName,
		nc:            nc,
		js:            js,
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

func (dm *Domain) start(cacheConfig *cache.Config, createDomainRouters bool) error {
	bucketName := fmt.Sprintf("%s_%s_cache_bucket", dm.name, cacheConfig.GetId())

	// Create application key value store bucket if does not exist --
	kvExists := false
	if kv, err := dm.js.KeyValue(bucketName); err == nil {
		dm.kv = kv
		kvExists = true
	}
	if !kvExists {
		var err error
		dm.kv, err = kv.CreateKeyValue(dm.nc, dm.js, &nats.KeyValueConfig{
			Bucket: bucketName,
		})
		if err != nil {
			return err
		}
		kvExists = true
	}
	if !kvExists {
		return fmt.Errorf("Nats KV was not inited")
	}
	// --------------------------------------------------------------

	if createDomainRouters {
		if dm.hubDomainName == dm.name {
			if err := dm.createHubSignalStream(); err != nil {
				return err
			}
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
	}

	lg.Logln(lg.TraceLevel, "Initializing the cache store...")
	dm.cache = cache.NewCacheStore(context.Background(), cacheConfig, dm.js, dm.kv)
	lg.Logln(lg.TraceLevel, "Cache store inited!")

	return nil
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
	}
	return dm.createStreamIfNotExists(sc)
}

func (dm *Domain) createEgressSignalStream() error {
	sc := &nats.StreamConfig{
		Name:      domainEgressStreamName,
		Subjects:  []string{fmt.Sprintf(DomainEgressSubjectsTmpl, dm.name, ">")},
		Retention: nats.InterestPolicy,
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
	lg.Logf(lg.TraceLevel, "Handling domain (domain=%s) router for sourceStreamName=%s\n", dm.name, sourceStreamName)

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
			//lg.Logf(lg.TraceLevel, "Routing (from_domain=%s) %s:%s -> %s\n", dm.name, sourceStreamName, msg.Subject, targetSubject)
			if err == nil {
				pubAck, err := dm.js.Publish(targetSubject, msg.Data)
				if err == nil {
					lg.Logf(lg.TraceLevel, "Routed (from_domain=%s) %s:%s -> (to_domain=%s) %s:%s\n", dm.name, sourceStreamName, msg.Subject, pubAck.Domain, pubAck.Stream, targetSubject)
					system.MsgOnErrorReturn(msg.Ack())
					return
				} else {
					lg.Logf(lg.ErrorLevel, "Domain (domain=%s) router with sourceStreamName=%s cannot republish message to subject %s: %s\n", dm.name, sourceStreamName, targetSubject, err)
					_, err := dm.js.Publish(msg.Subject, msg.Data)
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
		lg.Logf(lg.ErrorLevel, "Invalid subscription for domain (domain=%s) router with sourceStreamName=%s: %s\n", dm.name, sourceStreamName, err)
		return err
	}
	return nil
}
