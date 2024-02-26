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

func NewDomain(nc *nats.Conn, js nats.JetStreamContext, hubDomainName string) (s *Domain, e error) {
	accInfo, err := js.AccountInfo()
	if err != nil {
		return nil, err
	}

	domainName := accInfo.Domain
	if domainName == "" {
		domainName = HubDomainName
	}

	domain := &Domain{
		hubDomainName: hubDomainName,
		name:          domainName,
		nc:            nc,
		js:            js,
	}

	bucketName := fmt.Sprintf("%s_cache_bucket", hubDomainName)

	// Create application key value store bucket if does not exist --
	kvExists := false
	if kv, err := js.KeyValue(bucketName); err == nil {
		domain.kv = kv
		kvExists = true
	}
	if !kvExists {
		domain.kv, err = kv.CreateKeyValue(nc, js, &nats.KeyValueConfig{
			Bucket: bucketName,
		})
		if err != nil {
			return
		}
		kvExists = true
	}
	if !kvExists {
		return nil, fmt.Errorf("Nats KV was not inited")
	}
	// --------------------------------------------------------------

	return domain, nil
}

func (s *Domain) HubDomainName() string {
	return s.hubDomainName
}

func (s *Domain) Name() string {
	return s.name
}

func (s *Domain) Cache() *cache.Store {
	return s.cache
}

func (s *Domain) GetDomainFromObjectID(objectID string) string {
	domain := s.name
	if tokens := strings.Split(objectID, ObjectIDDomainSeparator); len(tokens) > 1 {
		if len(tokens) > 2 {
			lg.Logf(lg.WarnLevel, "GetDomainFromObjectID detected objectID=%s with multiple domains", objectID)
		}
		domain = tokens[0]
	}
	return domain
}

func (s *Domain) GetObjectIDWithoutDomain(objectID string) string {
	id := objectID
	if tokens := strings.Split(objectID, ObjectIDDomainSeparator); len(tokens) > 1 {
		if len(tokens) > 2 {
			lg.Logf(lg.WarnLevel, "GetObjectIDWithoutDomain detected objectID=%s with multiple domains", objectID)
		}
		id = tokens[len(tokens)-1]
	}
	return id
}

func (s *Domain) CreateObjectIDWithDomain(domain string, objectID string, domainReplace bool) (dmObjectID string) {
	dmObjectID = objectID
	if domainReplace || s.GetObjectIDWithoutDomain(objectID) == objectID {
		dmObjectID = domain + ObjectIDDomainSeparator + s.GetObjectIDWithoutDomain(objectID)
	}
	return
}

func (s *Domain) CreateObjectIDWithThisDomain(objectID string, domainReplace bool) string {
	return s.CreateObjectIDWithDomain(s.name, objectID, domainReplace)
}

func (s *Domain) CreateObjectIDWithHubDomain(objectID string, domainReplace bool) string {
	return s.CreateObjectIDWithDomain(s.hubDomainName, objectID, domainReplace)
}

func (s *Domain) start(cacheConfig *cache.Config) error {
	if s.hubDomainName == s.name {
		if err := s.createHubSignalStream(); err != nil {
			return err
		}
	}
	if err := s.createIngresSignalStream(); err != nil {
		return err
	}
	if err := s.createEgressSignalStream(); err != nil {
		return err
	}
	if err := s.createIngressRouter(); err != nil {
		return err
	}
	if err := s.createEgressRouter(); err != nil {
		return err
	}

	lg.Logln(lg.TraceLevel, "Initializing the cache store...")
	s.cache = cache.NewCacheStore(context.Background(), cacheConfig, s.js, s.kv)
	lg.Logln(lg.TraceLevel, "Cache store inited!")

	return nil
}

func (s *Domain) createIngressRouter() error {
	targetSubjectCalculator := func(msg *nats.Msg) (string, error) {
		return fmt.Sprintf(DomainIngressSubjectsTmpl, s.name, msg.Subject), nil
	}
	return s.createRouter(domainIngressStreamName, fmt.Sprintf(FromGlobalSignalTmpl, s.name, ">"), targetSubjectCalculator)
}

func (s *Domain) createEgressRouter() error {
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
	return s.createRouter(domainEgressStreamName, fmt.Sprintf(DomainEgressSubjectsTmpl, s.name, ">"), targetSubjectCalculator)
}

func (s *Domain) createHubSignalStream() error {
	sc := &nats.StreamConfig{
		Name:     hubEventStreamName,
		Subjects: []string{SignalPrefix + ".>"},
	}
	return s.createStreamIfNotExists(sc)
}

func (s *Domain) createIngresSignalStream() error {
	var ss *nats.StreamSource
	if s.hubDomainName == s.name {
		ss = &nats.StreamSource{
			Name:          hubEventStreamName,
			FilterSubject: fmt.Sprintf(FromGlobalSignalTmpl, s.name, ">"),
		}
	} else {
		ext := &nats.ExternalStream{
			APIPrefix: fmt.Sprintf(streamPrefix, s.hubDomainName),
		}
		ss = &nats.StreamSource{
			Name:          hubEventStreamName,
			FilterSubject: fmt.Sprintf(FromGlobalSignalTmpl, s.name, ">"),
			External:      ext,
		}
	}
	sc := &nats.StreamConfig{
		Name:    domainIngressStreamName,
		Sources: []*nats.StreamSource{ss},
	}
	return s.createStreamIfNotExists(sc)
}

func (s *Domain) createEgressSignalStream() error {
	sc := &nats.StreamConfig{
		Name:     domainEgressStreamName,
		Subjects: []string{fmt.Sprintf(DomainEgressSubjectsTmpl, s.name, ">")},
	}
	return s.createStreamIfNotExists(sc)
}

func (s *Domain) createStreamIfNotExists(sc *nats.StreamConfig) error {
	// Create streams if does not exist ------------------------------
	/* Each stream contains a single subject (topic).
	 * Differently named stream with overlapping subjects cannot exist!
	 */
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var existingStreams []string
	for info := range s.js.StreamsInfo(nats.Context(ctx)) {
		existingStreams = append(existingStreams, info.Config.Name)
	}
	if !slices.Contains(existingStreams, sc.Name) {
		_, err := s.js.AddStream(sc)
		return err
	}
	return nil
	// --------------------------------------------------------------
}

func (s *Domain) createRouter(sourceStreamName string, subject string, tsc targetSubjectCalculator) error {
	consumerName := sourceStreamName + "-" + s.name + "-consumer"
	consumerGroup := consumerName + "-group"
	lg.Logf(lg.TraceLevel, "Handling domain (domain=%s) router for sourceStreamName=%s\n", s.name, sourceStreamName)

	// Create stream consumer if does not exist ---------------------
	consumerExists := false
	for info := range s.js.Consumers(sourceStreamName, nats.MaxWait(10*time.Second)) {
		if info.Name == consumerName {
			consumerExists = true
		}
	}
	if !consumerExists {
		_, err := s.js.AddConsumer(sourceStreamName, &nats.ConsumerConfig{
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

	_, err := s.js.QueueSubscribe(
		subject,
		consumerGroup,
		func(msg *nats.Msg) {
			targetSubject, err := tsc(msg)
			//lg.Logf(lg.TraceLevel, "Routing (from_domain=%s) %s:%s -> %s\n", s.name, sourceStreamName, msg.Subject, targetSubject)
			if err == nil {
				pubAck, err := s.js.Publish(targetSubject, msg.Data)
				if err == nil {
					lg.Logf(lg.TraceLevel, "Routed (from_domain=%s) %s:%s -> (to_domain=%s) %s:%s\n", s.name, sourceStreamName, msg.Subject, pubAck.Domain, pubAck.Stream, targetSubject)
					system.MsgOnErrorReturn(msg.Ack())
					return
				} else {
					lg.Logf(lg.ErrorLevel, "Domain (domain=%s) router with sourceStreamName=%s cannot republish message: %s\n", s.name, sourceStreamName, err)
				}
			}
			system.MsgOnErrorReturn(msg.Nak())
		},
		nats.Bind(sourceStreamName, consumerName),
		nats.ManualAck(),
	)
	if err != nil {
		lg.Logf(lg.ErrorLevel, "Invalid subscription for domain (domain=%s) router with sourceStreamName=%s: %s\n", s.name, sourceStreamName, err)
		return err
	}
	return nil
}
