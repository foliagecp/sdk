package sharding

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/foliagecp/sdk/statefun/system"
)

type (
	targetSubjectCalculator func(msg *nats.Msg) (string, error)
)

const (
	signalPrefix               = "signal"
	fromGlobalSignalTmpl       = signalPrefix + ".%s.%s"
	shardSubjectsIngressPrefix = "$SI"
	shardSubjectsEgressPrefix  = "$SE"
	shardIngressSubjectsTmpl   = shardSubjectsIngressPrefix + ".%s.%s"
	shardEgressSubjectsTmpl    = shardSubjectsEgressPrefix + ".%s.%s"

	streamPrefix = "$JS.%s.API"

	hubEventStreamName     = "hub_events"
	shardIngressStreamName = "shard_ingress"
	shardEgressStreamName  = "shard_egress"

	routerConsumerMaxAckWaitMs           = 2000
	lostConnectionSingleMsgProcessTimeMs = 700
	maxPendingMessages                   = routerConsumerMaxAckWaitMs / lostConnectionSingleMsgProcessTimeMs
)

type Shard struct {
	HubDomainName string
	DomainName    string
	nc            *nats.Conn
	js            nats.JetStreamContext
}

func NewShard(nc *nats.Conn, js nats.JetStreamContext, hubDomainName string) (s *Shard, e error) {
	accInfo, err := js.AccountInfo()
	if err != nil {
		return nil, err
	}

	shard := &Shard{
		HubDomainName: hubDomainName,
		DomainName:    accInfo.Domain,
		nc:            nc,
		js:            js,
	}

	return shard, nil
}

func (s *Shard) Start() error {
	if s.HubDomainName == s.DomainName {
		if err := s.createHubSignalStream(); err != nil {
			return err
		}
	}
	if err := s.createIngresSignalStream(); err != nil {
		return err
	}
	if err := s.createEngresSignalStream(); err != nil {
		return err
	}
	if err := s.createIngressRouter(); err != nil {
		return err
	}
	if err := s.createEgressRouter(); err != nil {
		return err
	}
	return nil
}

func (s *Shard) createIngressRouter() error {
	targetSubjectCalculator := func(msg *nats.Msg) (string, error) {
		return fmt.Sprintf(shardIngressSubjectsTmpl, s.DomainName, msg.Subject), nil
	}
	return s.createRouter(shardIngressStreamName, fmt.Sprintf(fromGlobalSignalTmpl, s.DomainName, ">"), targetSubjectCalculator)
}

func (s *Shard) createEgressRouter() error {
	targetSubjectCalculator := func(msg *nats.Msg) (string, error) {
		tokens := strings.Split(msg.Subject, ".")
		if len(tokens) < 5 { // $SE.<domain_name>.signal.<signal_domain_name>.<function_name>
			return "", fmt.Errorf("not enough tokens in a signal's topic")
		}
		targetSubject := ""
		if tokens[1] == tokens[3] { // Signalling function is in the same domain
			tokens[0] = shardSubjectsIngressPrefix
			targetSubject = strings.Join(tokens, ".")
		} else {
			targetSubject = strings.Join(tokens[2:], ".")
		}
		return targetSubject, nil
	}
	return s.createRouter(shardEgressStreamName, fmt.Sprintf(shardEgressSubjectsTmpl, s.DomainName, ">"), targetSubjectCalculator)
}

func (s *Shard) createHubSignalStream() error {
	sc := &nats.StreamConfig{
		Name:     hubEventStreamName,
		Subjects: []string{signalPrefix + ".>"},
	}
	return s.createStreamIfNotExists(sc)
}

func (s *Shard) createIngresSignalStream() error {
	var ss *nats.StreamSource
	if s.HubDomainName == s.DomainName {
		ss = &nats.StreamSource{
			Name:          hubEventStreamName,
			FilterSubject: fmt.Sprintf(fromGlobalSignalTmpl, s.DomainName, ">"),
		}
	} else {
		ext := &nats.ExternalStream{
			APIPrefix: fmt.Sprintf(streamPrefix, s.HubDomainName),
		}
		ss = &nats.StreamSource{
			Name:          hubEventStreamName,
			FilterSubject: fmt.Sprintf(fromGlobalSignalTmpl, s.DomainName, ">"),
			External:      ext,
		}
	}
	sc := &nats.StreamConfig{
		Name:    shardIngressStreamName,
		Sources: []*nats.StreamSource{ss},
	}
	return s.createStreamIfNotExists(sc)
}

func (s *Shard) createEngresSignalStream() error {
	sc := &nats.StreamConfig{
		Name:     shardEgressStreamName,
		Subjects: []string{fmt.Sprintf(shardEgressSubjectsTmpl, s.DomainName, ">")},
	}
	return s.createStreamIfNotExists(sc)
}

func (s *Shard) createStreamIfNotExists(sc *nats.StreamConfig) error {
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
	} else {
		return fmt.Errorf("stream already exists")
	}
	// --------------------------------------------------------------
}

func (s *Shard) createRouter(sourceStreamName string, subject string, tsc targetSubjectCalculator) error {
	consumerName := sourceStreamName + "-" + s.DomainName + "-consumer"
	consumerGroup := consumerName + "-group"
	lg.Logf(lg.TraceLevel, "Handling shard (domain=%s) router for sourceStreamName=%s\n", s.DomainName, sourceStreamName)

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
			lg.Logf(lg.TraceLevel, "Routing (from_domain=%s) %s:%s -> %s\n", s.DomainName, sourceStreamName, msg.Subject, targetSubject)
			if err == nil {
				pubAck, err := s.js.Publish(targetSubject, msg.Data)
				if err == nil {
					lg.Logf(lg.TraceLevel, "Routed (from_domain=%s) %s:%s -> (to_domain=%s) %s:%s\n", s.DomainName, sourceStreamName, msg.Subject, pubAck.Domain, pubAck.Stream, targetSubject)
					msg.Ack()
					return
				} else {
					lg.Logf(lg.ErrorLevel, "Shard (domain=%s) router with sourceStreamName=%s cannot republish message: %s\n", s.DomainName, sourceStreamName, err)
				}
			}
			msg.Nak()
		},
		nats.Bind(sourceStreamName, consumerName),
		nats.ManualAck(),
	)
	if err != nil {
		lg.Logf(lg.ErrorLevel, "Invalid subscription for shard (domain=%s) router with sourceStreamName=%s: %s\n", s.DomainName, sourceStreamName, err)
		return err
	}
	return nil
}
