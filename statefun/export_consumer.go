package statefun

import (
	"fmt"

	"github.com/nats-io/nats.go"
)

// CreateExportConsumer creates a named durable pull consumer on the export events stream.
// Each dumper application should create its own consumer with a unique name.
// The consumer starts from the beginning of the stream (DeliverAll) and uses explicit ack.
func CreateExportConsumer(js nats.JetStreamContext, domain, consumerName string) (*nats.Subscription, error) {
	streamName := fmt.Sprintf(ExportStreamNameTmpl, domain)
	subject := fmt.Sprintf(ExportSubjectTmpl, domain)

	_, err := js.AddConsumer(streamName, &nats.ConsumerConfig{
		Name:          consumerName,
		Durable:       consumerName,
		FilterSubject: subject,
		AckPolicy:     nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverAllPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create export consumer %q: %w", consumerName, err)
	}

	sub, err := js.PullSubscribe(subject, consumerName)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe export consumer %q: %w", consumerName, err)
	}

	return sub, nil
}

// DeleteExportConsumer removes a durable consumer from the export events stream.
func DeleteExportConsumer(js nats.JetStreamContext, domain, consumerName string) error {
	streamName := fmt.Sprintf(ExportStreamNameTmpl, domain)
	return js.DeleteConsumer(streamName, consumerName)
}
