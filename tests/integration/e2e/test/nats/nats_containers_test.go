package nats

import (
	"context"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// If true - lets you inspect what's going on in all Nats containers.
// You have to stop tests manually, otherwise they close automatically after `testTimeout`
const manualStop = false
const testTimeout = time.Second * 600

// TestNATSClusterCommunication tests basic sub/pub/rep functionality inside cluster
// ToDo: tests for leaf nodes communication
func TestNATSClusterCommunication(t *testing.T) {
	ctx0, concel0 := context.WithTimeout(context.Background(), testTimeout)
	// terminate containers on system calls to avoid container leakage
	ctx, cancel := signal.NotifyContext(ctx0, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	defer cancel()
	defer concel0()
	clusterName := "testcluster"
	numberOfNodes := 3
	numberOfLeaves := 3

	// Start the NATS cluster
	ct, err := CreateCluster(ctx, clusterName, numberOfNodes, numberOfLeaves)
	if err != nil {
		t.Fatalf("Failed to create nats cluster: %s", err)
	}
	ct.Start(ctx)

	// CreateCluster a connection to the NATS cluster
	nc, err := nats.Connect(strings.Join(ct.GetClusterConnection(ctx), ","))
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %s", err)
	}
	defer nc.Close()

	// Test message publishing and subscription
	subject := "test"
	msg := "Hello, NATS!"

	// Subscribe to the subject
	sub, err := nc.SubscribeSync(subject)
	if err != nil {
		t.Fatalf("Failed to subscribe: %s", err)
	}

	// Publish a message
	err = nc.Publish(subject, []byte(msg))
	if err != nil {
		t.Fatalf("Failed to publish message: %s", err)
	}

	// Wait for the message to arrive
	_, err = sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("Failed to receive message: %s", err)
	}

	// Test round-trip messaging
	replySubject := "reply"
	replyMsg := "Reply from NATS!"

	_, err = nc.SubscribeSync(replySubject)
	if err != nil {
		t.Fatalf("Failed to subscribe to reply: %s", err)
	}

	// Set up a subscription to reply when a message is received
	_, err = nc.Subscribe(subject, func(m *nats.Msg) {
		err := nc.Publish(m.Reply, []byte(replyMsg))
		if err != nil {
			t.Fatalf("Failed to publish message reply: %s", err)
		}
	})
	if err != nil {
		t.Fatalf("Failed to setup reply handler: %s", err)
	}

	// Request and expect a reply
	msgReceived, err := nc.Request(subject, []byte(msg), 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to request/receive reply: %s", err)
	}

	if string(msgReceived.Data) != replyMsg {
		t.Errorf("Expected reply '%s' but got '%s'", replyMsg, string(msgReceived.Data))
	}

	if manualStop {
		<-ctx.Done()
	}
}
