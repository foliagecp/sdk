package tests

import (
	"fmt"
	"github.com/foliagecp/easyjson"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

const (
	natsURL = "nats://nats:foliage@nats1:4222,nats://nats:foliage@nats2:4222,nats://nats:foliage@nats3:4222"
	domain  = "hub"
	timeout = 2 * time.Second
)

func TestClusterOperations(t *testing.T) {
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err, "Failed to connect to NATS cluster")
	defer nc.Close()

	t.Logf("Connected to NATS: %s", natsURL)

	err = createType(t, nc, domain, "type_testing", "Test Type")
	if err != nil {
		t.Logf("Warning: Type creation failed: %v", err)
	}

	t.Log("Starting cluster write test...")
	for i := 0; i < 100; i++ {
		objectID := fmt.Sprintf("cluster_test%d", i)

		err := createObject(t, nc, domain, objectID, "type_testing", fmt.Sprintf("test_cluster_object %d", i))
		if err != nil {
			t.Errorf("clusterTest, creating object error: %v", err)
		} else {
			t.Logf("clusterTest, created object: %d", i)
		}
	}

	t.Log("Waiting 60 seconds before starting the read test. You can turn off the leader of KV and restart runtime foliage to flush cache")
	//TODO automate it?

	time.Sleep(60 * time.Second)

	t.Log("Starting cluster read test...")

	for i := 0; i < 100; i++ {
		objectID := fmt.Sprintf("cluster_test%d", i)
		obj, err := readObject(t, nc, domain, objectID)
		if err != nil {
			t.Errorf("clusterTest, read object error: %v", err)
		} else {
			t.Logf("clusterTest, read object %d: %v", i, obj.ToString())
		}
	}

	t.Log("Test completed")
}

func createType(t *testing.T, nc *nats.Conn, domain string, typeName string, description string) error {
	typeBody := easyjson.NewJSONObject()
	typeBody.SetByPath("description", easyjson.NewJSON(description))

	typePayload := easyjson.NewJSONObject()
	typePayload.SetByPath("body", typeBody)
	typePayload.SetByPath("upsert", easyjson.NewJSON(true))

	typeMessage := easyjson.NewJSONObject()
	typeMessage.SetByPath("payload", typePayload)

	typeSubject := fmt.Sprintf("request.%s.functions.cmdb.api.type.create.%s", domain, typeName)
	t.Logf("Creating type '%s' via request to %s", typeName, typeSubject)

	typeResponse, err := nc.Request(typeSubject, []byte(typeMessage.ToString()), timeout)
	if err != nil {
		return err
	}

	typeResponseJson := easyjson.NewJSON(string(typeResponse.Data))
	t.Logf("Type creation response: %s", typeResponseJson.ToString())
	return nil
}

func createObject(t *testing.T, nc *nats.Conn, domain string, objectID string, typeName string, objectName string) error {
	body := easyjson.NewJSONObjectWithKeyValue("name", easyjson.NewJSON(objectName))

	payload := easyjson.NewJSONObject()
	payload.SetByPath("body", body)
	payload.SetByPath("origin_type", easyjson.NewJSON(typeName))
	payload.SetByPath("upsert", easyjson.NewJSON(true))

	message := easyjson.NewJSONObject()
	message.SetByPath("payload", payload)

	requestSubject := fmt.Sprintf("request.%s.functions.cmdb.api.object.create.%s", domain, objectID)

	t.Logf("Creating object %s via request to %s", objectID, requestSubject)

	response, err := nc.Request(requestSubject, []byte(message.ToString()), timeout)
	if err != nil {
		return err
	}

	responseJson, ok := easyjson.JSONFromBytes(response.Data)
	if !ok {
		return fmt.Errorf("failed to parse response: %s", string(response.Data))
	}
	status, ok := responseJson.GetByPath("status").AsString()
	if ok && status == "ok" {
		return nil
	}

	return fmt.Errorf("object creation failed: %s", responseJson.ToString())
}

func readObject(t *testing.T, nc *nats.Conn, domain string, objectID string) (easyjson.JSON, error) {
	requestSubject := fmt.Sprintf("request.%s.functions.cmdb.api.object.read.%s", domain, objectID)

	message := easyjson.NewJSONObject()

	t.Logf("Reading object %s via request to %s", objectID, requestSubject)

	response, err := nc.Request(requestSubject, []byte(message.ToString()), timeout)
	if err != nil {
		return easyjson.JSON{}, err
	}

	responseJson, ok := easyjson.JSONFromBytes(response.Data)
	if !ok {
		return easyjson.JSON{}, fmt.Errorf("failed to parse response: %s", string(response.Data))
	}
	status, ok := responseJson.GetByPath("status").AsString()
	if status != "ok" || !ok {
		return easyjson.JSON{}, fmt.Errorf("object read failed: %s", responseJson.ToString())
	}

	objectData := responseJson.GetByPath("data")

	return objectData, nil
}
