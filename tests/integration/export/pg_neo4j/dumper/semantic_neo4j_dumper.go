package dumper

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// SemanticNeo4jDumper implements statefun.SemanticHandler and writes CMDB
// semantic events to Neo4j as nodes and relationships.
type SemanticNeo4jDumper struct {
	driver  neo4j.DriverWithContext
	ctx     context.Context
	session neo4j.SessionWithContext
	tx      neo4j.ExplicitTransaction
}

// NewSemanticNeo4jDumper creates a new Neo4j dumper connected to the given URI.
func NewSemanticNeo4jDumper(ctx context.Context, uri, user, password string) (*SemanticNeo4jDumper, error) {
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(user, password, ""))
	if err != nil {
		return nil, fmt.Errorf("create neo4j driver: %w", err)
	}
	if err := driver.VerifyConnectivity(ctx); err != nil {
		driver.Close(ctx)
		return nil, fmt.Errorf("verify neo4j connectivity: %w", err)
	}
	return &SemanticNeo4jDumper{driver: driver, ctx: ctx}, nil
}

// Close closes the driver.
func (d *SemanticNeo4jDumper) Close() {
	d.driver.Close(d.ctx)
}

// InitSchema creates indexes for fast lookups.
func (d *SemanticNeo4jDumper) InitSchema(ctx context.Context) error {
	session := d.driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	constraints := []string{
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:Type) REQUIRE n.id IS UNIQUE",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:Object) REQUIRE n.id IS UNIQUE",
	}
	for _, c := range constraints {
		if _, err := session.Run(ctx, c, nil); err != nil {
			return fmt.Errorf("create constraint: %w", err)
		}
	}
	return nil
}

// --- Batch support ---

func (d *SemanticNeo4jDumper) BeginBatch() {
	d.session = d.driver.NewSession(d.ctx, neo4j.SessionConfig{})
	tx, err := d.session.BeginTransaction(d.ctx)
	if err != nil {
		d.session.Close(d.ctx)
		d.session = nil
		d.tx = nil
		return
	}
	d.tx = tx
}

func (d *SemanticNeo4jDumper) CommitBatch() error {
	if d.tx == nil {
		return nil
	}
	err := d.tx.Commit(d.ctx)
	d.tx = nil
	if d.session != nil {
		d.session.Close(d.ctx)
		d.session = nil
	}
	return err
}

// --- SemanticHandler implementation ---

func (d *SemanticNeo4jDumper) OnTypePut(id string, body json.RawMessage) error {
	_, err := d.tx.Run(d.ctx,
		"MERGE (n:Type {id: $id}) SET n.body = $body",
		map[string]any{"id": id, "body": jsonStr(body)})
	return err
}

func (d *SemanticNeo4jDumper) OnTypeDelete(id string) error {
	_, err := d.tx.Run(d.ctx,
		"MATCH (n:Type {id: $id}) DETACH DELETE n",
		map[string]any{"id": id})
	return err
}

func (d *SemanticNeo4jDumper) OnObjectPut(id, typeID string, body json.RawMessage) error {
	// Ensure type node exists
	if typeID != "" {
		_, err := d.tx.Run(d.ctx,
			"MERGE (n:Type {id: $id})",
			map[string]any{"id": typeID})
		if err != nil {
			return err
		}
	}

	_, err := d.tx.Run(d.ctx,
		"MERGE (n:Object {id: $id}) SET n.type_id = $typeID, n.body = $body",
		map[string]any{"id": id, "typeID": typeID, "body": jsonStr(body)})
	return err
}

func (d *SemanticNeo4jDumper) OnObjectDelete(id string) error {
	_, err := d.tx.Run(d.ctx,
		"MATCH (n:Object {id: $id}) DETACH DELETE n",
		map[string]any{"id": id})
	return err
}

func (d *SemanticNeo4jDumper) OnTypeLinkPut(from, to, name, linkType string, body json.RawMessage, tags []string) error {
	_, err := d.tx.Run(d.ctx,
		"MATCH (a:Type {id: $from}), (b:Type {id: $to}) "+
			"MERGE (a)-[r:TYPE_LINK {name: $name}]->(b) "+
			"SET r.link_type = $lt, r.body = $body, r.tags = $tags",
		map[string]any{"from": from, "to": to, "name": name, "lt": linkType, "body": jsonStr(body), "tags": tags})
	return err
}

func (d *SemanticNeo4jDumper) OnTypeLinkDelete(from, name string) error {
	_, err := d.tx.Run(d.ctx,
		"MATCH (a:Type {id: $from})-[r:TYPE_LINK {name: $name}]->() DELETE r",
		map[string]any{"from": from, "name": name})
	return err
}

func (d *SemanticNeo4jDumper) OnObjectLinkPut(from, to, name, linkType string, body json.RawMessage, tags []string) error {
	_, err := d.tx.Run(d.ctx,
		"MATCH (a:Object {id: $from}), (b:Object {id: $to}) "+
			"MERGE (a)-[r:OBJECT_LINK {name: $name}]->(b) "+
			"SET r.link_type = $lt, r.body = $body, r.tags = $tags",
		map[string]any{"from": from, "to": to, "name": name, "lt": linkType, "body": jsonStr(body), "tags": tags})
	return err
}

func (d *SemanticNeo4jDumper) OnObjectLinkDelete(from, name string) error {
	_, err := d.tx.Run(d.ctx,
		"MATCH (a:Object {id: $from})-[r:OBJECT_LINK {name: $name}]->() DELETE r",
		map[string]any{"from": from, "name": name})
	return err
}

// --- Helpers ---

func jsonStr(b json.RawMessage) string {
	if len(b) == 0 {
		return "{}"
	}
	return string(b)
}

// --- Read helpers for verification ---

func (d *SemanticNeo4jDumper) CountNodes(ctx context.Context, label string) (int, error) {
	session := d.driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	result, err := session.Run(ctx, "MATCH (n:"+label+") RETURN count(n) AS cnt", nil)
	if err != nil {
		return 0, err
	}
	if result.Next(ctx) {
		cnt, _ := result.Record().Get("cnt")
		return int(cnt.(int64)), nil
	}
	return 0, result.Err()
}

func (d *SemanticNeo4jDumper) CountRelationships(ctx context.Context, relType string) (int, error) {
	session := d.driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	result, err := session.Run(ctx, "MATCH ()-[r:"+relType+"]->() RETURN count(r) AS cnt", nil)
	if err != nil {
		return 0, err
	}
	if result.Next(ctx) {
		cnt, _ := result.Record().Get("cnt")
		return int(cnt.(int64)), nil
	}
	return 0, result.Err()
}
