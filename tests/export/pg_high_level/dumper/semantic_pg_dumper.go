package dumper

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SemanticPGDumper implements statefun.SemanticHandler and writes CMDB semantic
// events (types, objects, type links, object links) to PostgreSQL using the
// high-level schema (types / objects / type_links / object_links).
type SemanticPGDumper struct {
	pool *pgxpool.Pool
	ctx  context.Context
}

// NewSemanticPGDumper creates a new SemanticPGDumper connected to the given PostgreSQL URL.
func NewSemanticPGDumper(ctx context.Context, pgURL string) (*SemanticPGDumper, error) {
	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		return nil, fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping PostgreSQL: %w", err)
	}
	return &SemanticPGDumper{pool: pool, ctx: ctx}, nil
}

// InitSchema creates the high-level tables if they don't exist.
func (d *SemanticPGDumper) InitSchema(ctx context.Context) error {
	_, err := d.pool.Exec(ctx, SchemaSQL)
	return err
}

// Close closes the connection pool.
func (d *SemanticPGDumper) Close() {
	d.pool.Close()
}

// --- SemanticHandler implementation ---

func (d *SemanticPGDumper) OnTypePut(id string, body json.RawMessage) error {
	b := jsonOrEmpty(body)
	_, err := d.pool.Exec(d.ctx,
		`INSERT INTO types (id, body) VALUES ($1, $2::jsonb)
		 ON CONFLICT (id) DO UPDATE SET body = $2::jsonb`,
		id, b)
	return err
}

func (d *SemanticPGDumper) OnTypeDelete(id string) error {
	_, err := d.pool.Exec(d.ctx, `DELETE FROM types WHERE id = $1`, id)
	return err
}

func (d *SemanticPGDumper) OnObjectPut(id, typeID string, body json.RawMessage) error {
	b := jsonOrEmpty(body)
	// Ensure referenced type exists (insert with empty body if not yet seen).
	if typeID != "" {
		_, err := d.pool.Exec(d.ctx,
			`INSERT INTO types (id) VALUES ($1) ON CONFLICT DO NOTHING`,
			typeID)
		if err != nil {
			return err
		}
	}

	typeIDPtr := &typeID
	if typeID == "" {
		typeIDPtr = nil
	}
	_, err := d.pool.Exec(d.ctx,
		`INSERT INTO objects (id, type_id, body) VALUES ($1, $2, $3::jsonb)
		 ON CONFLICT (id) DO UPDATE SET type_id = $2, body = $3::jsonb`,
		id, typeIDPtr, b)
	return err
}

func (d *SemanticPGDumper) OnObjectDelete(id string) error {
	_, err := d.pool.Exec(d.ctx, `DELETE FROM objects WHERE id = $1`, id)
	return err
}

func (d *SemanticPGDumper) OnTypeLinkPut(from, to, name, linkType string, body json.RawMessage, tags []string) error {
	b := jsonOrEmpty(body)
	if tags == nil {
		tags = []string{}
	}
	_, err := d.pool.Exec(d.ctx,
		`INSERT INTO type_links (from_type, to_type, name, link_type, tags, body)
		 VALUES ($1, $2, $3, $4, $5, $6::jsonb)
		 ON CONFLICT (from_type, name) DO UPDATE SET to_type = $2, link_type = $4, tags = $5, body = $6::jsonb`,
		from, to, name, linkType, tags, b)
	return err
}

func (d *SemanticPGDumper) OnTypeLinkDelete(from, name string) error {
	_, err := d.pool.Exec(d.ctx,
		`DELETE FROM type_links WHERE from_type = $1 AND name = $2`, from, name)
	return err
}

func (d *SemanticPGDumper) OnObjectLinkPut(from, to, name, linkType string, body json.RawMessage, tags []string) error {
	b := jsonOrEmpty(body)
	if tags == nil {
		tags = []string{}
	}
	_, err := d.pool.Exec(d.ctx,
		`INSERT INTO object_links (from_obj, to_obj, name, link_type, tags, body)
		 VALUES ($1, $2, $3, $4, $5, $6::jsonb)
		 ON CONFLICT (from_obj, name) DO UPDATE SET to_obj = $2, link_type = $4, tags = $5, body = $6::jsonb`,
		from, to, name, linkType, tags, b)
	return err
}

func (d *SemanticPGDumper) OnObjectLinkDelete(from, name string) error {
	_, err := d.pool.Exec(d.ctx,
		`DELETE FROM object_links WHERE from_obj = $1 AND name = $2`, from, name)
	return err
}

// --- Helpers ---

func jsonOrEmpty(b json.RawMessage) string {
	if len(b) == 0 {
		return "{}"
	}
	return string(b)
}

// --- Read helpers for tests / verification ---

func (d *SemanticPGDumper) CountTypes(ctx context.Context) (int, error) {
	var n int
	err := d.pool.QueryRow(ctx, `SELECT COUNT(*) FROM types`).Scan(&n)
	return n, err
}

func (d *SemanticPGDumper) CountObjects(ctx context.Context) (int, error) {
	var n int
	err := d.pool.QueryRow(ctx, `SELECT COUNT(*) FROM objects`).Scan(&n)
	return n, err
}

func (d *SemanticPGDumper) CountTypeLinks(ctx context.Context) (int, error) {
	var n int
	err := d.pool.QueryRow(ctx, `SELECT COUNT(*) FROM type_links`).Scan(&n)
	return n, err
}

func (d *SemanticPGDumper) CountObjectLinks(ctx context.Context) (int, error) {
	var n int
	err := d.pool.QueryRow(ctx, `SELECT COUNT(*) FROM object_links`).Scan(&n)
	return n, err
}

func (d *SemanticPGDumper) ReadObject(ctx context.Context, id string) (typeID string, body json.RawMessage, err error) {
	var typeIDPtr *string
	err = d.pool.QueryRow(ctx,
		`SELECT type_id, body FROM objects WHERE id = $1`, id,
	).Scan(&typeIDPtr, &body)
	if typeIDPtr != nil {
		typeID = *typeIDPtr
	}
	return
}
