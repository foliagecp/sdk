package dumper

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/foliagecp/sdk/statefun"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGDumper applies export events to a PostgreSQL database.
type PGDumper struct {
	pool *pgxpool.Pool
}

// NewPGDumper creates a new PGDumper connected to the given PostgreSQL URL.
func NewPGDumper(ctx context.Context, pgURL string) (*PGDumper, error) {
	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}

	return &PGDumper{pool: pool}, nil
}

// InitSchema creates the vertices and links tables if they don't exist.
func (d *PGDumper) InitSchema(ctx context.Context) error {
	_, err := d.pool.Exec(ctx, SchemaSQL)
	return err
}

// ApplyEvent applies all operations in an export event within a single PG transaction.
func (d *PGDumper) ApplyEvent(ctx context.Context, event statefun.ExportEvent) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, op := range event.Ops {
		if err := applyOp(ctx, tx, op); err != nil {
			return fmt.Errorf("failed to apply op %s: %w", op.Op, err)
		}
	}

	return tx.Commit(ctx)
}

func applyOp(ctx context.Context, tx pgx.Tx, op statefun.ExportOp) error {
	switch op.Op {
	case "vertex_put":
		body := "{}"
		if len(op.Body) > 0 {
			body = string(op.Body)
		}
		_, err := tx.Exec(ctx,
			`INSERT INTO vertices (id, body) VALUES ($1, $2::jsonb)
			 ON CONFLICT (id) DO UPDATE SET body = $2::jsonb`,
			op.ID, body)
		return err

	case "vertex_delete":
		_, err := tx.Exec(ctx, `DELETE FROM vertices WHERE id = $1`, op.ID)
		return err

	case "link_put":
		body := "{}"
		if len(op.Body) > 0 {
			body = string(op.Body)
		}
		tags := op.Tags
		if tags == nil {
			tags = []string{}
		}
		_, err := tx.Exec(ctx,
			`INSERT INTO links (from_id, to_id, name, link_type, tags, body)
			 VALUES ($1, $2, $3, $4, $5, $6::jsonb)
			 ON CONFLICT (from_id, name) DO UPDATE SET
			   to_id = $2, link_type = $4, tags = $5, body = $6::jsonb`,
			op.From, op.To, op.Name, op.LinkType, tags, body)
		return err

	case "link_delete":
		_, err := tx.Exec(ctx, `DELETE FROM links WHERE from_id = $1 AND name = $2`,
			op.From, op.Name)
		return err

	default:
		return fmt.Errorf("unknown operation type: %s", op.Op)
	}
}

// Close closes the database connection pool.
func (d *PGDumper) Close() {
	d.pool.Close()
}

// ReadVertex reads a vertex from PostgreSQL.
func (d *PGDumper) ReadVertex(ctx context.Context, id string) (json.RawMessage, error) {
	var body json.RawMessage
	err := d.pool.QueryRow(ctx, `SELECT body FROM vertices WHERE id = $1`, id).Scan(&body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

// ReadLink reads a link from PostgreSQL.
func (d *PGDumper) ReadLink(ctx context.Context, fromID, name string) (*statefun.ExportOp, error) {
	var op statefun.ExportOp
	var body json.RawMessage
	err := d.pool.QueryRow(ctx,
		`SELECT from_id, to_id, name, link_type, tags, body FROM links WHERE from_id = $1 AND name = $2`,
		fromID, name).Scan(&op.From, &op.To, &op.Name, &op.LinkType, &op.Tags, &body)
	if err != nil {
		return nil, err
	}
	op.Body = body
	return &op, nil
}

// CountVertices returns the number of vertices.
func (d *PGDumper) CountVertices(ctx context.Context) (int, error) {
	var count int
	err := d.pool.QueryRow(ctx, `SELECT COUNT(*) FROM vertices`).Scan(&count)
	return count, err
}

// CountLinks returns the number of links.
func (d *PGDumper) CountLinks(ctx context.Context) (int, error) {
	var count int
	err := d.pool.QueryRow(ctx, `SELECT COUNT(*) FROM links`).Scan(&count)
	return count, err
}
