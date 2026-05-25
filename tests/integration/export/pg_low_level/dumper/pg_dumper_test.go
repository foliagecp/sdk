package dumper_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/tests/integration/export/pg_low_level/dumper"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupPG(t *testing.T) (*dumper.PGDumper, context.Context) {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       "foliage_test",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(30 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "5432")
	require.NoError(t, err)

	pgURL := fmt.Sprintf("postgres://test:test@%s:%s/foliage_test?sslmode=disable", host, port.Port())

	d, err := dumper.NewPGDumper(ctx, pgURL)
	require.NoError(t, err)
	t.Cleanup(func() { d.Close() })

	err = d.InitSchema(ctx)
	require.NoError(t, err)

	return d, ctx
}

func TestPGDumper_VertexPut(t *testing.T) {
	d, ctx := setupPG(t)

	event := statefun.ExportEvent{
		TxID:   "tx-001",
		Domain: "hub",
		Ops: []statefun.ExportOp{
			{Op: "vertex_put", ID: "hub/srv01", Body: json.RawMessage(`{"cpu":16,"ram":64}`)},
		},
	}

	err := d.ApplyEvent(ctx, event)
	require.NoError(t, err)

	body, err := d.ReadVertex(ctx, "hub/srv01")
	require.NoError(t, err)
	assert.JSONEq(t, `{"cpu":16,"ram":64}`, string(body))
}

func TestPGDumper_VertexPut_Update(t *testing.T) {
	d, ctx := setupPG(t)

	// Create
	err := d.ApplyEvent(ctx, statefun.ExportEvent{
		TxID: "tx-001",
		Ops:  []statefun.ExportOp{{Op: "vertex_put", ID: "hub/v1", Body: json.RawMessage(`{"v":1}`)}},
	})
	require.NoError(t, err)

	// Update
	err = d.ApplyEvent(ctx, statefun.ExportEvent{
		TxID: "tx-002",
		Ops:  []statefun.ExportOp{{Op: "vertex_put", ID: "hub/v1", Body: json.RawMessage(`{"v":2}`)}},
	})
	require.NoError(t, err)

	body, err := d.ReadVertex(ctx, "hub/v1")
	require.NoError(t, err)
	assert.JSONEq(t, `{"v":2}`, string(body))
}

func TestPGDumper_VertexDelete(t *testing.T) {
	d, ctx := setupPG(t)

	// Create then delete
	err := d.ApplyEvent(ctx, statefun.ExportEvent{
		TxID: "tx-001",
		Ops:  []statefun.ExportOp{{Op: "vertex_put", ID: "hub/v1", Body: json.RawMessage(`{"a":1}`)}},
	})
	require.NoError(t, err)

	err = d.ApplyEvent(ctx, statefun.ExportEvent{
		TxID: "tx-002",
		Ops:  []statefun.ExportOp{{Op: "vertex_delete", ID: "hub/v1"}},
	})
	require.NoError(t, err)

	_, err = d.ReadVertex(ctx, "hub/v1")
	assert.ErrorIs(t, err, pgx.ErrNoRows)
}

func TestPGDumper_LinkPut(t *testing.T) {
	d, ctx := setupPG(t)

	event := statefun.ExportEvent{
		TxID: "tx-001",
		Ops: []statefun.ExportOp{
			{
				Op:       "link_put",
				From:     "hub/srv01",
				To:       "hub/rackA",
				Name:     "hosts",
				LinkType: "__object",
				Tags:     []string{"critical", "production"},
				Body:     json.RawMessage(`{"zone":1}`),
			},
		},
	}

	err := d.ApplyEvent(ctx, event)
	require.NoError(t, err)

	link, err := d.ReadLink(ctx, "hub/srv01", "hosts")
	require.NoError(t, err)
	assert.Equal(t, "hub/rackA", link.To)
	assert.Equal(t, "__object", link.LinkType)
	assert.ElementsMatch(t, []string{"critical", "production"}, link.Tags)
	assert.JSONEq(t, `{"zone":1}`, string(link.Body))
}

func TestPGDumper_LinkDelete(t *testing.T) {
	d, ctx := setupPG(t)

	// Create then delete
	err := d.ApplyEvent(ctx, statefun.ExportEvent{
		TxID: "tx-001",
		Ops: []statefun.ExportOp{
			{Op: "link_put", From: "hub/a", To: "hub/b", Name: "link1", LinkType: "t1"},
		},
	})
	require.NoError(t, err)

	err = d.ApplyEvent(ctx, statefun.ExportEvent{
		TxID: "tx-002",
		Ops:  []statefun.ExportOp{{Op: "link_delete", From: "hub/a", Name: "link1"}},
	})
	require.NoError(t, err)

	_, err = d.ReadLink(ctx, "hub/a", "link1")
	assert.ErrorIs(t, err, pgx.ErrNoRows)
}

func TestPGDumper_Idempotency(t *testing.T) {
	d, ctx := setupPG(t)

	event := statefun.ExportEvent{
		TxID: "tx-001",
		Ops: []statefun.ExportOp{
			{Op: "vertex_put", ID: "hub/v1", Body: json.RawMessage(`{"a":1}`)},
			{Op: "link_put", From: "hub/v1", To: "hub/v2", Name: "link1", LinkType: "t1", Body: json.RawMessage(`{"b":2}`)},
		},
	}

	// Apply twice — should be idempotent (ON CONFLICT DO UPDATE)
	err := d.ApplyEvent(ctx, event)
	require.NoError(t, err)
	err = d.ApplyEvent(ctx, event)
	require.NoError(t, err)

	count, err := d.CountVertices(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	linkCount, err := d.CountLinks(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, linkCount)
}

func TestPGDumper_TransactionAtomicity(t *testing.T) {
	d, ctx := setupPG(t)

	// Apply event with multiple ops — all should succeed or fail together
	event := statefun.ExportEvent{
		TxID: "tx-001",
		Ops: []statefun.ExportOp{
			{Op: "vertex_put", ID: "hub/v1", Body: json.RawMessage(`{"x":1}`)},
			{Op: "vertex_put", ID: "hub/v2", Body: json.RawMessage(`{"x":2}`)},
			{Op: "vertex_put", ID: "hub/v3", Body: json.RawMessage(`{"x":3}`)},
			{Op: "link_put", From: "hub/v1", To: "hub/v2", Name: "link12", LinkType: "t"},
			{Op: "link_put", From: "hub/v2", To: "hub/v3", Name: "link23", LinkType: "t"},
		},
	}

	err := d.ApplyEvent(ctx, event)
	require.NoError(t, err)

	count, err := d.CountVertices(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	linkCount, err := d.CountLinks(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, linkCount)
}

func TestPGDumper_MultipleTransactions(t *testing.T) {
	d, ctx := setupPG(t)

	for i := 0; i < 20; i++ {
		event := statefun.ExportEvent{
			TxID: fmt.Sprintf("tx-%03d", i),
			Ops: []statefun.ExportOp{
				{Op: "vertex_put", ID: fmt.Sprintf("hub/v%03d", i), Body: json.RawMessage(fmt.Sprintf(`{"i":%d}`, i))},
			},
		}
		err := d.ApplyEvent(ctx, event)
		require.NoError(t, err)
	}

	count, err := d.CountVertices(ctx)
	require.NoError(t, err)
	assert.Equal(t, 20, count)

	// Verify first and last
	body, err := d.ReadVertex(ctx, "hub/v000")
	require.NoError(t, err)
	assert.JSONEq(t, `{"i":0}`, string(body))

	body, err = d.ReadVertex(ctx, "hub/v019")
	require.NoError(t, err)
	assert.JSONEq(t, `{"i":19}`, string(body))
}

func BenchmarkPGDumper_BulkInsert(b *testing.B) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       "bench_test",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(30 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer container.Terminate(ctx)

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "5432")
	pgURL := fmt.Sprintf("postgres://test:test@%s:%s/bench_test?sslmode=disable", host, port.Port())

	d, err := dumper.NewPGDumper(ctx, pgURL)
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()
	_ = d.InitSchema(ctx)

	// Build a large event with 1000 vertex_put ops
	ops := make([]statefun.ExportOp, 1000)
	for i := 0; i < 1000; i++ {
		ops[i] = statefun.ExportOp{
			Op:   "vertex_put",
			ID:   fmt.Sprintf("hub/bench%04d", i),
			Body: json.RawMessage(fmt.Sprintf(`{"i":%d}`, i)),
		}
	}
	event := statefun.ExportEvent{TxID: "bench-tx", Ops: ops}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d.ApplyEvent(ctx, event)
	}
}
