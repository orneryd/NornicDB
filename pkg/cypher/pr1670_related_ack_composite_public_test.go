package cypher

// Read-only audit reproduction for NOR-375 on release f0074ec55 at the public
// Cypher path: after an acknowledged-but-undrained write renames both indexed
// values of a composite equality index, the final body is visible to a fresh
// reader (label scan) while the composite full/prefix seeks and MERGE still
// read only committed postings.

import (
	"context"
	"testing"
	"time"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestRelatedAckedCompositeMatchAndMergeSeeFinalMembership(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir:             t.TempDir(),
		WriteBehind:         true,
		WriteBehindInterval: time.Hour, // nothing flushes during the test
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	exec := NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"))
	ctx := context.Background()

	_, err = exec.Execute(ctx, "CREATE INDEX doc_tenant_id IF NOT EXISTS FOR (n:Doc) ON (n.tenant, n.id)", nil)
	require.NoError(t, err)
	indexes := engine.GetSchemaForNamespace("nornic").GetCompositeIndexesForLabel("Doc")
	require.Len(t, indexes, 1, "the fixture needs the composite equality index the seeks use")

	_, err = exec.Execute(ctx, "CREATE (:Doc {tenant: 'before', id: 'stable', c: 0})", nil)
	require.NoError(t, err)
	require.NoError(t, engine.FlushWriteBehind(ctx), "the seed must be committed so the index holds its postings")

	// Acknowledged rename of both indexed values; the write stays buffered.
	_, err = exec.Execute(ctx, "MATCH (n:Doc {tenant: 'before', id: 'stable'}) SET n.tenant = 'changed', n.id = 'changed', n.c = 2", nil)
	require.NoError(t, err)

	// Control: the acknowledged final image is already what a scan returns.
	control, err := exec.Execute(ctx, "MATCH (n:Doc) WHERE n.c = 2 RETURN n.tenant AS t, n.id AS i", nil)
	require.NoError(t, err)
	require.Len(t, control.Rows, 1, "the acknowledged final body must be visible")
	require.Equal(t, "changed", control.Rows[0][0])
	require.Equal(t, "changed", control.Rows[0][1])

	full, err := exec.Execute(ctx, "MATCH (n:Doc {tenant: 'changed', id: 'changed'}) RETURN n.c AS c", nil)
	require.NoError(t, err)
	require.Len(t, full.Rows, 1, "a full composite equality MATCH must find the acknowledged final node")
	require.Equal(t, int64(2), full.Rows[0][0])

	prefix, err := exec.Execute(ctx, "MATCH (n:Doc {tenant: 'changed'}) RETURN n.c AS c", nil)
	require.NoError(t, err)
	require.Len(t, prefix.Rows, 1, "a composite prefix MATCH must find the acknowledged final node")
	require.Equal(t, int64(2), prefix.Rows[0][0])

	merged, err := exec.Execute(ctx, "MERGE (n:Doc {tenant: 'changed', id: 'changed'}) RETURN n.c AS c", nil)
	require.NoError(t, err)
	require.Len(t, merged.Rows, 1)
	require.Equal(t, int64(2), merged.Rows[0][0], "MERGE must reuse the acknowledged final node")

	count, err := exec.Execute(ctx, "MATCH (n:Doc) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Len(t, count.Rows, 1)
	require.Equal(t, int64(1), count.Rows[0][0], "MERGE must not create a duplicate of the acknowledged node")
}

func TestRelatedAckedOrderedPagingIncludesLowerRankedHolder(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"))
	for _, q := range []string{"CREATE INDEX doc_rank FOR (n:Doc) ON (n.rank)", "CREATE (:Doc {id:'doc',rank:0}),(:Doc {id:'peer',rank:20})"} {
		_, err = exec.Execute(t.Context(), q, nil)
		require.NoError(t, err)
	}
	require.NoError(t, engine.FlushWriteBehind(t.Context()))
	_, err = exec.Execute(t.Context(), "MATCH (n:Doc {id:'doc'}) SET n.rank=10", nil)
	require.NoError(t, err)
	result, err := exec.Execute(t.Context(), "MATCH (n:Doc) WHERE n.rank >= 10 RETURN n.id,n.rank ORDER BY n.rank LIMIT 1", nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{"doc", int64(10)}}, result.Rows, "the durable higher-ranked peer must not hide an ACKed lower-ranked row")
}
