package cypher

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTemporalParam_SubstitutionPreservesTypedBinding(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporal_param_subst"))
	dt := time.Date(2026, 6, 9, 12, 0, 0, 0, time.FixedZone("UTC+2", 2*3600))

	query := "CREATE (n:T {uuid:$uuid, created_at:$dt}) RETURN n"
	out := exec.substituteParams(query, map[string]interface{}{
		"uuid": "t",
		"dt":   dt,
	})

	// Scalar params still substitute, but temporal params must remain bound so
	// parsePropertyValue can resolve the typed value from context.
	require.Contains(t, out, "uuid:'t'")
	require.True(t, strings.Contains(out, "created_at:$dt"), "expected temporal param to remain bound, got: %s", out)
}

func TestTemporalParam_CreateAndReadBackAsDateTime(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporal_param_roundtrip"))
	ctx := context.Background()
	dt := time.Date(2026, 6, 9, 12, 0, 0, 0, time.FixedZone("UTC+2", 2*3600))

	_, err := exec.Execute(ctx, "CREATE (n:T {uuid:'t', created_at:$dt})", map[string]interface{}{"dt": dt})
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:T {uuid:'t'}) RETURN n.created_at", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0], 1)

	got, ok := result.Rows[0][0].(CypherDateTime)
	require.True(t, ok, "expected CypherDateTime, got %T", result.Rows[0][0])
	require.Equal(t, dt.UTC(), got.Time.UTC())
}

func TestTemporalParam_UnwindNestedRowDatetimeRoundTripsAsDateTime(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporal_unwind_nested"))
	ctx := context.Background()
	dt := time.Date(2026, 6, 19, 12, 0, 0, 123456000, time.UTC)

	rows := []map[string]interface{}{
		{"uuid": "1", "created_at": dt},
	}

	_, err := exec.Execute(ctx, `
UNWIND $rows AS row
MERGE (n:X {uuid: row.uuid})
SET n.created_at = row.created_at
`, map[string]interface{}{"rows": rows})
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:X {uuid:'1'}) RETURN n.created_at", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	got, ok := result.Rows[0][0].(CypherDateTime)
	require.True(t, ok, "expected CypherDateTime, got %T (%v)", result.Rows[0][0], result.Rows[0][0])
	require.Equal(t, dt.UTC(), got.Time.UTC())
}

func TestTemporalParam_UnwindNestedRowDatetimeInMapLiteralRoundTripsAsDateTime(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporal_unwind_nested_map"))
	ctx := context.Background()
	dt := time.Date(2026, 6, 19, 12, 0, 0, 123456000, time.UTC)

	_, err := exec.Execute(ctx, `
UNWIND $rows AS row
MERGE (n:X {uuid: row.uuid})
SET n = {uuid: row.uuid, created_at: row.created_at}
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{"uuid": "1", "created_at": dt},
		},
	})
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:X {uuid:'1'}) RETURN n.created_at", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	got, ok := result.Rows[0][0].(CypherDateTime)
	require.True(t, ok, "expected CypherDateTime, got %T (%v)", result.Rows[0][0], result.Rows[0][0])
	require.Equal(t, dt.UTC(), got.Time.UTC())
}

func TestTemporalParam_UnwindNestedRowDatetimeOnRelationshipRoundTripsAsDateTime(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporal_unwind_nested_rel"))
	ctx := context.Background()
	dt := time.Date(2026, 6, 19, 12, 0, 0, 123456000, time.UTC)

	_, err := exec.Execute(ctx, "CREATE (:X {uuid:'a'}), (:X {uuid:'b'})", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
UNWIND $rows AS row
MATCH (a:X {uuid:'a'})
MATCH (b:X {uuid:'b'})
MERGE (a)-[r:MENTIONS {uuid: row.uuid}]->(b)
SET r.created_at = row.created_at
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{"uuid": "m1", "created_at": dt},
		},
	})
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (:X)-[r:MENTIONS {uuid:'m1'}]->(:X) RETURN r.created_at", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	got, ok := result.Rows[0][0].(CypherDateTime)
	require.True(t, ok, "expected CypherDateTime, got %T (%v)", result.Rows[0][0], result.Rows[0][0])
	require.Equal(t, dt.UTC(), got.Time.UTC())
}

func TestTemporalParam_UnwindNestedRowDatetimeInWholeRowMapsRoundTripsAsDateTime(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporal_unwind_nested_whole_row"))
	ctx := context.Background()
	dt := time.Date(2026, 6, 19, 12, 0, 0, 123456000, time.UTC)

	_, err := exec.Execute(ctx, `
UNWIND $rows AS row
MERGE (n:X {uuid: row.uuid})
SET n = row
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{"uuid": "1", "created_at": dt},
		},
	})
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "CREATE (:X {uuid:'a'}), (:X {uuid:'b'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
UNWIND $rows AS row
MATCH (a:X {uuid:'a'})
MATCH (b:X {uuid:'b'})
MERGE (a)-[r:MENTIONS {uuid: row.uuid}]->(b)
SET r += row
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{"uuid": "m1", "created_at": dt},
		},
	})
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:X {uuid:'1'}) RETURN n.created_at", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	nodeDateTime, ok := result.Rows[0][0].(CypherDateTime)
	require.True(t, ok, "expected node CypherDateTime, got %T (%v)", result.Rows[0][0], result.Rows[0][0])
	require.Equal(t, dt.UTC(), nodeDateTime.Time.UTC())

	result, err = exec.Execute(ctx, "MATCH (:X)-[r:MENTIONS {uuid:'m1'}]->(:X) RETURN r.created_at", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	relDateTime, ok := result.Rows[0][0].(CypherDateTime)
	require.True(t, ok, "expected relationship CypherDateTime, got %T (%v)", result.Rows[0][0], result.Rows[0][0])
	require.Equal(t, dt.UTC(), relDateTime.Time.UTC())
}
