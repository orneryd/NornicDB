//go:build integration
// +build integration

package cypher_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
)

// TestIntegration_CreateTranslatesTo_FromRealData reproduces the exact migration
// edge-creation query shape against a real on-disk dataset.
//
// Run with:
//
//	INTEGRATION_TEST=1 go test -tags=integration -v ./pkg/cypher -run TestIntegration_CreateTranslatesTo_FromRealData
func TestIntegration_CreateTranslatesTo_FromRealData(t *testing.T) {
	if os.Getenv("INTEGRATION_TEST") == "" {
		t.Skip("Set INTEGRATION_TEST=1 to run")
	}

	dataDir := os.Getenv("NORNICDB_REAL_DATA_DIR")
	if dataDir == "" {
		dataDir = "/usr/local/var/nornicdb/data"
	}
	if _, err := os.Stat(dataDir); err != nil {
		t.Skipf("real data dir unavailable: %s (%v)", dataDir, err)
	}

	db, err := nornicdb.Open(dataDir, nornicdb.DefaultConfig())
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	mgr, err := multidb.NewDatabaseManager(t.Context(), db.GetBaseStorageForManager(), multidb.DefaultConfig())
	require.NoError(t, err)

	const dbName = "caremark_translation"
	require.True(t, mgr.Exists(dbName), "expected database %q to exist in real data dir", dbName)

	store, err := mgr.GetStorage(dbName)
	require.NoError(t, err)

	exec := cypher.NewStorageExecutor(store)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	beforeRes, err := exec.Execute(ctx,
		"MATCH (:OriginalText)-[r:TRANSLATES_TO]->(:TranslatedText) RETURN count(r) AS c",
		nil,
	)
	require.NoError(t, err)
	require.NotEmpty(t, beforeRes.Rows)
	before := toInt64ForIntegration(t, beforeRes.Rows[0][0])

	// Exact key pull shape used by the migration script's get_original_join_keys.
	keysRes, err := exec.Execute(ctx, `
MATCH (o:OriginalText)
WHERE o.__tmpJoinKey IS NOT NULL
RETURN DISTINCT o.__tmpJoinKey AS joinKey
ORDER BY joinKey
LIMIT 200
`, nil)
	require.NoError(t, err)
	require.NotEmpty(t, keysRes.Rows, "no original join keys found")

	keys := make([]string, 0, len(keysRes.Rows))
	for _, row := range keysRes.Rows {
		require.NotEmpty(t, row)
		k, ok := row[0].(string)
		require.True(t, ok, "join key must be string, got %T", row[0])
		keys = append(keys, k)
	}

	createCtx, createCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer createCancel()

	diagRes, err := exec.Execute(ctx,
		"WITH $keys AS keys "+
			"UNWIND keys AS joinKey "+
			"MATCH (o:OriginalText) WHERE o.__tmpJoinKey = joinKey "+
			"MATCH (t:TranslatedText) WHERE t.__tmpJoinKey = joinKey "+
			"RETURN count(*) AS joinable_pairs",
		map[string]interface{}{"keys": keys},
	)
	require.NoError(t, err)
	require.NotEmpty(t, diagRes.Rows)
	joinablePairs := toInt64ForIntegration(t, diagRes.Rows[0][0])
	t.Logf("joinable_pairs=%d keys=%d", joinablePairs, len(keys))

	// Exact query shape used by scripts/migrate_mongodb_to_nornic.py apply_edge_batch.
	batchRes, err := exec.Execute(createCtx,
		"UNWIND $keys AS joinKey "+
			"MATCH (o:OriginalText) "+
			"WHERE o.__tmpJoinKey = joinKey "+
			"MATCH (t:TranslatedText) "+
			"WHERE t.__tmpJoinKey = joinKey "+
			"  AND NOT (o)-[:TRANSLATES_TO]->(t) "+
			"CREATE (o)-[:TRANSLATES_TO]->(t) "+
			"RETURN count(*) AS created_pairs",
		map[string]interface{}{"keys": keys},
	)
	require.NoError(t, err)
	require.NotEmpty(t, batchRes.Rows)
	require.NotEmpty(t, batchRes.Rows[0])
	createdPairs := toInt64ForIntegration(t, batchRes.Rows[0][0])
	t.Logf("created_pairs=%d keys=%d", createdPairs, len(keys))

	afterRes, err := exec.Execute(ctx,
		"MATCH (:OriginalText)-[r:TRANSLATES_TO]->(:TranslatedText) RETURN count(r) AS c",
		nil,
	)
	require.NoError(t, err)
	require.NotEmpty(t, afterRes.Rows)
	after := toInt64ForIntegration(t, afterRes.Rows[0][0])
	t.Logf("before=%d after=%d delta=%d", before, after, after-before)

	require.Greater(t, joinablePairs, int64(0), "expected at least one joinable pair in sample")
	require.GreaterOrEqual(t, after-before, int64(0))
	if createdPairs > 0 {
		require.GreaterOrEqual(t, after, before+createdPairs)
	}
}

func toInt64ForIntegration(t *testing.T, v interface{}) int64 {
	t.Helper()
	switch x := v.(type) {
	case nil:
		return 0
	case int64:
		return x
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case float64:
		return int64(x)
	default:
		t.Fatalf("unexpected numeric type %T (%v)", v, v)
		return 0
	}
}
