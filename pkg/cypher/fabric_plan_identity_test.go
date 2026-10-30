package cypher

import (
	"context"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newFabricIdentityExecutor(t testing.TB) *StorageExecutor {
	t.Helper()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	manager, err := multidb.NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "identity_part"}))
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "identity", []multidb.ConstituentRef{
		{Alias: "part", DatabaseName: "identity_part", Type: "local", AccessMode: "read_write"},
	}))
	store, err := manager.GetStorage(manager.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(store)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	exec.cache = nil // Exercise planning on every public call.
	return exec
}

// Author: #1640 lane (Codex). Cache identity belongs to the accepted statement;
// a cached fragment must not execute another literal or retain another source view.
func TestExecute_FabricPlanCache_ExactStatementIdentity(t *testing.T) {
	restore := config.WithCypherQueryNormalizationDisabled()
	t.Cleanup(restore)
	exec := newFabricIdentityExecutor(t)
	for index, sample := range []struct{ query, value string }{
		{"USE identity CALL { USE identity.part RETURN 'a  b' AS out } RETURN out", "a  b"},
		{"USE identity CALL { USE identity.part RETURN 'a b' AS out } RETURN out", "a b"},
		{"USE identity CALL { USE identity.part RETURN  'a  b' AS out } RETURN out", "a  b"},
		{"USE identity CALL { USE identity.part RETURN /* a  b */ 'a  b' AS out } RETURN out", "a  b"},
		{"USE identity CALL { USE identity.part RETURN /* a b */ 'a  b' AS out } RETURN out", "a  b"},
	} {
		t.Run(sample.query, func(t *testing.T) {
			beforeHits, beforeMisses, beforeSize := exec.fabricPlanCache.Stats()
			for repeat := 0; repeat < 2; repeat++ {
				result, err := exec.Execute(context.Background(), sample.query, nil)
				require.NoError(t, err)
				require.Equal(t, []string{"out"}, result.Columns)
				require.Equal(t, [][]interface{}{{sample.value}}, result.Rows)
			}
			hits, misses, size := exec.fabricPlanCache.Stats()
			require.Equal(t, beforeMisses+1, misses, "each accepted source view needs its own plan")
			require.Equal(t, beforeHits+1, hits, "an exact repeat must hit")
			require.Equal(t, beforeSize+1, size)
			require.Equal(t, index+1, size)
		})
	}
}

// Author: #1640 lane (Codex). Measure the actual planning hit caller, including
// its context/catalog boundary, separately from the cache-key microbenchmark.
func BenchmarkFabricPlanIdentityHotHit(b *testing.B) {
	exec := newFabricIdentityExecutor(b)
	catalog, err := exec.buildFabricCatalog()
	require.NoError(b, err)
	query := "USE identity CALL { USE identity.part RETURN 'a  b' AS out } RETURN out"
	_, err = exec.planFabricQuery(context.Background(), catalog, query, "identity")
	require.NoError(b, err)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.planFabricQuery(context.Background(), catalog, query, "identity"); err != nil {
			b.Fatal(err)
		}
	}
}
