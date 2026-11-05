package resolvers

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
)

// Measure the native setup path affected by namespace-aware search wiring.
// The same fixture is overlaid on the pre-guard baseline for paired A/B.
func BenchmarkGraphQLNativeCypherSetup(b *testing.B) {
	db, err := nornicdb.Open(b.TempDir(), nornicdb.DefaultConfig())
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, db.Close()) })
	manager, err := multidb.NewDatabaseManager(b.Context(), db.GetBaseStorageForManager(), nil)
	require.NoError(b, err)
	// The manager borrows DB storage; DB owns its shutdown.
	resolver := NewResolver(db, manager)
	ctx := context.Background()
	database := manager.DefaultDatabaseName()
	_, err = resolver.getCypherExecutor(ctx, database)
	require.NoError(b, err)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		executor, err := resolver.getCypherExecutor(ctx, database)
		if err != nil || executor == nil {
			b.Fatalf("native executor setup: %v", err)
		}
	}
}
