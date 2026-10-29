package nornicdb

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// requireIndexed waits out the deferred refresh, then checks the fulltext
// document count without a search (a search would clean an orphan).
func requireIndexed(t *testing.T, svc *search.Service, want int, why string) {
	t.Helper()
	settleQueuedRefreshes()
	require.Equal(t, want, svc.FulltextDocCount(), why)
}

// Every storage event reaches search, with relationship search on or off: a
// consumer registered only in some configurations fails here (#1482 round 3).
func TestDBConsumesEveryStorageEvent(t *testing.T) {
	for _, relationships := range []bool{false, true} {
		t.Run(map[bool]string{false: "relationship search off", true: "relationship search on"}[relationships], func(t *testing.T) {
			db, svc := openConsumerDB(t, relationships)
			dims := db.config.Memory.EmbeddingDimensions
			vector := func(axis int) []float32 {
				v := make([]float32, dims)
				v[axis] = 1
				return v
			}
			_, err := db.storage.CreateNode(&storage.Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"content": "alphaword"}})
			require.NoError(t, err)
			requireIndexed(t, svc, 1, "node create")
			require.NoError(t, db.storage.UpdateNode(&storage.Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"content": "bravoword"}}))
			settleQueuedRefreshes()
			require.Equal(t, 1, consumerDBHits(t, svc, "bravoword"), "node update")
			_, err = db.storage.CreateNode(&storage.Node{ID: "b", Labels: []string{"Doc"}})
			require.NoError(t, err)

			require.NoError(t, db.storage.CreateEdge(&storage.Edge{ID: "r", Type: "CITES", StartNode: "a", EndNode: "b", Properties: map[string]any{"vec": vector(0)}}))
			settleQueuedRefreshes()
			require.True(t, svc.HasRelationshipVectorEntries("CITES", "vec"), "edge create")
			require.NoError(t, db.storage.UpdateEdge(&storage.Edge{ID: "r", Type: "CITES", StartNode: "a", EndNode: "b", Properties: map[string]any{"note": "no vector"}}))
			settleQueuedRefreshes()
			require.False(t, svc.HasRelationshipVectorEntries("CITES", "vec"), "edge update")
			require.NoError(t, db.storage.UpdateEdge(&storage.Edge{ID: "r", Type: "CITES", StartNode: "a", EndNode: "b", Properties: map[string]any{"vec": vector(1)}}))
			settleQueuedRefreshes()
			require.True(t, svc.HasRelationshipVectorEntries("CITES", "vec"))
			require.NoError(t, db.storage.DeleteEdge("r"))
			settleQueuedRefreshes()
			require.False(t, svc.HasRelationshipVectorEntries("CITES", "vec"), "edge delete")

			indexed := svc.FulltextDocCount()
			require.NoError(t, db.storage.DeleteNode("a"))
			requireIndexed(t, svc, indexed-1, "node delete")
		})
	}
}

// Suppression through storage, by any caller, leaves and re-enters search.
func TestStorageNodeSuppressionRefreshesSearch(t *testing.T) {
	db, svc := openConsumerDB(t, false)
	_, err := db.storage.CreateNode(&storage.Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"content": "alphaword"}})
	require.NoError(t, err)
	requireIndexed(t, svc, 1, "the node is indexed")
	suppressor, ok := db.storage.(storage.ManualSuppressionEngine)
	require.True(t, ok)
	require.NoError(t, suppressor.SetManualSuppression(storage.StoredEntityNodeKind, "a", true))
	requireIndexed(t, svc, 0, "a hidden node stayed indexed")
	require.NoError(t, suppressor.SetManualSuppression(storage.StoredEntityNodeKind, "a", false))
	requireIndexed(t, svc, 1, "a node shown again was not indexed")
}

// A decay flip rewrites the stored body without UpdateNode; search follows it
// in both directions.
func TestDecayFlipsRefreshSearch(t *testing.T) {
	ctx := context.Background()
	config := DefaultConfig()
	config.Memory.EmbeddingEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Memory.DecayEnabled = true
	config.Database.AsyncWritesEnabled = false
	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	svc, err := db.EnsureSearchIndexes(ctx, db.defaultDatabaseName(), nil, SearchIndexBuildWait)
	require.NoError(t, err)
	base := db.GetBaseStorageForManager()
	be := unwrapToBadgerEngine(base)
	require.NotNil(t, be)
	schema := base.(storage.NamespaceSchemaProvider).GetSchemaForNamespace(db.defaultDatabaseName())
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: "short_doc", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 1, VisibilityThreshold: 0.10, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "short_doc_binding", ProfileRef: "short_doc", TargetLabels: []string{"Doc"}}))
	_, err = db.storage.CreateNode(&storage.Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"content": "alphaword"}})
	require.NoError(t, err)
	requireIndexed(t, svc, 1, "a fresh node is indexed")

	// Four half-lives later the score is below the threshold.
	time.Sleep(4 * time.Second)
	hidden, err := be.EnqueueDeindexIfSuppressed(db.defaultDatabaseName()+":a", false)
	require.NoError(t, err)
	require.True(t, hidden)
	requireIndexed(t, svc, 0, "a node decay suppressed stayed indexed")

	require.NoError(t, schema.AlterDecayProfile("short_doc", map[string]any{"decayEnabled": false}))
	_, err = be.EnqueueDeindexIfSuppressed(db.defaultDatabaseName()+":a", false)
	require.NoError(t, err)
	requireIndexed(t, svc, 1, "a node shown again by policy was not indexed")
}

// Dropping a database through the database manager (as the GraphQL and Bolt
// adapters do, and a replica applying the drop) drops its search state: a
// database recreated under the same name doesn't inherit the old index.
func TestDropDatabaseResetsSearchForEveryPath(t *testing.T) {
	ctx := context.Background()
	db, _ := openConsumerDB(t, false)
	manager, err := multidb.NewDatabaseManager(t.Context(), db.GetBaseStorageForManager(), nil)
	require.NoError(t, err)
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "tenant"}))
	tenant := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "tenant")
	_, err = tenant.CreateNode(&storage.Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"content": "alphaword"}})
	require.NoError(t, err)
	svc, err := db.EnsureSearchIndexes(ctx, "tenant", tenant, SearchIndexBuildWait)
	require.NoError(t, err)
	requireIndexed(t, svc, 1, "the tenant node is indexed")

	require.NoError(t, manager.DropDatabase(t.Context(), "tenant"))
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "tenant"}))
	svc, err = db.EnsureSearchIndexes(ctx, "tenant", storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "tenant"), SearchIndexBuildWait)
	require.NoError(t, err)
	requireIndexed(t, svc, 0, "a recreated database inherited the dropped one's index")
}

// A restore replaces every database, so search for each matches what was
// restored, not what was indexed before.
func TestRestoreResetsSearchForEveryDatabase(t *testing.T) {
	ctx := context.Background()
	db, _ := openConsumerDB(t, false)
	tenant := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "tenant")
	_, err := tenant.CreateNode(&storage.Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"content": "backupword"}})
	require.NoError(t, err)
	svc, err := db.EnsureSearchIndexes(ctx, "tenant", tenant, SearchIndexBuildWait)
	require.NoError(t, err)
	backup := filepath.Join(t.TempDir(), "backup.bin")
	require.NoError(t, db.Backup(ctx, backup))
	require.NoError(t, tenant.UpdateNode(&storage.Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"content": "laterword"}}))
	settleQueuedRefreshes()
	require.Equal(t, 1, consumerDBHits(t, svc, "laterword"))

	require.NoError(t, db.Restore(ctx, backup))
	svc, err = db.EnsureSearchIndexes(ctx, "tenant", storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "tenant"), SearchIndexBuildWait)
	require.NoError(t, err)
	require.Zero(t, consumerDBHits(t, svc, "laterword"), "the tenant's search kept a state the restore replaced")
	require.Equal(t, 1, consumerDBHits(t, svc, "backupword"))
}
