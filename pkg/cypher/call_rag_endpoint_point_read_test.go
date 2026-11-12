package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// retrieveRelationshipsQuery is db.retrieveRelationships over the fixture's one
// relationship, returning the row's relationship and both endpoint images.
const retrieveRelationshipsQuery = "CALL db.retrieveRelationships({query: 'zephyrine'}) YIELD relationship, start, end RETURN relationship, start, end"

// retrievalEndpointAbsentEngine reports one node id as no longer readable, as
// an endpoint deleted or hidden behind the relationship index answers, and
// delegates every other read to the store the search indexed.
type retrievalEndpointAbsentEngine struct {
	storage.Engine
	absent storage.NodeID
}

func (e retrievalEndpointAbsentEngine) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	if id == e.absent {
		return nil, storage.ErrNotFound
	}
	return e.Engine.GetNode(ctx, id)
}

// newRetrievalEndpointFixture is the graph these tests search: one CITES
// relationship whose start node carries a stored vector, indexed by the
// relationship search service the returned executor answers with.
func newRetrievalEndpointFixture(t testing.TB) (storage.Engine, *StorageExecutor, *search.Service) {
	t.Helper()
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	vector := []float32{0.25, 0.5, 0.75}
	_, err := store.CreateNode(t.Context(), &storage.Node{
		ID:              "a",
		Labels:          []string{"Doc"},
		Properties:      map[string]interface{}{"name": "a"},
		ChunkEmbeddings: [][]float32{vector},
		NamedEmbeddings: map[string][]float32{"title": vector},
		EmbedMeta:       map[string]interface{}{"has_embedding": true, "chunk_count": 1},
	})
	require.NoError(t, err)
	_, err = store.CreateNode(t.Context(), &storage.Node{ID: "b", Labels: []string{"Doc"}, Properties: map[string]interface{}{"name": "b"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(t.Context(), &storage.Edge{
		ID:         "r1",
		StartNode:  "a",
		EndNode:    "b",
		Type:       "CITES",
		Properties: map[string]interface{}{"reason": "zephyrine bridge"},
	}))
	svc := search.NewService(store, 3, nil)
	t.Cleanup(func() { _ = svc.Close() })
	svc.EnableRelationshipSearch(nil)
	svc.StartRelationshipIndex()
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	exec.SetSearchService(svc)
	return store, exec, svc
}

// TestRetrieveRelationshipsEndpointsAreCanonicalPointReads: each row's start and
// end are read through the statement's canonical point read, so a profiled
// statement records one node access per endpoint image instead of an unobserved
// raw read.
func TestRetrieveRelationshipsEndpointsAreCanonicalPointReads(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		ctx := context.Background()
		_, exec, _ := newRetrievalEndpointFixture(t)
		require.Eventually(t, func() bool {
			result, err := exec.Execute(ctx, retrieveRelationshipsQuery, nil)
			return err == nil && len(result.Rows) == 1
		}, 5*time.Second, 20*time.Millisecond)

		result, err := exec.Execute(ctx, "PROFILE "+retrieveRelationshipsQuery, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		start, ok := result.Rows[0][1].(*storage.Node)
		require.True(t, ok, "row value %T", result.Rows[0][1])
		require.Equal(t, "a", start.Properties["name"])
		end, ok := result.Rows[0][2].(*storage.Node)
		require.True(t, ok, "row value %T", result.Rows[0][2])
		require.Equal(t, "b", end.Properties["name"])

		access := planOperatorOfType(executionPlanFromResult(t, result).Root, queryprofile.KindNodeByIDSeek)
		require.NotNil(t, access, "each endpoint is the statement's canonical point read")
		rows, hits := access.ActualRows, access.DBHits
		require.Equal(t, int64(2), rows, "both endpoint images exist")
		require.Equal(t, int64(2), hits, "one access per endpoint")
	})
}

// TestRetrieveRelationshipsUnreadableEndpointIsOneAccessWithoutRow: an endpoint
// the statement cannot read is one point-read attempt with no row and the row's
// endpoint stays null; the endpoint it can read keeps the full stored image a
// native consumer reads, vectors included.
func TestRetrieveRelationshipsUnreadableEndpointIsOneAccessWithoutRow(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		ctx := context.Background()
		store, _, svc := newRetrievalEndpointFixture(t)
		exec := NewStorageExecutor(retrievalEndpointAbsentEngine{Engine: store, absent: "b"})
		exec.SetSearchService(svc)
		require.Eventually(t, func() bool {
			result, err := exec.Execute(ctx, retrieveRelationshipsQuery, nil)
			return err == nil && len(result.Rows) == 1
		}, 5*time.Second, 20*time.Millisecond)

		result, err := exec.Execute(ctx, "PROFILE "+retrieveRelationshipsQuery, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		start, ok := result.Rows[0][1].(*storage.Node)
		require.True(t, ok, "row value %T", result.Rows[0][1])
		require.Len(t, start.ChunkEmbeddings, 1, "the default point read keeps the stored vectors")
		require.Nil(t, result.Rows[0][2], "an unreadable endpoint is null")

		access := planOperatorOfType(executionPlanFromResult(t, result).Root, queryprofile.KindNodeByIDSeek)
		require.NotNil(t, access)
		rows, hits := access.ActualRows, access.DBHits
		require.Equal(t, int64(1), rows, "the unreadable endpoint is one attempt and no row")
		require.Equal(t, int64(2), hits, "both endpoints were attempted")
	})
}

func TestRetrieveRelationshipEndpointKeepsNativeConsumerVectors(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		const name = "image.endpointvectors"
		cleanupAdmissionProcedure(t, name)
		require.NoError(t, RegisterUserProcedure(ProcedureSpec{
			Name: name, Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 1,
			Params:  []ProcedureParam{{Name: "node", Type: "NODE"}},
			Returns: []ProcedureColumn{{Name: "intact", Type: "BOOLEAN"}},
		}, func(_ context.Context, _ *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
			node, ok := args[0].(*storage.Node)
			require.True(t, ok)
			require.Equal(t, [][]float32{{0.25, 0.5, 0.75}}, node.ChunkEmbeddings)
			require.Equal(t, map[string][]float32{"title": {0.25, 0.5, 0.75}}, node.NamedEmbeddings)
			return &ExecuteResult{Columns: []string{"intact"}, Rows: [][]interface{}{{true}}}, nil
		}))
		_, exec, _ := newRetrievalEndpointFixture(t)
		require.Eventually(t, func() bool {
			result, err := exec.Execute(t.Context(), retrieveRelationshipsQuery, nil)
			return err == nil && len(result.Rows) == 1
		}, 5*time.Second, 20*time.Millisecond)
		query := "PROFILE CALL db.retrieveRelationships({query: 'zephyrine'}) YIELD start CALL image.endpointvectors(start) YIELD intact RETURN intact"
		result, err := exec.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{true}}, result.Rows)
		plan := executionPlanFromResult(t, result)
		require.False(t, plan.TotalDBHitsObserved, "the native callback remains opaque")
		require.Contains(t, plan.Unobserved, "database reads: procedure callback: "+name)
	})
}

func BenchmarkRelationshipRetrievalEndpointObservation(b *testing.B) {
	_, exec, _ := newRetrievalEndpointFixture(b)
	require.Eventually(b, func() bool {
		result, err := exec.Execute(b.Context(), retrieveRelationshipsQuery, nil)
		return err == nil && len(result.Rows) == 1
	}, 5*time.Second, 20*time.Millisecond)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := exec.Execute(b.Context(), retrieveRelationshipsQuery, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 1 {
			b.Fatalf("returned %d rows; want one", len(result.Rows))
		}
	}
}
