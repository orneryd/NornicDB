package search

import (
	"context"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// relationshipEndpointGoneEngine reports one node id as no longer readable, as
// an endpoint deleted or hidden behind the relationship index answers, and
// delegates every other read to the engine the service indexed.
type relationshipEndpointGoneEngine struct {
	storage.Engine
	gone storage.NodeID
}

func (e relationshipEndpointGoneEngine) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	if id == e.gone {
		return nil, storage.ErrNotFound
	}
	return e.Engine.GetNode(ctx, id)
}

// TestSearchRelationshipsRecordsAdmittedRelationshipRead: the admitted loop's
// point read of a candidate relationship is one access and one row, recorded
// under the relationship point read, and a search without a filter reads no
// endpoint.
func TestSearchRelationshipsRecordsAdmittedRelationshipRead(t *testing.T) {
	engine := relationshipTestEngine(t)
	svc := startRelationshipService(t, engine, nil, nil)
	ledger := &fixtureowner.QueryReadObserver{}

	response, err := svc.SearchRelationships(queryprofile.WithObserver(context.Background(), ledger), "zephyrine", nil, &SearchOptions{Limit: 10})
	require.NoError(t, err)
	require.Len(t, response.Results, 1)
	require.Equal(t, "r1", response.Results[0].ID)

	rows, hits, recorded := ledger.Reads(queryprofile.KindRelationshipByIDSeek, "")
	require.True(t, recorded, "the admitted candidate's relationship read is observed")
	require.Equal(t, int64(1), rows)
	require.Equal(t, int64(1), hits)
	require.Equal(t, []string{profileKindRelationshipFulltextCandidates, queryprofile.KindRelationshipByIDSeek}, ledger.RecordedKinds(), "a search without a filter reads no endpoint")
}

// TestSearchRelationshipsRecordsRelationshipWhereEndpointReads: the filter
// reads both endpoints of each candidate it examines, each as one access and
// one row, through the full point read.
func TestSearchRelationshipsRecordsRelationshipWhereEndpointReads(t *testing.T) {
	engine := relationshipTestEngine(t)
	svc := startRelationshipService(t, engine, nil, nil)
	ledger := &fixtureowner.QueryReadObserver{}

	var starts []*storage.Node
	response, err := svc.SearchRelationships(queryprofile.WithObserver(context.Background(), ledger), "zephyrine", nil, &SearchOptions{
		Limit: 10,
		RelationshipWhere: func(_ context.Context, _ *storage.Edge, start, _ *storage.Node) (bool, error) {
			starts = append(starts, start)
			return true, nil
		},
	})
	require.NoError(t, err)
	require.Len(t, response.Results, 1)
	require.Len(t, starts, 1)
	require.NotNil(t, starts[0])

	rows, hits, recorded := ledger.Reads(queryprofile.KindNodeByIDSeek, "")
	require.True(t, recorded, "a filter's endpoint reads are observed")
	require.Equal(t, int64(2), rows, "both endpoint images exist")
	require.Equal(t, int64(2), hits, "one access per endpoint")
}

// TestSearchRelationshipsRecordsUnreadableEndpointOutcome: an endpoint the
// service cannot read is one attempt with no row, and the filter still sees
// null for it.
func TestSearchRelationshipsRecordsUnreadableEndpointOutcome(t *testing.T) {
	base := relationshipTestEngine(t)
	svc := startRelationshipService(t, relationshipEndpointGoneEngine{Engine: base, gone: "a"}, nil, nil)
	ledger := &fixtureowner.QueryReadObserver{}

	var starts []*storage.Node
	response, err := svc.SearchRelationships(queryprofile.WithObserver(context.Background(), ledger), "zephyrine", nil, &SearchOptions{
		Limit: 10,
		RelationshipWhere: func(_ context.Context, _ *storage.Edge, start, _ *storage.Node) (bool, error) {
			starts = append(starts, start)
			return true, nil
		},
	})
	require.NoError(t, err)
	require.Len(t, response.Results, 1)
	require.Len(t, starts, 1)
	require.Nil(t, starts[0], "an endpoint that cannot be read is null to the filter")

	rows, hits, recorded := ledger.Reads(queryprofile.KindNodeByIDSeek, "")
	require.True(t, recorded)
	require.Equal(t, int64(1), rows, "the unreadable endpoint is one attempt and no row")
	require.Equal(t, int64(2), hits, "both endpoints were attempted")
}

// TestSearchRelationshipsWhereFilterReceivesStoredVectors: the filter's
// endpoint images keep the vectors a native predicate reads, so a filter is
// never narrowed to an embedding-metadata image.
func TestSearchRelationshipsWhereFilterReceivesStoredVectors(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	engine := storage.NewNamespacedEngine(base, "test")
	vector := []float32{0.25, 0.5, 0.75}
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := engine.CreateNode(t.Context(), &storage.Node{
			ID:              id,
			Labels:          []string{"Doc"},
			Properties:      map[string]any{"name": "doc " + string(id)},
			ChunkEmbeddings: [][]float32{vector},
			NamedEmbeddings: map[string][]float32{"title": vector},
			EmbedMeta:       map[string]any{"has_embedding": true, "chunk_count": 1},
		})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(t.Context(), &storage.Edge{ID: "r1", StartNode: "a", EndNode: "b", Type: "CITES", Properties: map[string]any{"reason": "zephyrine bridge over the river"}}))
	svc := startRelationshipService(t, engine, nil, nil)

	checked := 0
	response, err := svc.SearchRelationships(context.Background(), "zephyrine", nil, &SearchOptions{
		Limit: 10,
		RelationshipWhere: func(_ context.Context, edge *storage.Edge, start, end *storage.Node) (bool, error) {
			if edge.ID != "r1" {
				return true, nil
			}
			require.NotNil(t, start)
			require.NotNil(t, end)
			require.Equal(t, [][]float32{vector}, start.ChunkEmbeddings, "the filter's endpoint keeps its stored vectors")
			require.Equal(t, map[string][]float32{"title": vector}, start.NamedEmbeddings)
			require.Equal(t, [][]float32{vector}, end.ChunkEmbeddings)
			checked++
			return true, nil
		},
	})
	require.NoError(t, err)
	require.Len(t, response.Results, 1)
	require.Equal(t, 1, checked)
}

// TestSearchRelationshipsRecordsNoPointReadWhenNoCandidateIsRead: a search that
// admits no candidate performs no point read and records none, since an unused
// read is not an attempt.
func TestSearchRelationshipsRecordsNoPointReadWhenNoCandidateIsRead(t *testing.T) {
	engine := relationshipTestEngine(t)
	svc := startRelationshipService(t, engine, nil, nil)
	ledger := &fixtureowner.QueryReadObserver{}

	response, err := svc.SearchRelationships(queryprofile.WithObserver(context.Background(), ledger), "absentterm", nil, &SearchOptions{
		Limit: 10,
		RelationshipWhere: func(context.Context, *storage.Edge, *storage.Node, *storage.Node) (bool, error) {
			return true, nil
		},
	})
	require.NoError(t, err)
	require.Empty(t, response.Results)
	_, _, edgeRead := ledger.Reads(queryprofile.KindRelationshipByIDSeek, "")
	_, _, nodeRead := ledger.Reads(queryprofile.KindNodeByIDSeek, "")
	require.False(t, edgeRead)
	require.False(t, nodeRead)
	rows, hits, queried := ledger.Reads(profileKindRelationshipFulltextCandidates, "")
	require.True(t, queried)
	require.Zero(t, rows)
	require.EqualValues(t, 1, hits, "the empty index query ran, without hydrating an image")
}
