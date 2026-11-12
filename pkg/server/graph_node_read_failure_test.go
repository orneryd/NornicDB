package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type graphNodeReadFailureEngine struct {
	*graphSnapshotIndexEngine
	failID storage.NodeID
	failAt int
	err    error
	reads  map[storage.NodeID]int
}

func (e *graphNodeReadFailureEngine) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	e.reads[id]++
	if id == e.failID && e.reads[id] == e.failAt {
		return nil, e.err
	}
	return e.graphSnapshotIndexEngine.GetNodeVisibleAt(ctx, id, storage.MVCCVersion{})
}

func (e *graphNodeReadFailureEngine) GetNodeVisibleAt(ctx context.Context, id storage.NodeID, _ storage.MVCCVersion) (*storage.Node, error) {
	return e.GetNode(ctx, id)
}

func TestGraphCollectorsPropagateNodeReadFailures(t *testing.T) {
	s := &Server{}
	filters := newGraphFilterSet(nil, nil)
	for _, failure := range []error{context.Canceled, context.DeadlineExceeded, storage.ErrStorageClosed, errors.New("node read failed")} {
		for _, tc := range []struct {
			name   string
			failID storage.NodeID
			failAt int
			run    func(context.Context, storage.Engine) (graphCollection, error)
		}{
			{"neighborhood_seed", "a", 1, func(ctx context.Context, e storage.Engine) (graphCollection, error) {
				return s.collectLatestNeighborhood(ctx, e, []string{"a"}, 1, 10, "out", filters)
			}},
			{"neighborhood_neighbor", "b", 1, func(ctx context.Context, e storage.Engine) (graphCollection, error) {
				return s.collectLatestNeighborhood(ctx, e, []string{"a"}, 1, 10, "out", filters)
			}},
			{"path_same_node", "a", 1, func(ctx context.Context, e storage.Engine) (graphCollection, error) {
				return s.collectLatestPath(ctx, e, "a", "a", 10, filters)
			}},
			{"path_neighbor", "b", 1, func(ctx context.Context, e storage.Engine) (graphCollection, error) {
				return s.collectLatestPath(ctx, e, "a", "b", 10, filters)
			}},
			{"path_reconstruction", "b", 2, func(ctx context.Context, e storage.Engine) (graphCollection, error) {
				return s.collectLatestPath(ctx, e, "a", "b", 10, filters)
			}},
			{"induced_current", "a", 1, func(ctx context.Context, e storage.Engine) (graphCollection, error) {
				return s.collectLatestInducedSubgraph(ctx, e, []string{"a"}, filters)
			}},
			{"induced_snapshot", "a", 1, func(ctx context.Context, e storage.Engine) (graphCollection, error) {
				return s.collectSnapshotInducedSubgraph(ctx, e, []string{"a"}, storage.MVCCVersion{}, filters)
			}},
		} {
			t.Run(failure.Error()+"/"+tc.name, func(t *testing.T) {
				base := newGraphSnapshotIndexEngine()
				engine := &graphNodeReadFailureEngine{
					graphSnapshotIndexEngine: base, failID: tc.failID, failAt: tc.failAt,
					err: failure, reads: make(map[storage.NodeID]int),
				}
				_, err := tc.run(t.Context(), engine)
				require.ErrorIs(t, err, failure)
				require.Equal(t, tc.failAt, engine.reads[tc.failID])
			})
		}
	}
}

func newGraphSnapshotIndexEngine() *graphSnapshotIndexEngine {
	return &graphSnapshotIndexEngine{
		nodes: map[storage.NodeID]*storage.Node{
			"a": {ID: "a"}, "b": {ID: "b"},
		},
		edges: []*storage.Edge{{ID: "ab", StartNode: "a", EndNode: "b", Type: "REL"}},
	}
}

func TestGraphCollectorsKeepMissingNodesDistinct(t *testing.T) {
	s := &Server{}
	filters := newGraphFilterSet(nil, nil)
	engine := func() *graphNodeReadFailureEngine {
		return &graphNodeReadFailureEngine{
			graphSnapshotIndexEngine: newGraphSnapshotIndexEngine(),
			failID:                   "a", failAt: 1, reads: make(map[storage.NodeID]int),
			err: errors.Join(errors.New("missing a"), storage.ErrNotFound),
		}
	}
	collection, err := s.collectLatestNeighborhood(t.Context(), engine(), []string{"a"}, 1, 10, "out", filters)
	require.NoError(t, err)
	require.Empty(t, collection.nodes)
	_, err = s.collectLatestPath(t.Context(), engine(), "a", "a", 10, filters)
	require.ErrorIs(t, err, storage.ErrNotFound)
	collection, err = s.collectLatestInducedSubgraph(t.Context(), engine(), []string{"a"}, filters)
	require.NoError(t, err)
	require.Empty(t, collection.nodes)
	collection, err = s.collectSnapshotInducedSubgraph(t.Context(), engine(), []string{"a"}, storage.MVCCVersion{}, filters)
	require.NoError(t, err)
	require.Empty(t, collection.nodes)
	notVisible := engine()
	notVisible.err = errors.Join(errors.New("a outside snapshot"), storage.ErrNotVisibleAtSnapshot)
	collection, err = s.collectSnapshotInducedSubgraph(t.Context(), notVisible, []string{"a"}, storage.MVCCVersion{}, filters)
	require.NoError(t, err)
	require.Empty(t, collection.nodes, "an entity outside the requested snapshot is absent, not a read failure")
}

func TestGraphEndpointsDoNotPublishCanceledReads(t *testing.T) {
	s, users := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, users, "admin")
	engine := getDefaultStorage(t, s)
	_, err := engine.CreateNode(t.Context(), &storage.Node{ID: "a"})
	require.NoError(t, err)
	for _, tc := range []struct{ endpoint, body string }{
		{"neighborhood", `{"node_ids":["a"],"depth":1}`},
		{"expand", `{"node_ids":["a"],"depth":1}`},
		{"path", `{"source_node_id":"a","target_node_id":"a"}`},
		{"path", `{"source_node_id":"a","target_node_id":"b"}`},
		{"temporal", `{"node_ids":["a"],"as_of":"2026-01-01T00:00:00Z"}`},
		{"diff", `{"node_ids":["a"],"as_of":"2026-01-01T00:00:00Z"}`},
		{"diff", `{"node_ids":["a"],"as_of":"2026-01-01T00:00:00Z","compare_to":"2025-01-01T00:00:00Z"}`},
	} {
		t.Run(tc.endpoint+"/"+tc.body, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			req := httptest.NewRequest(http.MethodPost, defaultGraphPath(s, tc.endpoint), strings.NewReader(tc.body)).WithContext(ctx)
			req.Header.Set("Authorization", token)
			rec := httptest.NewRecorder()
			s.buildRouter().ServeHTTP(rec, req)
			require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
			require.NotContains(t, rec.Body.String(), `"nodes"`)
		})
	}
}
