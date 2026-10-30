package cypher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// observationEngine is a storage engine that records the update requests a
// Cypher mutation sends, so a test can assert the observation (Before) and the
// image (After) that storage received. It delegates every other operation to
// the engine it wraps.
type observationEngine struct {
	storage.Engine
	nodeRequests []storage.NodeUpdateRequest
	edgeRequests []storage.EdgeUpdateRequest
	nodeErr      error
	edgeErr      error
}

func (e *observationEngine) UpdateNode(updateRequest storage.NodeUpdateRequest) error {
	e.nodeRequests = append(e.nodeRequests, updateRequest)
	if e.nodeErr != nil {
		return e.nodeErr
	}
	return e.Engine.UpdateNode(updateRequest)
}

func (e *observationEngine) UpdateEdge(updateRequest storage.EdgeUpdateRequest) error {
	e.edgeRequests = append(e.edgeRequests, updateRequest)
	if e.edgeErr != nil {
		return e.edgeErr
	}
	return e.Engine.UpdateEdge(updateRequest)
}

func (e *observationEngine) lastNodeRequest(t *testing.T) storage.NodeUpdateRequest {
	t.Helper()
	require.NotEmpty(t, e.nodeRequests, "the mutation must reach storage as an update")
	return e.nodeRequests[len(e.nodeRequests)-1]
}

func (e *observationEngine) lastEdgeRequest(t *testing.T) storage.EdgeUpdateRequest {
	t.Helper()
	require.NotEmpty(t, e.edgeRequests, "the mutation must reach storage as an update")
	return e.edgeRequests[len(e.edgeRequests)-1]
}

func TestFailedCypherMutationPreservesVersionTime(t *testing.T) {
	old := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	want := errors.New("storage rejected mutation")
	store := &observationEngine{nodeErr: want, edgeErr: want}
	node := &storage.Node{CreatedAt: old, UpdatedAt: old}
	edge := &storage.Edge{CreatedAt: old, UpdatedAt: old}
	require.ErrorIs(t, mutateNodeWithObservation(store, observeNode(node), node), want)
	require.Equal(t, old, node.UpdatedAt)
	require.Equal(t, old, node.CreatedAt)
	require.Equal(t, old, beforeNode(t, store).UpdatedAt)
	require.ErrorIs(t, mutateEdgeWithObservation(store, observeEdge(edge), edge), want)
	require.Equal(t, old, edge.UpdatedAt)
	require.Equal(t, old, edge.CreatedAt)
	require.Equal(t, old, beforeEdge(t, store).UpdatedAt)
}

// TestCypherMutationObservationIsTheUnchangedEntity checks the observation a
// mutation sends to storage: the entity as the caller read it, whose time stays
// at the version before the mutation, while the image carries the new time.
func TestCypherMutationObservationIsTheUnchangedEntity(t *testing.T) {
	old := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	source := storage.NewNamespacedEngine(newTestMemoryEngine(t), "clock")
	store := &observationEngine{Engine: source}
	node := &storage.Node{
		ID:         "n1",
		Labels:     []string{"Clock"},
		Properties: map[string]interface{}{"title": "v1"},
		CreatedAt:  old,
		UpdatedAt:  old,
	}
	_, err := source.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, mutateNodeWithObservation(store, observeNode(node), node))
	nodeRequest := store.lastNodeRequest(t)
	require.NotNil(t, nodeRequest.Before, "a Cypher mutation always observes the entity it writes")
	require.Same(t, node, nodeRequest.After)
	require.NotSame(t, node, nodeRequest.Before, "the observation is its own image")
	require.Equal(t, "v1", nodeRequest.Before.Properties["title"])
	require.Equal(t, []string{"Clock"}, nodeRequest.Before.Labels)
	require.Equal(t, old, nodeRequest.Before.UpdatedAt, "the observation keeps the version the caller read")
	require.Equal(t, old, nodeRequest.Before.CreatedAt)
	require.True(t, nodeRequest.After.UpdatedAt.After(old), "the image carries this mutation's time")

	edge := &storage.Edge{
		ID:         "e1",
		StartNode:  "a",
		EndNode:    "b",
		Type:       "VERSION",
		Properties: map[string]interface{}{"v": int64(1)},
		CreatedAt:  old,
		UpdatedAt:  old,
	}
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := source.CreateNode(&storage.Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, source.CreateEdge(edge))
	require.NoError(t, mutateEdgeWithObservation(store, observeEdge(edge), edge))
	edgeRequest := store.lastEdgeRequest(t)
	require.NotNil(t, edgeRequest.Before)
	require.Same(t, edge, edgeRequest.After)
	require.NotSame(t, edge, edgeRequest.Before)
	require.Equal(t, int64(1), edgeRequest.Before.Properties["v"])
	require.Equal(t, old, edgeRequest.Before.UpdatedAt)
	require.True(t, edgeRequest.After.UpdatedAt.After(old))
}

// A rejected mutation restores its timestamp without modifying the observation.
func TestCypherMutationFailureRestoresObservation(t *testing.T) {
	old := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	want := errors.New("storage rejected mutation")
	store := &observationEngine{Engine: newTestMemoryEngine(t), nodeErr: want, edgeErr: want}
	node := &storage.Node{ID: "n1", Labels: []string{"Clock"}, Properties: map[string]interface{}{"title": "v1"}, CreatedAt: old, UpdatedAt: old}
	require.ErrorIs(t, mutateNodeWithObservation(store, observeNode(node), node), want)
	require.Equal(t, old, node.UpdatedAt, "a failed mutation keeps the caller's version time")
	require.Equal(t, old, beforeNode(t, store).UpdatedAt, "and so does the observation it shared")
	edge := &storage.Edge{ID: "e1", Type: "VERSION", Properties: map[string]interface{}{"v": int64(1)}, CreatedAt: old, UpdatedAt: old}
	require.ErrorIs(t, mutateEdgeWithObservation(store, observeEdge(edge), edge), want)
	require.Equal(t, old, edge.UpdatedAt)
	require.Equal(t, old, beforeEdge(t, store).UpdatedAt)
}

// TestCypherMutationSendsNoBlindWrite checks that every Cypher mutation path
// this package owns observes the entity it writes, so storage is never asked to
// guess an entity's pre-change image.
func TestCypherMutationSendsNoBlindWrite(t *testing.T) {
	for _, tc := range []struct {
		query    string
		labels   []string
		property int64
	}{
		{"MATCH (n:Clock) SET n.title = 'v2'", []string{"Clock", "Old"}, 1},
		{"MATCH (n:Clock) SET n += {title: 'v2'}", []string{"Clock", "Old"}, 1},
		{"MATCH (n:Clock) SET n = {title: 'v2'}", []string{"Clock", "Old"}, 1},
		{"MATCH (n:Clock) SET n:Changed", []string{"Clock", "Old"}, 1},
		{"MATCH (n:Clock) REMOVE n.title", []string{"Clock", "Old"}, 1},
		{"MATCH (n:Clock) REMOVE n:Old", []string{"Clock", "Old"}, 1},
		{"MATCH (n:Clock) CALL db.create.setNodeVectorProperty(n, 'emb', [2.0,3.0])", []string{"Clock", "Old"}, 1},
		{"MATCH ()-[r:VERSION]->() SET r.v = 2", nil, 1},
		{"MATCH ()-[r:VERSION]->() REMOVE r.v", nil, 1},
		{"MATCH ()-[r:VERSION]->() CALL db.create.setRelationshipVectorProperty(r, 'emb', [2.0,3.0])", nil, 1},
	} {
		t.Run(tc.query, func(t *testing.T) {
			source := storage.NewNamespacedEngine(newTestMemoryEngine(t), "clock")
			old := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
			_, err := source.CreateNode(&storage.Node{ID: "a", Labels: []string{"Clock", "Old"}, Properties: map[string]interface{}{"title": "v1", "emb": []float64{1, 2}}, CreatedAt: old, UpdatedAt: old})
			require.NoError(t, err)
			_, err = source.CreateNode(&storage.Node{ID: "b", Labels: []string{"Other"}, Properties: map[string]interface{}{}, CreatedAt: old, UpdatedAt: old})
			require.NoError(t, err)
			require.NoError(t, source.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "VERSION", Properties: map[string]interface{}{"v": int64(1), "emb": []float64{1, 2}}, CreatedAt: old, UpdatedAt: old}))
			store := &observationEngine{Engine: source}
			_, err = NewStorageExecutor(store).Execute(context.Background(), tc.query, nil)
			require.NoError(t, err)
			require.Len(t, store.nodeRequests, map[bool]int{true: 1, false: 0}[tc.labels != nil], "a node mutation is one observed update")
			require.Len(t, store.edgeRequests, map[bool]int{true: 0, false: 1}[tc.labels != nil], "a relationship mutation is one observed update")
			for _, updateRequest := range store.nodeRequests {
				require.NotNil(t, updateRequest.Before, "storage receives the observed node")
				require.Equal(t, "v1", updateRequest.Before.Properties["title"], "the observation keeps the value the clause replaced")
				require.Equal(t, tc.labels, updateRequest.Before.Labels, "the observation keeps the labels the clause changed")
				require.True(t, old.Equal(updateRequest.Before.UpdatedAt), "the observation keeps the version the caller read")
				require.Equal(t, []float64{1, 2}, updateRequest.Before.Properties["emb"], "vector replacement must not rewrite the observed map")
			}
			for _, updateRequest := range store.edgeRequests {
				require.NotNil(t, updateRequest.Before, "storage receives the observed relationship")
				require.Equal(t, tc.property, updateRequest.Before.Properties["v"], "the observation keeps the value the clause replaced")
				require.True(t, old.Equal(updateRequest.Before.UpdatedAt))
				require.Equal(t, []float64{1, 2}, updateRequest.Before.Properties["emb"])
			}
		})
	}
}

func TestCypherMutationUpdatesVersionTime(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, explicit := range []bool{false, true} {
				for _, tc := range []struct {
					query           string
					edge, unchanged bool
				}{
					{"MATCH (n:Clock) SET n.title = 'v2'", false, false},
					{"MATCH (n:Clock) CALL db.create.setNodeVectorProperty(n, 'emb', [2.0,3.0])", false, false},
					{"MATCH ()-[r:VERSION]->() CALL db.create.setRelationshipVectorProperty(r, 'emb', [2.0,3.0])", true, false},
					{"MATCH (n:Clock) CALL db.create.setNodeVectorProperty(n, 'emb', [1.0,2.0])", false, true},
					{"MATCH ()-[r:VERSION]->() CALL db.create.setRelationshipVectorProperty(r, 'emb', [1.0,2.0])", true, true},
					{"MATCH (n:Clock) SET n += {title: 'v2'}", false, false},
					{"MATCH (n:Clock) SET n = {title: 'v2'}", false, false},
					{"MATCH (n:Clock) SET n:Changed", false, false},
					{"MATCH (n:Clock) REMOVE n.title", false, false},
					{"MATCH (n:Clock) REMOVE n:Old", false, false},
					{"MERGE (n:Clock) ON MATCH SET n.title = 'v2'", false, false},
					{"MATCH ()-[r:VERSION]->() SET r.v = 2", true, false},
					{"MATCH ()-[r:VERSION]->() SET r = {v: 2}", true, false},
					{"MATCH ()-[r:VERSION]->() SET r += {v: 2}", true, false},
					{"MATCH ()-[r:VERSION]->() REMOVE r.v", true, false},
					{"MATCH (n:Clock) SET n.title = 'v1'", false, true},
					{"MATCH (n:Clock) REMOVE n.missing", false, true},
					{"MATCH (n:Clock) REMOVE n:Missing", false, true},
					{"MATCH ()-[r:VERSION]->() REMOVE r.missing", true, true},
				} {
					t.Run(tc.query+map[bool]string{true: "/explicit", false: "/auto"}[explicit], func(t *testing.T) {
						ctx := context.Background()
						store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "clock")
						old := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
						for _, node := range []*storage.Node{
							{ID: "a", Labels: []string{"Clock", "Old"}, Properties: map[string]interface{}{"title": "v1", "emb": []float64{1, 2}}, CreatedAt: old, UpdatedAt: old},
							{ID: "b", Labels: []string{"Other"}, Properties: map[string]interface{}{}, CreatedAt: old, UpdatedAt: old},
						} {
							_, err := store.CreateNode(node)
							require.NoError(t, err)
						}
						require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "VERSION", Properties: map[string]interface{}{"v": int64(1), "emb": []float64{1, 2}}, CreatedAt: old, UpdatedAt: old}))
						exec := NewStorageExecutor(store)
						if explicit {
							_, err := exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
						}
						before := time.Now()
						_, err := exec.Execute(ctx, tc.query, nil)
						require.NoError(t, err)
						if explicit {
							_, err = exec.Execute(ctx, "COMMIT", nil)
							require.NoError(t, err)
						}
						var created, updated time.Time
						if tc.edge {
							edge, err := store.GetEdge("e")
							require.NoError(t, err)
							created, updated = edge.CreatedAt, edge.UpdatedAt
						} else {
							node, err := store.GetNode("a")
							require.NoError(t, err)
							created, updated = node.CreatedAt, node.UpdatedAt
						}
						require.True(t, created.Equal(old), "mutation must preserve creation time")
						if tc.unchanged {
							require.True(t, updated.Equal(old), "a no-op must preserve version time")
						} else {
							require.False(t, updated.Before(before), "successful mutation must advance version time")
							require.False(t, updated.After(time.Now()), "version time must describe this mutation")
						}
					})
				}
			}
		})
	}
}

// beforeNode returns the observation the last recorded node update carried.
func beforeNode(t *testing.T, store *observationEngine) *storage.Node {
	t.Helper()
	return store.lastNodeRequest(t).Before
}

// beforeEdge returns the observation the last recorded edge update carried.
func beforeEdge(t *testing.T, store *observationEngine) *storage.Edge {
	t.Helper()
	return store.lastEdgeRequest(t).Before
}

// SET and REMOVE reuse their failure snapshots as the storage observation.
// Mutating the live entity must not rewrite that snapshot or copy embeddings.
func TestCypherMutationSnapshotFieldsRemainImmutable(t *testing.T) {
	vector := []float32{1, 2, 3}
	node := &storage.Node{ID: "n", Labels: []string{"Keep", "Remove"}, Properties: map[string]interface{}{"keep": 1, "remove": 2}, ChunkEmbeddings: [][]float32{vector}}
	row := pipelineRow{"n": node, "alias": node}
	removals := removeObservations{}
	after, state := removals.mutableNode(row, "n", node)
	delete(after.Properties, "remove")
	after.Labels = after.Labels[:1]
	_, aliasState := removals.mutableNode(row, "alias", node)
	require.Same(t, state, aliasState, "aliased row variables share one pre-clause observation")
	require.Equal(t, 2, state.before.Properties["remove"])
	require.Equal(t, []string{"Keep", "Remove"}, state.before.Labels)
	require.Same(t, &vector[0], &state.before.ChunkEmbeddings[0][0], "immutable embedding arrays are shared")
	restoreSetEntities(removals.states)
	require.Equal(t, state.before.Properties, node.Properties)
	require.Equal(t, state.before.Labels, node.Labels)
}
