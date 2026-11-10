// The caller's context is the lifetime of every APOC storage point, path and
// traversal operation.
//
// These tests are the canonical-owner proof: they call InMemoryStorage directly,
// so they show what the contract itself does rather than what one helper does
// with it. A stopped caller is refused, a cancellation is reported as the
// caller's own error, and an ordinary miss keeps its own documented error.
package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// apocStorageFixture is one small graph and the node/relationship IDs in it:
// a -KNOWS-> b -KNOWS-> c, plus an isolated node d.
type apocStorageFixture struct {
	store     *InMemoryStorage
	a, b, c   int64
	isolated  int64
	firstLink int64
	lastLink  int64
}

func newAPOCStorageFixture(t *testing.T) *apocStorageFixture {
	t.Helper()
	ctx := t.Context()
	store := NewInMemoryStorage()
	fixture := &apocStorageFixture{store: store}
	create := func(name string) int64 {
		node, err := store.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": name})
		require.NoError(t, err)
		return node.ID
	}
	fixture.a = create("a")
	fixture.b = create("b")
	fixture.c = create("c")
	fixture.isolated = create("d")
	link := func(start, end int64) int64 {
		rel, err := store.CreateRelationship(ctx, start, end, "KNOWS", map[string]interface{}{"since": int64(2020)})
		require.NoError(t, err)
		return rel.ID
	}
	fixture.firstLink = link(fixture.a, fixture.b)
	fixture.lastLink = link(fixture.b, fixture.c)
	return fixture
}

// pointOperation is one context-first storage method reduced to the answer a
// caller receives: the value it published and the error it reported.
type pointOperation struct {
	name string
	call func(ctx context.Context) (interface{}, error)
}

// pointReads are the node, relationship, neighbour, path and traversal reads.
func (f *apocStorageFixture) pointReads() []pointOperation {
	return []pointOperation{
		{"GetNode", func(ctx context.Context) (interface{}, error) { return f.store.GetNode(ctx, f.a) }},
		{"GetRelationship", func(ctx context.Context) (interface{}, error) {
			return f.store.GetRelationship(ctx, f.firstLink)
		}},
		{"GetNodeRelationships", func(ctx context.Context) (interface{}, error) {
			return f.store.GetNodeRelationships(ctx, f.b, "", DirectionBoth)
		}},
		{"GetNodeNeighbors", func(ctx context.Context) (interface{}, error) {
			return f.store.GetNodeNeighbors(ctx, f.b, "", DirectionBoth)
		}},
		{"GetNodeDegree", func(ctx context.Context) (interface{}, error) {
			degree, err := f.store.GetNodeDegree(ctx, f.b, "", DirectionBoth)
			if err != nil {
				return nil, err
			}
			return degree, nil
		}},
		{"FindShortestPath", func(ctx context.Context) (interface{}, error) {
			return f.store.FindShortestPath(ctx, f.a, f.c, "", 3)
		}},
		{"FindAllPaths", func(ctx context.Context) (interface{}, error) {
			return f.store.FindAllPaths(ctx, f.a, f.c, "", 3)
		}},
		{"BFS", func(ctx context.Context) (interface{}, error) {
			return nil, f.store.BFS(ctx, f.a, "", 3, func(*Node) bool { return true })
		}},
		{"DFS", func(ctx context.Context) (interface{}, error) {
			return nil, f.store.DFS(ctx, f.a, "", 3, func(*Node) bool { return true })
		}},
	}
}

// pointMutations are the writes that prepare or perform a change.
func (f *apocStorageFixture) pointMutations() []pointOperation {
	return []pointOperation{
		{"CreateNode", func(ctx context.Context) (interface{}, error) {
			return f.store.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "created"})
		}},
		{"UpdateNode", func(ctx context.Context) (interface{}, error) {
			return nil, f.store.UpdateNode(ctx, f.a, PropertyUpdate{Set: map[string]interface{}{"age": int64(1)}})
		}},
		{"DeleteNode", func(ctx context.Context) (interface{}, error) {
			return nil, f.store.DeleteNode(ctx, f.isolated)
		}},
		{"AddLabels", func(ctx context.Context) (interface{}, error) {
			return nil, f.store.AddLabels(ctx, f.a, []string{"Employee"})
		}},
		{"RemoveLabels", func(ctx context.Context) (interface{}, error) {
			return nil, f.store.RemoveLabels(ctx, f.a, []string{"Person"})
		}},
		{"UpdateNodeLabels", func(ctx context.Context) (interface{}, error) {
			return nil, f.store.UpdateNodeLabels(ctx, f.a, []string{"OnlyLabel"})
		}},
		{"CreateRelationship", func(ctx context.Context) (interface{}, error) {
			return f.store.CreateRelationship(ctx, f.a, f.isolated, "KNOWS", map[string]interface{}{})
		}},
		{"UpdateRelationship", func(ctx context.Context) (interface{}, error) {
			return nil, f.store.UpdateRelationship(ctx, f.firstLink, PropertyUpdate{Set: map[string]interface{}{"weight": 0.5}})
		}},
		{"DeleteRelationship", func(ctx context.Context) (interface{}, error) {
			return nil, f.store.DeleteRelationship(ctx, f.firstLink)
		}},
		{"UpdateRelationshipType", func(ctx context.Context) (interface{}, error) {
			return nil, f.store.UpdateRelationshipType(ctx, f.firstLink, "LIKES")
		}},
	}
}

// operationNamed returns the fixture's own operation for name, so a subtest
// always drives the graph it just built.
func operationNamed(t *testing.T, operations []pointOperation, name string) func(ctx context.Context) (interface{}, error) {
	t.Helper()
	for _, operation := range operations {
		if operation.name == name {
			return operation.call
		}
	}
	t.Fatalf("no storage operation named %s", name)
	return nil
}

func operationNames(operations []pointOperation) []string {
	names := make([]string, 0, len(operations))
	for _, operation := range operations {
		names = append(names, operation.name)
	}
	return names
}

// graphState is the whole graph a refused mutation must leave untouched.
type graphState struct {
	nodes int
	edges int
}

func (f *apocStorageFixture) state(t *testing.T) graphState {
	t.Helper()
	ctx := t.Context()
	nodes, err := f.store.AllNodes(ctx)
	require.NoError(t, err)
	edges, err := f.store.AllRelationships(ctx)
	require.NoError(t, err)
	return graphState{nodes: len(nodes), edges: len(edges)}
}

// TestStoppedCallerReadsTheCallerError requires every read, path and traversal
// to report the caller's own cancellation instead of a value. A cancelled
// caller must never receive a not-found error, an empty collection or a zero
// count: those are answers to a read that ran.
func TestStoppedCallerReadsTheCallerError(t *testing.T) {
	for _, name := range operationNames(newAPOCStorageFixture(t).pointReads()) {
		name := name
		t.Run(name, func(t *testing.T) {
			fixture := newAPOCStorageFixture(t)
			call := operationNamed(t, fixture.pointReads(), name)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			value, err := call(ctx)

			require.ErrorIs(t, err, context.Canceled,
				"%s reports the caller's own cancellation", name)
			require.NotErrorIs(t, err, ErrNodeNotFound, name)
			require.NotErrorIs(t, err, ErrRelationshipNotFound, name)
			require.NotErrorIs(t, err, ErrPathNotFound, name)
			require.Nil(t, value,
				"%s publishes no collection or value for a stopped caller", name)
		})
	}
}

// TestStoppedCallerMutationsAreRefusedBeforeAdmission requires every write to
// refuse an already-stopped caller and to leave the graph exactly as it was.
func TestStoppedCallerMutationsAreRefusedBeforeAdmission(t *testing.T) {
	for _, name := range operationNames(newAPOCStorageFixture(t).pointMutations()) {
		name := name
		t.Run(name, func(t *testing.T) {
			fixture := newAPOCStorageFixture(t)
			call := operationNamed(t, fixture.pointMutations(), name)
			before := fixture.state(t)

			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			value, err := call(ctx)

			require.ErrorIs(t, err, context.Canceled,
				"%s refuses a caller that had already stopped", name)
			require.NotErrorIs(t, err, ErrNodeNotFound, name)
			require.NotErrorIs(t, err, ErrRelationshipNotFound, name)
			require.Nil(t, value, name)
			require.Equal(t, before, fixture.state(t),
				"%s must not change the graph for a caller that had already stopped", name)
		})
	}
}

// TestHealthyCallerRetriesAfterACancelledAttempt pins that a refused attempt
// leaves nothing behind: the same call with a live caller answers normally.
func TestHealthyCallerRetriesAfterACancelledAttempt(t *testing.T) {
	fixture := newAPOCStorageFixture(t)

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := fixture.store.FindShortestPath(cancelled, fixture.a, fixture.c, "", 3)
	require.ErrorIs(t, err, context.Canceled)
	err = fixture.store.AddLabels(cancelled, fixture.a, []string{"Employee"})
	require.ErrorIs(t, err, context.Canceled)
	err = fixture.store.DeleteNode(cancelled, fixture.isolated)
	require.ErrorIs(t, err, context.Canceled)

	ctx := t.Context()
	path, err := fixture.store.FindShortestPath(ctx, fixture.a, fixture.c, "", 3)
	require.NoError(t, err)
	require.Equal(t, 2, path.Length, "the retried search returns the real path")
	require.NoError(t, fixture.store.AddLabels(ctx, fixture.a, []string{"Employee"}))
	require.NoError(t, fixture.store.DeleteNode(ctx, fixture.isolated))
	require.Equal(t, graphState{nodes: 3, edges: 2}, fixture.state(t),
		"the retried writes took effect exactly once")
}

// TestMissingEntityKeepsItsOrdinaryResult is the control for the cancellation
// cases: with a live caller, a missing node, relationship or path still reports
// its own documented error, and an entity without relationships still answers
// with an empty collection rather than an error.
func TestMissingEntityKeepsItsOrdinaryResult(t *testing.T) {
	fixture := newAPOCStorageFixture(t)
	ctx := t.Context()
	const missing = int64(9999)

	_, err := fixture.store.GetNode(ctx, missing)
	require.ErrorIs(t, err, ErrNodeNotFound)
	_, err = fixture.store.GetRelationship(ctx, missing)
	require.ErrorIs(t, err, ErrRelationshipNotFound)
	_, err = fixture.store.FindShortestPath(ctx, missing, fixture.a, "", 3)
	require.ErrorIs(t, err, ErrPathNotFound,
		"an unreachable start keeps the path miss it always reported")
	_, err = fixture.store.FindAllPaths(ctx, missing, fixture.a, "", 3)
	require.ErrorIs(t, err, ErrNodeNotFound,
		"the all-paths read starts from a node it loads")

	relationships, err := fixture.store.GetNodeRelationships(ctx, fixture.isolated, "", DirectionBoth)
	require.NoError(t, err)
	require.Empty(t, relationships)
	require.NotNil(t, relationships, "an entity with no relationships is an empty collection, not an error")
	neighbors, err := fixture.store.GetNodeNeighbors(ctx, fixture.isolated, "", DirectionBoth)
	require.NoError(t, err)
	require.Empty(t, neighbors)
	degree, err := fixture.store.GetNodeDegree(ctx, fixture.isolated, "", DirectionBoth)
	require.NoError(t, err)
	require.Zero(t, degree)
}

// TestTraversalStopsWhenTheCallerStopsInsideIt cancels while the traversal is
// already running and requires the traversal to report that cancellation rather
// than a partial walk.
func TestTraversalStopsWhenTheCallerStopsInsideIt(t *testing.T) {
	fixture := newAPOCStorageFixture(t)
	ctx, cancel := context.WithCancel(t.Context())

	visited := 0
	err := fixture.store.BFS(ctx, fixture.a, "", 3, func(*Node) bool {
		visited++
		cancel()
		return true
	})
	require.ErrorIs(t, err, context.Canceled,
		"a traversal stopped by its caller reports the caller's cancellation")
	require.Equal(t, 1, visited, "the walk does not publish further nodes after the caller stopped")

	ctx, cancel = context.WithCancel(t.Context())
	err = fixture.store.DFS(ctx, fixture.a, "", 3, func(*Node) bool {
		cancel()
		return true
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestTraversalVisitorCancellationReturnsCallerError(t *testing.T) {
	for _, traversal := range []string{"BFS", "DFS"} {
		for _, keepGoing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/continue=%t", traversal, keepGoing), func(t *testing.T) {
				graph := NewInMemoryStorage()
				start, err := graph.CreateNode(t.Context(), nil, nil)
				require.NoError(t, err)
				next, err := graph.CreateNode(t.Context(), nil, nil)
				require.NoError(t, err)
				_, err = graph.CreateRelationship(t.Context(), start.ID, next.ID, "R", nil)
				require.NoError(t, err)
				request, cancel := context.WithCancel(t.Context())
				defer cancel()
				visits := 0
				visitor := func(*Node) bool { visits++; cancel(); return keepGoing }
				if traversal == "BFS" {
					err = graph.BFS(request, start.ID, "R", 2, visitor)
				} else {
					err = graph.DFS(request, start.ID, "R", 2, visitor)
				}
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, 1, visits, "no callback after the caller stops")
				visits = 0
				healthy := func(*Node) bool { visits++; return true }
				if traversal == "BFS" {
					err = graph.BFS(t.Context(), start.ID, "R", 2, healthy)
				} else {
					err = graph.DFS(t.Context(), start.ID, "R", 2, healthy)
				}
				require.NoError(t, err)
				require.Equal(t, 2, visits)
			})
		}
	}
}
