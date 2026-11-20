package cypher

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-357 graph-function scope audit. The evaluator's graph functions resolve
// their graph through StorageExecutor.FunctionStorage, whose selection state
// (graphIdentityScope) is installed only for a Fabric fragment. This test
// covers the reachable public shapes of that state -- a named constituent, a
// constituent reached through a composite alias, and both nested in a composite
// subquery -- for the builtin endpoint and GDS functions, and records what the
// statement's own database selection does.

type nor357GraphRequestKey struct{}

// nor357ObservedEngine is an explicit controlled observer of the native point
// reads a graph function performs. It is transparent: it keeps the storage
// package's EngineUnwrapper contract and forwards the namespace metadata, so it
// adds no capability a query path could take instead of the decorated engine.
// It records each read's context and can cancel the statement once, at an armed
// read.
type nor357ObservedEngine struct {
	storage.Engine
	mu       sync.Mutex
	reads    int
	seen     []context.Context
	cancel   context.CancelFunc
	cancelAt int
}

func (o *nor357ObservedEngine) GetInnerEngine() storage.Engine { return o.Engine }

func (o *nor357ObservedEngine) Namespace() string {
	if provider, ok := o.Engine.(interface{ Namespace() string }); ok {
		return provider.Namespace()
	}
	return ""
}

func (o *nor357ObservedEngine) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	o.beforeRead(ctx)
	return o.Engine.GetNode(ctx, id)
}

func (o *nor357ObservedEngine) GetEdge(ctx context.Context, id storage.EdgeID) (*storage.Edge, error) {
	o.beforeRead(ctx)
	return o.Engine.GetEdge(ctx, id)
}

// beforeRead records the caller's context and, at the armed read, cancels the
// statement before the decorated engine runs that read, as a client disconnect
// would.
func (o *nor357ObservedEngine) beforeRead(ctx context.Context) {
	o.mu.Lock()
	o.reads++
	o.seen = append(o.seen, ctx)
	cancel := o.cancel
	armed := cancel != nil && o.reads == o.cancelAt
	if armed {
		o.cancel = nil
	}
	o.mu.Unlock()
	if armed {
		cancel()
	}
}

func (o *nor357ObservedEngine) watch(cancel context.CancelFunc, at int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.reads = 0
	o.seen = nil
	o.cancel = cancel
	o.cancelAt = at
}

func (o *nor357ObservedEngine) snapshot() (int, []context.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.reads, append([]context.Context(nil), o.seen...)
}

// nor357CountingManager records the statement's own database selections, so a
// graph function's admission resolution would be visible as another call.
type nor357CountingManager struct {
	DatabaseManagerInterface
	mu    sync.Mutex
	calls []string
}

func (m *nor357CountingManager) GetStorageForUse(name, authToken string) (interface{}, error) {
	m.mu.Lock()
	m.calls = append(m.calls, name)
	m.mu.Unlock()
	return m.DatabaseManagerInterface.GetStorageForUse(name, authToken)
}

func (m *nor357CountingManager) take() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	calls := m.calls
	m.calls = nil
	return calls
}

// newNOR357GraphFunctionFixture builds composite comp with the local database
// one as constituent alias, over an observed base engine, and returns the
// default-database executor, the observer, the selection counter and the entry
// and edge parameters.
func newNOR357GraphFunctionFixture(t *testing.T, alias string) (*StorageExecutor, *nor357ObservedEngine, *nor357CountingManager, map[string]interface{}) {
	t.Helper()
	observed := &nor357ObservedEngine{Engine: newTestMemoryEngine(t)}
	manager, err := multidb.NewDatabaseManager(t.Context(), observed, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "one"}))
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "comp", []multidb.ConstituentRef{
		{Alias: alias, DatabaseName: "one", Type: "local", AccessMode: "read_write"},
	}))
	store, err := manager.GetStorage("one")
	require.NoError(t, err)
	start, err := store.CreateNode(t.Context(), &storage.Node{ID: "from", Labels: []string{"P"}, Properties: map[string]interface{}{"v": int64(7)}})
	require.NoError(t, err)
	end, err := store.CreateNode(t.Context(), &storage.Node{ID: "to", Labels: []string{"Q"}, Properties: map[string]interface{}{"v": int64(8)}})
	require.NoError(t, err)
	edge := &storage.Edge{ID: "rel", StartNode: start, EndNode: end, Type: "R"}
	require.NoError(t, store.CreateEdge(t.Context(), edge))
	counting := &nor357CountingManager{DatabaseManagerInterface: &testDatabaseManagerAdapter{manager: manager}}
	defaultStore, err := manager.GetStorage(manager.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(counting)
	exec.cache = nil
	return exec, observed, counting, map[string]interface{}{"r": edge, "id": string(start)}
}

// TestNOR357GraphFunctionNativeReadUsesStatementContext proves, on the public
// statement path, that the endpoint and GDS graph functions read under the
// caller's own context in every reachable graph-function selection scope: a
// named constituent, a constituent reached through a composite alias, and both
// nested in a composite subquery. A client cancellation ends the statement at
// the native read, a healthy retry publishes the value, and none of the shapes
// re-enters FunctionStorage admission: the only database resolution is the
// statement's own USE.
func TestNOR357GraphFunctionNativeReadUsesStatementContext(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, scope := range []struct {
			name, alias, format string
		}{
			{"selected_database", "one", "USE comp.one %s"},
			{"aliased_constituent", "a", "USE comp.a %s"},
			{"nested_composite", "one", "USE comp CALL { USE comp.one %s } RETURN v"},
			{"nested_aliased_composite", "a", "USE comp CALL { USE comp.a %s } RETURN v"},
		} {
			t.Run(scope.name, func(t *testing.T) {
				for _, function := range []struct {
					name, body string
				}{
					// rowEndpointPropertyNode (endpoint property projection)
					{"start_node_property", "RETURN startNode($r).v AS v"},
					// relationshipEndpointValue (the whole endpoint value)
					{"start_node_value", "WITH startNode($r) AS n RETURN n.v AS v"},
					{"gds_as_node", "RETURN gds.util.asNode($id).v AS v"},
				} {
					t.Run(function.name, func(t *testing.T) {
						exec, observed, counting, params := newNOR357GraphFunctionFixture(t, scope.alias)
						query := fmt.Sprintf(scope.format, function.body)
						want := [][]interface{}{{int64(7)}}
						request := context.WithValue(t.Context(), nor357GraphRequestKey{}, scope.name+"/"+function.name)
						requireMarker := func(seen []context.Context) {
							require.NotEmpty(t, seen, "the graph function must perform a native read")
							for _, read := range seen {
								require.Equal(t, request.Value(nor357GraphRequestKey{}), read.Value(nor357GraphRequestKey{}),
									"a graph function read must inherit the caller's context")
							}
						}

						// Healthy statement: the graph function reads the
						// selected graph and publishes its value.
						counting.take()
						observed.watch(nil, 0)
						result, err := exec.Execute(request, query, params)
						require.NoError(t, err)
						require.Equal(t, want, result.Rows)
						reads, seen := observed.snapshot()
						require.Greater(t, reads, 0)
						requireMarker(seen)
						require.Equal(t, []string{"one"}, counting.take(),
							"the graph function reuses the statement's selection instead of admitting another graph")

						// Live cancellation at the first native read: the
						// statement ends as the caller's cancellation, the
						// graph read does not continue, and nothing is
						// published.
						counting.take()
						canceled, cancel := context.WithCancel(request)
						defer cancel()
						observed.watch(cancel, 1)
						result, err = exec.Execute(canceled, query, params)
						require.ErrorIs(t, err, context.Canceled)
						require.Nil(t, result)
						reads, seen = observed.snapshot()
						require.Equal(t, 1, reads, "a canceled graph function must stop at the read it observed")
						requireMarker(seen)
						require.Equal(t, []string{"one"}, counting.take(),
							"cancellation must not add a graph admission resolution")

						// Healthy retry: the canceled statement poisons nothing.
						observed.watch(nil, 0)
						result, err = exec.Execute(request, query, params)
						require.NoError(t, err)
						require.Equal(t, want, result.Rows)
						_, seen = observed.snapshot()
						requireMarker(seen)
						require.Equal(t, []string{"one"}, counting.take())
					})
				}
			})
		}
	})
}
