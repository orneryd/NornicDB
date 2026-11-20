package cypher

import (
	"context"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-357 recovered linked-comment scope, reproduction stage. The recovered
// comment names three behaviours; this file records their current deterministic
// public behaviour on this tree without changing production code:
//
//  1. withParams' nil-context fallback (parameters.go): the comment proposed
//     substituting context.Background() for a nil statement context.
//  2. Row statement-context binding, including a relationship pattern and a
//     shortestPath() expression in row position (temporal_values.go
//     rowStatementContextOrBackground, and the row value bindings of
//     row_expression.go / pattern_expression.go).
//  3. The native point read of a GDS link prediction (linkprediction.go) and of
//     the endNode row endpoint (relationship_endpoint_projection.go).
//
// The endNode half of (3) is already covered on this tree by
// TestRowEndpointPublicCancellationAndRetry and
// TestRowEndpointOperationCancellationWithMetadata (both parsers, row programs
// on and off, live cancellation at the native read and a healthy retry), so it
// is referenced rather than duplicated here.

type nor357RecoveryRequestKey struct{}

// nor357RecoveryReadStore observes the native reads a statement performs and
// can cancel it immediately before one of them. The armed read returns the
// caller's cancellation, which is what a native engine does when it admits an
// already canceled request; the decoration is transparent otherwise (it
// forwards every method and implements no capability the executor could take
// instead of the decorated engine).
type nor357RecoveryReadStore struct {
	storage.Engine

	mu            sync.Mutex
	nodeReads     int
	adjacentReads int
	seen          []context.Context
	cancel        context.CancelFunc
	kind          string
	armAt         int
}

func (s *nor357RecoveryReadStore) beforeRead(ctx context.Context, kind string) error {
	s.mu.Lock()
	count := &s.nodeReads
	if kind == "adjacent" {
		count = &s.adjacentReads
	}
	*count++
	s.seen = append(s.seen, ctx)
	cancel := s.cancel
	fire := cancel != nil && s.kind == kind && *count == s.armAt
	if fire {
		s.cancel = nil
	}
	s.mu.Unlock()
	if fire {
		cancel()
		return ctx.Err()
	}
	return nil
}

func (s *nor357RecoveryReadStore) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	if err := s.beforeRead(ctx, "node"); err != nil {
		return nil, err
	}
	return s.Engine.GetNode(ctx, id)
}

func (s *nor357RecoveryReadStore) ReadAdjacentEdges(ctx context.Context, request storage.AdjacentEdgeRead) ([]*storage.Edge, error) {
	if err := s.beforeRead(ctx, "adjacent"); err != nil {
		return nil, err
	}
	return s.Engine.ReadAdjacentEdges(ctx, request)
}

// watch starts a fresh observation window: every native read is recorded, and
// the read of kind at position at cancels the statement (cancel nil: none).
func (s *nor357RecoveryReadStore) watch(cancel context.CancelFunc, kind string, at int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodeReads = 0
	s.adjacentReads = 0
	s.seen = nil
	s.cancel = cancel
	s.kind = kind
	s.armAt = at
}

func (s *nor357RecoveryReadStore) snapshot() (int, int, []context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nodeReads, s.adjacentReads, append([]context.Context(nil), s.seen...)
}

// requireNOR357RequestOnEveryRead requires every observed native read to carry
// the caller's request value. A read that received the internal fallback (or
// the wrong context) fails.
func requireNOR357RequestOnEveryRead(t *testing.T, request context.Context, seen []context.Context) {
	t.Helper()
	require.NotEmpty(t, seen, "the statement must perform a native read")
	want := request.Value(nor357RecoveryRequestKey{})
	for index, read := range seen {
		require.Equal(t, want, read.Value(nor357RecoveryRequestKey{}),
			"native read %d must inherit the caller's statement context", index)
	}
}

// TestNOR357WithParamsNilContextKeepsNilStatementContext records the current
// withParams contract (parameters.go): a nil parameter map returns the caller's
// context unchanged, and a parameter map over a nil context produces a layer
// whose parent is nil. withParams does not substitute context.Background(), and
// its only production callers (binding_where_compile.go, pattern_row.go) pass a
// statement context derived from the public Execute entry.
func TestNOR357WithParamsNilContextKeepsNilStatementContext(t *testing.T) {
	params := map[string]interface{}{"v": int64(7)}

	require.Nil(t, withParams(nil, nil),
		"a nil parameter map returns the caller's context unchanged, even when that context is nil")

	layer, ok := withParams(nil, params).(*queryParamsContext)
	require.True(t, ok, "a parameter map attaches the query-parameters layer")
	require.Nil(t, layer.Context,
		"withParams does not substitute context.Background() for a nil statement context")
	require.Equal(t, params, getParamsFromContext(layer),
		"the parameters are reachable from the nil-parented layer")

	background := withParams(context.Background(), params).(*queryParamsContext)
	require.Equal(t, context.Background(), background.Context,
		"a caller-supplied context is retained as the layer's parent")

	// rowStatementContextOrBackground is the only owner that returns a
	// background context for a row without statement state.
	require.Equal(t, context.Background(), rowStatementContextOrBackground(nil))
}

// TestNOR357PublicExecuteRequiresStatementContext records that the public
// Execute entry dereferences the statement context before any evaluator runs
// (transportread.Take -> ctx.Value in executor_entry.go), so a nil statement
// context cannot reach withParams from a public path: it panics at the entry.
func TestNOR357PublicExecuteRequiresStatementContext(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor357_withparams"))

	result, err := exec.Execute(t.Context(), "RETURN 1 AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

	require.Panics(t, func() { _, _ = exec.Execute(nil, "RETURN 1 AS v", nil) },
		"public Execute requires a statement context; a nil context never reaches withParams")
}

// TestNOR357RowStatementContextBindingIsSyntactic records how the row evaluator
// decides to bind the statement context (temporal_values.go needsRowStatementContext,
// row_expression.go rowValuesWithStatementContext): binding is conditional on the
// expression's shape. A relationship pattern and a shortestPath() call are two
// shapes that do bind; a plain value expression does not, so
// rowStatementContextOrBackground would hand that row a background context. On
// this tree that helper's only caller is evaluateExpressionFromValues
// (match_with_rel.go), which has no production caller.
func TestNOR357RowStatementContextBindingIsSyntactic(t *testing.T) {
	marker := context.WithValue(t.Context(), nor357RecoveryRequestKey{}, "row")
	bind := func(expression string) (map[string]interface{}, bool) {
		values := map[string]interface{}{"a": int64(1)}
		bound, didBind := rowValuesWithStatementContext(marker, expression, values)
		return bound, didBind
	}

	plain, bound := bind("a + 1")
	require.False(t, bound, "a plain value expression does not bind the statement context")
	require.Nil(t, rowStatementContext(plain))
	require.Equal(t, context.Background(), rowStatementContextOrBackground(plain),
		"a row without a binding falls back to the background context")

	for _, expression := range []string{
		"(a)-[:R]->(b)",
		"shortestPath((a)-[:T*1..3]->(b))",
	} {
		values, bound := bind(expression)
		require.True(t, bound, expression+" binds the statement context")
		require.Equal(t, marker, rowStatementContext(values), expression)
	}
}

// newNOR357RecoveryFixture creates the tiny graph the row-context tests use and
// returns the namespaced executor store (wrapping the observer), the observer,
// and the created nodes keyed by their id for use as point-read parameters.
// Nodes and relationships are created before any observation window, so only
// the statement under test is recorded.
func newNOR357RecoveryFixture(t *testing.T, labels, ids, types []string) (storage.Engine, *nor357RecoveryReadStore, map[string]interface{}) {
	t.Helper()
	observed := &nor357RecoveryReadStore{Engine: newTestMemoryEngine(t)}
	native := storage.NewNamespacedEngine(observed, "nor357_recovery")
	stored := make([]storage.NodeID, len(ids))
	for index, id := range ids {
		nodeID, err := native.CreateNode(t.Context(), &storage.Node{
			ID:         storage.NodeID(id),
			Labels:     labels,
			Properties: map[string]interface{}{"k": id},
		})
		require.NoError(t, err)
		stored[index] = nodeID
	}
	for index := 0; index < len(types); index++ {
		require.NoError(t, native.CreateEdge(t.Context(), &storage.Edge{
			ID:        storage.EdgeID(types[index] + "-" + ids[index] + ids[index+1]),
			StartNode: stored[index],
			EndNode:   stored[index+1],
			Type:      types[index],
		}))
	}
	params := make(map[string]interface{}, len(stored))
	for index, id := range stored {
		node, err := native.GetNode(t.Context(), id)
		require.NoError(t, err)
		params[ids[index]] = node
	}
	return native, observed, params
}

// TestNOR357RelationshipPatternPredicateUsesStatementContext is scope item 2 for
// a relationship-pattern row predicate: the predicate is a graph expression, so
// the row evaluator binds the statement context (needsRowStatementContext sees
// containsRelationshipPattern) and the predicate's adjacency reads run under
// the caller's context. A cancellation at the first adjacency read ends the
// statement, and a healthy retry publishes the predicate's row.
func TestNOR357RelationshipPatternPredicateUsesStatementContext(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		native, store, params := newNOR357RecoveryFixture(t, []string{"RP"}, []string{"a", "b"}, []string{"R"})
		exec := NewStorageExecutor(native, QueryCachePolicy{MaxEntries: 0, TTL: 0})
		const query = "WITH $a AS a, $b AS b WHERE (a)-[:R]->(b) RETURN a.k AS k"
		want := [][]interface{}{{"a"}}
		request := context.WithValue(t.Context(), nor357RecoveryRequestKey{}, "pattern")

		// Healthy statement: the pattern predicate reads the graph and holds.
		store.watch(nil, "adjacent", 0)
		result, err := exec.Execute(request, query, params)
		require.NoError(t, err)
		require.Equal(t, want, result.Rows)
		_, healthyReads, seen := store.snapshot()
		require.Greater(t, healthyReads, 0, "the pattern predicate must read adjacency")
		requireNOR357RequestOnEveryRead(t, request, seen)
		t.Logf("healthy pattern predicate: %d adjacency reads", healthyReads)

		// Live cancellation at the predicate's first adjacency read.
		canceled, cancel := context.WithCancel(request)
		defer cancel()
		store.watch(cancel, "adjacent", 1)
		result, err = exec.Execute(canceled, query, params)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, result)
		_, canceledReads, seen := store.snapshot()
		require.Equal(t, 1, canceledReads,
			"the canceled predicate stops at the adjacency read it observed")
		requireNOR357RequestOnEveryRead(t, canceled, seen)

		// Healthy retry: the canceled statement poisons nothing.
		store.watch(nil, "adjacent", 0)
		result, err = exec.Execute(request, query, params)
		require.NoError(t, err)
		require.Equal(t, want, result.Rows)
		_, retryReads, seen := store.snapshot()
		require.Greater(t, retryReads, 0)
		requireNOR357RequestOnEveryRead(t, request, seen)
		t.Logf("healthy retry: %d adjacency reads", retryReads)
	})
}

// TestNOR357ShortestPathRowExpressionUsesStatementContext is scope item 2 for a
// shortestPath() projection: the row evaluator reaches the shortest-path search
// (row_function_expression.go) with the caller's context, so the search's
// adjacency reads inherit it, a cancellation at the first one ends the
// statement, and a healthy retry publishes the same path length.
func TestNOR357ShortestPathRowExpressionUsesStatementContext(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		native, store, params := newNOR357RecoveryFixture(t, []string{"SP"}, []string{"a", "c", "b"}, []string{"T", "T"})
		exec := NewStorageExecutor(native, QueryCachePolicy{MaxEntries: 0, TTL: 0})
		const query = "WITH $a AS a, $b AS b RETURN length(shortestPath((a)-[:T*1..3]->(b))) AS hops"
		want := [][]interface{}{{int64(2)}}
		request := context.WithValue(t.Context(), nor357RecoveryRequestKey{}, "shortest")

		store.watch(nil, "adjacent", 0)
		result, err := exec.Execute(request, query, params)
		require.NoError(t, err)
		require.Equal(t, want, result.Rows)
		_, healthyReads, seen := store.snapshot()
		require.Greater(t, healthyReads, 0, "the shortest-path search must read adjacency")
		requireNOR357RequestOnEveryRead(t, request, seen)
		t.Logf("healthy shortestPath: %d adjacency reads", healthyReads)

		canceled, cancel := context.WithCancel(request)
		defer cancel()
		store.watch(cancel, "adjacent", 1)
		result, err = exec.Execute(canceled, query, params)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, result)
		_, canceledReads, seen := store.snapshot()
		require.Equal(t, 1, canceledReads,
			"the canceled search stops at the adjacency read it observed")
		requireNOR357RequestOnEveryRead(t, canceled, seen)

		store.watch(nil, "adjacent", 0)
		result, err = exec.Execute(request, query, params)
		require.NoError(t, err)
		require.Equal(t, want, result.Rows)
		_, retryReads, seen := store.snapshot()
		require.Greater(t, retryReads, 0)
		requireNOR357RequestOnEveryRead(t, request, seen)
		t.Logf("healthy retry: %d adjacency reads", retryReads)
	})
}

// TestNOR357LinkPredictionNativeReadUsesStatementContext is scope item 3 for a
// GDS link prediction: the hybrid predict.stream procedure's native point read
// (linkPredictionConfig.readNode, graph_entity_identity.go) runs under the
// caller's context, a cancellation at that read ends the statement, and a
// healthy retry returns the same predictions. The procedure itself ignores the
// read error and the canceled score reads, so the record also reports how many
// point reads the canceled statement still performed before the statement's
// terminal cancellation check ended it.
func TestNOR357LinkPredictionNativeReadUsesStatementContext(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		native, store, params := newNOR357RecoveryFixture(t, []string{"LP"}, []string{"a", "b", "c"}, []string{"R", "R"})
		exec := NewStorageExecutor(native, QueryCachePolicy{MaxEntries: 0, TTL: 0})
		const query = "CALL gds.linkPrediction.predict.stream({sourceNode: $a, topK: 3}) " +
			"YIELD node1, node2, score RETURN node1, node2, score"
		request := context.WithValue(t.Context(), nor357RecoveryRequestKey{}, "linkprediction")

		// Healthy statement: one prediction (a reaches c through b).
		store.watch(nil, "node", 0)
		healthy, err := exec.Execute(request, query, params)
		require.NoError(t, err)
		require.Len(t, healthy.Rows, 1)
		healthyReads, _, seen := store.snapshot()
		require.Greater(t, healthyReads, 0, "the hybrid score must perform its point reads")
		requireNOR357RequestOnEveryRead(t, request, seen)
		t.Logf("healthy predict.stream: %d point reads", healthyReads)
		want := healthy.Rows

		// Live cancellation at the first native point read.
		canceled, cancel := context.WithCancel(request)
		defer cancel()
		store.watch(cancel, "node", 1)
		result, err := exec.Execute(canceled, query, params)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, result)
		canceledReads, _, seen := store.snapshot()
		require.GreaterOrEqual(t, canceledReads, 1,
			"the canceled statement observed the native read it was canceled at")
		requireNOR357RequestOnEveryRead(t, canceled, seen)
		t.Logf("canceled predict.stream: %d point reads after the first canceled read", canceledReads)

		// Healthy retry: the canceled statement poisons nothing.
		store.watch(nil, "node", 0)
		result, err = exec.Execute(request, query, params)
		require.NoError(t, err)
		require.Equal(t, want, result.Rows)
		retryReads, _, seen := store.snapshot()
		require.Greater(t, retryReads, 0)
		requireNOR357RequestOnEveryRead(t, request, seen)
		t.Logf("healthy retry: %d point reads", retryReads)
	})
}
