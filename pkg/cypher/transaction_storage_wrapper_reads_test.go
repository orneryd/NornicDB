package cypher

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// recordingVisibilityEngine is the engine a transaction wrapper routes its
// snapshot-visible reads to. It records exactly what the wrapper asked for (so
// a test can pin the version and the identifiers it passed down) and can cancel
// the caller's context inside the read itself, which is the cancellation that
// arrives at read exit.
type recordingVisibilityEngine struct {
	*storage.MemoryEngine
	nodes            []*storage.Node
	edges            []*storage.Edge
	readErr          error
	cancelAtReadExit bool
	cancel           context.CancelFunc
	namespace        string

	label   string
	version storage.MVCCVersion
	node    storage.NodeID
	other   storage.NodeID
}

// Pin the optional capability: a signature drift must fail compilation.
var _ storage.MVCCIndexedVisibilityEngine = (*recordingVisibilityEngine)(nil)

// Namespace is the engine's own namespace, empty for an unscoped engine.
func (e *recordingVisibilityEngine) Namespace() string { return e.namespace }

func (e *recordingVisibilityEngine) GetNodesByLabelVisibleAt(ctx context.Context, label string, version storage.MVCCVersion) ([]*storage.Node, error) {
	e.label, e.version = label, version
	e.cancelCaller(ctx)
	return e.nodes, e.readErr
}

func (e *recordingVisibilityEngine) GetOutgoingEdgesVisibleAt(ctx context.Context, nodeID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	e.node, e.version = nodeID, version
	e.cancelCaller(ctx)
	return e.edges, e.readErr
}

func (e *recordingVisibilityEngine) GetIncomingEdgesVisibleAt(ctx context.Context, nodeID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	e.node, e.version = nodeID, version
	e.cancelCaller(ctx)
	return e.edges, e.readErr
}

func (e *recordingVisibilityEngine) GetEdgesByTypeVisibleAt(ctx context.Context, edgeType string, version storage.MVCCVersion) ([]*storage.Edge, error) {
	e.label, e.version = edgeType, version
	e.cancelCaller(ctx)
	return e.edges, e.readErr
}

func (e *recordingVisibilityEngine) GetEdgesBetweenVisibleAt(ctx context.Context, startID, endID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	e.node, e.other, e.version = startID, endID, version
	e.cancelCaller(ctx)
	return e.edges, e.readErr
}

func (e *recordingVisibilityEngine) cancelCaller(ctx context.Context) {
	if e.cancel != nil {
		e.cancel()
		return
	}
	if e.cancelAtReadExit {
		// One observation of the caller's context: a cancelAtErrContext whose
		// trigger is this observation cancels here, after its own read work.
		_ = ctx.Err()
	}
}

// cancelAtErrContext is a real context created by context.WithCancel whose
// cancellation fires from its at-th Err observation. That lets a test cancel at
// an exact point of a real read — at read exit, or mid projection — with the
// cancel function's own context.Canceled as the reported error, and never with
// an error value manufactured for the test.
type cancelAtErrContext struct {
	context.Context
	cancel context.CancelFunc
	at     int
	seen   atomic.Int64
}

func newCancelAtErrContext(at int) (*cancelAtErrContext, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	return &cancelAtErrContext{Context: ctx, cancel: cancel, at: at}, cancel
}

func (c *cancelAtErrContext) Err() error {
	if c.seen.Add(1) == int64(c.at) {
		c.cancel()
	}
	return c.Context.Err()
}

// A snapshot-visible read answered by the underlying engine ends with the
// caller's cancellation, not with the collection the read returned: the
// wrapper's post-read detag/copy/filter must observe the context the caller
// actually canceled.
func TestTransactionWrapperVisibleAtCancelsAtReadExit(t *testing.T) {
	version := storage.MVCCVersion{CommitSequence: 42}
	tenantEdge := &storage.Edge{ID: "tenant:e1", StartNode: "tenant:n1", EndNode: "tenant:n2", Type: "REL"}
	tenantNode := &storage.Node{ID: "tenant:n1", Labels: []string{"Doc"}}
	engine := &recordingVisibilityEngine{
		MemoryEngine:     newTestMemoryEngine(t),
		nodes:            []*storage.Node{tenantNode},
		edges:            []*storage.Edge{tenantEdge},
		cancelAtReadExit: true,
	}
	wrapper := &transactionStorageWrapper{underlying: engine, namespace: "tenant", separator: ":"}

	reads := []struct {
		name string
		read func(ctx context.Context) (int, error)
	}{
		{
			name: "GetNodesByLabelVisibleAt",
			read: func(ctx context.Context) (int, error) {
				nodes, err := wrapper.GetNodesByLabelVisibleAt(ctx, "Doc", version)
				return len(nodes), err
			},
		},
		{
			name: "GetOutgoingEdgesVisibleAt",
			read: func(ctx context.Context) (int, error) {
				edges, err := wrapper.GetOutgoingEdgesVisibleAt(ctx, "n1", version)
				return len(edges), err
			},
		},
		{
			name: "GetIncomingEdgesVisibleAt",
			read: func(ctx context.Context) (int, error) {
				edges, err := wrapper.GetIncomingEdgesVisibleAt(ctx, "n2", version)
				return len(edges), err
			},
		},
		{
			name: "GetEdgesByTypeVisibleAt",
			read: func(ctx context.Context) (int, error) {
				edges, err := wrapper.GetEdgesByTypeVisibleAt(ctx, "REL", version)
				return len(edges), err
			},
		},
		{
			name: "GetEdgesBetweenVisibleAt",
			read: func(ctx context.Context) (int, error) {
				edges, err := wrapper.GetEdgesBetweenVisibleAt(ctx, "n1", "n2", version)
				return len(edges), err
			},
		},
	}
	for _, read := range reads {
		t.Run(read.name, func(t *testing.T) {
			// Observation 1 is the engine's own read, which cancels on return.
			ctx, cancel := newCancelAtErrContext(1)
			defer cancel()
			count, err := read.read(ctx)
			require.ErrorIs(t, err, context.Canceled, "the caller's cancellation must reach the caller")
			require.Zero(t, count, "a canceled read returns no collection")
		})
	}
}

// The projection observes cancellation while it runs, not only before it
// starts: a large collection ends early with the caller's error and no partial
// collection at all.
func TestTransactionWrapperProjectionStopsMidCollection(t *testing.T) {
	version := storage.MVCCVersion{CommitSequence: 7}
	const records = 4097 // more than two projection checks
	build := func() ([]*storage.Node, []*storage.Edge) {
		nodes := make([]*storage.Node, 0, records)
		edges := make([]*storage.Edge, 0, records)
		for i := 0; i < records; i++ {
			suffix := strconv.Itoa(i)
			nodes = append(nodes, &storage.Node{ID: storage.NodeID("tenant:n" + suffix), Labels: []string{"Doc"}})
			edges = append(edges, &storage.Edge{
				ID:        storage.EdgeID("tenant:e" + suffix),
				StartNode: "tenant:n1",
				EndNode:   "tenant:n2",
				Type:      "REL",
			})
		}
		return nodes, edges
	}
	nodes, edges := build()
	engine := &recordingVisibilityEngine{MemoryEngine: newTestMemoryEngine(t), nodes: nodes, edges: edges}
	wrapper := &transactionStorageWrapper{underlying: engine, namespace: "tenant", separator: ":"}

	t.Run("nodes", func(t *testing.T) {
		// Observation 1 is the engine's read; observation 3 is a projection
		// check after the first one.
		ctx, cancel := newCancelAtErrContext(3)
		defer cancel()
		projected, err := wrapper.GetNodesByLabelVisibleAt(ctx, "Doc", version)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, projected, "a canceled projection returns no partial collection")
	})

	t.Run("edges", func(t *testing.T) {
		ctx, cancel := newCancelAtErrContext(3)
		defer cancel()
		projected, err := wrapper.GetEdgesByTypeVisibleAt(ctx, "REL", version)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, projected, "a canceled projection returns no partial collection")
	})
}

// A caller that canceled before the read sees the cancellation on every
// collection read the wrapper performs, whichever engine answers it, and never
// a collection.
func TestTransactionWrapperReadsRejectCanceledCaller(t *testing.T) {
	base := newTestMemoryEngine(t)
	tenant := storage.NewNamespacedEngine(base, "a")
	_, err := tenant.CreateNode(t.Context(), &storage.Node{ID: "x", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = tenant.CreateNode(t.Context(), &storage.Node{ID: "y", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, tenant.CreateEdge(t.Context(), &storage.Edge{ID: "r", StartNode: "x", EndNode: "y", Type: "REL"}))
	tx, err := base.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("a"))
	wrapper := &transactionStorageWrapper{
		tx: tx, underlying: tenant, namespace: "a", separator: ":",
		mutatedNodeIDs: map[string]struct{}{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	reads := []struct {
		name string
		read func(ctx context.Context) (int, error)
	}{
		{
			name: "GetNodesByLabel",
			read: func(ctx context.Context) (int, error) {
				nodes, err := wrapper.GetNodesByLabel(ctx, "N")
				return len(nodes), err
			},
		},
		{
			name: "AllNodes",
			read: func(ctx context.Context) (int, error) {
				nodes, err := wrapper.AllNodes(ctx)
				return len(nodes), err
			},
		},
		{
			name: "GetEdgesByType",
			read: func(ctx context.Context) (int, error) {
				edges, err := wrapper.GetEdgesByType(ctx, "REL")
				return len(edges), err
			},
		},
		{
			name: "AllEdges",
			read: func(ctx context.Context) (int, error) {
				edges, err := wrapper.AllEdges(ctx)
				return len(edges), err
			},
		},
		{
			name: "GetOutgoingEdges",
			read: func(ctx context.Context) (int, error) {
				edges, err := wrapper.GetOutgoingEdges(ctx, "x")
				return len(edges), err
			},
		},
		{
			name: "GetIncomingEdges",
			read: func(ctx context.Context) (int, error) {
				edges, err := wrapper.GetIncomingEdges(ctx, "y")
				return len(edges), err
			},
		},
		{
			name: "GetEdgesBetween",
			read: func(ctx context.Context) (int, error) {
				edges, err := wrapper.GetEdgesBetween(ctx, "x", "y")
				return len(edges), err
			},
		},
		{
			name: "GetStoredOutgoingEdges",
			read: func(ctx context.Context) (int, error) {
				edges, err := wrapper.GetStoredOutgoingEdges(ctx, "x")
				return len(edges), err
			},
		},
		{
			name: "GetStoredIncomingEdges",
			read: func(ctx context.Context) (int, error) {
				edges, err := wrapper.GetStoredIncomingEdges(ctx, "y")
				return len(edges), err
			},
		},
		{
			name: "GetStoredNodesByLabel",
			read: func(ctx context.Context) (int, error) {
				nodes, err := wrapper.GetStoredNodesByLabel(ctx, "N")
				return len(nodes), err
			},
		},
	}
	for _, read := range reads {
		t.Run(read.name, func(t *testing.T) {
			count, err := read.read(ctx)
			require.ErrorIs(t, err, context.Canceled, "the caller's cancellation must reach the caller")
			require.Zero(t, count, "a canceled read returns no collection")
		})
	}
}

// A real caller CancelFunc fired by the read must also stop scopes which do
// not need any ID projection. No context-observation wrapper drives this test.
func TestTransactionWrapperVisibleAtCancellationAcrossScopes(t *testing.T) {
	reads := []struct {
		name string
		read func(context.Context, *transactionStorageWrapper) (int, error)
	}{
		{"label", func(ctx context.Context, w *transactionStorageWrapper) (int, error) {
			v, e := w.GetNodesByLabelVisibleAt(ctx, "Doc", storage.MVCCVersion{})
			return len(v), e
		}},
		{"outgoing", func(ctx context.Context, w *transactionStorageWrapper) (int, error) {
			v, e := w.GetOutgoingEdgesVisibleAt(ctx, "n1", storage.MVCCVersion{})
			return len(v), e
		}},
		{"incoming", func(ctx context.Context, w *transactionStorageWrapper) (int, error) {
			v, e := w.GetIncomingEdgesVisibleAt(ctx, "n2", storage.MVCCVersion{})
			return len(v), e
		}},
		{"type", func(ctx context.Context, w *transactionStorageWrapper) (int, error) {
			v, e := w.GetEdgesByTypeVisibleAt(ctx, "REL", storage.MVCCVersion{})
			return len(v), e
		}},
		{"between", func(ctx context.Context, w *transactionStorageWrapper) (int, error) {
			v, e := w.GetEdgesBetweenVisibleAt(ctx, "n1", "n2", storage.MVCCVersion{})
			return len(v), e
		}},
	}
	for _, scope := range []struct{ name, namespace, sourceNamespace string }{{"unscoped", "", ""}, {"projected", "tenant", ""}, {"already_scoped", "tenant", "tenant"}} {
		for _, read := range reads {
			t.Run(scope.name+"/"+read.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				engine := &recordingVisibilityEngine{MemoryEngine: newTestMemoryEngine(t), namespace: scope.sourceNamespace, cancel: cancel, nodes: []*storage.Node{{ID: "tenant:n1", Labels: []string{"Doc"}}}, edges: []*storage.Edge{{ID: "tenant:e1", StartNode: "tenant:n1", EndNode: "tenant:n2", Type: "REL"}}}
				wrapper := &transactionStorageWrapper{underlying: engine, namespace: scope.namespace, separator: ":"}
				count, err := read.read(ctx, wrapper)
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, count)
			})
		}
	}
}
