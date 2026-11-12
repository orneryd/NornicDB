package storage

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

// compositeCancelProbe is one constituent of a composite merge read that ends
// the caller's context while it is still returning its own rows. The
// cancellation is the caller's real context.CancelFunc, so the composite sees
// exactly the context.Canceled a live caller produces when it stops waiting.
type compositeCancelProbe struct {
	Engine
	cancel context.CancelFunc
	nodes  []*Node
	edges  []*Edge

	mu    sync.Mutex
	reads int
}

func (p *compositeCancelProbe) beginRead() {
	p.mu.Lock()
	p.reads++
	p.mu.Unlock()
	p.cancel()
}

func (p *compositeCancelProbe) readCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.reads
}

func (p *compositeCancelProbe) AllNodes(context.Context) ([]*Node, error) {
	p.beginRead()
	return p.nodes, nil
}

func (p *compositeCancelProbe) AllEdges(context.Context) ([]*Edge, error) {
	p.beginRead()
	return p.edges, nil
}

func (p *compositeCancelProbe) GetNodesByLabel(context.Context, string) ([]*Node, error) {
	p.beginRead()
	return p.nodes, nil
}

func (p *compositeCancelProbe) GetEdgesByType(context.Context, string) ([]*Edge, error) {
	p.beginRead()
	return p.edges, nil
}

func (p *compositeCancelProbe) ReadAdjacentEdges(context.Context, AdjacentEdgeRead) ([]*Edge, error) {
	p.beginRead()
	return p.edges, nil
}

func (p *compositeCancelProbe) GetNodesByLabelVisibleAt(context.Context, string, MVCCVersion) ([]*Node, error) {
	p.beginRead()
	return p.nodes, nil
}

func (p *compositeCancelProbe) ReadAdjacentEdgesVisibleAt(context.Context, AdjacentEdgeRead, MVCCVersion) ([]*Edge, error) {
	p.beginRead()
	return p.edges, nil
}

func (p *compositeCancelProbe) GetEdgesByTypeVisibleAt(context.Context, string, MVCCVersion) ([]*Edge, error) {
	p.beginRead()
	return p.edges, nil
}

var compositeCancelProbeAliases = []string{"source-a", "source-b", "source-c"}

func newCompositeCancelProbes(t *testing.T, cancel context.CancelFunc) map[string]*compositeCancelProbe {
	t.Helper()
	probes := make(map[string]*compositeCancelProbe, len(compositeCancelProbeAliases))
	for _, alias := range compositeCancelProbeAliases {
		node := &Node{ID: NodeID(prefixTestID("cancel-node-" + alias)), Labels: []string{"Doc"}}
		edge := &Edge{ID: EdgeID(prefixTestID("cancel-edge-" + alias)), StartNode: node.ID, EndNode: node.ID, Type: "REL"}
		probes[alias] = &compositeCancelProbe{
			Engine: newTestMemoryEngine(t),
			cancel: cancel,
			nodes:  []*Node{node},
			edges:  []*Edge{edge},
		}
	}
	return probes
}

func newCompositeReadOnly(constituents map[string]Engine) *CompositeEngine {
	names := make(map[string]string, len(constituents))
	modes := make(map[string]string, len(constituents))
	for alias := range constituents {
		names[alias] = alias
		modes[alias] = "read"
	}
	return NewCompositeEngine(constituents, names, modes)
}

func compositeCancelProbeConstituents(probes map[string]*compositeCancelProbe) map[string]Engine {
	constituents := make(map[string]Engine, len(probes))
	for alias, probe := range probes {
		constituents[alias] = probe
	}
	return constituents
}

// TestCompositeMergeReadsStopOnCallerCancellation reproduces a caller that
// cancels while the composite is still walking constituents: the walk must stop
// before reading another source and must not publish the rows it already
// collected as a successful result.
func TestCompositeMergeReadsStopOnCallerCancellation(t *testing.T) {
	tests := []struct {
		name string
		read func(composite *CompositeEngine, ctx context.Context) (int, error)
	}{
		{name: "all nodes", read: func(composite *CompositeEngine, ctx context.Context) (int, error) {
			nodes, err := composite.AllNodes(ctx)
			return len(nodes), err
		}},
		{name: "all edges", read: func(composite *CompositeEngine, ctx context.Context) (int, error) {
			edges, err := composite.AllEdges(ctx)
			return len(edges), err
		}},
		{name: "nodes by label", read: func(composite *CompositeEngine, ctx context.Context) (int, error) {
			nodes, err := composite.GetNodesByLabel(ctx, "Doc")
			return len(nodes), err
		}},
		{name: "edges by type", read: func(composite *CompositeEngine, ctx context.Context) (int, error) {
			edges, err := composite.GetEdgesByType(ctx, "REL")
			return len(edges), err
		}},
		{name: "outgoing edges", read: func(composite *CompositeEngine, ctx context.Context) (int, error) {
			edges, err := composite.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Node: "cancel-node"})
			return len(edges), err
		}},
		{name: "incoming edges", read: func(composite *CompositeEngine, ctx context.Context) (int, error) {
			edges, err := composite.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentIncoming, Node: "cancel-node"})
			return len(edges), err
		}},
		{name: "edges between", read: func(composite *CompositeEngine, ctx context.Context) (int, error) {
			edges, err := composite.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentBetween, Node: "cancel-start", Other: "cancel-end"})
			return len(edges), err
		}},
		{name: "qualified edges between", read: func(composite *CompositeEngine, ctx context.Context) (int, error) {
			edges, err := composite.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentBetween, Node: NodeID(SourceNodeElementID("source-a", "cancel-start")), Other: "cancel-end"})
			return len(edges), err
		}},
		{name: "qualified edges between visible at", read: func(composite *CompositeEngine, ctx context.Context) (int, error) {
			edges, err := composite.ReadAdjacentEdgesVisibleAt(ctx, AdjacentEdgeRead{Direction: AdjacentBetween, Node: NodeID(SourceNodeElementID("source-a", "cancel-start")), Other: "cancel-end"}, MVCCVersion{})
			return len(edges), err
		}},
		{name: "nodes by label visible at", read: func(composite *CompositeEngine, ctx context.Context) (int, error) {
			nodes, err := composite.GetNodesByLabelVisibleAt(ctx, "Doc", MVCCVersion{})
			return len(nodes), err
		}},
		{name: "edges by type visible at", read: func(composite *CompositeEngine, ctx context.Context) (int, error) {
			edges, err := composite.GetEdgesByTypeVisibleAt(ctx, "REL", MVCCVersion{})
			return len(edges), err
		}},
		{name: "outgoing edges visible at", read: func(composite *CompositeEngine, ctx context.Context) (int, error) {
			edges, err := composite.ReadAdjacentEdgesVisibleAt(ctx, AdjacentEdgeRead{Direction: AdjacentOutgoing, Node: "cancel-node"}, MVCCVersion{})
			return len(edges), err
		}},
		{name: "incoming edges visible at", read: func(composite *CompositeEngine, ctx context.Context) (int, error) {
			edges, err := composite.ReadAdjacentEdgesVisibleAt(ctx, AdjacentEdgeRead{Direction: AdjacentIncoming, Node: "cancel-node"}, MVCCVersion{})
			return len(edges), err
		}},
		{name: "edges between visible at", read: func(composite *CompositeEngine, ctx context.Context) (int, error) {
			edges, err := composite.ReadAdjacentEdgesVisibleAt(ctx, AdjacentEdgeRead{Direction: AdjacentBetween, Node: "cancel-start", Other: "cancel-end"}, MVCCVersion{})
			return len(edges), err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			probes := newCompositeCancelProbes(t, cancel)
			composite := newCompositeReadOnly(compositeCancelProbeConstituents(probes))

			published, err := test.read(composite, ctx)

			require.ErrorIs(t, err, context.Canceled, "a cancelled caller receives its own context error")
			require.Zero(t, published, "a cancelled caller receives no partial collection")
			require.Equal(t, 1, compositeCancelProbeReadTotal(probes), "the walk stops at the source that ended the caller")
		})
	}
}

func compositeCancelProbeReadTotal(probes map[string]*compositeCancelProbe) int {
	total := 0
	for _, probe := range probes {
		total += probe.readCount()
	}
	return total
}

// compositeStreamCancelProbe streams two rows and ends the caller's context
// after the first one. It deliberately ignores the caller context, like a
// constituent that does not observe cancellation itself, so the composite owns
// stopping the walk and withholding the second row.
type compositeStreamCancelProbe struct {
	*MemoryEngine
	cancel context.CancelFunc
	nodes  []*Node
	edges  []*Edge

	mu      sync.Mutex
	streams int
}

func (p *compositeStreamCancelProbe) beginStream() {
	p.mu.Lock()
	p.streams++
	p.mu.Unlock()
	p.cancel()
}

func (p *compositeStreamCancelProbe) streamCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.streams
}

func (p *compositeStreamCancelProbe) StreamNodes(_ context.Context, visit func(*Node) error) error {
	for index, node := range p.nodes {
		if index == 1 {
			p.beginStream()
		}
		if err := visit(node); err != nil {
			return err
		}
	}
	return nil
}

func (p *compositeStreamCancelProbe) StreamEdges(_ context.Context, visit func(*Edge) error) error {
	for index, edge := range p.edges {
		if index == 1 {
			p.beginStream()
		}
		if err := visit(edge); err != nil {
			return err
		}
	}
	return nil
}

func newCompositeStreamCancelProbes(t *testing.T, cancel context.CancelFunc) map[string]*compositeStreamCancelProbe {
	t.Helper()
	probes := make(map[string]*compositeStreamCancelProbe, len(compositeCancelProbeAliases))
	for _, alias := range compositeCancelProbeAliases {
		node := &Node{ID: NodeID(prefixTestID("stream-cancel-node-" + alias)), Labels: []string{"Doc"}}
		edge := &Edge{ID: EdgeID(prefixTestID("stream-cancel-edge-" + alias)), StartNode: node.ID, EndNode: node.ID, Type: "REL"}
		probes[alias] = &compositeStreamCancelProbe{
			MemoryEngine: newTestMemoryEngine(t),
			cancel:       cancel,
			nodes:        []*Node{node, {ID: NodeID(prefixTestID("stream-cancel-second-" + alias)), Labels: []string{"Doc"}}},
			edges:        []*Edge{edge, {ID: EdgeID(prefixTestID("stream-cancel-second-edge-" + alias)), StartNode: node.ID, EndNode: node.ID, Type: "REL"}},
		}
	}
	return probes
}

func compositeStreamCancelProbeConstituents(probes map[string]*compositeStreamCancelProbe) map[string]Engine {
	constituents := make(map[string]Engine, len(probes))
	for alias, probe := range probes {
		constituents[alias] = probe
	}
	return constituents
}

func compositeStreamCancelProbeStreamTotal(probes map[string]*compositeStreamCancelProbe) int {
	total := 0
	for _, probe := range probes {
		total += probe.streamCount()
	}
	return total
}

// TestCompositeStreamsStopOnCallerCancellation reproduces a caller that cancels
// between two streamed rows: no row after the cancellation may be published and
// the remaining constituents must not be walked.
func TestCompositeStreamsStopOnCallerCancellation(t *testing.T) {
	t.Run("stream nodes", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		probes := newCompositeStreamCancelProbes(t, cancel)
		composite := newCompositeReadOnly(compositeStreamCancelProbeConstituents(probes))

		var visited []NodeID
		err := composite.StreamNodes(ctx, func(node *Node) error {
			visited = append(visited, node.ID)
			return nil
		})

		require.ErrorIs(t, err, context.Canceled)
		consulted := compositeSingleConsultedStreamProbe(t, probes)
		require.Equal(t, []NodeID{probes[consulted].nodes[0].ID}, visited, "only the row that preceded the cancellation is published")
	})

	t.Run("stream edges", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		probes := newCompositeStreamCancelProbes(t, cancel)
		composite := newCompositeReadOnly(compositeStreamCancelProbeConstituents(probes))

		var visited []EdgeID
		err := composite.StreamEdges(ctx, func(edge *Edge) error {
			visited = append(visited, edge.ID)
			return nil
		})

		require.ErrorIs(t, err, context.Canceled)
		consulted := compositeSingleConsultedStreamProbe(t, probes)
		require.Equal(t, []EdgeID{probes[consulted].edges[0].ID}, visited, "only the row that preceded the cancellation is published")
	})

	t.Run("stream fallback", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		probes := newCompositeCancelProbes(t, cancel)
		composite := newCompositeReadOnly(compositeCancelProbeConstituents(probes))

		visited := 0
		err := composite.StreamNodes(ctx, func(*Node) error {
			visited++
			return nil
		})

		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, visited, "a materialised fallback publishes nothing after the cancellation")
		require.Equal(t, 1, compositeCancelProbeReadTotal(probes), "the walk stops at the source that ended the caller")
	})

	t.Run("pre-cancelled caller", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		probes := newCompositeStreamCancelProbes(t, cancel)
		composite := newCompositeReadOnly(compositeStreamCancelProbeConstituents(probes))

		visited := 0
		err := composite.StreamNodes(ctx, func(*Node) error {
			visited++
			return nil
		})

		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, visited)
		require.Zero(t, compositeStreamCancelProbeStreamTotal(probes), "no source is read for an already cancelled caller")
	})

	t.Run("expired deadline", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		t.Cleanup(cancel)
		probes := newCompositeStreamCancelProbes(t, cancel)
		composite := newCompositeReadOnly(compositeStreamCancelProbeConstituents(probes))

		visited := 0
		err := composite.StreamNodes(ctx, func(*Node) error {
			visited++
			return nil
		})

		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Zero(t, visited)
		require.Zero(t, compositeStreamCancelProbeStreamTotal(probes), "no source is read for an expired deadline")
	})
}

// compositeSingleConsultedStreamProbe asserts exactly one source was walked and
// returns its alias, so the published row can be matched to its source.
func compositeSingleConsultedStreamProbe(t *testing.T, probes map[string]*compositeStreamCancelProbe) string {
	t.Helper()
	consulted := ""
	for alias, probe := range probes {
		if probe.streamCount() == 0 {
			continue
		}
		require.Equal(t, 1, probe.streamCount(), "source %s is walked once", alias)
		require.Empty(t, consulted, "only one source is walked after the caller cancels")
		consulted = alias
	}
	require.NotEmpty(t, consulted)
	return consulted
}

// compositeFailingReadProbe fails every composite fan-out read with one shared
// cause so the error representation each operation publishes stays observable.
type compositeFailingReadProbe struct {
	Engine
	err error
}

func (p *compositeFailingReadProbe) AllNodes(context.Context) ([]*Node, error) { return nil, p.err }
func (p *compositeFailingReadProbe) AllEdges(context.Context) ([]*Edge, error) { return nil, p.err }

func (p *compositeFailingReadProbe) GetNodesByLabel(context.Context, string) ([]*Node, error) {
	return nil, p.err
}

func (p *compositeFailingReadProbe) GetEdgesByType(context.Context, string) ([]*Edge, error) {
	return nil, p.err
}

func (p *compositeFailingReadProbe) ReadAdjacentEdges(context.Context, AdjacentEdgeRead) ([]*Edge, error) {
	return nil, p.err
}

func (p *compositeFailingReadProbe) GetNodesByLabelVisibleAt(context.Context, string, MVCCVersion) ([]*Node, error) {
	return nil, p.err
}

func (p *compositeFailingReadProbe) ReadAdjacentEdgesVisibleAt(context.Context, AdjacentEdgeRead, MVCCVersion) ([]*Edge, error) {
	return nil, p.err
}

func (p *compositeFailingReadProbe) GetEdgesByTypeVisibleAt(context.Context, string, MVCCVersion) ([]*Edge, error) {
	return nil, p.err
}

// TestCompositeFanOutPreservesConstituentErrorRepresentation pins the two error
// contracts the converged fan-out must keep distinct: ordinary engine reads
// publish a localized constituent error, while GetNodesByLabel and the MVCC
// indexed-visibility merges publish the constituent error unchanged.
func TestCompositeFanOutPreservesConstituentErrorRepresentation(t *testing.T) {
	const alias = "source-a"

	localized := []struct {
		name string
		read func(composite *CompositeEngine) error
	}{
		{name: "all nodes", read: func(composite *CompositeEngine) error {
			_, err := composite.AllNodes(context.Background())
			return err
		}},
		{name: "all edges", read: func(composite *CompositeEngine) error {
			_, err := composite.AllEdges(context.Background())
			return err
		}},
		{name: "edges by type", read: func(composite *CompositeEngine) error {
			_, err := composite.GetEdgesByType(context.Background(), "REL")
			return err
		}},
		{name: "outgoing edges", read: func(composite *CompositeEngine) error {
			_, err := composite.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Node: "node-1"})
			return err
		}},
		{name: "incoming edges", read: func(composite *CompositeEngine) error {
			_, err := composite.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentIncoming, Node: "node-1"})
			return err
		}},
		{name: "edges between", read: func(composite *CompositeEngine) error {
			_, err := composite.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentBetween, Node: "node-1", Other: "node-2"})
			return err
		}},
	}

	unchanged := []struct {
		name string
		read func(composite *CompositeEngine) error
	}{
		{name: "nodes by label", read: func(composite *CompositeEngine) error {
			_, err := composite.GetNodesByLabel(context.Background(), "Doc")
			return err
		}},
		{name: "nodes by label visible at", read: func(composite *CompositeEngine) error {
			_, err := composite.GetNodesByLabelVisibleAt(context.Background(), "Doc", MVCCVersion{})
			return err
		}},
		{name: "edges by type visible at", read: func(composite *CompositeEngine) error {
			_, err := composite.GetEdgesByTypeVisibleAt(context.Background(), "REL", MVCCVersion{})
			return err
		}},
		{name: "outgoing edges visible at", read: func(composite *CompositeEngine) error {
			_, err := composite.ReadAdjacentEdgesVisibleAt(context.Background(), AdjacentEdgeRead{Direction: AdjacentOutgoing, Node: "node-1"}, MVCCVersion{})
			return err
		}},
		{name: "incoming edges visible at", read: func(composite *CompositeEngine) error {
			_, err := composite.ReadAdjacentEdgesVisibleAt(context.Background(), AdjacentEdgeRead{Direction: AdjacentIncoming, Node: "node-1"}, MVCCVersion{})
			return err
		}},
		{name: "edges between visible at", read: func(composite *CompositeEngine) error {
			_, err := composite.ReadAdjacentEdgesVisibleAt(context.Background(), AdjacentEdgeRead{Direction: AdjacentBetween, Node: "node-1", Other: "node-2"}, MVCCVersion{})
			return err
		}},
	}

	for _, test := range localized {
		t.Run(test.name+" localizes", func(t *testing.T) {
			readErr := errors.New(test.name + " failed")
			composite := newCompositeReadOnly(map[string]Engine{alias: &compositeFailingReadProbe{Engine: newTestMemoryEngine(t), err: readErr}})

			err := test.read(composite)

			var localizedErr *localization.LocalizedError
			require.ErrorAs(t, err, &localizedErr)
			require.Equal(t, localization.MessageStorageCompositeConstituentQueryFailed, localizedErr.Message.ID)
			require.Equal(t, alias, localizedErr.Message.Data["Alias"])
			require.ErrorIs(t, err, readErr)
		})
	}

	for _, test := range unchanged {
		t.Run(test.name+" keeps the constituent error", func(t *testing.T) {
			readErr := errors.New(test.name + " failed")
			composite := newCompositeReadOnly(map[string]Engine{alias: &compositeFailingReadProbe{Engine: newTestMemoryEngine(t), err: readErr}})

			err := test.read(composite)

			require.Equal(t, readErr, err)
			var localizedErr *localization.LocalizedError
			require.False(t, errors.As(err, &localizedErr))
		})
	}
}

// compositePairCountProbe counts node-pair reads served by its constituent so a
// qualified pair can be shown to route to its own source only.
type compositePairCountProbe struct {
	Engine

	mu        sync.Mutex
	pairReads int
}

func (p *compositePairCountProbe) ReadAdjacentEdges(ctx context.Context, request AdjacentEdgeRead) ([]*Edge, error) {
	if request.Direction == AdjacentBetween {
		p.mu.Lock()
		p.pairReads++
		p.mu.Unlock()
	}
	return p.Engine.ReadAdjacentEdges(ctx, request)
}

func (p *compositePairCountProbe) readCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pairReads
}

// TestCompositeFanOutPreservesSourceIdentityAndPairRouting pins source identity
// across the converged fan-outs: equal local IDs stay distinct per constituent,
// every published row carries its source database, and a qualified node pair
// reaches only its own constituent.
func TestCompositeFanOutPreservesSourceIdentityAndPairRouting(t *testing.T) {
	const same, target = NodeID("raw:same"), NodeID("raw:target")
	const relation = EdgeID("raw:same-rel")

	engines := make(map[string]Engine, 2)
	counted := make(map[string]*compositePairCountProbe, 2)
	for _, database := range []string{"one", "two"} {
		engine := newTestMemoryEngine(t)
		for _, id := range []NodeID{same, target} {
			_, err := engine.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"db": database}})
			require.NoError(t, err)
		}
		require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: relation, StartNode: same, EndNode: target, Type: "REL"}))
		engines[database] = engine
		counted[database] = &compositePairCountProbe{Engine: engine}
	}

	composite := newCompositeReadOnly(engines)
	ctx := context.Background()

	nodes, err := composite.AllNodes(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"one", "one", "two", "two"}, nodeDatabases(nodes))
	require.Equal(t, map[string][]NodeID{"one": {same, target}, "two": {same, target}}, nodeIDsByDatabase(nodes))

	edges, err := composite.AllEdges(ctx)
	require.NoError(t, err)
	require.Len(t, edges, 2)
	require.ElementsMatch(t, []string{"one", "two"}, edgeDatabases(edges))

	streamedNodes := make(map[string][]NodeID)
	require.NoError(t, composite.StreamNodes(ctx, func(node *Node) error {
		database := node.RuntimeMetadata().Database
		streamedNodes[database] = append(streamedNodes[database], node.ID)
		return nil
	}))
	require.Equal(t, map[string][]NodeID{"one": {same, target}, "two": {same, target}}, streamedNodes)

	streamedEdges := make(map[string]EdgeID)
	require.NoError(t, composite.StreamEdges(ctx, func(edge *Edge) error {
		streamedEdges[edge.Database] = edge.ID
		return nil
	}))
	require.Equal(t, map[string]EdgeID{"one": relation, "two": relation}, streamedEdges)

	routed := newCompositeReadOnly(map[string]Engine{"one": counted["one"], "two": counted["two"]})
	routedEdges, err := routed.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentBetween, Node: NodeID("4:one:raw:same"), Other: NodeID("4:one:raw:target")})
	require.NoError(t, err)
	require.Len(t, routedEdges, 1)
	require.Equal(t, "one", routedEdges[0].Database)
	require.Equal(t, 1, counted["one"].readCount(), "the qualified pair reaches its own constituent")
	require.Zero(t, counted["two"].readCount(), "a qualified pair never fans out to another constituent")

	crossSource, err := routed.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentBetween, Node: NodeID("4:one:raw:same"), Other: NodeID("4:two:raw:target")})
	require.NoError(t, err)
	require.Empty(t, crossSource)
	require.Equal(t, 1, counted["one"].readCount())
	require.Zero(t, counted["two"].readCount())
}

func nodeDatabases(nodes []*Node) []string {
	databases := make([]string, 0, len(nodes))
	for _, node := range nodes {
		databases = append(databases, node.RuntimeMetadata().Database)
	}
	return databases
}

func nodeIDsByDatabase(nodes []*Node) map[string][]NodeID {
	ids := make(map[string][]NodeID, len(nodes))
	for _, node := range nodes {
		database := node.RuntimeMetadata().Database
		ids[database] = append(ids[database], node.ID)
	}
	for database, values := range ids {
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		ids[database] = values
	}
	return ids
}

func edgeDatabases(edges []*Edge) []string {
	databases := make([]string, 0, len(edges))
	for _, edge := range edges {
		databases = append(databases, edge.Database)
	}
	return databases
}

// TestCompositeFanOutPreservesEmptyResults pins the nil/empty result shapes the
// converged fan-out must keep: label scans return an allocated empty slice for
// an empty composite, the other merges return nil.
func TestCompositeFanOutPreservesEmptyResults(t *testing.T) {
	empty := NewCompositeEngine(nil, nil, nil)
	ctx := context.Background()

	labels, err := empty.GetNodesByLabel(ctx, "Doc")
	require.NoError(t, err)
	require.NotNil(t, labels, "an empty composite publishes an allocated empty label scan")
	require.Empty(t, labels)

	for name, read := range map[string]func() (int, error){
		"all nodes": func() (int, error) {
			nodes, err := empty.AllNodes(ctx)
			return len(nodes), err
		},
		"all edges": func() (int, error) {
			edges, err := empty.AllEdges(ctx)
			return len(edges), err
		},
		"edges by type": func() (int, error) {
			edges, err := empty.GetEdgesByType(ctx, "REL")
			return len(edges), err
		},
		"incoming edges": func() (int, error) {
			edges, err := empty.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentIncoming, Node: "node-1"})
			return len(edges), err
		},
		"outgoing edges": func() (int, error) {
			edges, err := empty.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Node: "node-1"})
			return len(edges), err
		},
		"edges between": func() (int, error) {
			edges, err := empty.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentBetween, Node: "node-1", Other: "node-2"})
			return len(edges), err
		},
		"nodes by label visible at": func() (int, error) {
			nodes, err := empty.GetNodesByLabelVisibleAt(ctx, "Doc", MVCCVersion{})
			return len(nodes), err
		},
	} {
		t.Run(name, func(t *testing.T) {
			length, err := read()
			require.NoError(t, err)
			require.Zero(t, length)
		})
	}

	visited := 0
	require.NoError(t, empty.StreamNodes(ctx, func(*Node) error {
		visited++
		return nil
	}))
	require.Zero(t, visited)
}

// compositeProjectedProbe counts projected visits so the projected preflight is
// observable: every source's capability must be validated before the first
// visitor runs.
type compositeProjectedProbe struct {
	*MemoryEngine

	mu       sync.Mutex
	visits   int
	streamed int
}

func (p *compositeProjectedProbe) StreamNodesByLabelProjected(ctx context.Context, label string, properties []string, visit func(*Node) error) error {
	p.mu.Lock()
	p.streamed++
	p.mu.Unlock()
	return p.MemoryEngine.StreamNodesByLabelProjected(ctx, label, properties, func(node *Node) error {
		p.mu.Lock()
		p.visits++
		p.mu.Unlock()
		return visit(node)
	})
}

// TestCompositeProjectedStreamRejectsUnsupportedSourceBeforeFirstVisit keeps the
// existing ErrNotImplemented contract: an unsupported constituent is refused by
// the preflight, before any supported source visits a node.
func TestCompositeStreamNodesByLabelProjectedRejectsUnsupportedSourceBeforeFirstVisit(t *testing.T) {
	supported := &compositeProjectedProbe{MemoryEngine: newTestMemoryEngine(t)}
	_, err := supported.MemoryEngine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("projected-unsupported")), Labels: []string{"Doc"}})
	require.NoError(t, err)
	unsupported := struct{ Engine }{newTestMemoryEngine(t)}

	composite := newCompositeReadOnly(map[string]Engine{"supported": supported, "unsupported": unsupported})
	visited := 0
	err = composite.StreamNodesByLabelProjected(context.Background(), "Doc", nil, func(*Node) error {
		visited++
		return nil
	})

	require.ErrorIs(t, err, ErrNotImplemented)
	require.Zero(t, visited, "the caller's visitor never runs")
	require.Zero(t, supported.streamed, "the supported source is not streamed before the preflight completes")
}

func (p *compositeStreamCancelProbe) StreamNodeChunks(_ context.Context, _ int, visit func([]*Node) error) error {
	p.beginStream()
	return visit(p.nodes)
}

func TestCompositeChunkReadDoesNotPublishAfterSourceCancellation(t *testing.T) {
	for _, streamed := range []bool{false, true} {
		name := "materialized"
		if streamed {
			name = "streamed"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			var composite *CompositeEngine
			var reads func() int
			if streamed {
				probes := newCompositeStreamCancelProbes(t, cancel)
				composite = newCompositeReadOnly(compositeStreamCancelProbeConstituents(probes))
				reads = func() int { return compositeStreamCancelProbeStreamTotal(probes) }
			} else {
				probes := newCompositeCancelProbes(t, cancel)
				composite = newCompositeReadOnly(compositeCancelProbeConstituents(probes))
				reads = func() int { return compositeCancelProbeReadTotal(probes) }
			}
			callbacks := 0
			err := composite.StreamNodeChunks(ctx, 256, func([]*Node) error { callbacks++; return nil })
			require.ErrorIs(t, err, context.Canceled)
			require.Zero(t, callbacks)
			require.Equal(t, 1, reads())
		})
	}
}

func TestCompositeSourceProjectionPreservesOwnership(t *testing.T) {
	ctx := context.Background()
	t.Run("nodes", func(t *testing.T) {
		unbound := &Node{ID: "unbound"}
		bound := nodeFromDatabase(&Node{ID: "bound"}, "existing")
		input := []*Node{nil, bound, unbound}
		result, err := entitiesFromDatabase(ctx, input, "source")
		require.NoError(t, err)
		require.Len(t, result, 3)
		require.Nil(t, result[0])
		require.Same(t, bound, result[1])
		require.NotSame(t, unbound, result[2])
		require.Empty(t, unbound.RuntimeMetadata().Database)
		require.Equal(t, "source", result[2].RuntimeMetadata().Database)
		require.Equal(t, "existing", bound.RuntimeMetadata().Database)
		require.Same(t, unbound, input[2])
		again, err := entitiesFromDatabase(ctx, result, "other")
		require.NoError(t, err)
		require.Same(t, &result[0], &again[0], "already-bound slices retain their backing storage")
	})
	t.Run("edges", func(t *testing.T) {
		unbound := &Edge{ID: "unbound"}
		bound := &Edge{ID: "bound", Database: "existing"}
		input := []*Edge{nil, bound, unbound}
		result, err := entitiesFromDatabase(ctx, input, "source")
		require.NoError(t, err)
		require.Len(t, result, 3)
		require.Nil(t, result[0])
		require.Same(t, bound, result[1])
		require.NotSame(t, unbound, result[2])
		require.Empty(t, unbound.Database)
		require.Equal(t, "source", result[2].Database)
		require.Equal(t, "existing", bound.Database)
		require.Same(t, unbound, input[2])
		again, err := entitiesFromDatabase(ctx, result, "other")
		require.NoError(t, err)
		require.Same(t, &result[0], &again[0], "already-bound slices retain their backing storage")
	})
}
