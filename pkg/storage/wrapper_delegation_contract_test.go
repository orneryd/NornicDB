package storage

// The wrapper delegation contract: every method on the Engine contract and on
// every optional capability interface exposed by the production wrapper stack
// (WAL, Async, Namespaced, Composite, size-tracking) MUST be handled — never
// answered with ErrNotImplemented when the wrapped engine supports the
// operation — and MUST be delegated to the inner engine. The spy at the bottom
// of each stack records every call, so a wrapper that stubs a method instead of
// delegating fails this test.
//
// Stacks under test:
//
//	badger                  (spy baseline)
//	wal                     WAL(spy)
//	async                   Async(spy)
//	wal+async               Async(WAL(spy))
//	namespaced+wal+async    Namespaced(Async(WAL(spy)), "tenant")
//
// A second stack instance per configuration covers Close, which tears the
// engine down.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// delegationContractEngine is the compile-time assertion that every wrapper
// stack root implements the full Engine contract plus every optional
// capability interface the production stack forwards. A stack root that drops
// a method fails this test at build time.
type delegationContractEngine interface {
	Engine
	EngineUnwrapper
	StreamingEngine
	PrefixStreamingEngine
	NodeWithoutEmbeddingsStreamer
	PrefixNodeWithoutEmbeddingsReader
	ProjectedPrefixNodeReader
	ProjectedNodeReader
	NodeIterator
	EdgesBetweenMatcher
	EmbeddingCountProvider
	EmbeddingSidecarUpdater
	NamespaceLister
	NamespaceSchemaProvider
	PrefixStatsEngine
	NamespaceLabelStatsProvider
	CountStoreReadEngine
	StartupMaintenanceStateEngine
	MVCCMaintenanceEngine
	TemporalMaintenanceEngine
	StorageEventNotifier
	MVCCLatestEffectiveEngine
	MVCCVisibilityEngine
	MVCCIndexedVisibilityEngine
	MVCCHeadEngine
	MVCCLifecycleEngine
	MVCCLifecycleScheduleEngine
	MVCCLifecycleDebtEngine
}

type delegationSpyEngine struct {
	*MemoryEngine

	mu       sync.Mutex
	calls    map[string]int
	lastOpts StreamNodesOptions
}

func newDelegationSpyEngine(t *testing.T) *delegationSpyEngine {
	t.Helper()
	base := newTestMemoryEngine(t)
	return &delegationSpyEngine{MemoryEngine: base, calls: map[string]int{}}
}

func (s *delegationSpyEngine) record(name string) {
	s.mu.Lock()
	s.calls[name]++
	s.mu.Unlock()
}

func (s *delegationSpyEngine) count(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[name]
}

// --- Engine contract ---------------------------------------------------------

func (s *delegationSpyEngine) CreateNode(ctx context.Context, node *Node) (NodeID, error) {
	s.record("CreateNode")
	return s.MemoryEngine.CreateNode(ctx, node)
}

func (s *delegationSpyEngine) GetNode(ctx context.Context, id NodeID) (*Node, error) {
	s.record("GetNode")
	return s.MemoryEngine.GetNode(ctx, id)
}

func (s *delegationSpyEngine) UpdateNode(ctx context.Context, updateRequest NodeUpdateRequest) error {
	s.record("UpdateNode")
	return s.MemoryEngine.UpdateNode(ctx, updateRequest)
}

func (s *delegationSpyEngine) DeleteNode(ctx context.Context, id NodeID) error {
	s.record("DeleteNode")
	return s.MemoryEngine.DeleteNode(ctx, id)
}

func (s *delegationSpyEngine) CreateEdge(ctx context.Context, edge *Edge) error {
	s.record("CreateEdge")
	return s.MemoryEngine.CreateEdge(ctx, edge)
}

func (s *delegationSpyEngine) GetEdge(ctx context.Context, id EdgeID) (*Edge, error) {
	s.record("GetEdge")
	return s.MemoryEngine.GetEdge(ctx, id)
}

func (s *delegationSpyEngine) UpdateEdge(ctx context.Context, updateRequest EdgeUpdateRequest) error {
	s.record("UpdateEdge")
	return s.MemoryEngine.UpdateEdge(ctx, updateRequest)
}

func (s *delegationSpyEngine) DeleteEdge(ctx context.Context, id EdgeID) error {
	s.record("DeleteEdge")
	return s.MemoryEngine.DeleteEdge(ctx, id)
}

func (s *delegationSpyEngine) GetNodesByLabel(ctx context.Context, label string) ([]*Node, error) {
	s.record("GetNodesByLabel")
	return s.MemoryEngine.GetNodesByLabel(ctx, label)
}

func (s *delegationSpyEngine) GetFirstNodeByLabel(ctx context.Context, label string) (*Node, error) {
	s.record("GetFirstNodeByLabel")
	return s.MemoryEngine.GetFirstNodeByLabel(ctx, label)
}

// The scoped label reads (ScopedLabelNodeReader, #851) are how the wrappers
// delegate the label reads above; they record under the same names.
func (s *delegationSpyEngine) GetNodesByLabelInScope(ctx context.Context, scope, label string) ([]*Node, error) {
	s.record("GetNodesByLabel")
	return s.MemoryEngine.GetNodesByLabelInScope(ctx, scope, label)
}

func (s *delegationSpyEngine) GetFirstNodeByLabelInScope(scope, label string) (*Node, error) {
	s.record("GetFirstNodeByLabel")
	return s.MemoryEngine.GetFirstNodeByLabelInScope(context.Background(), scope, label)
}

func (s *delegationSpyEngine) StreamNodesByLabelProjectedInScope(ctx context.Context, scope, label string, properties []string, visit func(*Node) error) error {
	s.record("StreamNodesByLabelProjected")
	return s.MemoryEngine.StreamNodesByLabelProjectedInScope(ctx, scope, label, properties, visit)
}

func (s *delegationSpyEngine) GetNodesByLabelVisibleAtInScope(ctx context.Context, scope, label string, version MVCCVersion) ([]*Node, error) {
	s.record("GetNodesByLabelVisibleAt")
	return s.MemoryEngine.GetNodesByLabelVisibleAtInScope(ctx, scope, label, version)
}

func (s *delegationSpyEngine) GetOutgoingEdges(ctx context.Context, nodeID NodeID) ([]*Edge, error) {
	s.record("GetOutgoingEdges")
	return s.MemoryEngine.GetOutgoingEdges(ctx, nodeID)
}

func (s *delegationSpyEngine) GetIncomingEdges(ctx context.Context, nodeID NodeID) ([]*Edge, error) {
	s.record("GetIncomingEdges")
	return s.MemoryEngine.GetIncomingEdges(ctx, nodeID)
}

func (s *delegationSpyEngine) GetEdgesBetween(ctx context.Context, startID, endID NodeID) ([]*Edge, error) {
	s.record("GetEdgesBetween")
	return s.MemoryEngine.GetEdgesBetween(ctx, startID, endID)
}

func (s *delegationSpyEngine) GetEdgeBetween(ctx context.Context, startID, endID NodeID, edgeType string) (*Edge, error) {
	s.record("GetEdgeBetween")
	return s.MemoryEngine.GetEdgeBetween(ctx, startID, endID, edgeType)
}

func (s *delegationSpyEngine) GetEdgesByType(ctx context.Context, edgeType string) ([]*Edge, error) {
	s.record("GetEdgesByType")
	return s.MemoryEngine.GetEdgesByType(ctx, edgeType)
}

func (s *delegationSpyEngine) AllNodes(ctx context.Context) ([]*Node, error) {
	s.record("AllNodes")
	return s.MemoryEngine.AllNodes(ctx)
}

func (s *delegationSpyEngine) AllEdges(ctx context.Context) ([]*Edge, error) {
	s.record("AllEdges")
	return s.MemoryEngine.AllEdges(ctx)
}

func (s *delegationSpyEngine) StreamNodesWithOptions(ctx context.Context, opts StreamNodesOptions, fn func(node *Node) error) error {
	s.record("StreamNodesWithOptions")
	s.mu.Lock()
	s.lastOpts = opts
	s.mu.Unlock()
	return s.MemoryEngine.StreamNodesWithOptions(ctx, opts, fn)
}

func (s *delegationSpyEngine) GetInDegree(ctx context.Context, nodeID NodeID) (int, error) {
	s.record("GetInDegree")
	return s.MemoryEngine.GetInDegree(ctx, nodeID)
}

func (s *delegationSpyEngine) GetOutDegree(ctx context.Context, nodeID NodeID) (int, error) {
	s.record("GetOutDegree")
	return s.MemoryEngine.GetOutDegree(ctx, nodeID)
}

func (s *delegationSpyEngine) GetSchema() *SchemaManager {
	s.record("GetSchema")
	return s.MemoryEngine.GetSchema()
}

func (s *delegationSpyEngine) BulkCreateNodes(ctx context.Context, nodes []*Node) error {
	s.record("BulkCreateNodes")
	return s.MemoryEngine.BulkCreateNodes(ctx, nodes)
}

func (s *delegationSpyEngine) BulkCreateEdges(ctx context.Context, edges []*Edge) error {
	s.record("BulkCreateEdges")
	return s.MemoryEngine.BulkCreateEdges(ctx, edges)
}

func (s *delegationSpyEngine) BulkDeleteNodes(ctx context.Context, ids []NodeID) error {
	s.record("BulkDeleteNodes")
	return s.MemoryEngine.BulkDeleteNodes(ctx, ids)
}

func (s *delegationSpyEngine) BulkDeleteEdges(ctx context.Context, ids []EdgeID) error {
	s.record("BulkDeleteEdges")
	return s.MemoryEngine.BulkDeleteEdges(ctx, ids)
}

func (s *delegationSpyEngine) BatchGetNodes(ctx context.Context, ids []NodeID) (map[NodeID]*Node, error) {
	s.record("BatchGetNodes")
	return s.MemoryEngine.BatchGetNodes(ctx, ids)
}

func (s *delegationSpyEngine) Close() error {
	s.record("Close")
	return s.MemoryEngine.Close()
}

func (s *delegationSpyEngine) NodeCount(ctx context.Context) (int64, error) {
	s.record("NodeCount")
	return s.MemoryEngine.NodeCount(ctx)
}

func (s *delegationSpyEngine) EdgeCount(ctx context.Context) (int64, error) {
	s.record("EdgeCount")
	return s.MemoryEngine.EdgeCount(ctx)
}

func (s *delegationSpyEngine) DeleteByPrefix(prefix string) (int64, int64, error) {
	s.record("DeleteByPrefix")
	return s.MemoryEngine.DeleteByPrefix(prefix)
}

// --- Optional capabilities ---------------------------------------------------

func (s *delegationSpyEngine) MatchEdgesBetween(ctx context.Context, startID, endID NodeID, edgeType string, properties []string, match func(*Edge) bool) ([]*Edge, error) {
	s.record("MatchEdgesBetween")
	return s.MemoryEngine.MatchEdgesBetween(ctx, startID, endID, edgeType, properties, match)
}

func (s *delegationSpyEngine) GetNodeProjected(ctx context.Context, id NodeID, properties []string) (*Node, error) {
	s.record("GetNodeProjected")
	return s.MemoryEngine.GetNodeProjected(ctx, id, properties)
}

func (s *delegationSpyEngine) IterateNodes(fn func(*Node) bool) error {
	s.record("IterateNodes")
	return s.MemoryEngine.IterateNodes(fn)
}

func (s *delegationSpyEngine) PendingEmbeddingsCount() int {
	s.record("PendingEmbeddingsCount")
	return s.MemoryEngine.PendingEmbeddingsCount()
}

func (s *delegationSpyEngine) UpdateNodeEmbeddingSidecar(ctx context.Context, node *Node) error {
	s.record("UpdateNodeEmbeddingSidecar")
	return s.MemoryEngine.UpdateNodeEmbeddingSidecar(ctx, node)
}

func (s *delegationSpyEngine) ListNamespaces() []string {
	s.record("ListNamespaces")
	return s.MemoryEngine.ListNamespaces()
}

func (s *delegationSpyEngine) GetSchemaForNamespace(namespace string) *SchemaManager {
	s.record("GetSchemaForNamespace")
	return s.MemoryEngine.GetSchemaForNamespace(namespace)
}

func (s *delegationSpyEngine) NodeCountByPrefix(ctx context.Context, prefix string) (int64, error) {
	s.record("NodeCountByPrefix")
	return s.MemoryEngine.NodeCountByPrefix(ctx, prefix)
}

func (s *delegationSpyEngine) EdgeCountByPrefix(ctx context.Context, prefix string) (int64, error) {
	s.record("EdgeCountByPrefix")
	return s.MemoryEngine.EdgeCountByPrefix(ctx, prefix)
}

func (s *delegationSpyEngine) NodeCountByLabelInNamespace(ctx context.Context, namespace, label string) (int64, error) {
	s.record("NodeCountByLabelInNamespace")
	return s.MemoryEngine.NodeCountByLabelInNamespace(ctx, namespace, label)
}

func (s *delegationSpyEngine) ReadCountStore(ctx context.Context, request CountStoreReadRequest) (CountStoreRead, error) {
	s.record("ReadCountStore")
	return s.MemoryEngine.ReadCountStore(ctx, request)
}

func (s *delegationSpyEngine) ConsumeCleanShutdownMarker(ctx context.Context) (bool, error) {
	s.record("ConsumeCleanShutdownMarker")
	return s.MemoryEngine.ConsumeCleanShutdownMarker(ctx)
}

func (s *delegationSpyEngine) MarkCleanShutdown(ctx context.Context) error {
	s.record("MarkCleanShutdown")
	return s.MemoryEngine.MarkCleanShutdown(ctx)
}

func (s *delegationSpyEngine) RebuildMVCCHeads(ctx context.Context) error {
	s.record("RebuildMVCCHeads")
	return s.MemoryEngine.RebuildMVCCHeads(ctx)
}

func (s *delegationSpyEngine) PruneMVCCVersions(ctx context.Context, opts MVCCPruneOptions) (int64, error) {
	s.record("PruneMVCCVersions")
	return s.MemoryEngine.PruneMVCCVersions(ctx, opts)
}

func (s *delegationSpyEngine) RebuildTemporalIndexes(ctx context.Context) error {
	s.record("RebuildTemporalIndexes")
	return s.MemoryEngine.RebuildTemporalIndexes(ctx)
}

func (s *delegationSpyEngine) PruneTemporalHistory(ctx context.Context, opts TemporalPruneOptions) (int64, error) {
	s.record("PruneTemporalHistory")
	return s.MemoryEngine.PruneTemporalHistory(ctx, opts)
}

func (s *delegationSpyEngine) OnNodeCreated(callback NodeEventCallback) {
	s.record("OnNodeCreated")
	s.MemoryEngine.OnNodeCreated(callback)
}

func (s *delegationSpyEngine) OnNodeUpdated(callback NodeEventCallback) {
	s.record("OnNodeUpdated")
	s.MemoryEngine.OnNodeUpdated(callback)
}

func (s *delegationSpyEngine) OnNodeDeleted(callback NodeDeleteCallback) {
	s.record("OnNodeDeleted")
	s.MemoryEngine.OnNodeDeleted(callback)
}

func (s *delegationSpyEngine) OnEdgeCreated(callback EdgeEventCallback) {
	s.record("OnEdgeCreated")
	s.MemoryEngine.OnEdgeCreated(callback)
}

func (s *delegationSpyEngine) OnEdgeUpdated(callback EdgeEventCallback) {
	s.record("OnEdgeUpdated")
	s.MemoryEngine.OnEdgeUpdated(callback)
}

func (s *delegationSpyEngine) OnEdgeDeleted(callback EdgeDeleteCallback) {
	s.record("OnEdgeDeleted")
	s.MemoryEngine.OnEdgeDeleted(callback)
}

func (s *delegationSpyEngine) GetNodeLatestEffective(ctx context.Context, id NodeID) (*Node, error) {
	s.record("GetNodeLatestEffective")
	return s.MemoryEngine.GetNode(context.Background(), id)
}

func (s *delegationSpyEngine) GetEdgeLatestEffective(ctx context.Context, id EdgeID) (*Edge, error) {
	s.record("GetEdgeLatestEffective")
	return s.MemoryEngine.GetEdge(context.Background(), id)
}

func (s *delegationSpyEngine) GetNodeLatestVisible(ctx context.Context, id NodeID) (*Node, error) {
	s.record("GetNodeLatestVisible")
	return s.MemoryEngine.GetNodeLatestVisible(ctx, id)
}

func (s *delegationSpyEngine) GetEdgeLatestVisible(ctx context.Context, id EdgeID) (*Edge, error) {
	s.record("GetEdgeLatestVisible")
	return s.MemoryEngine.GetEdgeLatestVisible(ctx, id)
}

func (s *delegationSpyEngine) GetNodeVisibleAt(ctx context.Context, id NodeID, version MVCCVersion) (*Node, error) {
	s.record("GetNodeVisibleAt")
	return s.MemoryEngine.GetNodeVisibleAt(ctx, id, version)
}

func (s *delegationSpyEngine) GetEdgeVisibleAt(ctx context.Context, id EdgeID, version MVCCVersion) (*Edge, error) {
	s.record("GetEdgeVisibleAt")
	return s.MemoryEngine.GetEdgeVisibleAt(ctx, id, version)
}

func (s *delegationSpyEngine) GetNodesByLabelVisibleAt(ctx context.Context, label string, version MVCCVersion) ([]*Node, error) {
	s.record("GetNodesByLabelVisibleAt")
	return s.MemoryEngine.GetNodesByLabelVisibleAt(ctx, label, version)
}

func (s *delegationSpyEngine) GetOutgoingEdgesVisibleAt(ctx context.Context, nodeID NodeID, version MVCCVersion) ([]*Edge, error) {
	s.record("GetOutgoingEdgesVisibleAt")
	return s.MemoryEngine.GetOutgoingEdgesVisibleAt(ctx, nodeID, version)
}

func (s *delegationSpyEngine) GetIncomingEdgesVisibleAt(ctx context.Context, nodeID NodeID, version MVCCVersion) ([]*Edge, error) {
	s.record("GetIncomingEdgesVisibleAt")
	return s.MemoryEngine.GetIncomingEdgesVisibleAt(ctx, nodeID, version)
}

func (s *delegationSpyEngine) GetEdgesByTypeVisibleAt(ctx context.Context, edgeType string, version MVCCVersion) ([]*Edge, error) {
	s.record("GetEdgesByTypeVisibleAt")
	return s.MemoryEngine.GetEdgesByTypeVisibleAt(ctx, edgeType, version)
}

func (s *delegationSpyEngine) GetEdgesBetweenVisibleAt(ctx context.Context, startID, endID NodeID, version MVCCVersion) ([]*Edge, error) {
	s.record("GetEdgesBetweenVisibleAt")
	return s.MemoryEngine.GetEdgesBetweenVisibleAt(ctx, startID, endID, version)
}

func (s *delegationSpyEngine) RegisterSnapshotReader(info SnapshotReaderInfo) func() {
	s.record("RegisterSnapshotReader")
	return s.MemoryEngine.RegisterSnapshotReader(info)
}

func (s *delegationSpyEngine) LifecycleStatus() map[string]interface{} {
	s.record("LifecycleStatus")
	return s.MemoryEngine.LifecycleStatus()
}

func (s *delegationSpyEngine) TriggerPruneNow(ctx context.Context) error {
	s.record("TriggerPruneNow")
	return s.MemoryEngine.TriggerPruneNow(ctx)
}

func (s *delegationSpyEngine) PauseLifecycle() {
	s.record("PauseLifecycle")
	s.MemoryEngine.PauseLifecycle()
}

func (s *delegationSpyEngine) ResumeLifecycle() {
	s.record("ResumeLifecycle")
	s.MemoryEngine.ResumeLifecycle()
}

func (s *delegationSpyEngine) SetLifecycleSchedule(interval time.Duration) error {
	s.record("SetLifecycleSchedule")
	return s.MemoryEngine.SetLifecycleSchedule(interval)
}

func (s *delegationSpyEngine) TopLifecycleDebtKeys(limit int) []MVCCLifecycleDebtKey {
	s.record("TopLifecycleDebtKeys")
	return s.MemoryEngine.TopLifecycleDebtKeys(limit)
}

func (s *delegationSpyEngine) GetNodeCurrentHead(ctx context.Context, id NodeID) (MVCCHead, error) {
	s.record("GetNodeCurrentHead")
	return s.MemoryEngine.GetNodeCurrentHead(ctx, id)
}

func (s *delegationSpyEngine) GetEdgeCurrentHead(ctx context.Context, id EdgeID) (MVCCHead, error) {
	s.record("GetEdgeCurrentHead")
	return s.MemoryEngine.GetEdgeCurrentHead(ctx, id)
}

func (s *delegationSpyEngine) StreamNodes(ctx context.Context, fn func(*Node) error) error {
	s.record("StreamNodes")
	return s.MemoryEngine.StreamNodes(ctx, fn)
}

func (s *delegationSpyEngine) StreamEdges(ctx context.Context, fn func(*Edge) error) error {
	s.record("StreamEdges")
	return s.MemoryEngine.StreamEdges(ctx, fn)
}

func (s *delegationSpyEngine) StreamNodeChunks(ctx context.Context, chunkSize int, fn func([]*Node) error) error {
	s.record("StreamNodeChunks")
	return s.MemoryEngine.StreamNodeChunks(ctx, chunkSize, fn)
}

func (s *delegationSpyEngine) StreamNodesWithoutEmbeddings(ctx context.Context, fn func(*Node) error) error {
	s.record("StreamNodesWithoutEmbeddings")
	return s.MemoryEngine.StreamNodesWithoutEmbeddings(ctx, fn)
}

func (s *delegationSpyEngine) StreamNodesByPrefix(ctx context.Context, prefix string, fn func(*Node) error) error {
	s.record("StreamNodesByPrefix")
	return s.MemoryEngine.StreamNodesByPrefix(ctx, prefix, fn)
}

func (s *delegationSpyEngine) StreamNodesByPrefixProjected(ctx context.Context, prefix string, properties []string, fn func(*Node) error) error {
	s.record("StreamNodesByPrefixProjected")
	return s.MemoryEngine.StreamNodesByPrefixProjected(ctx, prefix, properties, fn)
}

func (s *delegationSpyEngine) StreamNodesByPrefixWithoutEmbeddings(ctx context.Context, prefix string, fn func(*Node) error) error {
	s.record("StreamNodesByPrefixWithoutEmbeddings")
	return s.MemoryEngine.StreamNodesByPrefixWithoutEmbeddings(ctx, prefix, fn)
}

func (s *delegationSpyEngine) Flush() error {
	s.record("Flush")
	return nil
}

func (s *delegationSpyEngine) GetInnerEngine() Engine {
	s.record("GetInnerEngine")
	return s.MemoryEngine
}

// --- Contract batteries ------------------------------------------------------

func newWrapperDelegationStack(t *testing.T, name string, spy *delegationSpyEngine) delegationContractEngine {
	t.Helper()
	switch name {
	case "badger":
		return spy
	case "wal":
		walLog, err := NewWAL(t.TempDir(), &WALConfig{SyncMode: "none"})
		require.NoError(t, err)
		t.Cleanup(func() { _ = walLog.Close() })
		return NewWALEngine(spy, walLog)
	case "namespaced+wal":
		walLog, err := NewWAL(t.TempDir(), &WALConfig{SyncMode: "none"})
		require.NoError(t, err)
		t.Cleanup(func() { _ = walLog.Close() })
		inner := NewWALEngine(spy, walLog)
		return NewNamespacedEngine(inner, "tenant")
	default:
		t.Fatalf("unknown stack %q", name)
		return nil
	}
}

func flushDelegationStack(t *testing.T, engine Engine) {
	t.Helper()
	for {
		if flusher, ok := engine.(interface{ Flush() error }); ok {
			require.NoError(t, flusher.Flush())
			return
		}
		if unwrapper, ok := engine.(EngineUnwrapper); ok {
			engine = unwrapper.GetInnerEngine()
			continue
		}
		return
	}
}

func assertNoNotImplemented(t *testing.T, stack, method string, err error) {
	t.Helper()
	require.NotErrorIsf(t, err, ErrNotImplemented, "%s.%s must delegate, not return ErrNotImplemented", stack, method)
}

func TestWrapperDelegationContract_EngineMethodsAlwaysDelegate(t *testing.T) {
	for _, stack := range []string{"badger", "wal", "namespaced+wal"} {
		t.Run(stack, func(t *testing.T) {
			spy := newDelegationSpyEngine(t)
			engine := newWrapperDelegationStack(t, stack, spy)
			ctx := context.Background()
			id := func(s string) NodeID { return NodeID("test:" + s) }
			eid := func(s string) EdgeID { return EdgeID("test:" + s) }
			delPrefix := "test:del:"

			// Seed graph: n1 -KNOWS-> n2, plus disposable records.
			_, err := engine.CreateNode(t.Context(), &Node{ID: id("n1"), Labels: []string{"Person"}, Properties: map[string]any{"name": "alice"}, ChunkEmbeddings: [][]float32{{0.1, 0.2}}})
			require.NoError(t, err)
			_, err = engine.CreateNode(t.Context(), &Node{ID: id("n2"), Labels: []string{"Person"}})
			require.NoError(t, err)
			require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: eid("e1"), StartNode: id("n1"), EndNode: id("n2"), Type: "KNOWS"}))
			_, err = engine.CreateNode(t.Context(), &Node{ID: id("temp"), Labels: []string{"Temp"}})
			require.NoError(t, err)
			require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: eid("temp:e"), StartNode: id("n1"), EndNode: id("temp"), Type: "TEMP"}))
			flushDelegationStack(t, engine)

			check := func(method string, err error) {
				t.Helper()
				assertNoNotImplemented(t, stack, method, err)
				require.GreaterOrEqualf(t, spy.count(method), 1, "%s.%s was not delegated to the inner engine", stack, method)
			}

			_, err = engine.GetNode(t.Context(), id("n1"))
			check("GetNode", err)
			_, err = engine.GetNodesByLabel(ctx, "Person")
			check("GetNodesByLabel", err)
			_, err = engine.GetFirstNodeByLabel(t.Context(), "Person")
			check("GetFirstNodeByLabel", err)
			_, err = engine.GetOutgoingEdges(ctx, id("n1"))
			check("GetOutgoingEdges", err)
			_, err = engine.GetIncomingEdges(ctx, id("n2"))
			check("GetIncomingEdges", err)
			_, err = engine.GetEdgesBetween(ctx, id("n1"), id("n2"))
			check("GetEdgesBetween", err)
			matched, err := engine.MatchEdgesBetween(context.Background(), id("n1"), id("n2"), "KNOWS", []string{}, func(*Edge) bool { return true })
			check("MatchEdgesBetween", err)
			require.Lenf(t, matched, 1, "%s.MatchEdgesBetween must find the seeded edge", stack)
			edge, err := engine.GetEdgeBetween(t.Context(), id("n1"), id("n2"), "KNOWS")
			require.NoError(t, err)
			if stack == "badger" || stack == "wal" {
				check("GetEdgeBetween", nil)
			} else {
				// AsyncEngine composes the lookup over its merged edge overlay;
				// assert the handled result instead of inner delegation.
				require.NotNilf(t, edge, "%s.GetEdgeBetween must resolve the seeded edge", stack)
			}
			_, err = engine.GetEdgesByType(ctx, "KNOWS")
			check("GetEdgesByType", err)
			_, err = engine.AllNodes(ctx)
			check("AllNodes", err)
			_, err = engine.AllEdges(ctx)
			check("AllEdges", err)
			_, err = engine.GetEdge(t.Context(), eid("e1"))
			check("GetEdge", err)

			var streamed int
			err = engine.StreamNodesWithOptions(ctx, StreamNodesOptions{WithEmbeddings: true, ApplyDecayFilter: true}, func(node *Node) error {
				streamed++
				return nil
			})
			check("StreamNodesWithOptions", err)
			require.Positive(t, streamed)

			inDegree, err := engine.GetInDegree(t.Context(), id("n2"))
			require.NoError(t, err)
			outDegree, err := engine.GetOutDegree(t.Context(), id("n1"))
			require.NoError(t, err)
			if stack == "badger" || stack == "wal" {
				check("GetInDegree", nil)
				check("GetOutDegree", nil)
			} else {
				// AsyncEngine composes degree reads over its merged adjacency
				// overlay; assert the handled results.
				require.Equal(t, 1, inDegree, "%s.GetInDegree must count the seeded edge", stack)
				require.Equal(t, 2, outDegree, "%s.GetOutDegree must count both seeded edges", stack)
			}
			schema := engine.GetSchema()
			if stack == "namespaced+wal" {
				// NamespacedEngine scopes the schema per namespace (Neo4j
				// per-database semantics) via GetSchemaForNamespace.
				require.NotNil(t, schema, "%s.GetSchema must return a schema manager", stack)
			} else {
				check("GetSchema", nil)
			}
			_, err = engine.BatchGetNodes(ctx, []NodeID{id("n1"), id("n2")})
			check("BatchGetNodes", err)
			_, err = engine.NodeCount(t.Context())
			if stack == "namespaced+wal" {
				// NamespacedEngine scopes NodeCount through the prefix-count
				// capability instead of a full inner count.
				assertNoNotImplemented(t, stack, "NodeCount", err)
				require.GreaterOrEqual(t, spy.count("NodeCountByPrefix"), 1, "%s.NodeCount must delegate through the prefix-count capability", stack)
			} else {
				check("NodeCount", err)
			}
			_, err = engine.EdgeCount(t.Context())
			if stack == "namespaced+wal" {
				assertNoNotImplemented(t, stack, "EdgeCount", err)
				require.GreaterOrEqual(t, spy.count("EdgeCountByPrefix"), 1, "%s.EdgeCount must delegate through the prefix-count capability", stack)
			} else {
				check("EdgeCount", err)
			}

			// Mutations through the wrapper must reach the inner engine.
			// AsyncEngine stages writes and flushes them as bulk operations, so
			// on async-bearing stacks the contract is verified by observable
			// state after flush; synchronous stacks assert exact delegation.
			require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: id("n1"), Labels: []string{"Person"}, Properties: map[string]any{"name": "alice2"}}}))
			require.NoError(t, engine.UpdateEdge(t.Context(), EdgeUpdateRequest{After: &Edge{ID: eid("e1"), StartNode: id("n1"), EndNode: id("n2"), Type: "KNOWS"}}))
			require.NoError(t, engine.BulkCreateNodes(t.Context(), []*Node{{ID: id("b1")}, {ID: id("b2")}}))
			require.NoError(t, engine.BulkCreateEdges(t.Context(), []*Edge{{ID: eid("b:e"), StartNode: id("b1"), EndNode: id("b2"), Type: "BULK"}}))
			require.NoError(t, engine.BulkDeleteNodes(t.Context(), []NodeID{id("b1"), id("b2")}))
			require.NoError(t, engine.BulkDeleteEdges(t.Context(), []EdgeID{eid("b:e")}))
			require.NoError(t, engine.DeleteEdge(t.Context(), eid("temp:e")))
			require.NoError(t, engine.DeleteNode(t.Context(), id("temp")))
			flushDelegationStack(t, engine)

			if stack == "badger" || stack == "wal" {
				for _, method := range []string{"UpdateNode", "UpdateEdge", "BulkCreateNodes", "BulkCreateEdges", "BulkDeleteNodes", "BulkDeleteEdges", "DeleteNode", "DeleteEdge"} {
					check(method, nil)
				}
			} else {
				updated, err := engine.GetNode(t.Context(), id("n1"))
				require.NoError(t, err)
				require.Equal(t, "alice2", updated.Properties["name"], "%s.UpdateNode must persist through the stack", stack)
				edge, err := engine.GetEdgeBetween(t.Context(), id("n1"), id("n2"), "KNOWS")
				require.NoError(t, err)
				require.NotNil(t, edge, "%s.UpdateEdge must persist through the stack", stack)
				_, err = engine.GetNode(t.Context(), id("b1"))
				require.ErrorIs(t, err, ErrNotFound, "%s.BulkDeleteNodes must delete through the stack", stack)
				_, err = engine.GetEdge(t.Context(), eid("b:e"))
				require.ErrorIs(t, err, ErrNotFound, "%s.BulkDeleteEdges must delete through the stack", stack)
				_, err = engine.GetNode(t.Context(), id("temp"))
				require.ErrorIs(t, err, ErrNotFound, "%s.DeleteNode must delete through the stack", stack)
				_, err = engine.GetEdge(t.Context(), eid("temp:e"))
				require.ErrorIs(t, err, ErrNotFound, "%s.DeleteEdge must delete through the stack", stack)
			}

			_, err = engine.CreateNode(t.Context(), &Node{ID: NodeID(delPrefix + "one")})
			require.NoError(t, err)
			require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: EdgeID(delPrefix + "e"), StartNode: NodeID(delPrefix + "one"), EndNode: id("n1"), Type: "DEL"}))
			flushDelegationStack(t, engine)
			nodesDeleted, edgesDeleted, err := engine.DeleteByPrefix(delPrefix)
			check("DeleteByPrefix", err)
			require.GreaterOrEqual(t, nodesDeleted, int64(1), "%s.DeleteByPrefix must delete its scoped records", stack)
			require.GreaterOrEqual(t, edgesDeleted, int64(1), "%s.DeleteByPrefix must delete its scoped edges", stack)
		})
	}
}

func TestWrapperDelegationContract_CapabilityMethodsAlwaysDelegate(t *testing.T) {
	for _, stack := range []string{"badger", "wal", "namespaced+wal"} {
		t.Run(stack, func(t *testing.T) {
			spy := newDelegationSpyEngine(t)
			engine := newWrapperDelegationStack(t, stack, spy)
			ctx := context.Background()
			id := func(s string) NodeID { return NodeID("test:" + s) }
			eid := func(s string) EdgeID { return EdgeID("test:" + s) }
			namespace := "test"
			if stack == "namespaced+wal" {
				namespace = "tenant"
			}

			_, err := engine.CreateNode(t.Context(), &Node{ID: id("n1"), Labels: []string{"Person"}, Properties: map[string]any{"name": "alice"}, ChunkEmbeddings: [][]float32{{0.1, 0.2}}})
			require.NoError(t, err)
			_, err = engine.CreateNode(t.Context(), &Node{ID: id("n2"), Labels: []string{"Person"}})
			require.NoError(t, err)
			require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: eid("e1"), StartNode: id("n1"), EndNode: id("n2"), Type: "KNOWS"}))
			flushDelegationStack(t, engine)

			check := func(method string, err error) {
				t.Helper()
				assertNoNotImplemented(t, stack, method, err)
				require.GreaterOrEqualf(t, spy.count(method), 1, "%s.%s was not delegated to the inner engine", stack, method)
			}

			_, err = engine.GetNodeProjected(t.Context(), id("n1"), []string{"name"})
			check("GetNodeProjected", err)
			require.NoError(t, engine.IterateNodes(func(node *Node) bool { return true }))
			check("IterateNodes", nil)
			_ = engine.PendingEmbeddingsCount()
			check("PendingEmbeddingsCount", nil)
			// The sidecar writeback never stages a node body: it lands in the
			// embedding key space immediately and must be visible through the
			// stack (a flush is still run to settle async staging of the create).
			require.NoError(t, engine.UpdateNodeEmbeddingSidecar(context.Background(), embeddingWriteback(t, engine, id("n1"), [][]float32{{0.3, 0.4}}, map[string]any{"chunk_count": 1}, time.Time{})))
			assertNoNotImplemented(t, stack, "UpdateNodeEmbeddingSidecar", nil)
			flushDelegationStack(t, engine)
			updated, err := engine.GetNode(t.Context(), id("n1"))
			require.NoError(t, err)
			require.NotNil(t, updated)
			require.NotEmptyf(t, updated.ChunkEmbeddings, "%s.UpdateNodeEmbeddingSidecar must persist embeddings through the stack", stack)
			_ = engine.ListNamespaces()
			check("ListNamespaces", nil)
			_ = engine.GetSchemaForNamespace("tenant")
			check("GetSchemaForNamespace", nil)
			_, err = engine.NodeCountByPrefix(context.Background(), "")
			check("NodeCountByPrefix", err)
			_, err = engine.EdgeCountByPrefix(context.Background(), "")
			check("EdgeCountByPrefix", err)
			_, err = engine.NodeCountByLabelInNamespace(t.Context(), namespace, "Person")
			if stack == "badger" || stack == "wal" {
				check("NodeCountByLabelInNamespace", err)
			} else {
				// AsyncEngine composes the namespace-scoped label count over its
				// pending-write overlay; assert the handled result instead of
				// inner delegation.
				assertNoNotImplemented(t, stack, "NodeCountByLabelInNamespace", err)
				got, gerr := engine.NodeCountByLabelInNamespace(t.Context(), namespace, "Person")
				require.NoError(t, gerr)
				require.GreaterOrEqual(t, got, int64(1), "%s.NodeCountByLabelInNamespace must count seeded nodes", stack)
			}

			answer, readErr := engine.ReadCountStore(ctx, CountStoreReadRequest{Kind: CountStoreNodesByLabel, Namespace: namespace, Name: "Person", HiddenLabelPrefix: "_"})
			check("ReadCountStore", readErr)
			require.True(t, answer.DescribesRows)
			require.GreaterOrEqual(t, answer.Count, int64(2))
			_, err = engine.ConsumeCleanShutdownMarker(ctx)
			check("ConsumeCleanShutdownMarker", err)
			require.NoError(t, engine.MarkCleanShutdown(ctx))
			check("MarkCleanShutdown", nil)
			require.NoError(t, engine.RebuildMVCCHeads(ctx))
			check("RebuildMVCCHeads", nil)
			_, err = engine.PruneMVCCVersions(ctx, MVCCPruneOptions{})
			check("PruneMVCCVersions", err)
			require.NoError(t, engine.RebuildTemporalIndexes(ctx))
			check("RebuildTemporalIndexes", nil)
			_, err = engine.PruneTemporalHistory(ctx, TemporalPruneOptions{})
			check("PruneTemporalHistory", err)

			var nodeEvents int32
			engine.OnNodeCreated(func(*Node) { atomic.AddInt32(&nodeEvents, 1) })
			engine.OnNodeUpdated(func(*Node) {})
			engine.OnNodeDeleted(func(NodeID) {})
			engine.OnEdgeCreated(func(*Edge) {})
			engine.OnEdgeUpdated(func(*Edge) {})
			engine.OnEdgeDeleted(func(EdgeID) {})
			for _, method := range []string{"OnNodeCreated", "OnNodeUpdated", "OnNodeDeleted", "OnEdgeCreated", "OnEdgeUpdated", "OnEdgeDeleted"} {
				check(method, nil)
			}
			// Functional contract: a write through the stack must reach the
			// registered callback.
			_, err = engine.CreateNode(t.Context(), &Node{ID: id("cb:fire")})
			require.NoError(t, err)
			flushDelegationStack(t, engine)
			require.GreaterOrEqual(t, atomic.LoadInt32(&nodeEvents), int32(1), "%s.OnNodeCreated callback must fire for writes through the stack", stack)

			version := MVCCVersion{}
			_, err = engine.GetNodeLatestEffective(context.Background(), id("n1"))
			check("GetNodeLatestEffective", err)
			_, err = engine.GetEdgeLatestEffective(context.Background(), eid("e1"))
			check("GetEdgeLatestEffective", err)
			_, err = engine.GetNodeLatestVisible(t.Context(), id("n1"))
			check("GetNodeLatestVisible", err)
			_, err = engine.GetEdgeLatestVisible(t.Context(), eid("e1"))
			check("GetEdgeLatestVisible", err)
			_, err = engine.GetNodeVisibleAt(t.Context(), id("n1"), version)
			check("GetNodeVisibleAt", err)
			_, err = engine.GetEdgeVisibleAt(t.Context(), eid("e1"), version)
			check("GetEdgeVisibleAt", err)
			_, err = engine.GetNodesByLabelVisibleAt(ctx, "Person", version)
			check("GetNodesByLabelVisibleAt", err)
			_, err = engine.GetOutgoingEdgesVisibleAt(ctx, id("n1"), version)
			check("GetOutgoingEdgesVisibleAt", err)
			_, err = engine.GetIncomingEdgesVisibleAt(ctx, id("n2"), version)
			check("GetIncomingEdgesVisibleAt", err)
			_, err = engine.GetEdgesByTypeVisibleAt(ctx, "KNOWS", version)
			check("GetEdgesByTypeVisibleAt", err)
			_, err = engine.GetEdgesBetweenVisibleAt(ctx, id("n1"), id("n2"), version)
			check("GetEdgesBetweenVisibleAt", err)
			_, err = engine.GetNodeCurrentHead(context.Background(), id("n1"))
			check("GetNodeCurrentHead", err)
			_, err = engine.GetEdgeCurrentHead(context.Background(), eid("e1"))
			check("GetEdgeCurrentHead", err)

			release := engine.RegisterSnapshotReader(SnapshotReaderInfo{ReaderID: "contract"})
			check("RegisterSnapshotReader", nil)
			release()
			_ = engine.LifecycleStatus()
			check("LifecycleStatus", nil)
			require.NoError(t, engine.TriggerPruneNow(ctx))
			check("TriggerPruneNow", nil)
			engine.PauseLifecycle()
			check("PauseLifecycle", nil)
			engine.ResumeLifecycle()
			check("ResumeLifecycle", nil)
			require.NoError(t, engine.SetLifecycleSchedule(time.Minute))
			check("SetLifecycleSchedule", nil)
			_ = engine.TopLifecycleDebtKeys(2)
			check("TopLifecycleDebtKeys", nil)

			// Legacy stream methods are thin wrappers over the unified
			// StreamNodesWithOptions kernel: on the wrapper stacks the kernel
			// is the delegation surface the spy records.
			checkLegacyStream := func(method string, err error) {
				t.Helper()
				assertNoNotImplemented(t, stack, method, err)
				require.GreaterOrEqualf(t, spy.count(method)+spy.count("StreamNodesWithOptions"), 1,
					"%s.%s must route to the inner engine through the streaming kernel", stack, method)
			}

			var legacyStreamed int
			require.NoError(t, engine.StreamNodes(ctx, func(node *Node) error { legacyStreamed++; return nil }))
			checkLegacyStream("StreamNodes", nil)
			require.NoError(t, engine.StreamEdges(ctx, func(edge *Edge) error { return nil }))
			check("StreamEdges", nil)
			require.NoError(t, engine.StreamNodesByPrefix(ctx, "", func(node *Node) error { return nil }))
			checkLegacyStream("StreamNodesByPrefix", nil)
			require.NoError(t, engine.StreamNodesByPrefixProjected(ctx, "", []string{"name"}, func(node *Node) error { return nil }))
			checkLegacyStream("StreamNodesByPrefixProjected", nil)
			require.NoError(t, engine.StreamNodesByPrefixWithoutEmbeddings(ctx, "", func(node *Node) error { return nil }))
			checkLegacyStream("StreamNodesByPrefixWithoutEmbeddings", nil)
			require.NoError(t, engine.StreamNodesWithoutEmbeddings(ctx, func(node *Node) error { return nil }))
			checkLegacyStream("StreamNodesWithoutEmbeddings", nil)
			require.NoError(t, engine.StreamNodeChunks(ctx, 2, func(nodes []*Node) error { return nil }))
			checkLegacyStream("StreamNodeChunks", nil)
			require.Positive(t, legacyStreamed)
		})
	}
}

func TestWrapperDelegationContract_CloseIsHandled(t *testing.T) {
	for _, stack := range []string{"badger", "wal", "namespaced+wal"} {
		t.Run(stack, func(t *testing.T) {
			spy := newDelegationSpyEngine(t)
			engine := newWrapperDelegationStack(t, stack, spy)
			assertNoNotImplemented(t, stack, "Close", engine.Close())
			if stack == "namespaced+wal" {
				// NamespacedEngine deliberately does not close its inner engine:
				// it is shared across namespaces and the DatabaseManager owns it.
				return
			}
			require.GreaterOrEqual(t, spy.count("Close"), 1, "%s.Close was not delegated to the inner engine", stack)
		})
	}
}
