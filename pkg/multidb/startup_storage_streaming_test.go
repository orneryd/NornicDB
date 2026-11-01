package multidb

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

var errStartupMaterialization = errors.New("startup must not materialize the full corpus")

// streamStoredEntities is the streaming kernel the startup engine's node and
// edge streams share: read the entities for ctx, then visit each accepted
// entity, stopping on context cancellation or a visitor error. accept reports
// whether an entity is visited and carries the per-stream bookkeeping; nil
// accepts every entity.
func streamStoredEntities[T any](ctx context.Context, read func(context.Context) ([]T, error), accept func(T) bool, visit func(T) error) error {
	entities, err := read(ctx)
	if err != nil {
		return err
	}
	for _, entity := range entities {
		if err := ctx.Err(); err != nil {
			return err
		}
		if accept != nil && !accept(entity) {
			continue
		}
		if err := visit(entity); err != nil {
			return err
		}
	}
	return nil
}

type startupStreamingEngine struct {
	storage.Engine
	storage.StoredEntityReader
	storage.ScopedStoredEntityReader
	MemoryEngine                         *storage.MemoryEngine
	allNodesCalls                        int
	allEdgesCalls                        int
	nodeStreamCalls                      int
	edgeStreamCalls                      int
	prefixes                             []string
	prefixCallbackRuns                   int
	snapshotNodeScans, snapshotEdgeScans int
}

func (e *startupStreamingEngine) GetInnerEngine() storage.Engine { return e.MemoryEngine }

func (e *startupStreamingEngine) WithStoredEntityRead(kind storage.StoredEntityReadKind, _ string, read func() error) error {
	switch kind {
	case storage.StoredEntityNodeSnapshotScan:
		e.snapshotNodeScans++
	case storage.StoredEntityEdgeSnapshotScan:
		e.snapshotEdgeScans++
	}
	return read()
}

func (e *startupStreamingEngine) AllNodes(ctx context.Context) ([]*storage.Node, error) {
	e.allNodesCalls++
	return nil, errStartupMaterialization
}

func (e *startupStreamingEngine) AllEdges(ctx context.Context) ([]*storage.Edge, error) {
	e.allEdgesCalls++
	return nil, errStartupMaterialization
}

func (e *startupStreamingEngine) StreamNodes(ctx context.Context, visit func(*storage.Node) error) error {
	e.nodeStreamCalls++
	return streamStoredEntities(ctx, e.MemoryEngine.AllNodes, nil, visit)
}

func (e *startupStreamingEngine) StreamEdges(ctx context.Context, visit func(*storage.Edge) error) error {
	e.edgeStreamCalls++
	return streamStoredEntities(ctx, e.MemoryEngine.AllEdges, nil, visit)
}

func (e *startupStreamingEngine) StreamNodeChunks(ctx context.Context, chunkSize int, visit func([]*storage.Node) error) error {
	if chunkSize <= 0 {
		chunkSize = 1
	}
	var chunk []*storage.Node
	err := e.StreamNodes(ctx, func(node *storage.Node) error {
		chunk = append(chunk, node)
		if len(chunk) < chunkSize {
			return nil
		}
		if err := visit(chunk); err != nil {
			return err
		}
		chunk = chunk[:0]
		return nil
	})
	if err != nil || len(chunk) == 0 {
		return err
	}
	return visit(chunk)
}

func (e *startupStreamingEngine) StreamNodesByPrefix(ctx context.Context, prefix string, visit func(*storage.Node) error) error {
	e.prefixes = append(e.prefixes, prefix)
	return streamStoredEntities(ctx, e.MemoryEngine.AllNodes, func(node *storage.Node) bool {
		if !strings.HasPrefix(string(node.ID), prefix) {
			return false
		}
		e.prefixCallbackRuns++
		return true
	}, visit)
}

// StreamNodesWithOptions models the unified kernel explicitly so the embedded
// MemoryEngine's promoted implementation does not shadow the prefix recording.
func (e *startupStreamingEngine) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	// An unprefixed stream is a whole-store node stream, as StreamNodes is
	// (storage sizing streams stored nodes this way).
	prefix := opts.Prefix
	if prefix == "" {
		e.nodeStreamCalls++
	} else {
		e.prefixes = append(e.prefixes, prefix)
	}
	return streamStoredEntities(ctx, e.MemoryEngine.AllNodes, func(node *storage.Node) bool {
		if prefix != "" && !strings.HasPrefix(string(node.ID), prefix) {
			return false
		}
		e.prefixCallbackRuns++
		return true
	}, visit)
}

// Regression: initially reported in #373.
func TestStartupCleanupScansOnlyLeakedSystemNodePrefixes(t *testing.T) {
	engine := &startupStreamingEngine{MemoryEngine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))}
	engine.Engine = engine.MemoryEngine
	engine.StoredEntityReader = engine.MemoryEngine
	engine.ScopedStoredEntityReader = engine.MemoryEngine
	t.Cleanup(func() { _ = engine.Close() })

	nodes := []*storage.Node{
		{ID: "nornic:ordinary", Labels: []string{"Application"}, Properties: map[string]interface{}{"payload": strings.Repeat("x", 4096)}},
		{ID: "nornic:system:databases:metadata", Labels: []string{"_System"}},
		{ID: "nornic:system:migration:legacy", Labels: []string{"_System"}},
		{ID: "nornic:system:user:admin", Labels: []string{"_System"}},
		{ID: "nornic:system:user:application", Labels: []string{"Application"}},
		{ID: "nornic:system:other", Labels: []string{"_System"}},
	}
	for _, node := range nodes {
		_, err := engine.MemoryEngine.CreateNode(node)
		require.NoError(t, err)
	}

	manager := &DatabaseManager{
		inner: engine,
		databases: map[string]*DatabaseInfo{
			"nornic": {Name: "nornic", Type: "standard"},
			"system": {Name: "system", Type: "system"},
		},
		config: DefaultConfig(),
	}
	manager.cleanupLeakedSystemNodes()

	require.Zero(t, engine.allNodesCalls)
	require.Equal(t, []string{
		"nornic:system:databases:metadata",
		"nornic:system:migration:",
		"nornic:system:user:",
	}, engine.prefixes)
	require.Equal(t, 4, engine.prefixCallbackRuns)

	for _, id := range []storage.NodeID{
		"nornic:system:databases:metadata",
		"nornic:system:migration:legacy",
		"nornic:system:user:admin",
	} {
		_, err := engine.MemoryEngine.GetNode(id)
		require.Error(t, err)
	}
	for _, id := range []storage.NodeID{
		"nornic:ordinary",
		"nornic:system:user:application",
		"nornic:system:other",
	} {
		_, err := engine.MemoryEngine.GetNode(id)
		require.NoError(t, err)
	}
}

func TestStorageSizeReconciliationStreamsEntities(t *testing.T) {
	engine := &startupStreamingEngine{MemoryEngine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))}
	engine.Engine = engine.MemoryEngine
	engine.StoredEntityReader = engine.MemoryEngine
	engine.ScopedStoredEntityReader = engine.MemoryEngine
	t.Cleanup(func() { _ = engine.Close() })
	node := &storage.Node{ID: "nornic:node", Labels: []string{"Application"}, Properties: map[string]interface{}{"payload": strings.Repeat("x", 4096)}}
	edge := &storage.Edge{ID: "nornic:edge", StartNode: node.ID, EndNode: node.ID, Type: "SELF", Properties: map[string]interface{}{"weight": int64(1)}}
	_, err := engine.MemoryEngine.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, engine.MemoryEngine.CreateEdge(edge))

	wantNodeSize, err := calculateNodeSize(node)
	require.NoError(t, err)
	wantEdgeSize, err := calculateEdgeSize(edge)
	require.NoError(t, err)

	manager := &DatabaseManager{}
	nodeSize, edgeSize, err := manager.calculateStorageSizeFromEngine(engine)
	require.NoError(t, err)
	require.Equal(t, wantNodeSize, nodeSize)
	require.Equal(t, wantEdgeSize, edgeSize)
	require.Zero(t, engine.allNodesCalls)
	require.Zero(t, engine.allEdgesCalls)
	require.Equal(t, 1, engine.nodeStreamCalls)
	require.Equal(t, 1, engine.edgeStreamCalls)
}

func TestDatabaseManagerRestartDefersStorageSizeScan(t *testing.T) {
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })

	first, err := NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	require.NotNil(t, first)
	_, err = base.CreateNode(&storage.Node{
		ID:         "nornic:large-node",
		Labels:     []string{"Document"},
		Properties: map[string]any{"payload": strings.Repeat("x", 4096)},
	})
	require.NoError(t, err)

	engine := &startupStreamingEngine{MemoryEngine: base}
	engine.Engine = engine.MemoryEngine
	engine.StoredEntityReader = engine.MemoryEngine
	engine.ScopedStoredEntityReader = engine.MemoryEngine
	restarted, err := NewDatabaseManager(t.Context(), engine, nil)
	require.NoError(t, err)
	require.NotNil(t, restarted)
	require.Zero(t, engine.nodeStreamCalls)
	require.Zero(t, engine.edgeStreamCalls)
}

func TestWriteDoesNotForceDeferredStorageSizeScanWithoutByteLimit(t *testing.T) {
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	_, err := NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	_, err = base.CreateNode(&storage.Node{ID: "nornic:existing", Labels: []string{"Document"}})
	require.NoError(t, err)

	engine := &startupStreamingEngine{MemoryEngine: base}
	engine.Engine = engine.MemoryEngine
	engine.StoredEntityReader = engine.MemoryEngine
	engine.ScopedStoredEntityReader = engine.MemoryEngine
	restarted, err := NewDatabaseManager(t.Context(), engine, nil)
	require.NoError(t, err)
	engine.prefixes = nil // Ignore the narrowly scoped leaked-metadata cleanup.
	database, err := restarted.GetDefaultStorage()
	require.NoError(t, err)
	_, err = database.CreateNode(&storage.Node{ID: "new", Labels: []string{"Document"}})
	require.NoError(t, err)
	require.Zero(t, engine.nodeStreamCalls)
	require.Zero(t, engine.edgeStreamCalls)
	require.Empty(t, engine.prefixes)
	require.Zero(t, engine.snapshotNodeScans)
	require.Zero(t, engine.snapshotEdgeScans)

	total, _, _ := restarted.GetStorageSize(restarted.DefaultDatabaseName())
	require.Positive(t, total)
	require.Equal(t, 1, engine.snapshotNodeScans)
	require.Equal(t, 1, engine.snapshotEdgeScans)
	require.Zero(t, engine.allNodesCalls)
	require.Zero(t, engine.allEdgesCalls)
}

// Storage sizing streams stored relationships (storage.StreamStoredEdgesByType
// and its scoped form): the spy counts them as it counts StreamEdges.
func (e *startupStreamingEngine) StreamStoredEdgesByType(ctx context.Context, edgeType string, visit func(*storage.Edge) error) error {
	e.edgeStreamCalls++
	return e.MemoryEngine.StreamStoredEdgesByType(ctx, edgeType, visit)
}

func (e *startupStreamingEngine) StreamStoredEdgesByTypeInScope(ctx context.Context, scope, edgeType string, visit func(*storage.Edge) error) error {
	e.edgeStreamCalls++
	return e.MemoryEngine.StreamStoredEdgesByTypeInScope(ctx, scope, edgeType, visit)
}
