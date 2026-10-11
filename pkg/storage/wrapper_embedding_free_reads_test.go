package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type embeddingFreeReadSpy struct {
	Engine
	fullReads       int
	fullBatchReads  int
	lightReads      int
	lightBatchReads int
	projectedScans  int
}

type engineWithoutLightRead struct{ Engine }

func (s *embeddingFreeReadSpy) StreamNodesWithOptions(ctx context.Context, opts StreamNodesOptions, visit func(*Node) error) error {
	if opts.StripEmbeddings {
		s.projectedScans++
	}
	return s.Engine.StreamNodesWithOptions(ctx, opts, visit)
}

func (s *embeddingFreeReadSpy) GetNode(id NodeID) (*Node, error) {
	s.fullReads++
	return s.Engine.GetNode(id)
}

func (s *embeddingFreeReadSpy) BatchGetNodes(ids []NodeID) (map[NodeID]*Node, error) {
	s.fullBatchReads++
	return s.Engine.BatchGetNodes(ids)
}

func (s *embeddingFreeReadSpy) GetNodeWithoutEmbeddings(id NodeID) (*Node, error) {
	s.lightReads++
	return s.Engine.(NodeWithoutEmbeddingsReader).GetNodeWithoutEmbeddings(id)
}

func (s *embeddingFreeReadSpy) BatchGetNodesWithoutEmbeddings(ids []NodeID) (map[NodeID]*Node, error) {
	s.lightBatchReads++
	return s.Engine.(BatchNodeWithoutEmbeddingsReader).BatchGetNodesWithoutEmbeddings(ids)
}

func TestEmbeddingFreeBatchReadsTraverseNamespacedWALStack(t *testing.T) {
	badger := createTestBadgerEngine(t)
	spy := &embeddingFreeReadSpy{Engine: badger}
	walLog, err := NewWAL(t.TempDir(), &WALConfig{SyncMode: "none"})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, walLog.Close()) })
	wal := NewWALEngine(spy, walLog)
	tenant := NewNamespacedEngine(wal, "library")

	node := &Node{
		ID:         "persisted",
		Labels:     []string{"Transcript"},
		Properties: map[string]any{"state": "ready"},
		ChunkEmbeddings: [][]float32{
			make([]float32, 4096),
			make([]float32, 4096),
		},
		NamedEmbeddings: map[string][]float32{"visual": make([]float32, 4096)},
	}
	_, err = tenant.CreateNode(node)
	require.NoError(t, err)
	pending := &Node{
		ID:              "pending",
		Labels:          []string{"Transcript"},
		Properties:      map[string]any{"state": "queued"},
		ChunkEmbeddings: [][]float32{make([]float32, 4096)},
	}
	_, err = tenant.CreateNode(pending)
	require.NoError(t, err)

	require.True(t, tenant.BatchGetNodesWithoutEmbeddingsSupported())
	light, err := tenant.BatchGetNodesWithoutEmbeddings([]NodeID{node.ID, pending.ID})
	require.NoError(t, err)
	require.Equal(t, []string{"Transcript"}, light[node.ID].Labels)
	require.Equal(t, "ready", light[node.ID].Properties["state"])
	require.Empty(t, light[node.ID].ChunkEmbeddings)
	require.Empty(t, light[node.ID].NamedEmbeddings)
	require.Equal(t, "queued", light[pending.ID].Properties["state"])
	require.Empty(t, light[pending.ID].ChunkEmbeddings)
	require.Equal(t, 1, spy.lightBatchReads)
	require.Zero(t, spy.lightReads)
	require.Zero(t, spy.fullBatchReads, "wrapper stack must not load full embedding-bearing nodes")
	require.Zero(t, spy.fullReads, "wrapper stack must not load full embedding-bearing nodes")
}

func TestEmbeddingFreeSingleReadsTraverseWALStack(t *testing.T) {
	badger := createTestBadgerEngine(t)
	spy := &embeddingFreeReadSpy{Engine: badger}
	walLog, err := NewWAL(t.TempDir(), &WALConfig{SyncMode: "none"})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, walLog.Close()) })
	wal := NewWALEngine(spy, walLog)

	node := &Node{ID: "nornic:persisted", ChunkEmbeddings: [][]float32{make([]float32, 4096)}}
	_, err = wal.CreateNode(node)
	require.NoError(t, err)

	light, err := wal.GetNodeWithoutEmbeddings(node.ID)
	require.NoError(t, err)
	require.Empty(t, light.ChunkEmbeddings)
	require.Equal(t, 1, spy.lightReads)
	require.Zero(t, spy.fullReads)
}

func TestEmbeddingFreeSingleReadFallbackStripsEmbeddings(t *testing.T) {
	t.Run("wal", func(t *testing.T) {
		badger, err := NewBadgerEngineInMemory()
		require.NoError(t, err)
		t.Cleanup(func() { _ = badger.Close() })
		id := NodeID("test:light-read")
		_, err = badger.CreateNode(&Node{
			ID: id, Properties: map[string]any{"keep": "visible"},
			ChunkEmbeddings: [][]float32{{1, 0}},
			NamedEmbeddings: map[string][]float32{"named": {0, 1}},
		})
		require.NoError(t, err)
		fallback := &engineWithoutLightRead{Engine: badger}
		log, err := NewWAL(t.TempDir(), nil)
		require.NoError(t, err)
		wal := NewWALEngine(fallback, log)
		t.Cleanup(func() { _ = wal.Close() })
		reader := wal
		light, err := reader.GetNodeWithoutEmbeddings(id)
		require.NoError(t, err)
		require.Equal(t, "visible", light.Properties["keep"])
		require.Empty(t, light.ChunkEmbeddings)
		require.Empty(t, light.NamedEmbeddings)
		original, err := badger.GetNode(id)
		require.NoError(t, err)
		require.NotEmpty(t, original.ChunkEmbeddings)
		require.NotEmpty(t, original.NamedEmbeddings)
		_, err = reader.GetNodeWithoutEmbeddings("test:missing")
		require.ErrorIs(t, err, ErrNotFound)
	})
}

func TestNamespacedEmbeddingFreeReadFallbackStripsEmbeddings(t *testing.T) {
	badger, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.Close() })

	tenant := NewNamespacedEngine(&engineWithoutLightRead{Engine: badger}, "library")
	node := &Node{
		ID:              "test:namespaced-light-read",
		Properties:      map[string]any{"keep": "visible"},
		EmbedMeta:       map[string]any{"embedding_model": "test-model"},
		ChunkEmbeddings: [][]float32{{1, 0}},
		NamedEmbeddings: map[string][]float32{"named": {0, 1}},
	}
	_, err = tenant.CreateNode(node)
	require.NoError(t, err)

	light, err := tenant.GetNodeWithoutEmbeddings(node.ID)
	require.NoError(t, err)
	require.Equal(t, node.ID, light.ID)
	require.Equal(t, "visible", light.Properties["keep"])
	require.Equal(t, "test-model", light.EmbedMeta["embedding_model"])
	require.Empty(t, light.ChunkEmbeddings)
	require.Empty(t, light.NamedEmbeddings)

	full, err := tenant.GetNode(node.ID)
	require.NoError(t, err)
	require.NotEmpty(t, full.ChunkEmbeddings)
	require.NotEmpty(t, full.NamedEmbeddings)
}

func BenchmarkEmbeddingFreeSingleReadFallback(b *testing.B) {
	badger, err := NewBadgerEngineInMemory()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = badger.Close() })
	id := NodeID("test:light-read-bench")
	_, err = badger.CreateNode(&Node{
		ID: id, Properties: map[string]any{"keep": "visible"},
		ChunkEmbeddings: [][]float32{{1, 0}},
		NamedEmbeddings: map[string][]float32{"named": {0, 1}},
	})
	if err != nil {
		b.Fatal(err)
	}
	fallback := &engineWithoutLightRead{Engine: badger}
	namespacedID := NodeID("test:namespaced-light-read-bench")
	namespaced := NewNamespacedEngine(fallback, "benchmark")
	_, err = namespaced.CreateNode(&Node{
		ID: namespacedID, Properties: map[string]any{"keep": "visible"},
		ChunkEmbeddings: [][]float32{{1, 0}},
		NamedEmbeddings: map[string][]float32{"named": {0, 1}},
	})
	if err != nil {
		b.Fatal(err)
	}
	log, err := NewWAL(b.TempDir(), nil)
	if err != nil {
		b.Fatal(err)
	}
	wal := NewWALEngine(fallback, log)
	b.Cleanup(func() { _ = wal.Close() })
	for _, entry := range []struct {
		name string
		id   NodeID
		read func(NodeID) (*Node, error)
	}{
		{name: "full", id: id, read: badger.GetNode},
		{name: "native-light", id: id, read: badger.GetNodeWithoutEmbeddings},
		{name: "wal-fallback", id: id, read: wal.GetNodeWithoutEmbeddings},
		{name: "namespaced-full-baseline", id: namespacedID, read: namespaced.GetNode},
		{name: "namespaced-fallback", id: namespacedID, read: namespaced.GetNodeWithoutEmbeddings},
	} {
		b.Run(entry.name, func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				if _, err := entry.read(entry.id); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestEmbeddingFreePrefixScansTraverseNamespacedWALStack(t *testing.T) {
	badger := createTestBadgerEngine(t)
	spy := &embeddingFreeReadSpy{Engine: badger}
	walLog, err := NewWAL(t.TempDir(), &WALConfig{SyncMode: "none"})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, walLog.Close()) })
	wal := NewWALEngine(spy, walLog)
	tenant := NewNamespacedEngine(wal, "library")

	for _, node := range []*Node{
		{ID: "persisted", Properties: map[string]any{"keep": "yes", "drop": "large"}, EmbedMeta: map[string]any{"embedding_failed": true}, ChunkEmbeddings: [][]float32{make([]float32, 4096)}},
		{ID: "pending", Properties: map[string]any{"keep": "also", "drop": "large"}, EmbedMeta: map[string]any{"embedding_failed": true}, ChunkEmbeddings: [][]float32{make([]float32, 4096)}},
	} {
		_, err = tenant.CreateNode(node)
		require.NoError(t, err)
	}

	seen := make(map[NodeID]*Node)
	err = tenant.StreamNodesByPrefixWithoutEmbeddings(context.Background(), "", func(node *Node) error {
		seen[node.ID] = node
		return nil
	})
	require.NoError(t, err)
	require.Len(t, seen, 2)
	for _, node := range seen {
		require.Empty(t, node.ChunkEmbeddings)
		require.Empty(t, node.NamedEmbeddings)
		require.Empty(t, node.Properties)
		require.Equal(t, true, node.EmbedMeta["embedding_failed"])
	}
	require.Equal(t, 1, spy.projectedScans)
}

func TestEmbeddingFreeBatchCapabilityRejectsUnsupportedInnerEngine(t *testing.T) {
	base := newEngineWithoutEmbeddingFreeReads(t)
	walLog, err := NewWAL(t.TempDir(), &WALConfig{SyncMode: "none"})
	require.NoError(t, err)
	wal := NewWALEngine(base, walLog)
	tenant := NewNamespacedEngine(wal, "unsupported")

	require.False(t, wal.BatchGetNodesWithoutEmbeddingsSupported())
	require.False(t, tenant.BatchGetNodesWithoutEmbeddingsSupported())
	_, err = tenant.CreateNode(&Node{
		ID:              "cached",
		ChunkEmbeddings: [][]float32{make([]float32, 4096)},
	})
	require.NoError(t, err)
	_, err = tenant.BatchGetNodesWithoutEmbeddings([]NodeID{"cached"})
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = wal.BatchGetNodesWithoutEmbeddings([]NodeID{"missing"})
	require.ErrorIs(t, err, ErrNotImplemented)
}

type engineWithoutEmbeddingFreeReads struct{ Engine }

func newEngineWithoutEmbeddingFreeReads(t *testing.T) Engine {
	t.Helper()
	base := NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	return &engineWithoutEmbeddingFreeReads{Engine: base}
}

func BenchmarkNamespacedWALBatchNodeReads(b *testing.B) {
	badger, err := NewBadgerEngineInMemory()
	require.NoError(b, err)
	walLog, err := NewWAL(b.TempDir(), &WALConfig{SyncMode: "none"})
	require.NoError(b, err)
	wal := NewWALEngine(badger, walLog)
	tenant := NewNamespacedEngine(wal, "benchmark")

	ids := make([]NodeID, 16)
	for index := range ids {
		ids[index] = NodeID("node-" + string(rune('a'+index)))
		node := &Node{
			ID:         ids[index],
			Labels:     []string{"Transcript"},
			Properties: map[string]any{"state": "ready"},
		}
		for chunk := 0; chunk < 8; chunk++ {
			node.ChunkEmbeddings = append(node.ChunkEmbeddings, make([]float32, 1024))
		}
		_, err = tenant.CreateNode(node)
		require.NoError(b, err)
	}

	b.Run("embedding_free", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := tenant.BatchGetNodesWithoutEmbeddings(ids); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("full", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := tenant.BatchGetNodes(ids); err != nil {
				b.Fatal(err)
			}
		}
	})
}
