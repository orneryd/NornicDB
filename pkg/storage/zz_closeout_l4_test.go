package storage

// v1.5.2 close-out lane L4, H-68 (NOR-223): deleting nodes or shrinking their
// embeddings removes every separately stored vector key, on the Badger engine
// and through the server's stack (WAL -> namespace), for chunk and named
// embeddings, single and bulk deletes. Test file only; not for merge.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func l4StorageReport(t *testing.T, item, check string, pass bool, detail map[string]any) {
	t.Helper()
	detail["item"] = item
	detail["check"] = check
	detail["pass"] = pass
	data, _ := json.Marshal(detail)
	fmt.Fprintf(os.Stdout, "L4_RESULT %s\n", data)
	if !pass {
		t.Errorf("%s %s: expected behaviour does not hold: %s", item, check, data)
	}
}

// l4EmbeddingKeys counts every separately stored embedding key in the store.
func l4EmbeddingKeys(t *testing.T, engine *BadgerEngine) int {
	t.Helper()
	n := 0
	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = []byte{prefixEmbedding}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			n++
		}
		return nil
	}))
	return n
}

func l4VectorNode(id string, chunks int) *Node {
	vectors := make([][]float32, chunks)
	for i := range vectors {
		vectors[i] = make([]float32, 256)
		vectors[i][0] = float32(i + 1)
	}
	return &Node{ID: NodeID(id), Labels: []string{"Doc"}, Properties: map[string]any{"content": strings.Repeat("x", 60*1024)}, ChunkEmbeddings: vectors}
}

func TestCloseoutL4H68VectorKeys(t *testing.T) {
	t.Run("badger/shrink-still-separate", func(t *testing.T) {
		eng := newTestEngine(t)
		node := l4VectorNode("test:shrink", 16)
		_, err := eng.CreateNode(t.Context(), node)
		require.NoError(t, err)
		before := l4EmbeddingKeys(t, eng)
		node.ChunkEmbeddings = node.ChunkEmbeddings[:4]
		require.NoError(t, eng.UpdateNode(t.Context(), NodeUpdateRequest{After: node}))
		after := l4EmbeddingKeys(t, eng)
		read, err := eng.GetNode(t.Context(), node.ID)
		require.NoError(t, err)
		l4StorageReport(t, "H-68", "badger/shrink-still-separate", before == 16 && after <= 4 && len(read.ChunkEmbeddings) == 4,
			map[string]any{"keysBefore": before, "keysAfter": after, "chunksRead": len(read.ChunkEmbeddings)})
	})
	t.Run("badger/named-delete", func(t *testing.T) {
		eng := newTestEngine(t)
		named := map[string][]float32{}
		for i := 0; i < 8; i++ {
			v := make([]float32, 1024)
			v[0] = float32(i + 1)
			named[fmt.Sprintf("v%d", i)] = v
		}
		node := &Node{ID: "test:named", Labels: []string{"Doc"}, Properties: map[string]any{"content": strings.Repeat("y", 60*1024)}, NamedEmbeddings: named}
		_, err := eng.CreateNode(t.Context(), node)
		require.NoError(t, err)
		before := l4EmbeddingKeys(t, eng)
		require.NoError(t, eng.DeleteNode(t.Context(), node.ID))
		after := l4EmbeddingKeys(t, eng)
		l4StorageReport(t, "H-68", "badger/named-delete", after == 0, map[string]any{"keysBefore": before, "keysAfter": after})
	})
	t.Run("badger/bulk-delete", func(t *testing.T) {
		eng := newTestEngine(t)
		ids := []NodeID{}
		for i := 0; i < 5; i++ {
			node := l4VectorNode(fmt.Sprintf("test:bulk%d", i), 8)
			_, err := eng.CreateNode(t.Context(), node)
			require.NoError(t, err)
			ids = append(ids, node.ID)
		}
		before := l4EmbeddingKeys(t, eng)
		require.NoError(t, eng.BulkDeleteNodes(t.Context(), ids))
		after := l4EmbeddingKeys(t, eng)
		l4StorageReport(t, "H-68", "badger/bulk-delete", before == 40 && after == 0, map[string]any{"keysBefore": before, "keysAfter": after})
	})
	t.Run("server-stack/delete-and-shrink", func(t *testing.T) {
		dir := t.TempDir()
		base, err := NewBadgerEngine(dir)
		require.NoError(t, err)
		t.Cleanup(func() { _ = base.Close() })
		wal, err := NewWAL(filepath.Join(dir, "wal"), nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wal.Close() })
		engine := NewNamespacedEngine(NewWALEngine(base, wal), "l4")
		deleted := l4VectorNode("gone", 16)
		_, err = engine.CreateNode(t.Context(), deleted)
		require.NoError(t, err)
		shrunk := l4VectorNode("kept", 16)
		_, err = engine.CreateNode(t.Context(), shrunk)
		require.NoError(t, err)
		before := l4EmbeddingKeys(t, base)
		require.NoError(t, engine.DeleteNode(t.Context(), deleted.ID))
		current, err := engine.GetNode(t.Context(), shrunk.ID)
		require.NoError(t, err)
		current.ChunkEmbeddings = current.ChunkEmbeddings[:2]
		current.Properties = map[string]any{"content": "small"}
		require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: current}))
		after := l4EmbeddingKeys(t, base)
		l4StorageReport(t, "H-68", "server-stack/delete-and-shrink", before == 32 && after == 0, map[string]any{"keysBefore": before, "keysAfter": after})
	})
}
