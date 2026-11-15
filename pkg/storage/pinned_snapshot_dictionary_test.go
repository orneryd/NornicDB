package storage

// A pinned stored snapshot is a physical read point, so the stored edge body it
// transcodes must resolve its node identities and property names in that same
// physical view. Serializing the pin runs with the file writer unlocked, so a
// namespace a DROP emptied and re-created keeps its stored bodies' bindings only
// in the pin (readStoredBinding, dictionary_binding_read.go). These tests pin a
// namespace, re-create it with other IDs and other property names, and read the
// pin through both the snapshot writer's relationship records and the public
// snapshot file writer.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// pinnedDictionaryOriginal is the namespace population of the pin: two nodes and
// one relationship whose stored body references both node numbers and two
// property-key tokens.
func pinnedDictionaryOriginal(t *testing.T, engine *BadgerEngine) (map[string]any, *Edge) {
	t.Helper()
	ctx := context.Background()
	for _, id := range []NodeID{"app:a", "app:b"} {
		_, err := engine.CreateNode(ctx, &Node{ID: id, Labels: []string{"Doc"}})
		require.NoError(t, err)
	}
	properties := map[string]any{"state": "original", "note": strings.Repeat("x", 64)}
	edge := &Edge{ID: "app:rel", StartNode: "app:a", EndNode: "app:b", Type: "REL", Properties: properties}
	require.NoError(t, engine.CreateEdge(ctx, edge))
	return properties, edge
}

// pinnedDictionaryReplacement re-creates the same namespace after the DROP with
// other node and relationship IDs and other property names, so every stored
// binding of the pin's bodies means something else in the current view.
func pinnedDictionaryReplacement(t *testing.T, engine *BadgerEngine) {
	t.Helper()
	ctx := context.Background()
	_, _, err := engine.DeleteByPrefix("app:")
	require.NoError(t, err)
	for _, id := range []NodeID{"app:c", "app:d"} {
		_, err := engine.CreateNode(ctx, &Node{ID: id, Labels: []string{"Doc"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(ctx, &Edge{ID: "app:new_rel", StartNode: "app:c", EndNode: "app:d", Type: "REL",
		Properties: map[string]any{"replacement": "second"}}))
}

func TestPinnedSnapshotRetainsStoredEdgeBindingsAcrossNamespaceRecreation(t *testing.T) {
	ctx := context.Background()
	engine := newTestEngine(t)
	originalProperties, _ := pinnedDictionaryOriginal(t, engine)

	// The snapshot caller pins the stored population before it serializes.
	source, err := openStoredSnapshotSource(ctx, engine)
	require.NoError(t, err)
	defer source.view.Close()

	pinnedDictionaryReplacement(t, engine)
	// The current view holds the replacement bindings, not the pin's.
	live, err := engine.GetEdge(ctx, "app:new_rel")
	require.NoError(t, err)
	require.Equal(t, map[string]any{"replacement": "second"}, live.Properties)
	_, err = engine.GetEdge(ctx, "app:rel")
	require.ErrorIs(t, err, ErrNotFound)

	// The canonical stored decoder resolves the pin's own body in the pin's
	// physical view. It is the oracle the snapshot transcoder must reproduce.
	var decoded *Edge
	require.NoError(t, source.view.withView(ctx, func(txn *badger.Txn) error {
		var readErr error
		decoded, readErr = engine.readEdgeInTxn(txn, "app:rel")
		return readErr
	}))
	require.NotNil(t, decoded)
	require.Equal(t, NodeID("app:a"), decoded.StartNode)
	require.Equal(t, NodeID("app:b"), decoded.EndNode)
	require.Equal(t, "REL", decoded.Type)
	require.Equal(t, originalProperties, decoded.Properties)

	// The snapshot writer's relationship records keep the exact original
	// identities, property names and values through the raw property transcoder.
	records := pinnedSnapshotEdgeRecords(t, ctx, source.StreamEdges)
	require.Len(t, records, 1)
	record, ok := records["app:rel"]
	require.True(t, ok, "the pin holds the relationship stored before the DROP")
	require.True(t, record.transcoded, "the stored body must take the raw property transcoder, not the decoded fallback")
	require.Equal(t, "REL", record.edgeType)
	require.Equal(t, NodeID("app:a"), record.start)
	require.Equal(t, NodeID("app:b"), record.end)
	require.Equal(t, originalProperties, record.properties)

	// The same pin through the public snapshot file writer.
	path := filepath.Join(t.TempDir(), "pinned.snapshot")
	require.NoError(t, saveCompactionSnapshot(ctx, compactionSnapshot{engine: engine, source: source, sequence: 11}, path))
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	visitor := &collectingSnapshotVisitor{}
	metadata, err := ReadSnapshot(ctx, file, visitor)
	require.NoError(t, err)
	require.Equal(t, uint64(1), metadata.EdgeCount)
	require.Len(t, visitor.edges, 1)
	require.Equal(t, EdgeID("app:rel"), visitor.edges[0].ID)
	require.Equal(t, NodeID("app:a"), visitor.edges[0].StartNode)
	require.Equal(t, NodeID("app:b"), visitor.edges[0].EndNode)
	require.Equal(t, "REL", visitor.edges[0].Type)
	require.Equal(t, originalProperties, visitor.edges[0].Properties)
}
