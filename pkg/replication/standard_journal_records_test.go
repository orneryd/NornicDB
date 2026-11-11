package replication

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-412: a replicator built on an engine and a journal that name no record
// policy replicates what it replicated before the single-node fix. An update
// that keeps a node's vectors is replicated as the node's whole image
// (semantic version 3), and the replication journal holds ordinary frames
// only. This test uses only API that predates NOR-412, so it runs unchanged
// on that code.
func TestReplicatedUpdateCarriesTheWholeNodeImage(t *testing.T) {
	native := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	// Only "text" feeds the embedding, so the update below keeps the vectors.
	native.SetEmbeddingTextPolicy(storage.EmbeddingTextPolicy{Include: []string{"text"}})
	fixture := setupTestAdapter(t, adapterFixtureOptions{backing: storage.NewNamespacedEngine(native, "test"), standalone: true, timeout: 30 * time.Second})
	ctx := context.Background()
	vectors := [][]float32{{1, 2, 3}, {4, 5, 6}}
	named := map[string][]float32{"title": {7, 8}}
	image := func(properties map[string]any) *storage.Node {
		return &storage.Node{ID: "test:emb", Labels: []string{"Doc"}, Properties: properties, ChunkEmbeddings: vectors, NamedEmbeddings: named}
	}
	_, err := fixture.engine.CreateNode(ctx, image(map[string]any{"text": "kept"}))
	require.NoError(t, err)
	// The update changes one property and carries the vectors it read.
	require.NoError(t, fixture.engine.UpdateNode(ctx, storage.NodeUpdateRequest{After: image(map[string]any{"text": "kept", "flag": true})}))
	require.NoError(t, fixture.adapter.FlushWAL())

	entries, err := fixture.adapter.GetWALEntries(0, 100)
	require.NoError(t, err)
	updates := 0
	for _, entry := range entries {
		envelope, ops, err := storage.DecodeEntityTransaction(entry.Command.Data)
		if err != nil {
			continue // not a transaction command
		}
		for _, op := range ops {
			if op.Type != storage.OpUpdateNode {
				continue
			}
			updates++
			require.Equal(t, uint16(3), envelope.Version)
			require.Equal(t, vectors, op.Node.ChunkEmbeddings, "the replicated update carries the managed vectors")
			require.Equal(t, named, op.Node.NamedEmbeddings, "and the named vectors")
		}
	}
	require.Equal(t, 1, updates, "the update was replicated as one transaction command")

	raw, err := os.ReadFile(filepath.Join(fixture.walDir, "wal.log"))
	require.NoError(t, err)
	frames := 0
	for offset := 0; offset+9 <= len(raw); frames++ {
		require.Equal(t, "WALE", string(raw[offset:offset+4]))
		require.Equal(t, uint8(6), raw[offset+4], "the replication journal holds ordinary frames")
		offset += (9 + int(binary.LittleEndian.Uint32(raw[offset+5:])) + 4 + 8 + 7) &^ 7
	}
	require.Positive(t, frames)
}
