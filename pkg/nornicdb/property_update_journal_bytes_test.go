package nornicdb

import (
	"context"
	"io/fs"
	"path/filepath"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// B30/B37 on the public managed path: a real nornicdb.Open server whose
// standalone WAL uses the large record policy, written through the production
// Cypher executor. A property-only update that keeps the node's managed chunk
// vectors and its named vector reaches the journal as a whole node image, so
// the record grows with vectors the statement never touched.
//
// The desired bound is not met at baseline 2950aa88f; the failing assertion is
// the reproduction. The measured record and the preserved result are logged.

// propertyUpdateJournalPublicBound is the largest journal record a
// property-only Cypher update may write however many unchanged vectors the
// node holds. It is a desired bound, not a current one.
const propertyUpdateJournalPublicBound = 8 << 10

// propertyUpdateJournalPublicDims is the fixture's vector dimension count.
const propertyUpdateJournalPublicDims = 256

// propertyUpdateJournalPublicChunks is the fixture's managed chunk count.
const propertyUpdateJournalPublicChunks = 256

func propertyUpdateJournalPublicVector(index int) []float32 {
	vector := make([]float32, propertyUpdateJournalPublicDims)
	for j := range vector {
		vector[j] = float32(1) + float32(index*propertyUpdateJournalPublicDims+j)/7
	}
	return vector
}

// propertyUpdateJournalPublicFixture opens a server with the fixture
// configuration, stores the node with the fixture named vector and managed
// chunk vectors, and returns the store, its native engine, the server
// directory, the config and the namespaced node ID.
func propertyUpdateJournalPublicFixture(t *testing.T, chunks int, named bool) (*DB, *storage.BadgerEngine, string, *Config, storage.NodeID) {
	t.Helper()
	dir := t.TempDir()
	config := startupConfig()
	config.Memory.EmbeddingDimensions = propertyUpdateJournalPublicDims
	db, err := Open(dir, config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	engine := unwrapToBadgerEngine(db.GetBaseStorageForManager())
	node := &storage.Node{ID: "doc:journal", Labels: []string{"Doc"}, Properties: map[string]any{"text": "alpha"}}
	if named {
		node.NamedEmbeddings = map[string][]float32{"title": propertyUpdateJournalPublicVector(0)}
	}
	_, err = db.GetStorage().CreateNode(ctx, node)
	require.NoError(t, err)
	id := storage.NodeID(db.defaultDatabaseName() + ":doc:journal")
	stored, err := engine.GetNode(ctx, id)
	require.NoError(t, err)
	if named {
		require.Len(t, stored.NamedEmbeddings["title"], propertyUpdateJournalPublicDims)
	}
	embedded := storage.CopyNode(stored)
	embedded.ChunkEmbeddings = make([][]float32, chunks)
	for i := range embedded.ChunkEmbeddings {
		embedded.ChunkEmbeddings[i] = propertyUpdateJournalPublicVector(i + 1)
	}
	embedded.EmbedMeta = map[string]any{
		"has_embedding": true, "chunk_count": chunks, "embedding_dimensions": propertyUpdateJournalPublicDims,
	}
	require.NoError(t, engine.UpdateNodeEmbeddingSidecar(ctx, embedded))
	stored, err = engine.GetNode(ctx, id)
	require.NoError(t, err)
	require.Len(t, stored.ChunkEmbeddings, chunks)
	return db, engine, dir, config, id
}

// requirePropertyUpdateJournalPublicVectors checks the stored chunk vectors
// (seeded from index 1) and the named vector (seeded from index 0).
func requirePropertyUpdateJournalPublicVectors(t *testing.T, node *storage.Node, chunks int) {
	t.Helper()
	require.Len(t, node.ChunkEmbeddings, chunks)
	for i, vector := range node.ChunkEmbeddings {
		fixtureowner.RequireFloat32Vector(t, "managed chunk", vector, propertyUpdateJournalPublicDims, func(j int) float32 {
			return float32(1) + float32((i+1)*propertyUpdateJournalPublicDims+j)/7
		})
	}
	fixtureowner.RequireFloat32Vector(t, "named title", node.NamedEmbeddings["title"], propertyUpdateJournalPublicDims, func(j int) float32 {
		return float32(1) + float32(j)/7
	})
}

// propertyUpdateJournalPublicBytes is the size of every file of the journal
// directory.
func propertyUpdateJournalPublicBytes(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	require.NoError(t, filepath.WalkDir(dir, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		info, infoErr := entry.Info()
		if infoErr == nil {
			total += info.Size()
		}
		return infoErr
	}))
	return total
}

// propertyUpdateJournalPublicRecords returns the entity-commit records the
// public statements after before wrote.
func propertyUpdateJournalPublicRecords(t *testing.T, walDir string, before int) []storage.WALEntry {
	t.Helper()
	entries, err := storage.ReadWALEntriesFromDir(t.Context(), walDir)
	require.NoError(t, err)
	require.Greater(t, len(entries), before, "the statement must be journalled")
	var records []storage.WALEntry
	for _, entry := range entries[before:] {
		if entry.Operation == storage.OpEntityCommit {
			records = append(records, entry)
		}
	}
	return records
}

func TestPropertyOnlyCypherUpdateJournalsUnchangedVectorBytes(t *testing.T) {
	db, engine, dir, config, id := propertyUpdateJournalPublicFixture(t, propertyUpdateJournalPublicChunks, true)
	ctx := context.Background()
	walDir := filepath.Join(dir, "wal")

	before, err := storage.ReadWALEntriesFromDir(ctx, walDir)
	require.NoError(t, err)
	journalBefore := propertyUpdateJournalPublicBytes(t, walDir)

	// The public statement changes one property and nothing the embedding text
	// policy feeds.
	_, err = db.ExecuteCypher(ctx, "MATCH (n:Doc {text: 'alpha'}) SET n.flag = true", nil)
	require.NoError(t, err)

	records := propertyUpdateJournalPublicRecords(t, walDir, len(before))
	require.Len(t, records, 1, "one committed statement, one journal record")
	record := len(records[0].Data)
	grown := propertyUpdateJournalPublicBytes(t, walDir) - journalBefore
	t.Logf("public Cypher update: unchanged chunk bytes=%d named vector bytes=%d record bytes=%d journal bytes added=%d",
		propertyUpdateJournalPublicChunks*propertyUpdateJournalPublicDims*4, propertyUpdateJournalPublicDims*4, record, grown)

	// The stored result is correct today and must stay correct.
	stored, err := engine.GetNode(ctx, id)
	require.NoError(t, err)
	require.Equal(t, true, stored.Properties["flag"])
	require.Equal(t, "alpha", stored.Properties["text"])
	requirePropertyUpdateJournalPublicVectors(t, stored, propertyUpdateJournalPublicChunks)

	// Desired outcome: the record is bounded, independent of the vectors the
	// statement left unchanged.
	require.LessOrEqual(t, record, propertyUpdateJournalPublicBound,
		"a property-only Cypher update journalled %d bytes", record)

	// The stored vectors also survive a close and reopen of the public server.
	require.NoError(t, db.Close())
	reopened, err := Open(dir, config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	recovered, err := unwrapToBadgerEngine(reopened.GetBaseStorageForManager()).GetNode(ctx, id)
	require.NoError(t, err)
	require.Equal(t, true, recovered.Properties["flag"])
	requirePropertyUpdateJournalPublicVectors(t, recovered, propertyUpdateJournalPublicChunks)
}

// TestPropertyUpdateJournalPublicGrowthIsUnchangedVectors separates the cause
// from the edit: identical Cypher property updates journal records whose size
// follows the unchanged vector count, and every stored result stays exact.
func TestPropertyUpdateJournalPublicGrowthIsUnchangedVectors(t *testing.T) {
	counts := []int{16, propertyUpdateJournalPublicChunks}
	sizes := make([]int, 0, len(counts))
	for _, chunks := range counts {
		db, engine, dir, _, id := propertyUpdateJournalPublicFixture(t, chunks, false)
		walDir := filepath.Join(dir, "wal")
		before, err := storage.ReadWALEntriesFromDir(t.Context(), walDir)
		require.NoError(t, err)
		_, err = db.ExecuteCypher(t.Context(), "MATCH (n:Doc {text: 'alpha'}) SET n.flag = true", nil)
		require.NoError(t, err)
		records := propertyUpdateJournalPublicRecords(t, walDir, len(before))
		require.Len(t, records, 1)
		sizes = append(sizes, len(records[0].Data))
		t.Logf("%d unchanged chunk vectors: public journal record bytes=%d", chunks, len(records[0].Data))

		stored, err := engine.GetNode(t.Context(), id)
		require.NoError(t, err)
		require.Equal(t, true, stored.Properties["flag"])
		require.Len(t, stored.ChunkEmbeddings, chunks)
	}
	t.Logf("public journal grew %d bytes between %d and %d unchanged vectors", sizes[1]-sizes[0], counts[0], counts[1])
	require.LessOrEqual(t, sizes[1]-sizes[0], 64,
		"public property records must not scale with unchanged managed coordinates")
}
