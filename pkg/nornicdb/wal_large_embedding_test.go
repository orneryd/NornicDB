package nornicdb

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The provider returns an ordinary multi-chunk document result whose old WAL
// node image exceeded 16 MiB. Count requests to detect rejected-save retry loops.
type largeWALEmbedder struct {
	unitVectorEmbedder
	calls atomic.Int32
}

func (e *largeWALEmbedder) EmbedDocumentChunks(context.Context, string, int, int) (*embed.DocumentChunkResult, error) {
	e.calls.Add(1)
	const chunks = 2048
	r := &embed.DocumentChunkResult{Chunks: make([]string, chunks), Embeddings: make([][]float32, chunks), Model: e.Model()}
	for i := range r.Embeddings {
		r.Chunks[i] = "document chunk"
		r.Embeddings[i] = make([]float32, e.dims)
		for j := range r.Embeddings[i] {
			r.Embeddings[i][j] = float32(i*e.dims+j+1) / 3
		}
	}
	return r, nil
}

func TestLargeWALEmbeddingPublicWorkerSearchAndReopen(t *testing.T) {
	const dims, chunks = 2048, 2048
	cfg := DefaultConfig()
	cfg.Database.AsyncWritesEnabled = false
	cfg.Database.StrictDurability = true
	cfg.Database.WALSyncMode = "immediate"
	cfg.Database.WALAutoCompactionEnabled = false
	cfg.Memory.DecayEnabled, cfg.Memory.AutoLinksEnabled = false, false
	cfg.Memory.EmbeddingEnabled, cfg.Memory.EmbeddingDimensions = true, dims
	cfg.EmbeddingWorker.NumWorkers = 1
	cfg.EmbeddingWorker.MaxRetries = 1
	cfg.EmbeddingWorker.BatchDelay = 0
	cfg.EmbeddingWorker.TriggerDebounceDelay = 0
	root := t.TempDir()
	db, err := Open(root, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	e := &largeWALEmbedder{unitVectorEmbedder: unitVectorEmbedder{dims: dims}}
	db.SetEmbedder(e)
	svc, err := db.EnsureSearchIndexes(t.Context(), db.defaultDatabaseName(), nil, SearchIndexBuildWait)
	require.NoError(t, err)
	result, err := db.ExecuteCypher(t.Context(), "CREATE (n:LargeDocument {body:'bravo filler'}) RETURN id(n)", nil)
	require.NoError(t, err)
	id := storage.NodeID(result.Rows[0][0].(string))
	// Race/coverage instrumentation includes a multi-million-value native
	// sidecar write; allow that work to finish before checking its outcome.
	waitCtx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	waitErr := db.WaitForEmbeddings(waitCtx)
	node, err := db.GetStorage().GetNode(t.Context(), id)
	require.NoError(t, err)
	t.Logf("provider calls=%d stored chunks=%d failure marker=%v wait error=%v", e.calls.Load(), len(node.ChunkEmbeddings), node.EmbedMeta["embedding_failed"], waitErr)
	require.NoError(t, waitErr)
	require.Len(t, node.ChunkEmbeddings, chunks)
	require.EqualValues(t, 1, e.calls.Load(), "an accepted embedding must not be re-requested")
	response := vectorOnlySearch(t, svc, node.ChunkEmbeddings[0])
	require.Len(t, response.Results, 1)
	require.Equal(t, id, response.Results[0].NodeID)
	require.Positive(t, response.Results[0].VectorRank)
	entries, err := storage.ReadWALEntriesFromDir(t.Context(), filepath.Join(root, "wal"))
	require.NoError(t, err)
	var audits int
	for _, entry := range entries {
		if entry.Operation == storage.OpUpdateEmbedding {
			audits++
			require.Less(t, len(entry.Data), 1024, "the audit must not scale with document or vector size")
			_, err := storage.EntryPayloadJSON(entry)
			require.NoError(t, err, "db.txlog must still render the audit")
		}
	}
	require.Equal(t, 1, audits)
	require.NoError(t, db.Close())
	db, err = Open(root, cfg)
	require.NoError(t, err)
	reopened, err := db.GetStorage().GetNode(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, node.ChunkEmbeddings, reopened.ChunkEmbeddings)
	svc, err = db.EnsureSearchIndexes(t.Context(), db.defaultDatabaseName(), nil, SearchIndexBuildWait)
	require.NoError(t, err)
	response = vectorOnlySearch(t, svc, reopened.ChunkEmbeddings[0])
	require.Len(t, response.Results, 1)
	require.Equal(t, id, response.Results[0].NodeID)
}
