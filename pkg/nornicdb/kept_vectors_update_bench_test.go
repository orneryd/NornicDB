package nornicdb

import (
	"context"
	"io/fs"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkKeptVectorsPropertyUpdate measures one committed transaction that
// changes one property of a node holding 1,500 chunk vectors of 2,048
// dimensions and keeps them (NOR-412), on a server opened the way an operator
// opens a single node: synchronous writes, the journal synced on every
// commit. The embedding worker's sidecar holds the vectors, as on a deployed
// server. It reports the journal bytes each update writes.
func BenchmarkKeptVectorsPropertyUpdate(b *testing.B) {
	const chunks, dims = 1500, 2048
	dir := b.TempDir()
	config := DefaultConfig()
	config.Database.AsyncWritesEnabled = false
	config.Database.StrictDurability = true
	config.Database.WALSyncMode = "immediate"
	config.Database.WALAutoCompactionEnabled = false
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Memory.EmbeddingEnabled = false
	config.Memory.EmbeddingDimensions = dims
	config.EmbeddingWorker.PropertiesInclude = []string{"text"}
	config.EmbeddingWorker.IncludeLabels = false
	db, err := Open(dir, config)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	base := db.GetBaseStorageForManager()
	engine := unwrapToBadgerEngine(base)
	if _, err := db.GetStorage().CreateNode(ctx, &storage.Node{ID: "kept", Labels: []string{"Doc"}, Properties: map[string]any{"text": "unchanged text", "n": int64(-1)}}); err != nil {
		b.Fatal(err)
	}
	stored, err := engine.GetNode(ctx, "nornic:kept")
	if err != nil {
		b.Fatal(err)
	}
	embedded := storage.CopyNode(stored)
	embedded.ChunkEmbeddings = make([][]float32, chunks)
	for i := range embedded.ChunkEmbeddings {
		vector := make([]float32, dims)
		for j := range vector {
			vector[j] = float32(i*dims+j) / 7
		}
		embedded.ChunkEmbeddings[i] = vector
	}
	embedded.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": chunks, "embedding_dimensions": dims, "embedding_model": "synthetic"}
	if err := engine.UpdateNodeEmbeddingSidecar(ctx, embedded); err != nil {
		b.Fatal(err)
	}
	journalBytes := func() int64 {
		var total int64
		_ = filepath.WalkDir(filepath.Join(dir, "wal"), func(_ string, entry fs.DirEntry, err error) error {
			if err == nil && !entry.IsDir() {
				if info, err := entry.Info(); err == nil {
					total += info.Size()
				}
			}
			return nil
		})
		return total
	}
	submitter := base.(storage.EntityCommitSubmitter)
	before := journalBytes()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		tx, err := storage.BeginEntityTransaction(ctx, engine, storage.CommitModeExplicit)
		if err != nil {
			b.Fatal(err)
		}
		if err := tx.SetNamespace("nornic"); err != nil {
			b.Fatal(err)
		}
		node, err := tx.GetNode(ctx, "nornic:kept")
		if err != nil {
			b.Fatal(err)
		}
		node = storage.CopyNode(node)
		node.Properties["n"] = int64(i)
		if err := tx.UpdateGraphNode(ctx, storage.NodeUpdateRequest{After: node}); err != nil {
			b.Fatal(err)
		}
		prepared, err := tx.PrepareEntityCommit(ctx, storage.EntityCommitRequest{LogicalID: "kept-update-" + strconv.Itoa(i)})
		if err != nil {
			b.Fatal(err)
		}
		receipt, err := submitter.SubmitEntityCommit(ctx, prepared, nil)
		if err != nil {
			b.Fatal(err)
		}
		if err := receipt.Wait(ctx); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(journalBytes()-before)/float64(b.N), "journal-bytes/op")

	got, err := engine.GetNode(ctx, "nornic:kept")
	if err != nil {
		b.Fatal(err)
	}
	if len(got.ChunkEmbeddings) != chunks || got.Properties["n"] != int64(b.N-1) {
		b.Fatalf("after %d updates the node holds %d chunk vectors and n=%v", b.N, len(got.ChunkEmbeddings), got.Properties["n"])
	}
}
