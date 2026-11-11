package nornicdb

import (
	"context"
	"io/fs"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkSingleNodeCommit measures one committed transaction that changes
// one property of one node, on a server opened with nornicdb.Open with the
// journal synced on every commit. It reports the journal bytes each commit
// writes (NOR-412).
//
//   - small: a node with two properties and no vectors, synchronous writes
//     and strict durability, an explicit commit.
//   - small-async-autocommit: the same node with asynchronous writes, without
//     strict durability, an implicit commit.
//   - spanning: a node with 1,800 chunk vectors of 2,048 dimensions, stored
//     through the embedding sidecar; synchronous writes. The journal bytes
//     the benchmark reports for it are more than one 16 MiB frame.
func BenchmarkSingleNodeCommit(b *testing.B) {
	b.Run("small", func(b *testing.B) { benchmarkSingleNodeCommit(b, 0, false) })
	b.Run("small-async-autocommit", func(b *testing.B) { benchmarkSingleNodeCommit(b, 0, true) })
	b.Run("spanning", func(b *testing.B) { benchmarkSingleNodeCommit(b, 1800, false) })
}

func benchmarkSingleNodeCommit(b *testing.B, chunks int, async bool) {
	const dims = 2048
	dir := b.TempDir()
	config := DefaultConfig()
	config.Database.AsyncWritesEnabled = async
	config.Database.StrictDurability = !async
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
	if _, err := db.GetStorage().CreateNode(ctx, &storage.Node{ID: "bench", Labels: []string{"Doc"}, Properties: map[string]any{"text": "unchanged text", "n": int64(-1)}}); err != nil {
		b.Fatal(err)
	}
	if chunks > 0 {
		stored, err := engine.GetNode(ctx, "nornic:bench")
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
		mode := storage.CommitModeExplicit
		if async {
			mode = storage.CommitModeImplicit
		}
		tx, err := storage.BeginEntityTransaction(ctx, engine, mode)
		if err != nil {
			b.Fatal(err)
		}
		if err := tx.SetNamespace("nornic"); err != nil {
			b.Fatal(err)
		}
		node, err := tx.GetNode(ctx, "nornic:bench")
		if err != nil {
			b.Fatal(err)
		}
		node = storage.CopyNode(node)
		node.Properties["n"] = int64(i)
		if err := tx.UpdateGraphNode(ctx, storage.NodeUpdateRequest{After: node}); err != nil {
			b.Fatal(err)
		}
		prepared, err := tx.PrepareEntityCommit(ctx, storage.EntityCommitRequest{LogicalID: "bench-update-" + strconv.Itoa(i)})
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

	got, err := engine.GetNode(ctx, "nornic:bench")
	if err != nil {
		b.Fatal(err)
	}
	if len(got.ChunkEmbeddings) != chunks || got.Properties["n"] != int64(b.N-1) {
		b.Fatalf("after %d updates the node holds %d chunk vectors and n=%v", b.N, len(got.ChunkEmbeddings), got.Properties["n"])
	}
}
