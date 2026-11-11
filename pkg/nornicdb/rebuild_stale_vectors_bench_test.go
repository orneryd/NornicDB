package nornicdb_test

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Costs of the paths a rebuild-safe embedding journal touches, on a server's
// journaled storage: the embedding worker's write-back and "regenerate
// embeddings". Each reports the journal bytes it writes besides its time.
// The write-back is the storage call the worker uses, made directly; no
// worker runs. BenchmarkRegenerateEmbeddings times the clear only
// (DB.ClearAllEmbeddings): nothing embeds the nodes again inside the timer.

const rsvBenchNodes = 256

func rsvBenchVectors() [][]float32 {
	out := make([][]float32, 4)
	for i := range out {
		v := make([]float32, 1024)
		for j := range v {
			v[j] = float32(i*1024+j) / 1024
		}
		out[i] = v
	}
	return out
}

type rsvBench struct {
	b       *testing.B
	db      *nornicdb.DB
	journal *storage.WALEngine
	walDir  string
	nodes   []*storage.Node
}

func rsvBenchOpen(b *testing.B) *rsvBench {
	b.Helper()
	root := b.TempDir()
	c := nornicdb.DefaultConfig()
	c.Database.AsyncWritesEnabled = false
	c.Database.WALAutoCompactionEnabled = false
	c.Memory.DecayEnabled = false
	c.Memory.AutoLinksEnabled = false
	c.Memory.EmbeddingEnabled = false
	db, err := nornicdb.Open(root, c)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	journal, ok := db.GetBaseStorageForManager().(*storage.WALEngine)
	if !ok {
		b.Fatal("the server's storage is not journaled")
	}
	bench := &rsvBench{b: b, db: db, journal: journal, walDir: filepath.Join(root, "wal")}
	ctx := context.Background()
	for i := 0; i < rsvBenchNodes; i++ {
		id, err := db.GetStorage().CreateNode(ctx, &storage.Node{ID: storage.NodeID(fmt.Sprintf("bench-%d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"text": fmt.Sprintf("text %d", i)}})
		if err != nil {
			b.Fatal(err)
		}
		node, err := db.GetStorage().GetNode(ctx, id)
		if err != nil {
			b.Fatal(err)
		}
		node = storage.CopyNode(node)
		node.ID = storage.NodeID("nornic:" + string(node.ID))
		bench.nodes = append(bench.nodes, node)
	}
	return bench
}

// writeBack stores vectors for node i through the journaled storage, as the
// embedding worker does.
func (r *rsvBench) writeBack(i int) {
	payload := storage.CopyNode(r.nodes[i%len(r.nodes)])
	payload.ChunkEmbeddings = rsvBenchVectors()
	payload.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": 4, "embedding_dimensions": 1024, "embedding_model": "bench"}
	if err := r.journal.UpdateNodeEmbeddingSidecar(context.Background(), payload); err != nil {
		r.b.Fatal(err)
	}
}

func (r *rsvBench) journalBytes() int64 {
	if err := r.journal.GetWAL().Sync(); err != nil {
		r.b.Fatal(err)
	}
	var total int64
	err := filepath.WalkDir(r.walDir, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		info, err := entry.Info()
		if err == nil {
			total += info.Size()
		}
		return err
	})
	if err != nil {
		r.b.Fatal(err)
	}
	return total
}

func BenchmarkJournaledEmbeddingWriteBack(b *testing.B) {
	r := rsvBenchOpen(b)
	before := r.journalBytes()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.writeBack(i)
	}
	b.StopTimer()
	b.ReportMetric(float64(r.journalBytes()-before)/float64(b.N), "journal-B/op")
}

func BenchmarkRegenerateEmbeddings(b *testing.B) {
	r := rsvBenchOpen(b)
	var journal int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for n := range r.nodes {
			r.writeBack(n)
		}
		before := r.journalBytes()
		b.StartTimer()
		cleared, err := r.db.ClearAllEmbeddings()
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		if cleared != rsvBenchNodes {
			b.Fatalf("cleared %d nodes, want %d", cleared, rsvBenchNodes)
		}
		journal += r.journalBytes() - before
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(journal)/float64(b.N*rsvBenchNodes), "journal-B/node")
}
