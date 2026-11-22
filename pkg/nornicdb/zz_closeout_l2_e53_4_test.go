//go:build closeout_l2_e53_4

package nornicdb_test

// E53.4: with asynchronous writes, ClearAllEmbeddings() right after
// CREATE ... WITH EMBEDDING must clear the node (and the worker must embed
// it again).

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const l2E53_4Dims = 16

// l2E53_4Provider: Embed returns [1, 0, ...]; ChunkText returns the text as
// one chunk; EmbedDocumentChunks returns 3 chunks whose values change with
// every call.
type l2E53_4Provider struct {
	calls atomic.Int32
}

func (p *l2E53_4Provider) Embed(context.Context, string) ([]float32, error) {
	v := make([]float32, l2E53_4Dims)
	v[0] = 1
	return v, nil
}

func (p *l2E53_4Provider) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i], _ = p.Embed(ctx, texts[i])
	}
	return out, nil
}

func (p *l2E53_4Provider) ChunkText(text string, _, _ int) ([]string, error) {
	return []string{text}, nil
}
func (p *l2E53_4Provider) Dimensions() int { return l2E53_4Dims }
func (p *l2E53_4Provider) Model() string   { return "l2-e53-4-stub" }
func (p *l2E53_4Provider) Backend() string { return "cpu" }

func (p *l2E53_4Provider) EmbedDocumentChunks(context.Context, string, int, int) (*embed.DocumentChunkResult, error) {
	call := int(p.calls.Add(1))
	const chunks = 3
	result := &embed.DocumentChunkResult{Chunks: make([]string, chunks), Embeddings: make([][]float32, chunks), Model: p.Model()}
	for i := range result.Chunks {
		result.Chunks[i] = "chunk"
		v := make([]float32, l2E53_4Dims)
		for j := range v {
			v[j] = float32(call*1000 + i*l2E53_4Dims + j + 1)
		}
		result.Embeddings[i] = v
	}
	return result, nil
}

var _ embed.Embedder = (*l2E53_4Provider)(nil)

func l2E53_4Config(async bool) *nornicdb.Config {
	c := nornicdb.DefaultConfig()
	c.Memory.DecayEnabled = false
	c.Memory.AutoLinksEnabled = false
	c.Memory.EmbeddingEnabled = true
	c.Memory.EmbeddingDimensions = l2E53_4Dims
	c.EmbeddingWorker.PropertiesInclude = []string{"text"}
	c.EmbeddingWorker.IncludeLabels = false
	c.EmbeddingWorker.NumWorkers = 1
	c.EmbeddingWorker.MaxRetries = 0
	c.EmbeddingWorker.BatchDelay = 0
	c.EmbeddingWorker.TriggerDebounceDelay = 0
	c.Database.StrictDurability = false
	c.Database.WALAutoCompactionEnabled = false
	c.Database.WALSnapshotInterval = 200 * time.Millisecond
	c.Database.AsyncWritesEnabled = async
	return c
}

type l2E53_4Round struct {
	createdChunks int
	createdFirst  float32
	createdHas    any
	cleared       int
	clearErr      error
	afterChunks   int
	afterNeeds    bool
	reembedded    bool
	providerCalls int32
	reembedWait   time.Duration
}

func (r l2E53_4Round) String() string {
	return fmt.Sprintf("created_chunks=%d first=%v has_embedding=%v cleared=%d clear_err=%v after_chunks=%d after_needs_embedding=%v reembedded=%v provider_calls=%d reembed_wait=%s",
		r.createdChunks, r.createdFirst, r.createdHas, r.cleared, r.clearErr, r.afterChunks, r.afterNeeds, r.reembedded, r.providerCalls, r.reembedWait)
}

func l2E53_4Node(t *testing.T, db *nornicdb.DB) *storage.Node {
	t.Helper()
	ctx := context.Background()
	nodes, err := db.GetStorage().GetNodesByLabel(ctx, "Doc")
	if err != nil {
		t.Fatalf("GetNodesByLabel: %v", err)
	}
	for _, n := range nodes {
		if n.Properties["key"] == "d" {
			full, err := db.GetStorage().GetNode(ctx, n.ID)
			if err != nil {
				t.Fatalf("GetNode: %v", err)
			}
			return full
		}
	}
	t.Fatalf("no Doc node with key 'd' (%d Doc nodes)", len(nodes))
	return nil
}

// l2E53_4RunRound: steps 1-5 on a fresh store. The 2-minute re-embed wait is
// only spent when the clear reported the node (a clear that reported 0 is
// already decisive and leaves nothing to re-embed).
func l2E53_4RunRound(t *testing.T, async bool) l2E53_4Round {
	t.Helper()
	ctx := context.Background()
	db, err := nornicdb.Open(filepath.Join(t.TempDir(), "data"), l2E53_4Config(async))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	provider := &l2E53_4Provider{}
	db.SetEmbedder(provider)
	if _, err := db.EnsureSearchIndexes(ctx, "nornic", nil, nornicdb.SearchIndexBuildWait); err != nil {
		t.Fatalf("EnsureSearchIndexes: %v", err)
	}

	if _, err := db.ExecuteCypher(ctx, "CREATE (:Doc {key:'d', text:'small text'}) WITH EMBEDDING", nil); err != nil {
		t.Fatalf("CREATE WITH EMBEDDING: %v", err)
	}
	var r l2E53_4Round
	created := l2E53_4Node(t, db)
	r.createdChunks = len(created.ChunkEmbeddings)
	if r.createdChunks > 0 && len(created.ChunkEmbeddings[0]) > 0 {
		r.createdFirst = created.ChunkEmbeddings[0][0]
	}
	r.createdHas = created.EmbedMeta["has_embedding"]
	if r.createdChunks == 0 {
		t.Fatalf("harness: CREATE ... WITH EMBEDDING stored no vector (%s)", r)
	}

	r.cleared, r.clearErr = db.ClearAllEmbeddings()
	after := l2E53_4Node(t, db)
	r.afterChunks = len(after.ChunkEmbeddings)
	r.afterNeeds = storage.NodeNeedsEmbedding(after)

	if r.cleared == 1 && r.clearErr == nil {
		start := time.Now()
		deadline := start.Add(2 * time.Minute)
		for time.Now().Before(deadline) {
			wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			_ = db.WaitForEmbeddings(wctx)
			cancel()
			n := l2E53_4Node(t, db)
			if len(n.ChunkEmbeddings) == 3 && provider.calls.Load() > 0 {
				r.reembedded = true
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		r.reembedWait = time.Since(start)
	}
	r.providerCalls = provider.calls.Load()
	return r
}

// Repro: asynchronous writes, 3 rounds, each on a fresh store. Defect: a
// round in which the clear reports 0 nodes or leaves the vector.
func TestL2_e53_4_Repro(t *testing.T) {
	defects := 0
	for round := 1; round <= 3; round++ {
		r := l2E53_4RunRound(t, true)
		bad := r.clearErr != nil || r.cleared != 1 || r.afterChunks != 0 || !r.reembedded
		t.Logf("round %d async: %s defect=%v", round, r, bad)
		if bad {
			defects++
		}
	}
	if defects > 0 {
		t.Logf("L2RESULT E53.4 DEFECT async rounds_with_defect=%d/3", defects)
		t.Errorf("asynchronous writes: %d of 3 rounds did not clear (or re-embed) the node created just before", defects)
		return
	}
	t.Logf("L2RESULT E53.4 OK async rounds_with_defect=0/3")
}

// Control: synchronous writes, 3 rounds: the clear reports 1, the node has no
// vectors and needs embedding, and the worker re-embeds it within 2 minutes.
func TestL2_e53_4_Control(t *testing.T) {
	for round := 1; round <= 3; round++ {
		r := l2E53_4RunRound(t, false)
		t.Logf("round %d sync: %s", round, r)
		if r.clearErr != nil || r.cleared != 1 || r.afterChunks != 0 || !r.afterNeeds || !r.reembedded {
			t.Errorf("control round %d: want cleared=1, no vectors, needs embedding, re-embedded; got %s", round, r)
		}
	}
}

// Cause: the entry's likely mechanism (not established there) is that the
// clear's selection scan reads the physical store before the asynchronous
// create has left the write-behind buffer. At the pin
// BadgerEngine.clearEmbeddingsForPrefix (pkg/storage/badger_stats.go) calls
// FlushWriteBehind before its scan. This diagnostic counts the physical
// Badger records that name the node right after the create (0 = the create
// rests only in the buffer), then runs the clear and logs its count, in both
// write modes; then the same with an explicit FlushWriteBehind before the
// clear (the seam that bypasses the buffered state).
func TestL2_e53_4_Cause(t *testing.T) {
	for _, tc := range []struct {
		async, flush bool
	}{{true, false}, {true, true}, {false, false}} {
		ctx := context.Background()
		db, err := nornicdb.Open(filepath.Join(t.TempDir(), "data"), l2E53_4Config(tc.async))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		provider := &l2E53_4Provider{}
		db.SetEmbedder(provider)
		if _, err := db.EnsureSearchIndexes(ctx, "nornic", nil, nornicdb.SearchIndexBuildWait); err != nil {
			_ = db.Close()
			t.Fatalf("EnsureSearchIndexes: %v", err)
		}
		if _, err := db.ExecuteCypher(ctx, "CREATE (:Doc {key:'d', text:'small text'}) WITH EMBEDDING", nil); err != nil {
			_ = db.Close()
			t.Fatalf("CREATE WITH EMBEDDING: %v", err)
		}
		node := l2E53_4Node(t, db)
		qualified := string(node.ID)
		if scoped, ok := db.GetStorage().(*storage.NamespacedEngine); ok {
			qualified = string(scoped.PrefixNodeID(node.ID))
		}
		records := -1
		badgerEngine, isBadger := storage.UnwrapEngine(db.GetBaseStorageForManager()).(*storage.BadgerEngine)
		if isBadger && badgerEngine.DB() != nil {
			records = 0
			suffix := []byte(qualified)
			_ = badgerEngine.DB().View(func(txn *badger.Txn) error {
				it := txn.NewIterator(badger.DefaultIteratorOptions)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					if bytes.HasSuffix(it.Item().Key(), suffix) {
						records++
					}
				}
				return nil
			})
		}
		flushErr := error(nil)
		if tc.flush && isBadger {
			flushErr = badgerEngine.FlushWriteBehind(ctx)
		}
		cleared, clearErr := db.ClearAllEmbeddings()
		after := l2E53_4Node(t, db)
		t.Logf("CAUSE async=%v explicit_flush=%v (err %v) physical_records_before_clear=%d cleared=%d clear_err=%v after_chunks=%d",
			tc.async, tc.flush, flushErr, records, cleared, clearErr, len(after.ChunkEmbeddings))
		_ = db.Close()
	}
}
