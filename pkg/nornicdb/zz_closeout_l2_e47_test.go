//go:build closeout_l2_e47

package nornicdb_test

// E47: when the journal refuses an embedding clear, vector search must still
// find the nodes, which keep their vectors.

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const l2E47Dims = 16

func l2E47Config() *nornicdb.Config {
	c := nornicdb.DefaultConfig()
	c.Database.AsyncWritesEnabled = false
	c.Database.WALAutoCompactionEnabled = false
	c.Database.WALSnapshotInterval = 50 * time.Millisecond
	c.Memory.DecayEnabled = false
	c.Memory.AutoLinksEnabled = false
	c.Memory.EmbeddingEnabled = false
	c.Memory.EmbeddingDimensions = l2E47Dims
	c.EmbeddingWorker.PropertiesInclude = []string{"text"}
	c.EmbeddingWorker.IncludeLabels = false
	c.EmbeddingWorker.NumWorkers = 1
	c.EmbeddingWorker.MaxRetries = 0
	c.EmbeddingWorker.BatchDelay = 0
	c.EmbeddingWorker.TriggerDebounceDelay = 0
	return c
}

func l2E47Vector() []float32 {
	v := make([]float32, l2E47Dims)
	v[0] = 1
	return v
}

func l2E47Hits(t *testing.T, svc *search.Service) (int, error) {
	t.Helper()
	opts := search.DefaultSearchOptions()
	opts.Limit = 5
	resp, err := svc.Search(context.Background(), "", l2E47Vector(), opts)
	if err != nil {
		return 0, err
	}
	return len(resp.Results), nil
}

// l2E47WAL finds the journal wrapper under the database's base storage.
func l2E47WAL(t *testing.T, db *nornicdb.DB) *storage.WALEngine {
	t.Helper()
	engine := db.GetBaseStorageForManager()
	for depth := 0; depth < 16 && engine != nil; depth++ {
		if w, ok := engine.(*storage.WALEngine); ok {
			return w
		}
		u, ok := engine.(storage.EngineUnwrapper)
		if !ok {
			break
		}
		engine = u.GetInnerEngine()
	}
	t.Fatalf("no *storage.WALEngine under the base storage (%T)", db.GetBaseStorageForManager())
	return nil
}

type l2E47Outcome struct {
	hitsBefore  int
	cleared     int
	clearErr    error
	chunksAfter int
	hitsAfter   int
	searchErr   error
	countBefore int
	countAfter  int
}

// l2E47Run: steps 1-5 of the entry. refuse=true closes the journal before
// the clear (the entry's case); preClearIndex=true additionally empties the
// vector index the way the 2dbc67e96 code did before running the clear (the
// Cause diagnostic).
func l2E47Run(t *testing.T, refuse, preClearIndex bool) l2E47Outcome {
	t.Helper()
	ctx := context.Background()
	db, err := nornicdb.Open(filepath.Join(t.TempDir(), "data"), l2E47Config())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.GetStorage().CreateNode(ctx, &storage.Node{
		ID:              "v",
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"key": "v", "text": "t"},
		ChunkEmbeddings: [][]float32{l2E47Vector()},
		EmbedMeta:       map[string]any{"has_embedding": true},
	}); err != nil {
		t.Fatalf("CreateNode: %v", err)
	}
	svc, err := db.GetOrCreateSearchService("nornic", db.GetStorage())
	if err != nil {
		t.Fatalf("GetOrCreateSearchService: %v", err)
	}
	if err := svc.BuildIndexes(ctx); err != nil {
		t.Fatalf("BuildIndexes: %v", err)
	}
	var out l2E47Outcome
	// The build may still be finishing (the ANTLR job saw "search index being
	// built"): retry the precondition search, bounded, until it answers.
	readyDeadline := time.Now().Add(60 * time.Second)
	for {
		out.hitsBefore, err = l2E47Hits(t, svc)
		if err == nil || time.Now().After(readyDeadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("search before the clear (not ready within 60 s): %v", err)
	}
	out.countBefore = svc.EmbeddingCount()

	if refuse {
		if err := l2E47WAL(t, db).GetWAL().Close(); err != nil {
			t.Logf("closing the journal: %v", err)
		}
	}
	if preClearIndex {
		if err := svc.ClearVectorIndex(); err != nil {
			t.Fatalf("ClearVectorIndex: %v", err)
		}
	}
	out.cleared, out.clearErr = db.ClearAllEmbeddings()

	node, err := db.GetStorage().GetNode(ctx, "v")
	if err != nil {
		t.Fatalf("read node after the clear: %v", err)
	}
	out.chunksAfter = len(node.ChunkEmbeddings)

	// The index follows the store through refreshes; poll (bounded) until the
	// search agrees with the stored state or the deadline passes.
	want := 0
	if out.chunksAfter > 0 {
		want = 1
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		out.hitsAfter, out.searchErr = l2E47Hits(t, svc)
		if (out.searchErr == nil && out.hitsAfter == want) || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	out.countAfter = svc.EmbeddingCount()
	return out
}

func l2E47Line(o l2E47Outcome) []any {
	return []any{o.hitsBefore, o.countBefore, o.cleared, o.clearErr, o.chunksAfter, o.hitsAfter, o.searchErr, o.countAfter}
}

const l2E47Format = "hits_before=%d count_before=%d cleared=%d clear_err=%v node_chunks_after=%d hits_after=%d search_err=%v embedding_count_after=%d"

func TestL2_e47_Repro(t *testing.T) {
	o := l2E47Run(t, true, false)
	if o.hitsBefore != 1 {
		t.Fatalf("harness: search before the clear found %d (want 1)", o.hitsBefore)
	}
	if o.clearErr == nil {
		t.Logf("note: the journal did not refuse the clear (cleared=%d); the entry's condition was not produced", o.cleared)
	}
	// Defect: the node keeps its vector but search no longer finds it.
	defect := o.chunksAfter > 0 && (o.searchErr != nil || o.hitsAfter != 1)
	if defect {
		t.Logf("L2RESULT E47 DEFECT "+l2E47Format, l2E47Line(o)...)
		t.Errorf("after a refused clear the node keeps %d chunk vector(s) but search returns %d (err %v), EmbeddingCount %d", o.chunksAfter, o.hitsAfter, o.searchErr, o.countAfter)
		return
	}
	if o.chunksAfter == 0 && o.clearErr != nil {
		// The clear reported a failure but the vector is gone: not the entry's
		// defect, but store and error disagree; report as a defect too.
		t.Logf("L2RESULT E47 DEFECT "+l2E47Format, l2E47Line(o)...)
		t.Errorf("the clear failed (%v) but the node lost its vectors", o.clearErr)
		return
	}
	t.Logf("L2RESULT E47 OK "+l2E47Format, l2E47Line(o)...)
}

// Control: the same sequence with the journal open: the clear succeeds, the
// node has no vectors and search returns nothing.
func TestL2_e47_Control(t *testing.T) {
	o := l2E47Run(t, false, false)
	t.Logf("L2RESULT E47-control "+l2E47Format, l2E47Line(o)...)
	if o.hitsBefore != 1 || o.clearErr != nil || o.cleared != 1 || o.chunksAfter != 0 || o.searchErr != nil || o.hitsAfter != 0 {
		t.Errorf("control: want hits_before=1 cleared=1 no error, 0 chunks, 0 hits; got "+l2E47Format, l2E47Line(o)...)
	}
}

// Cause: the entry names clearEmbeddings (pkg/nornicdb/db.go at 2dbc67e96)
// calling svc.ClearVectorIndex() before the journaled clear. At the pin
// clearEmbeddings no longer calls it (the index follows each reset through
// the storage commit). This test-side copy of the old order (ClearVectorIndex,
// then the refused clear) shows the failure exactly at that step: the index
// is empty while the node keeps its vector. It runs the pin's order too.
func TestL2_e47_Cause(t *testing.T) {
	old := l2E47Run(t, true, true)
	t.Logf("CAUSE old order (index cleared first): "+l2E47Format, l2E47Line(old)...)
	pin := l2E47Run(t, true, false)
	t.Logf("CAUSE pin order (no index clear): "+l2E47Format, l2E47Line(pin)...)
}
