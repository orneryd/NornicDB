//go:build closeout_l2_e53_9

package storage_test

// E53.9: a large node without vectors reports its embedding metadata
// differently by read path (engine read, transaction read, read after reopen).

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type l2E53_9Read struct {
	name   string
	meta   map[string]any
	chunks int
}

func l2E53_9Describe(rs []l2E53_9Read) string {
	parts := make([]string, 0, len(rs))
	for _, r := range rs {
		parts = append(parts, fmt.Sprintf("%s: EmbedMeta=%v (nil=%v) chunks=%d", r.name, r.meta, r.meta == nil, r.chunks))
	}
	return strings.Join(parts, "; ")
}

// l2E53_9SameMeta treats a nil and an empty map as the same metadata; any
// other difference (a key such as chunk_count present on one read only)
// counts as a difference.
func l2E53_9SameMeta(a, b map[string]any) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return reflect.DeepEqual(a, b)
}

// l2E53_9ThreeReads writes the node, then reads it A) through the engine,
// B) through a transaction pinned to the namespace, C) through the engine
// after a close and reopen of the same directory.
func l2E53_9ThreeReads(t *testing.T, text string) []l2E53_9Read {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	id := storage.NodeID("nornic:large")

	b, err := storage.NewBadgerEngine(dir)
	if err != nil {
		t.Fatalf("NewBadgerEngine: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = b.Close()
		}
	}()
	if _, err := b.CreateNode(ctx, &storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": text}}); err != nil {
		t.Fatalf("CreateNode: %v", err)
	}

	var reads []l2E53_9Read

	a, err := b.GetNode(ctx, id)
	if err != nil {
		t.Fatalf("read A GetNode: %v", err)
	}
	reads = append(reads, l2E53_9Read{"A engine GetNode", a.EmbedMeta, len(a.ChunkEmbeddings)})

	tx, err := b.BeginTransaction(ctx, storage.OrdinaryBegin)
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	if err := tx.SetNamespace("nornic"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("SetNamespace: %v", err)
	}
	bn, err := tx.GetNode(ctx, id)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("read B tx.GetNode: %v", err)
	}
	reads = append(reads, l2E53_9Read{"B transaction GetNode", bn.EmbedMeta, len(bn.ChunkEmbeddings)})
	_ = tx.Rollback()

	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	closed = true

	b2, err := storage.NewBadgerEngine(dir)
	if err != nil {
		t.Fatalf("reopen NewBadgerEngine: %v", err)
	}
	defer b2.Close()
	c, err := b2.GetNode(ctx, id)
	if err != nil {
		t.Fatalf("read C GetNode after reopen: %v", err)
	}
	reads = append(reads, l2E53_9Read{"C GetNode after reopen", c.EmbedMeta, len(c.ChunkEmbeddings)})
	return reads
}

func l2E53_9Check(t *testing.T, id string, reads []l2E53_9Read) {
	t.Helper()
	same := true
	for _, r := range reads[1:] {
		if !l2E53_9SameMeta(reads[0].meta, r.meta) || reads[0].chunks != r.chunks {
			same = false
		}
	}
	if same {
		t.Logf("L2RESULT %s OK %s", id, l2E53_9Describe(reads))
		return
	}
	t.Logf("L2RESULT %s DEFECT %s", id, l2E53_9Describe(reads))
	t.Errorf("embedding metadata differs by read path: %s", l2E53_9Describe(reads))
}

// Repro: a 1,300,000-byte text property makes the body larger than the
// inline node size limit (50 KB at the pin), so it is encoded by the
// "large body" branch.
func TestL2_e53_9_Repro(t *testing.T) {
	reads := l2E53_9ThreeReads(t, strings.Repeat("x", 1300000))
	l2E53_9Check(t, "E53.9", reads)
}

// Control: the same node with a small text (inline body); all three reads
// must agree.
func TestL2_e53_9_Control(t *testing.T) {
	reads := l2E53_9ThreeReads(t, "small text")
	l2E53_9Check(t, "E53.9-control", reads)
}

// Cause: the large-body encoder (encodeNodeInTxn / encodeNodeV1Body,
// pkg/storage/badger_helpers.go) adds EmbedMeta["chunk_count"] = 0 to a body
// stored with "embeddings stored separately", while the engine's first read
// is served from the write's own image (no codec). This logs the other read
// paths of the same node (stored read, query-metadata read) before and after
// reopen, so the branch that adds the key is visible.
func TestL2_e53_9_Cause(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	id := storage.NodeID("nornic:large")
	text := strings.Repeat("x", 1300000)

	b, err := storage.NewBadgerEngine(dir)
	if err != nil {
		t.Fatalf("NewBadgerEngine: %v", err)
	}
	if _, err := b.CreateNode(ctx, &storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": text}}); err != nil {
		_ = b.Close()
		t.Fatalf("CreateNode: %v", err)
	}
	logPaths := func(stage string, e *storage.BadgerEngine) {
		if n, err := e.GetNode(ctx, id); err == nil {
			t.Logf("CAUSE %s GetNode EmbedMeta=%v nil=%v chunks=%d", stage, n.EmbedMeta, n.EmbedMeta == nil, len(n.ChunkEmbeddings))
		} else {
			t.Logf("CAUSE %s GetNode err=%v", stage, err)
		}
		if n, err := e.GetStoredNode(ctx, id); err == nil {
			t.Logf("CAUSE %s GetStoredNode EmbedMeta=%v nil=%v chunks=%d", stage, n.EmbedMeta, n.EmbedMeta == nil, len(n.ChunkEmbeddings))
		} else {
			t.Logf("CAUSE %s GetStoredNode err=%v", stage, err)
		}
		if n, err := e.GetQueryNode(ctx, id); err == nil {
			t.Logf("CAUSE %s GetQueryNode EmbedMeta=%v nil=%v chunks=%d", stage, n.EmbedMeta, n.EmbedMeta == nil, len(n.ChunkEmbeddings))
		} else {
			t.Logf("CAUSE %s GetQueryNode err=%v", stage, err)
		}
	}
	logPaths("before-reopen", b)
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	b2, err := storage.NewBadgerEngine(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer b2.Close()
	logPaths("after-reopen", b2)

	// Size threshold: the same check at a text just under and just over the
	// inline limit, so the switch is shown to be the body size.
	for _, n := range []int{40 * 1024, 60 * 1024} {
		reads := l2E53_9ThreeReads(t, strings.Repeat("y", n))
		t.Logf("CAUSE text=%d bytes: %s", n, l2E53_9Describe(reads))
	}
}
