package storage

// Batch 2 earlier-issue probes on the final build (research only, untracked).

import (
	"context"
	"reflect"
	"testing"
	"time"

	fixture "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// L507: closing the engine from a WAL settlement callback.
func TestZZEI2WALSettlementCanCloseEngine(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	base, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	dir := t.TempDir()
	wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "immediate"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	tx, err := base.BeginTransaction(t.Context())
	require.NoError(t, err)
	_, err = tx.CreateNode(t.Context(), &Node{ID: "test:callback-close", Properties: map[string]any{"v": "stored"}})
	require.NoError(t, err)
	p, err := tx.PrepareEntityCommit(t.Context(), EntityCommitRequest{LogicalID: "zzei2-callback-close"})
	require.NoError(t, err)
	closed := make(chan struct{})
	var closeErr error
	var waited time.Duration
	receipt, err := NewWALEngine(base, wal).SubmitEntityCommit(t.Context(), p, func(CommitOutcome) {
		start := time.Now()
		go func() { closeErr = base.Close(); close(closed) }()
		select {
		case <-closed:
		case <-time.After(2 * time.Second):
			t.Error("WAL settlement holds an engine lifetime lease: Close waits for the callback to return")
		}
		waited = time.Since(start)
	})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, receipt.Wait(ctx))
	select {
	case <-closed:
		t.Logf("ZZEI2 L507 Close inside the settlement callback: waited %s in the callback; close error=%v", waited, closeErr)
		require.NoError(t, closeErr)
	case <-ctx.Done():
		t.Fatal("Close failed to finish after callback returned")
	}
}

// L507 control: the native (non-WAL) settlement path.
func TestZZEI2NativeSettlementCanCloseEngine(t *testing.T) {
	base, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	tx, err := base.BeginTransaction(t.Context())
	require.NoError(t, err)
	_, err = tx.CreateNode(t.Context(), &Node{ID: "test:callback-close", Properties: map[string]any{"v": "stored"}})
	require.NoError(t, err)
	p, err := tx.PrepareEntityCommit(t.Context(), EntityCommitRequest{LogicalID: "zzei2-native-close"})
	require.NoError(t, err)
	closed := make(chan struct{})
	var closeErr error
	receipt, err := base.SubmitEntityCommit(t.Context(), p, func(CommitOutcome) {
		go func() { closeErr = base.Close(); close(closed) }()
		select {
		case <-closed:
		case <-time.After(2 * time.Second):
			t.Error("native settlement: Close waits for the callback")
		}
	})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, receipt.Wait(ctx))
	select {
	case <-closed:
		t.Logf("ZZEI2 L507 control native: close error=%v", closeErr)
	case <-ctx.Done():
		t.Fatal("native Close did not finish")
	}
}

// L512: full node streams and named vectors after the body cache warms.
func TestZZEI2FullStreamNamedVectors(t *testing.T) {
	for _, separate := range []bool{false, true} {
		t.Run(map[bool]string{false: "inline", true: "separate_chunks"}[separate], func(t *testing.T) {
			engine := fixture.Own(t, fixture.Track(t, NewMemoryEngine(), ErrStorageClosed))
			ctx := context.Background()
			node := &Node{ID: "test:n", NamedEmbeddings: map[string][]float32{"title": {1, 2}}, Properties: map[string]any{"text": "keep"}}
			if separate {
				node.ChunkEmbeddings = make([][]float32, 16)
				for i := range node.ChunkEmbeddings {
					node.ChunkEmbeddings[i] = make([]float32, 1536)
					node.ChunkEmbeddings[i][0] = 1
				}
				node.EmbedMeta = map[string]any{"chunk_count": int64(16)}
			}
			if _, err := engine.CreateNode(ctx, node); err != nil {
				t.Fatal(err)
			}
			for round := 0; round < 3; round++ {
				count := 0
				err := engine.StreamNodes(ctx, func(got *Node) error {
					count++
					t.Logf("ZZEI2 L512 %v round %d: named vectors=%v chunks=%d", separate, round, got.NamedEmbeddings, len(got.ChunkEmbeddings))
					if !reflect.DeepEqual(got.NamedEmbeddings, node.NamedEmbeddings) {
						t.Errorf("round %d named vectors = %v, want %v", round, got.NamedEmbeddings, node.NamedEmbeddings)
					}
					if len(got.ChunkEmbeddings) != len(node.ChunkEmbeddings) {
						t.Errorf("round %d chunks = %d", round, len(got.ChunkEmbeddings))
					}
					return nil
				})
				if err != nil || count != 1 {
					t.Fatalf("round %d count=%d err=%v", round, count, err)
				}
				full, err := engine.GetNode(ctx, "test:n")
				require.NoError(t, err)
				t.Logf("ZZEI2 L512 %v round %d: GetNode named vectors=%v", separate, round, full.NamedEmbeddings)
			}
		})
	}
}

// L518: MatchEdgesBetween predicates through a traced namespaced engine.
func TestZZEI2TracedMatcherIdentity(t *testing.T) {
	base := fixture.Own(t, fixture.Track(t, NewMemoryEngine(), ErrStorageClosed))
	ctx := context.Background()
	tenant := NewNamespacedEngine(base, "a")
	for _, id := range []NodeID{"x", "y"} {
		if _, err := tenant.CreateNode(ctx, &Node{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	if err := tenant.CreateEdge(ctx, &Edge{ID: "r", StartNode: "x", EndNode: "y", Type: "R"}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"traced-namespaced", "namespaced-traced", "namespaced"} {
		t.Run(name, func(t *testing.T) {
			var view Engine
			switch name {
			case "traced-namespaced":
				view = NewTracedEngine(tenant)
			case "namespaced-traced":
				view = NewNamespacedEngine(NewTracedEngine(base), "a")
			default:
				view = tenant
			}
			general, err := view.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentBetween, Node: "x", Other: "y"})
			if err != nil || len(general) != 1 {
				t.Fatalf("general: edges=%v err=%v", general, err)
			}
			want := general[0].Identity()
			var got EntityIdentity
			edges, err := MatchEdgesBetween(ctx, view, "x", "y", "R", nil, func(candidate *Edge) bool {
				got = candidate.Identity()
				return got == want
			})
			t.Logf("ZZEI2 L518 %s: adjacent read identity=%+v, matcher candidate identity=%+v, matched=%d err=%v", name, want, got, len(edges), err)
			if err != nil || len(edges) != 1 {
				t.Fatalf("source-qualified match: got identity=%+v want=%+v; matched=%v err=%v", got, want, edges, err)
			}
		})
	}
}

// L522 (private): traced chunk reads and manual suppression.
func TestZZEI2ChunkVisibility(t *testing.T) {
	base := fixture.Own(t, fixture.Track(t, NewMemoryEngine(), ErrStorageClosed))
	ctx := context.Background()
	tenant := NewNamespacedEngine(base, "a")
	for _, id := range []NodeID{"visible", "hidden"} {
		if _, err := tenant.CreateNode(ctx, &Node{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	if err := base.SetManualSuppression(ctx, StoredEntityNodeKind, "a:hidden", true); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"namespaced-traced", "namespaced"} {
		t.Run(name, func(t *testing.T) {
			view := NewNamespacedEngine(base, "a")
			if name == "namespaced-traced" {
				view = NewNamespacedEngine(NewTracedEngine(base), "a")
			}
			var nodes, chunks []NodeID
			if err := view.StreamNodes(ctx, func(node *Node) error { nodes = append(nodes, node.ID); return nil }); err != nil {
				t.Fatal(err)
			}
			if err := view.StreamNodeChunks(ctx, 10, func(batch []*Node) error {
				for _, node := range batch {
					chunks = append(chunks, node.ID)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			t.Logf("ZZEI2 L522 %s: StreamNodes=%v StreamNodeChunks=%v", name, nodes, chunks)
			if !reflect.DeepEqual(nodes, []NodeID{"visible"}) {
				t.Errorf("StreamNodes=%v", nodes)
			}
			if !reflect.DeepEqual(chunks, nodes) {
				t.Errorf("StreamNodeChunks=%v, want the visible StreamNodes population %v", chunks, nodes)
			}
		})
	}
}
