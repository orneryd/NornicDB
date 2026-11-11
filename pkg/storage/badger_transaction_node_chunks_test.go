package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// makeStoredChunkVector constructs a chunk larger than the inline node budget.
func makeStoredChunkVector(value float32) []float32 {
	v := make([]float32, 16000)
	for i := range v {
		v[i] = value
	}
	return v
}

func TestTransactionNodeUpdateChunkPersistence(t *testing.T) {
	for _, outcome := range []string{"grow", "shrink", "invalidate"} {
		for _, rollback := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rollback=%v", outcome, rollback), func(t *testing.T) {
				dir := t.TempDir()
				open := func() *BadgerEngine {
					b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
					require.NoError(t, err)
					b.SetEmbeddingsEnabled(true)
					b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
					return b
				}
				b := open()
				original := &Node{ID: "test:chunks", Labels: []string{"Doc"}, Properties: map[string]any{"text": "same"}, CreatedAt: time.Unix(1700000000, 0).UTC(), UpdatedAt: time.Unix(1700000000, 1).UTC(), ChunkEmbeddings: [][]float32{makeStoredChunkVector(0.25), makeStoredChunkVector(0.5)}, EmbedMeta: map[string]any{"embedding_model": "original", "chunk_count": 2}}
				_, err := b.CreateNode(t.Context(), original)
				require.NoError(t, err)
				originalStored, err := b.GetStoredNode(t.Context(), original.ID)
				require.NoError(t, err)
				tx, err := b.BeginTransaction(t.Context())
				require.NoError(t, err)
				defer tx.Rollback()
				require.NoError(t, tx.SetNamespace("test"))
				update := func(node *Node) error {
					return tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: node, Policy: ClientNodeEdits})
				}

				first := CopyNode(originalStored)
				first.ChunkEmbeddings = [][]float32{makeStoredChunkVector(1), makeStoredChunkVector(2)}
				first.EmbedMeta = map[string]any{"embedding_model": "first", "chunk_count": 2}
				require.NoError(t, update(first))
				final := CopyNode(first)
				final.EmbedMeta = map[string]any{"embedding_model": "final", "chunk_count": 1}
				switch outcome {
				case "grow":
					final.ChunkEmbeddings = [][]float32{makeStoredChunkVector(3)}
				case "shrink":
					final.ChunkEmbeddings = [][]float32{{4, 5}}
				case "invalidate":
					final.Properties["text"] = "changed"

				}
				require.NoError(t, update(final))
				wantVectors := final.ChunkEmbeddings
				wantModel := "final"
				if outcome == "invalidate" {
					wantVectors = nil
					wantModel = ""
				}
				if rollback {
					require.NoError(t, tx.Rollback())
					wantVectors = originalStored.ChunkEmbeddings
					wantModel = "original"
				} else {
					require.NoError(t, tx.CommitContext(context.Background()))
				}
				require.NoError(t, b.Close())
				b = open()
				got, err := b.GetStoredNode(t.Context(), original.ID)
				require.NoError(t, err)
				require.Equal(t, wantVectors, got.ChunkEmbeddings)
				if wantModel == "" {
					require.Nil(t, got.EmbedMeta)
				} else {
					require.Equal(t, wantModel, got.EmbedMeta["embedding_model"])
				}
				if !rollback {
					require.NoError(t, b.withView(func(snapshot *badger.Txn) error {
						if outcome == "grow" {
							_, err := snapshot.Get(embeddingKey(original.ID, 1))
							require.ErrorIs(t, err, badger.ErrKeyNotFound)
						} else {
							keys := 0
							err := visitEmbeddingKeys(snapshot, original.ID, func([]byte) error { keys++; return nil })
							require.NoError(t, err)
							require.Zero(t, keys, "inline or invalidated image must not retain old committed/pending chunk and sidecar keys")
						}
						return nil
					}))
				}
			})
		}
	}
}

func BenchmarkTransactionNodeUpdateWithoutVectors(b *testing.B) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngine(b.TempDir()))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = engine.Close() })
	_, err = engine.CreateNode(b.Context(), &Node{ID: "test:bench-node", Labels: []string{"L"}, Properties: map[string]any{"x": 0}})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := engine.BeginTransaction(b.Context())
		if err != nil {
			b.Fatal(err)
		}
		if err := tx.UpdateGraphNode(b.Context(), NodeUpdateRequest{After: &Node{ID: "test:bench-node", Labels: []string{"L"}, Properties: map[string]any{"x": i}}, Policy: ClientNodeEdits}); err != nil {
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTransactionNodeUpdateSeparateEmbeddings measures one public
// write-behind update whose node keeps its vectors in the embedding sidecar,
// with the buffer drained between updates so the predecessor comes from
// storage. It extends the update owner's bounded allocation reference
// (BenchmarkTransactionNodeUpdateWithoutVectors) to separately stored
// embeddings in a small and a large payload, and of a node whose text is too
// large to keep its vectors inline. The operation covers the whole
// public update (staging, validation, publication and flushing). Compare
// revisions with identical fixtures; this does not isolate validation latency.
func BenchmarkTransactionNodeUpdateSeparateEmbeddings(b *testing.B) {
	for _, fixture := range []struct {
		name   string
		chunks [][]float32
		text   string
	}{
		{name: "small", chunks: [][]float32{make([]float32, 8)}, text: "same"},
		{name: "large", chunks: [][]float32{makeStoredChunkVector(0.25), makeStoredChunkVector(0.5)}, text: "same"},
		{name: "large text", chunks: [][]float32{make([]float32, 8)}, text: strings.Repeat("large text ", 6000)},
	} {
		b.Run(fixture.name, func(b *testing.B) {
			ctx := context.Background()
			engine := newWriteBehindTestEngine(b, time.Hour, writeBehindTestOptions{manualDrain: true})
			engine.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
			node := &Node{ID: "test:bench-preimage", Labels: []string{"Doc"}, Properties: map[string]any{"text": fixture.text, "n": int64(0)}}
			if _, err := engine.CreateNode(ctx, node); err != nil {
				b.Fatal(err)
			}
			if err := engine.FlushWriteBehind(ctx); err != nil {
				b.Fatal(err)
			}
			embedded := embeddingWriteback(b, engine, node.ID, fixture.chunks, map[string]any{"chunk_count": len(fixture.chunks)}, time.Time{})
			if err := engine.UpdateNodeEmbeddingSidecar(ctx, embedded); err != nil {
				b.Fatal(err)
			}
			if err := engine.FlushWriteBehind(ctx); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx, err := engine.BeginTransaction(ctx)
				if err != nil {
					b.Fatal(err)
				}
				if err := tx.SetImplicit(true); err != nil {
					b.Fatal(err)
				}
				if err := tx.SetNamespace("test"); err != nil {
					b.Fatal(err)
				}
				after := &Node{ID: node.ID, Labels: []string{"Doc"}, Properties: map[string]any{"text": fixture.text, "n": int64(i + 1)}}
				if err := tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: after, Policy: ClientNodeEdits}); err != nil {
					b.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					b.Fatal(err)
				}
				if err := engine.FlushWriteBehind(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
