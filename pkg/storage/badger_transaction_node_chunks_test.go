package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

func TestTransactionNodeUpdateChunkPersistence(t *testing.T) {
	vector := func(value float32) []float32 {
		v := make([]float32, 16000)
		for i := range v {
			v[i] = value
		}
		return v
	}
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
				original := &Node{ID: "test:chunks", Labels: []string{"Doc"}, Properties: map[string]any{"text": "same"}, CreatedAt: time.Unix(1700000000, 0).UTC(), UpdatedAt: time.Unix(1700000000, 1).UTC(), ChunkEmbeddings: [][]float32{vector(0.25), vector(0.5)}, EmbedMeta: map[string]any{"embedding_model": "original", "chunk_count": 2}}
				_, err := b.CreateNode(original)
				require.NoError(t, err)
				originalStored, err := b.GetStoredNode(original.ID)
				require.NoError(t, err)
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				require.NoError(t, tx.SetNamespace("test"))
				update := func(node *Node) error { return tx.UpdateGraphNode(node, ClientNodeEdits) }

				first := CopyNode(originalStored)
				first.ChunkEmbeddings = [][]float32{vector(1), vector(2)}
				first.EmbedMeta = map[string]any{"embedding_model": "first", "chunk_count": 2}
				require.NoError(t, update(first))
				final := CopyNode(first)
				final.EmbedMeta = map[string]any{"embedding_model": "final", "chunk_count": 1}
				switch outcome {
				case "grow":
					final.ChunkEmbeddings = [][]float32{vector(3)}
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
				got, err := b.GetStoredNode(original.ID)
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
	_, err = engine.CreateNode(&Node{ID: "test:bench-node", Labels: []string{"L"}, Properties: map[string]any{"x": 0}})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := engine.BeginTransaction()
		if err != nil {
			b.Fatal(err)
		}
		if err := tx.UpdateGraphNode(&Node{ID: "test:bench-node", Labels: []string{"L"}, Properties: map[string]any{"x": i}}, ClientNodeEdits); err != nil {
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}
