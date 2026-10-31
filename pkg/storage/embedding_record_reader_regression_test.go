package storage

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEmbeddingRecordReaderAppendsPersistedAndStagedValues(t *testing.T) {
	engine := newTestEngine(t)
	key := []byte("embedding-reader-append")
	require.NoError(t, engine.withUpdate(updateRequest{}, func(txn *badger.Txn) error { return txn.Set(key, []byte("disk")) }))
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		for _, tc := range []struct {
			name   string
			commit *bufferedCommit
			want   string
		}{
			{"persisted", nil, "prefix:disk"},
			{"staged", &bufferedCommit{writes: map[string][]byte{string(key): []byte("stage")}}, "prefix:stage"},
			{"staged empty", &bufferedCommit{writes: map[string][]byte{string(key): {}}}, "prefix:"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got, err := (embeddingRecordReader{txn: txn, commit: tc.commit}).read(key, []byte("prefix:"))
				require.NoError(t, err)
				require.Equal(t, tc.want, string(got))
			})
		}
		_, err := (embeddingRecordReader{txn: txn, commit: &bufferedCommit{deletes: map[string]bool{string(key): true}}}).read(key, nil)
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
}

func TestEmbeddingRecordReaderReassemblesPersistedAndMixedShards(t *testing.T) {
	for _, mode := range []string{"persisted", "staged", "mixed"} {
		t.Run(mode, func(t *testing.T) {
			engine := newTestEngine(t)
			id := NodeID("test:reader-shards")
			vector := []float32{1.25, -2.5, 3.75, 0}
			encoded, err := encodeEmbedding(vector)
			require.NoError(t, err)
			parts := [][]byte{encoded[:len(encoded)/3], encoded[len(encoded)/3 : 2*len(encoded)/3], encoded[2*len(encoded)/3:]}
			commit := &bufferedCommit{writes: map[string][]byte{}}
			require.NoError(t, engine.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
				require.NoError(t, txn.Set(embeddingKey(id, 0), makeEmbeddingShardMarker(len(parts))))
				for i, part := range parts {
					key := embeddingChunkPartKey(id, 0, i)
					if mode == "staged" || (mode == "mixed" && i == 1) {
						commit.writes[string(key)] = part
					} else {
						require.NoError(t, txn.Set(key, part))
					}
				}
				return nil
			}))
			require.NoError(t, engine.withView(func(txn *badger.Txn) error {
				node := &Node{ID: id}
				require.NoError(t, loadEmbeddingChunksFrom(embeddingRecordReader{txn: txn, commit: commit}, node, id, 1))
				require.Equal(t, [][]float32{vector}, node.ChunkEmbeddings)
				return nil
			}))
		})
	}
}
