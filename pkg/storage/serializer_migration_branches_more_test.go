package storage

import (
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func writeLegacyGobEmbedding(t *testing.T, db *badger.DB, nodeID NodeID, chunk int, emb []float32) {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(emb))
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set(embeddingKey(nodeID, chunk), buf.Bytes())
	}))
}

func TestMigrateBadgerToMsgpackWithDB_AdditionalErrorAndEdgeBranches(t *testing.T) {
	t.Run("detect serializer failure bubbles up", func(t *testing.T) {
		db := openTestBadgerDB(t)
		require.NoError(t, db.Update(func(txn *badger.Txn) error {
			badHeader := append([]byte(serializationMagic), byte(99), serializerIDMsgpack)
			return txn.Set(nodeKey(NodeID("test:bad-hdr")), badHeader)
		}))

		_, err := MigrateBadgerToMsgpackWithDB(db, "dir", SerializerMigrationOptions{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported serialization version")
	})

	t.Run("id dictionary load failure in compact-edge path", func(t *testing.T) {
		db := openTestBadgerDB(t)

		// Seed legacy data so hasData=true.
		writeLegacyGobNode(t, db, &Node{ID: NodeID("test:n1"), Labels: []string{"L"}})
		// Corrupt id-dictionary forward value => loadFromBadger fails.
		require.NoError(t, db.Update(func(txn *badger.Txn) error {
			return txn.Set(nodeIDForwardKey(NodeID("test:bad-dict")), []byte{0x01})
		}))

		_, err := MigrateBadgerToMsgpackWithDB(db, "dir", SerializerMigrationOptions{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "loading id dictionary for migration")
	})

	t.Run("converts legacy embeddings and tracks stats", func(t *testing.T) {
		db := openTestBadgerDB(t)

		writeLegacyGobNode(t, db, &Node{
			ID:         NodeID("test:n-embed"),
			Labels:     []string{"Doc"},
			Properties: map[string]any{"title": "x"},
		})
		writeLegacyGobEmbedding(t, db, NodeID("test:n-embed"), 0, []float32{1, 2, 3})
		writeLegacyGobEmbedding(t, db, NodeID("test:n-embed"), 1, []float32{4, 5})

		stats, err := MigrateBadgerToMsgpackWithDB(db, "dir", SerializerMigrationOptions{BatchSize: 1})
		require.NoError(t, err)
		require.GreaterOrEqual(t, stats.NodesConverted, 1)
		require.Equal(t, 2, stats.EmbeddingsConverted)
		require.GreaterOrEqual(t, stats.TotalScanned, 3)
	})
}

func TestMigratePrefixToMsgpack_HeaderAndEncodeErrorBranches(t *testing.T) {
	db := openTestBadgerDB(t)

	// Malformed header trips splitSerializationHeader error branch.
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		badHeader := append([]byte(serializationMagic), byte(77), serializerIDMsgpack)
		return txn.Set([]byte{prefixNode, 'n', 'x'}, badHeader)
	}))

	_, _, _, err := migratePrefixToMsgpack(
		db,
		func() migrationBatch { return db.NewWriteBatch() },
		prefixNode,
		"node",
		func(data []byte) (any, error) { return decodeNodeV1(data) },
		func(v any) ([]byte, error) { return encodeValue(v) },
		SerializerMigrationOptions{DryRun: true},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported serialization version")

	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte{prefixNode, 'n', 'x'})
	}))

	// Valid node decode + forced encode failure branch.
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		legacyNode := &Node{ID: NodeID("test:n-enc"), Labels: []string{"Doc"}}
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(legacyNode); err != nil {
			return err
		}
		return txn.Set([]byte{prefixNode, 'n', 'y'}, buf.Bytes())
	}))

	_, _, _, err = migratePrefixToMsgpack(
		db,
		func() migrationBatch { return db.NewWriteBatch() },
		prefixNode,
		"node",
		func(data []byte) (any, error) { return decodeNodeV1(data) },
		func(v any) ([]byte, error) { return nil, errTestForced("encode") },
		SerializerMigrationOptions{DryRun: true},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "encode node value")
}

type errTestForced string

func (e errTestForced) Error() string { return string(e) }
