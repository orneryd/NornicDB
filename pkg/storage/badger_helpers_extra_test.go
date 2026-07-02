package storage

import (
	"bytes"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestMaxMVCCVersion_IsTopOfRange(t *testing.T) {
	v := maxMVCCVersion()
	// CommitSequence is the max uint64.
	require.Equal(t, ^uint64(0), v.CommitSequence)
	// CommitTimestamp is the max representable time.Time. Compare
	// against any other version: maxMVCCVersion must be greater.
	other := MVCCVersion{CommitTimestamp: time.Now(), CommitSequence: 1000}
	require.Equal(t, 1, v.Compare(other))
}

func TestExtractEdgeNumIDAndMVCCVersionFromVersionKey(t *testing.T) {
	v := MVCCVersion{
		CommitTimestamp: time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
		CommitSequence:  42,
	}
	key := mvccEdgeVersionKey(7, v)

	num, gotV, err := extractEdgeNumIDAndMVCCVersionFromVersionKey(key)
	require.NoError(t, err)
	require.Equal(t, uint64(7), num)
	require.Equal(t, 0, gotV.Compare(v))

	// Bad-shape rejected.
	_, _, err = extractEdgeNumIDAndMVCCVersionFromVersionKey([]byte{prefixMVCCEdge, 0, 0})
	require.Error(t, err)

	// Wrong prefix rejected.
	wrong := append([]byte(nil), key...)
	wrong[0] = prefixMVCCNode
	_, _, err = extractEdgeNumIDAndMVCCVersionFromVersionKey(wrong)
	require.Error(t, err)
}

func TestEngineEncodeNode_RequiresNamespacePrefix(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })

	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		// Missing namespace prefix → error.
		_, _, err := e.encodeNode(txn, &Node{
			ID:         "no-prefix",
			Labels:     []string{"L"},
			Properties: map[string]any{"a": int64(1)},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "lacks namespace prefix")

		// With a prefix → succeeds.
		body, _, err := e.encodeNode(txn, &Node{
			ID:         "test:n",
			Labels:     []string{"L"},
			Properties: map[string]any{"a": int64(1)},
		})
		require.NoError(t, err)
		require.NotEmpty(t, body)
		require.Equal(t, byte(nodeFormatTokenizedV1), body[0])
		return nil
	}))
}

func TestExtractMVCCVersionFromKey_Errors(t *testing.T) {
	_, err := extractMVCCVersionFromKey([]byte{1, 2, 3})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid mvcc key length")
}

func TestExtractMVCCLogicalKeyAndVersion_NodeAndAdjacencyShapes(t *testing.T) {
	v := MVCCVersion{
		CommitTimestamp: time.Date(2025, 5, 2, 0, 0, 0, 0, time.UTC),
		CommitSequence:  7,
	}

	nodeKey := mvccNodeVersionKey(44, v)
	logical, gotV, err := extractMVCCLogicalKeyAndVersion(nodeKey)
	require.NoError(t, err)
	require.Equal(t, nodeKey[:9], logical)
	require.Equal(t, 0, gotV.Compare(v))

	adjKey := mvccOutgoingAdjacencyKey(11, 22, v)
	logical, gotV, err = extractMVCCLogicalKeyAndVersion(adjKey)
	require.NoError(t, err)
	require.Equal(t, adjKey[:17], logical)
	require.Equal(t, 0, gotV.Compare(v))

	badAdjKey := append([]byte{0xFF}, bytes.Repeat([]byte{0}, 32)...)
	_, _, err = extractMVCCLogicalKeyAndVersion(badAdjKey)
	require.Error(t, err)

}

func TestExtractNodeNumIDAndMVCCVersionFromVersionKey(t *testing.T) {
	v := MVCCVersion{
		CommitTimestamp: time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
		CommitSequence:  100,
	}
	key := mvccNodeVersionKey(99, v)
	num, gotV, err := extractNodeNumIDAndMVCCVersionFromVersionKey(key)
	require.NoError(t, err)
	require.Equal(t, uint64(99), num)
	require.Equal(t, 0, gotV.Compare(v))

	_, _, err = extractNodeNumIDAndMVCCVersionFromVersionKey([]byte{prefixMVCCNode})
	require.Error(t, err)

	wrong := append([]byte(nil), key...)
	wrong[0] = prefixMVCCEdge
	_, _, err = extractNodeNumIDAndMVCCVersionFromVersionKey(wrong)
	require.Error(t, err)
}

func TestExtractEdgeNumIDFromEdgeTypeKey(t *testing.T) {
	key := edgeTypeIndexKey("KNOWS", 987)
	num, ok := extractEdgeNumIDFromEdgeTypeKey(key)
	require.True(t, ok)
	require.Equal(t, uint64(987), num)

	_, ok = extractEdgeNumIDFromEdgeTypeKey([]byte{prefixEdgeTypeIndex, 'r'})
	require.False(t, ok)
}

func TestEncodeNodeV1Body_LargeEmbeddingsAreExternalized(t *testing.T) {
	node := &Node{
		ID:              "test:legacy-large",
		Labels:          []string{"Document"},
		Properties:      map[string]any{"title": "legacy"},
		ChunkEmbeddings: makeLargeChunkEmbeddings(),
	}

	body, storedSeparately, err := encodeNodeV1Body(node)
	require.NoError(t, err)
	require.True(t, storedSeparately)
	require.NotEmpty(t, body)

	decoded, err := decodeNodeV1(body)
	require.NoError(t, err)
	require.Equal(t, node.ID, decoded.ID)
	require.Equal(t, node.Labels, decoded.Labels)
	require.True(t, decoded.EmbeddingsStoredSeparately)
	require.Nil(t, decoded.ChunkEmbeddings)
	require.EqualValues(t, len(node.ChunkEmbeddings), decoded.EmbedMeta["chunk_count"])
}

func TestEncodeNodeV1Body_PreservesExistingChunkCount(t *testing.T) {
	node := &Node{
		ID:              "test:legacy-large-existing-count",
		Labels:          []string{"Document"},
		Properties:      map[string]any{"title": "legacy"},
		ChunkEmbeddings: makeLargeChunkEmbeddings(),
		EmbedMeta:       map[string]any{"chunk_count": 99},
	}

	body, storedSeparately, err := encodeNodeV1Body(node)
	require.NoError(t, err)
	require.True(t, storedSeparately)

	decoded, err := decodeNodeV1(body)
	require.NoError(t, err)
	require.EqualValues(t, 99, decoded.EmbedMeta["chunk_count"])
}

func TestBadgerHelpers_DictionaryKeyWrappersAndLookups(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })

	version := MVCCVersion{
		CommitTimestamp: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		CommitSequence:  17,
	}

	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		_, err := engine.outgoingIndexKeyString(txn, "test:n1", "test:e1")
		require.NoError(t, err)
		_, err = engine.incomingIndexKeyString(txn, "test:n2", "test:e1")
		require.NoError(t, err)
		_, err = engine.edgeTypeIndexKeyString(txn, "REL", "test:e1")
		require.NoError(t, err)
		_, err = engine.labelIndexKeyString(txn, "Person", "test:n1")
		require.NoError(t, err)

		_, err = engine.mvccNodeHeadKeyString(txn, "test:n1")
		require.NoError(t, err)
		_, err = engine.mvccEdgeHeadKeyString(txn, "test:e1")
		require.NoError(t, err)
		_, err = engine.mvccNodeVersionKeyString(txn, "test:n1", version)
		require.NoError(t, err)
		_, err = engine.mvccEdgeVersionKeyString(txn, "test:e1", version)
		require.NoError(t, err)
		return nil
	}))

	require.NotNil(t, engine.outgoingIndexKeyStringLookup("test:n1", "test:e1"))
	require.NotNil(t, engine.incomingIndexKeyStringLookup("test:n2", "test:e1"))
	require.NotNil(t, engine.edgeTypeIndexKeyStringLookup("REL", "test:e1"))
	require.NotNil(t, engine.labelIndexKeyStringLookup("Person", "test:n1"))
	require.NotNil(t, engine.mvccNodeHeadKeyStringLookup("test:n1"))
	require.NotNil(t, engine.mvccEdgeHeadKeyStringLookup("test:e1"))
	require.NotNil(t, engine.mvccNodeVersionKeyStringLookup("test:n1", version))
	require.NotNil(t, engine.mvccEdgeVersionKeyStringLookup("test:e1", version))
	require.NotNil(t, engine.mvccNodeVersionPrefixString("test:n1"))
	require.NotNil(t, engine.mvccEdgeVersionPrefixString("test:e1"))
	require.NotNil(t, engine.outgoingIndexPrefixString("test:n1"))
	require.NotNil(t, engine.incomingIndexPrefixString("test:n2"))

	require.Nil(t, engine.outgoingIndexKeyStringLookup("test:missing", "test:e1"))
	require.Nil(t, engine.incomingIndexKeyStringLookup("test:n2", "test:missing"))
	require.Nil(t, engine.edgeTypeIndexKeyStringLookup("REL", "test:missing"))
	require.Nil(t, engine.labelIndexKeyStringLookup("Person", "test:missing"))
	require.Nil(t, engine.mvccNodeHeadKeyStringLookup("test:missing"))
	require.Nil(t, engine.mvccEdgeHeadKeyStringLookup("test:missing"))
	require.Nil(t, engine.mvccNodeVersionKeyStringLookup("test:missing", version))
	require.Nil(t, engine.mvccEdgeVersionKeyStringLookup("test:missing", version))
	require.Nil(t, engine.mvccNodeVersionPrefixString("test:missing"))
	require.Nil(t, engine.mvccEdgeVersionPrefixString("test:missing"))
	require.Nil(t, engine.outgoingIndexPrefixString("test:missing"))
	require.Nil(t, engine.incomingIndexPrefixString("test:missing"))
}

func TestBadgerHelpers_EdgeBetweenIndexWriteDeletePaths(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })

	edge := &Edge{ID: "test:e-between", StartNode: "test:start", EndNode: "test:end", Type: "REL"}
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		require.NoError(t, engine.writeEdgeBetweenIndexesInTxn(txn, edge))

		indexKey := engine.edgeBetweenIndexKeyFromStringIDs(edge.StartNode, edge.EndNode, edge.Type, edge.ID)
		require.NotNil(t, indexKey)
		headKey := engine.edgeBetweenHeadKeyFromStringIDs(edge.StartNode, edge.EndNode, edge.Type)
		require.NotNil(t, headKey)

		_, err := txn.Get(indexKey)
		require.NoError(t, err)
		headItem, err := txn.Get(headKey)
		require.NoError(t, err)
		require.NoError(t, headItem.Value(func(val []byte) error {
			require.Equal(t, []byte(edge.ID), val)
			return nil
		}))

		// Non-matching head should remain untouched.
		require.NoError(t, txn.Set(headKey, []byte("test:other")))
		require.NoError(t, engine.deleteEdgeBetweenIndexesInTxn(txn, edge))
		_, err = txn.Get(indexKey)
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		headItem, err = txn.Get(headKey)
		require.NoError(t, err)
		require.NoError(t, headItem.Value(func(val []byte) error {
			require.Equal(t, []byte("test:other"), val)
			return nil
		}))

		// Matching head should be deleted.
		require.NoError(t, txn.Set(headKey, []byte(edge.ID)))
		require.NoError(t, engine.deleteEdgeBetweenHeadIfMatchesInTxn(txn, edge))
		_, err = txn.Get(headKey)
		require.ErrorIs(t, err, badger.ErrKeyNotFound)

		// Missing dictionary IDs: should be a safe no-op path.
		require.NoError(t, engine.deleteEdgeBetweenIndexesInTxn(txn, &Edge{
			ID:        "test:missing",
			StartNode: "test:missing-start",
			EndNode:   "test:missing-end",
			Type:      "REL",
		}))
		return nil
	}))
}

func TestBadgerHelpers_EmbeddingShardMarkerHelpers(t *testing.T) {
	prefix := embeddingChunkPartPrefix("test:node", 7)
	expectedPrefix := append(embeddingKey("test:node", 7), 0x00)
	require.True(t, bytes.Equal(expectedPrefix, prefix))

	partKey := embeddingChunkPartKey("test:node", 7, 3)
	require.True(t, bytes.HasPrefix(partKey, prefix))

	count, ok := parseEmbeddingShardMarker([]byte("not-a-shard-marker"))
	require.False(t, ok)
	require.Zero(t, count)

	count, ok = parseEmbeddingShardMarker([]byte(embeddingShardMarker))
	require.False(t, ok)
	require.Zero(t, count)

	// Uvarint decode succeeds but count=0 should still be rejected.
	zeroCountMarker := append([]byte(embeddingShardMarker), byte(0))
	count, ok = parseEmbeddingShardMarker(zeroCountMarker)
	require.False(t, ok)
	require.Zero(t, count)

	marker := makeEmbeddingShardMarker(4)
	count, ok = parseEmbeddingShardMarker(marker)
	require.True(t, ok)
	require.Equal(t, 4, count)
}

func TestBadgerHelpers_MVCCIncomingAdjacencyKeyString_EdgeAllocationErrors(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })

	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		_, err := engine.labelIndexKeyString(txn, "N", "test:node-only")
		require.NoError(t, err)
		readTxn := engine.db.NewTransaction(false)
		defer readTxn.Discard()
		_, err = engine.mvccIncomingAdjacencyKeyString(readTxn, "test:node-only", "test:missing-edge", MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 1})
		require.Error(t, err)
		return nil
	}))
}
