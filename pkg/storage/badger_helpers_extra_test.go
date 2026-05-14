package storage

import (
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
