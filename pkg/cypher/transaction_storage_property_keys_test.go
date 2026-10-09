package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A transaction reads key names through the engine it reads through: a
// namespaced view, or a registry with the transaction's namespace (#907).
func TestTransactionWrapperPropertyKeyKnown(t *testing.T) {
	engine := newTestMemoryEngine(t)
	engine.NotePropertyKeysInNamespace("db", map[string]interface{}{"k": int64(1)})
	require.True(t, (&transactionStorageWrapper{underlying: storage.NewNamespacedEngine(engine, "db")}).PropertyKeyKnown("k"))
	require.True(t, (&transactionStorageWrapper{underlying: engine, namespace: "db"}).PropertyKeyKnown("k"))
	require.False(t, (&transactionStorageWrapper{underlying: engine, namespace: "other"}).PropertyKeyKnown("k"))
}
