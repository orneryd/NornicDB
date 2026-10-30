package storage

import (
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_RebuildUniqueConstraintValues_PropertyCompositeAndErrors(t *testing.T) {
	t.Run("rebuild populates unique/property/composite caches", func(t *testing.T) {
		engine := newIsolatedBadgerEngine(t)

		_, err := engine.CreateNode(&Node{
			ID:     "alpha:u1",
			Labels: []string{"User"},
			Properties: map[string]interface{}{
				"email": "a@example.com",
				"name":  "Alice",
				"first": "Alice",
				"last":  "A",
			},
		})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{
			ID:     "alpha:u2",
			Labels: []string{"User"},
			Properties: map[string]interface{}{
				"email": "b@example.com",
				"name":  "Bob",
				"first": "Bob",
				"last":  "B",
			},
		})
		require.NoError(t, err)

		sm := NewSchemaManager()
		require.NoError(t, sm.AddUniqueConstraint("uc_user_email", "User", "email"))
		require.NoError(t, sm.AddPropertyIndex("idx_user_name", "User", []string{"name"}))
		require.NoError(t, sm.AddCompositeIndex("idx_user_full_name", "User", []string{"first", "last"}))

		require.NoError(t, engine.rebuildUniqueConstraintValues("alpha", sm))

		uc, ok := sm.uniqueConstraints["User:email"]
		require.True(t, ok)
		uc.mu.RLock()
		require.True(t, uc.valuesCacheComplete)
		require.Len(t, uc.values, 2)
		uc.mu.RUnlock()

		pidx, ok := sm.GetPropertyIndex("User", "name")
		require.True(t, ok)
		pidx.mu.RLock()
		require.Len(t, pidx.values, 2)
		pidx.mu.RUnlock()

		// Every equality index lives in compositeIndexes now: the constraint's
		// email index, the arity-1 name index and the arity-2 full-name index.
		cidxs := sm.GetCompositeIndexesForLabel("User")
		require.Len(t, cidxs, 3)
		var fullName *CompositeIndex
		for _, idx := range cidxs {
			if len(idx.Properties) == 2 {
				fullName = idx
			}
		}
		require.NotNil(t, fullName)
		fullName.mu.RLock()
		require.NotEmpty(t, fullName.fullIndex)
		fullName.mu.RUnlock()
	})

	t.Run("rebuild returns error on duplicate unique values", func(t *testing.T) {
		engine := newIsolatedBadgerEngine(t)
		_, err := engine.CreateNode(&Node{ID: "dup:u1", Labels: []string{"User"}, Properties: map[string]interface{}{"email": "same@example.com"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: "dup:u2", Labels: []string{"User"}, Properties: map[string]interface{}{"email": "same@example.com"}})
		require.NoError(t, err)

		sm := NewSchemaManager()
		require.NoError(t, sm.AddUniqueConstraint("uc_user_email", "User", "email"))

		err = engine.rebuildUniqueConstraintValues("dup", sm)
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "unique"))
	})

	t.Run("rebuild returns decode error for malformed node payload", func(t *testing.T) {
		engine := newIsolatedBadgerEngine(t)
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			return txn.Set(nodeKey("broken:n1"), []byte("not-msgpack-node"))
		}))

		sm := NewSchemaManager()
		require.NoError(t, sm.AddUniqueConstraint("uc_user_email", "User", "email"))

		err := engine.rebuildUniqueConstraintValues("broken", sm)
		require.Error(t, err)
		require.Contains(t, err.Error(), "decode node")
	})
}

func TestParseSchemaNamespaceFromKey_MissingTerminator(t *testing.T) {
	ns, ok := parseSchemaNamespaceFromKey([]byte{prefixSchema, 'n', 's'})
	require.False(t, ok)
	require.Empty(t, ns)
}
