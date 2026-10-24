package storage

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestNodeCacheWriterReferencesRequireScope(t *testing.T) {
	for _, body := range []string{
		"update := db.Update; _ = update",
		"accept(db.Update)",
		"commit := txn.CommitAt; _ = commit",
	} {
		source := "package storage; func writer(db *managedBadgerDB, txn *Txn) { " + body + " }"
		require.NotEmpty(t, writerScopeCallViolations("new_writer.go", []byte(source)), body)
	}
	require.NotEmpty(t, writerScopeCallViolations("new_writer.go", []byte("package storage; var update = (*managedBadgerDB).Update")))
	// Taking a method value with a compile-required scope remains valid.
	source := "package storage; func writer(db *managedBadgerDB) { update := db.updateWithScope; _ = update }"
	require.Empty(t, writerScopeCallViolations("new_writer.go", []byte(source)))
}

func TestNodeCacheScopeOwnerRejectsReferences(t *testing.T) {
	for _, body := range []string{
		"fence := b.fenceUnknownNodeCacheWritesLocked; _ = fence",
		"issue := b.cacheCommitNodeWrites; _ = issue",
		"check := b.invalidateNodeCacheScopeLocked; _ = check",
		"store := b.nodeCacheCommitTs.Store; _ = store",
	} {
		source := "package storage; func (b *BadgerEngine) writer() { " + body + " }"
		require.NotEmpty(t, nodeCacheScopeOwnerViolations("new_writer.go", []byte(source)), body)
	}
	require.NotEmpty(t, nodeCacheScopeOwnerViolations("new_writer.go", []byte("package storage; var fence = (*BadgerEngine).fenceUnknownNodeCacheWritesLocked")))
}

// The guard must visit a borrowed inventory once per actual Badger write set,
// rather than rescan it for every key in a large replay batch.
func TestNodeCacheScopeGuardVisitsInventoryOnce(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	txn := db.NewTransaction(true)
	defer txn.Discard()
	const count = 128
	ids := make([]NodeID, count)
	for i := range ids {
		ids[i] = NodeID(fmt.Sprintf("test:scope-inventory-%d", i))
		require.NoError(t, txn.Set(nodeKey(ids[i]), []byte{1}))
	}
	visits := 0
	scope := nodeCacheWriteScope{known: true, declared: true, nodeIDs: func(visit func(NodeID)) {
		for _, id := range ids {
			visits++
			visit(id)
		}
	}}
	require.NoError(t, checkNodeCacheScope(txn, scope))
	require.Equal(t, count, visits, "actual-key validation must not multiply inventory traversal")
}
