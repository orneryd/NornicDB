package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestNodeUniqueIndexPrefixDeletion(t *testing.T) {
	engine := createTestBadgerEngine(t)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddUniqueConstraint("doc_id", "Doc", "id"))
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(NewNamespacedEngine(engine, "test"), schema))
	_, err := engine.CreateNode(&Node{ID: "test:remove-first", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "released"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:keep", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "held"}})
	require.NoError(t, err)
	deleted, _, err := engine.DeleteByPrefix("test:remove-")
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted)
	_, err = engine.CreateNode(&Node{ID: "test:replacement", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "released"}})
	require.NoError(t, err, "deleting a node prefix releases its reservations with its data")
	_, err = engine.CreateNode(&Node{ID: "test:duplicate", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "held"}})
	var violation *ConstraintViolationError
	require.ErrorAs(t, err, &violation, "another prefix still holds its key")
}

func TestNodeUniqueIndexCommitReadFailure(t *testing.T) {
	for _, keyConflict := range []bool{false, true} {
		t.Run(fmt.Sprintf("key_conflict=%t", keyConflict), func(t *testing.T) {
			b, _ := newUniqueIndexFixture(t)
			_, err := b.CreateNode(uniqueIndexNode("test:a", "original"))
			require.NoError(t, err)
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			require.NoError(t, tx.SetDeferredConstraintValidation(true))
			before, err := tx.GetNode("test:a")
			require.NoError(t, err)
			require.NoError(t, tx.UpdateNodeObserved(before, uniqueIndexNode("test:a", "claimed")))
			peer := uniqueIndexNode("test:a", "original")
			if keyConflict {
				peer.Properties["id"] = "claimed"
			}
			peer.Properties["other"] = "peer"
			require.NoError(t, b.UpdateNode(peer))
			require.NoError(t, b.db.Update(func(txn *badger.Txn) error {
				if err := txn.Delete(append(nodeUniquePrefix("test", "Doc", "id"), 0)); err != nil {
					return err
				}
				return txn.Set(nodeKey("test:a"), []byte{0xff})
			}))
			var failure *CommitFailureError
			var violation *ConstraintViolationError
			err = tx.checkNodeWriteConflict("test:a")
			require.ErrorAs(t, err, &failure)
			require.NotErrorIs(t, err, ErrConflict)
			require.False(t, errors.As(err, &violation))
			err = tx.Commit()
			require.ErrorAs(t, err, &failure)
			require.NotErrorIs(t, err, ErrConflict)
			require.False(t, errors.As(err, &violation))
		})
	}
}

func TestNodeUniqueIndexMigrationReadFailure(t *testing.T) {
	dir := t.TempDir()
	b, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	schema := b.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddUniqueConstraint("doc_id", "Doc", "id"))
	_, err = b.CreateNode(uniqueIndexNode("test:a", "held"))
	require.NoError(t, err)
	require.NoError(t, b.db.Update(func(txn *badger.Txn) error { return txn.Set(nodeUniqueReverseKey("test:a"), []byte("malformed")) }))
	require.NoError(t, b.Close())
	reopened, err := NewBadgerEngine(dir)
	if reopened != nil {
		t.Cleanup(func() { _ = reopened.Close() })
	}
	var failure *CommitFailureError
	require.ErrorAs(t, err, &failure, "startup must report malformed index metadata, never silently rebuild over it")
	require.NotErrorIs(t, err, ErrConflict)
	require.Nil(t, reopened)
}

func TestNodeUniqueIndexMetadataFailuresKeepStatus(t *testing.T) {
	t.Run("previous schema decode", func(t *testing.T) {
		b, schema := newUniqueIndexFixture(t)
		malformed := []byte("malformed schema")
		require.NoError(t, b.db.Update(func(txn *badger.Txn) error { return txn.Set(schemaKey("test"), malformed) }))
		err := schema.AddUniqueConstraint("other_id", "Other", "id")
		var failure *CommitFailureError
		var decode *json.SyntaxError
		require.ErrorAs(t, err, &failure)
		require.ErrorAs(t, err, &decode)
		require.NotErrorIs(t, err, ErrConflict)
		require.NoError(t, b.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(schemaKey("test"))
			require.NoError(t, err)
			data, err := item.ValueCopy(nil)
			require.NoError(t, err)
			require.Equal(t, malformed, data, "a failed definition read cannot publish a replacement")
			return nil
		}))
	})
	t.Run("node projection decode", func(t *testing.T) {
		b, _ := newUniqueIndexFixture(t)
		writer := b.db.testTxn(true)
		defer writer.Discard()
		err := b.nodeUniqueRecordInTxn(writer, nodeKey("test:a"), []byte{0xff}, false)
		var failure *CommitFailureError
		require.ErrorAs(t, err, &failure)
		require.NotErrorIs(t, err, ErrConflict)
	})
}

func TestNodeUniqueIndexAcknowledgedTransitions(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	schema := b.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddUniqueConstraint("doc_id", "Doc", "id"))
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(NewNamespacedEngine(b, "test"), schema))
	_, err = b.CreateNode(uniqueIndexNode("test:a", "seed"))
	require.NoError(t, err)
	ack := func(value string) {
		t.Helper()
		tx, err := b.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetImplicit(true))
		require.NoError(t, tx.UpdateNode(uniqueIndexNode("test:a", value)))
		require.NoError(t, tx.Commit())
	}
	ack("old")
	id, found, _, complete, err := schema.LookupUniqueConstraintValueForPlanningChecked("Doc", "id", "old")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:a"), id)
	require.True(t, found)
	require.True(t, complete)
	require.NoError(t, schema.CheckUniqueConstraint("Doc", "id", "old", "test:a"))
	require.Error(t, schema.CheckUniqueConstraint("Doc", "id", "old", "test:other"))
	ack("renamed")
	_, err = b.CreateNode(uniqueIndexNode("test:b", "old"))
	require.NoError(t, err, "a released acknowledged key can be claimed by a direct writer")
	// Replay sees the older intermediate image too. It must not steal the
	// direct holder's reservation before publishing the newer acknowledged image.
	require.NoError(t, b.FlushWriteBehind())
	for value, want := range map[string]NodeID{"old": "test:b", "renamed": "test:a", "seed": ""} {
		id, found, _, complete, err := schema.LookupUniqueConstraintValueForPlanningChecked("Doc", "id", value)
		require.NoError(t, err)
		require.Equal(t, want, id)
		require.Equal(t, want != "", found)
		require.True(t, complete)
	}
	// Even a caller below logical validation cannot publish a duplicate key:
	// the data/index transaction's own conflict guard rejects it.
	writer := b.db.testTxn(true)
	defer writer.Discard()
	require.ErrorIs(t, b.writeNodeUniqueImageInTxn(writer, "test:third", uniqueIndexNode("test:third", "old"), false), ErrConflict)
}

func TestNodeUniqueIndexAcknowledgedKeyConflict(t *testing.T) {
	for _, implicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("implicit=%t", implicit), func(t *testing.T) {
			b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			schema := b.GetSchemaForNamespace("test")
			require.NoError(t, schema.AddUniqueConstraint("doc_id", "Doc", "id"))
			require.NoError(t, RefreshUniqueConstraintValuesForEngine(NewNamespacedEngine(b, "test"), schema))
			_, err = b.CreateNode(uniqueIndexNode("test:a", "original"))
			require.NoError(t, err)
			loser, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, loser.SetImplicit(implicit))
			require.NoError(t, loser.UpdateNode(uniqueIndexNode("test:a", "claimed")))
			peer, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, peer.SetImplicit(true))
			require.NoError(t, peer.UpdateNode(uniqueIndexNode("test:a", "claimed")))
			require.NoError(t, peer.Commit())
			// The peer is acknowledged but deliberately not flushed.
			err = loser.Commit()
			var violation *ConstraintViolationError
			require.ErrorAs(t, err, &violation)
			require.Equal(t, ConstraintUnique, violation.Type)
			require.ErrorIs(t, err, ErrConflict)
		})
	}
}

func TestNodeUniqueIndexOwnedReadErrors(t *testing.T) {
	t.Run("discarded index reader", func(t *testing.T) {
		b, _ := newUniqueIndexFixture(t)
		reader := b.db.testTxn(false)
		reader.Discard()
		key, _ := nodeUniqueKey("test", "Doc", "id", "held")
		_, err := readNodeUniqueKey(reader, key)
		var failure *CommitFailureError
		require.ErrorAs(t, err, &failure)
		require.ErrorIs(t, err, badger.ErrDiscardedTxn)
		_, err = readNodeUniqueReverse(reader, "test:a")
		require.ErrorAs(t, err, &failure)
		require.ErrorIs(t, err, badger.ErrDiscardedTxn)
	})
	t.Run("malformed reverse aborts backfill and prefix drop", func(t *testing.T) {
		b, schema := newUniqueIndexFixture(t)
		_, err := b.CreateNode(uniqueIndexNode("test:a", "held"))
		require.NoError(t, err)
		require.NoError(t, b.db.Update(func(txn *badger.Txn) error { return txn.Set(nodeUniqueReverseKey("test:a"), []byte("malformed")) }))
		var failure *CommitFailureError
		err = RefreshUniqueConstraintValuesForEngine(NewNamespacedEngine(b, "test"), schema)
		require.ErrorAs(t, err, &failure)
		_, _, err = b.DeleteByPrefix("test:a")
		require.ErrorAs(t, err, &failure)
		err = b.DeleteNode("test:a")
		require.ErrorAs(t, err, &failure, "an ordinary delete must also preserve the body when its index metadata cannot be read")
		node, err := b.GetNode("test:a")
		require.NoError(t, err)
		require.Equal(t, "held", node.Properties["id"])
		id, found, _, complete, err := schema.LookupUniqueConstraintValueForPlanningChecked("Doc", "id", "held")
		require.NoError(t, err)
		require.Equal(t, NodeID("test:a"), id)
		require.True(t, found)
		require.True(t, complete, "failed backfill leaves the previous complete index intact")
	})
	t.Run("legacy data corruption reaches every checked consumer", func(t *testing.T) {
		b, schema := newUniqueIndexFixture(t)
		require.NoError(t, b.db.DropPrefix(nodeUniqueNamespacePrefix("test")))
		require.NoError(t, b.db.Update(func(txn *badger.Txn) error { return txn.Set(nodeKey("test:a"), []byte{0xff}) }))
		_, _, _, _, err := schema.LookupUniqueConstraintValueForPlanningChecked("Doc", "id", "held")
		var failure *CommitFailureError
		require.ErrorAs(t, err, &failure)
		err = schema.CheckUniqueConstraint("Doc", "id", "held", "test:other")
		require.ErrorAs(t, err, &failure)
		require.NotErrorIs(t, err, ErrConflict)
		// The compatibility hint cannot return an error; it must report an
		// incomplete answer so its callers cannot infer that the value is free.
		_, _, _, complete := schema.LookupUniqueConstraintValueForPlanning("Doc", "id", "held")
		require.False(t, complete)
	})
}

func newUniqueIndexFixture(t *testing.T) (*BadgerEngine, *SchemaManager) {
	t.Helper()
	b := createTestBadgerEngine(t)
	schema := b.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddUniqueConstraint("doc_id", "Doc", "id"))
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(NewNamespacedEngine(b, "test"), schema))
	return b, schema
}

func uniqueIndexNode(id NodeID, value interface{}) *Node {
	return &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": value}}
}

func TestNodeUniqueIndexAtomicTransitions(t *testing.T) {
	b, schema := newUniqueIndexFixture(t)
	_, err := b.CreateNode(uniqueIndexNode("test:a", "old"))
	require.NoError(t, err)
	check := func(value string, expected NodeID) {
		t.Helper()
		id, found, constrained, complete, err := schema.LookupUniqueConstraintValueForPlanningChecked("Doc", "id", value)
		require.NoError(t, err)
		require.True(t, constrained)
		require.True(t, complete)
		require.Equal(t, expected != "", found)
		require.Equal(t, expected, id)
	}
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.UpdateNode(uniqueIndexNode("test:a", "new")))
	check("old", "test:a")
	check("new", "")
	require.NoError(t, tx.Rollback())
	check("old", "test:a")
	check("new", "")

	// The native transaction enrolls the index write together with the body.
	// Discarding it after mutation must publish neither half of the rename.
	require.NoError(t, b.db.View(func(reader *badger.Txn) error {
		writer := b.db.testTxn(true)
		defer writer.Discard()
		require.NoError(t, b.writeNodeUniqueImageInTxn(writer, "test:a", uniqueIndexNode("test:a", "new"), false))
		oldKey, _ := nodeUniqueKey("test", "Doc", "id", "old")
		newKey, _ := nodeUniqueKey("test", "Doc", "id", "new")
		old, err := readNodeUniqueKey(writer, oldKey)
		require.NoError(t, err)
		require.Empty(t, old)
		newID, err := readNodeUniqueKey(writer, newKey)
		require.NoError(t, err)
		require.Equal(t, NodeID("test:a"), newID)
		old, err = readNodeUniqueKey(reader, oldKey)
		require.NoError(t, err)
		require.Equal(t, NodeID("test:a"), old)
		return nil
	}))
	check("old", "test:a")
	check("new", "")

	tx, err = b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetDeferredConstraintValidation(true))
	// Creation is staged first deliberately; final-state validation and
	// removal-before-addition publication must not depend on write order.
	_, err = tx.CreateNode(uniqueIndexNode("test:b", "old"))
	require.NoError(t, err)
	require.NoError(t, tx.UpdateNode(uniqueIndexNode("test:a", "new")))
	require.NoError(t, tx.Commit())
	check("old", "test:b")
	check("new", "test:a")
	require.NoError(t, b.DeleteNode("test:a"))
	check("new", "")
	_, err = b.CreateNode(uniqueIndexNode("test:a", "new"))
	require.NoError(t, err)
	check("new", "test:a")
}

func TestNodeUniqueIndexRestoreAndDefinitionLifecycle(t *testing.T) {
	for _, completed := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy_backfill", true: "stored_index"}[completed], func(t *testing.T) {
			source := createTestBadgerEngine(t)
			schema := source.GetSchemaForNamespace("test")
			require.NoError(t, schema.AddUniqueConstraint("doc_id", "Doc", "id"))
			_, err := source.CreateNode(uniqueIndexNode("test:a", "held"))
			require.NoError(t, err)
			if completed {
				require.NoError(t, RefreshUniqueConstraintValuesForEngine(NewNamespacedEngine(source, "test"), schema))
			} else {
				// A backup from before this prefix existed contains only bodies
				// and the persisted constraint definition.
				require.NoError(t, source.db.DropPrefix([]byte{prefixNodeUnique}))
			}
			backup := filepath.Join(t.TempDir(), "unique.backup")
			require.NoError(t, source.Backup(backup))
			target := createTestBadgerEngine(t)
			require.NoError(t, target.Restore(backup))
			schema = target.GetSchemaForNamespace("test")
			id, found, _, complete, err := schema.LookupUniqueConstraintValueForPlanningChecked("Doc", "id", "held")
			require.NoError(t, err)
			require.Equal(t, NodeID("test:a"), id)
			require.True(t, found)
			require.True(t, complete)
			require.Empty(t, schema.uniqueConstraints["Doc:id"].values, "live constraints keep no side registry")
			require.NoError(t, target.DeleteNode("test:a"))
			_, err = target.CreateNode(uniqueIndexNode("test:replacement", "held"))
			require.NoError(t, err)
			require.NoError(t, schema.DropConstraint("doc_id"))
			require.NoError(t, target.UpdateNode(uniqueIndexNode("test:replacement", "renamed")))
			require.NoError(t, schema.AddUniqueConstraint("doc_id", "Doc", "id"))
			require.NoError(t, RefreshUniqueConstraintValuesForEngine(NewNamespacedEngine(target, "test"), schema))
			_, err = target.CreateNode(uniqueIndexNode("test:reuse", "held"))
			require.NoError(t, err)
			_, err = target.CreateNode(uniqueIndexNode("test:duplicate", "renamed"))
			var violation *ConstraintViolationError
			require.ErrorAs(t, err, &violation)
			_, _, err = target.DeleteByPrefix("test:")
			require.NoError(t, err)
			_, err = target.CreateNode(uniqueIndexNode("test:reuse", "renamed"))
			require.NoError(t, err, "namespace deletion releases all reservations")
		})
	}
}

func TestNodeUniqueIndexStorageReadFailure(t *testing.T) {
	b, _ := newUniqueIndexFixture(t)
	_, err := b.CreateNode(uniqueIndexNode("test:a", "old"))
	require.NoError(t, err)
	require.NoError(t, b.db.Update(func(txn *badger.Txn) error { return txn.Set(nodeUniqueReverseKey("test:a"), []byte("malformed")) }))
	err = b.UpdateNode(uniqueIndexNode("test:a", "new"))
	var failure *CommitFailureError
	var syntax *json.SyntaxError
	var violation *ConstraintViolationError
	require.ErrorAs(t, err, &failure)
	require.ErrorAs(t, err, &syntax, "original storage cause survives")
	require.NotErrorIs(t, err, ErrConflict)
	require.False(t, errors.As(err, &violation))
	node, err := b.GetNode("test:a")
	require.NoError(t, err)
	require.Equal(t, "old", node.Properties["id"], "failed index validation cannot publish data")
}

func TestNodeUniqueIndexDeleteRecreateTransition(t *testing.T) {
	b, schema := newUniqueIndexFixture(t)
	_, err := b.CreateNode(uniqueIndexNode("test:a", "old"))
	require.NoError(t, err)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetDeferredConstraintValidation(true))
	require.NoError(t, tx.DeleteNode("test:a"))
	_, err = tx.CreateNode(uniqueIndexNode("test:b", "old"))
	require.NoError(t, err)
	_, err = tx.CreateNode(uniqueIndexNode("test:a", "new"))
	require.NoError(t, err)
	require.NoError(t, tx.Commit(), "recreating an ID still releases its previous key before additions")
	for value, expected := range map[string]NodeID{"old": "test:b", "new": "test:a"} {
		id, found, _, complete, err := schema.LookupUniqueConstraintValueForPlanningChecked("Doc", "id", value)
		require.NoError(t, err)
		require.True(t, found)
		require.True(t, complete)
		require.Equal(t, expected, id)
	}
}
