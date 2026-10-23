package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestWriteBehindConflictBaselineControls(t *testing.T) {
	for _, name := range []string{
		"absent_at_begin_remains_unique",
		"historical_decode_error_propagates",
		"suppressed_baseline_still_owns_key",
		"tombstoned_at_begin_remains_unique",
	} {
		t.Run(name, func(t *testing.T) {
			existed := name != "absent_at_begin_remains_unique"
			corrupt := name == "historical_decode_error_propagates"
			suppressed := name == "suppressed_baseline_still_owns_key"
			tombstoned := name == "tombstoned_at_begin_remains_unique"
			engine, err := NewBadgerEngineWithOptions(BadgerOptions{
				DataDir: t.TempDir(), LowMemory: true, WriteBehind: true,
				WriteBehindInterval: time.Hour, WriteBehindMaxOps: 100000,
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			schema := engine.GetSchemaForNamespace("test")
			require.NoError(t, schema.AddConstraint(Constraint{
				Name: "doc_id", Type: ConstraintUnique, Label: "Doc", Properties: []string{"id"},
			}))
			constraint := schema.uniqueConstraints["Doc:id"]
			constraint.mu.Lock()
			constraint.valuesCacheComplete = true
			constraint.mu.Unlock()
			node := &Node{ID: "test:doc", Labels: []string{"Doc"}, Properties: map[string]any{"id": "original"}}
			var original MVCCHead
			if existed {
				_, err = engine.CreateNode(node)
				require.NoError(t, err)
				original, err = engine.GetNodeCurrentHead(node.ID)
				require.NoError(t, err)
				if tombstoned {
					require.NoError(t, engine.DeleteNode(node.ID))
				}
			}
			tx, err := engine.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			require.NoError(t, tx.SetNamespace("test"))
			if existed && !tombstoned {
				node.Properties["summary_input"] = "peer"
				require.NoError(t, engine.UpdateNode(node))
				require.NoError(t, engine.db.Update(func(writer *badger.Txn) error {
					if suppressed {
						baseline := copyNode(node)
						baseline.VisibilitySuppressed = true
						return engine.writeNodeMVCCVersionInTxn(writer, baseline, original.Version)
					}
					key := engine.mvccNodeVersionKeyStringLookup(node.ID, original.Version)
					require.NotNil(t, key)
					return writer.Set(key, []byte{0xc1})
				}))
			} else {
				_, err = engine.CreateNode(node)
				require.NoError(t, err)
			}
			tx.pendingNodes[node.ID] = copyNode(node)
			err = tx.classifyUpdateConflictAsUniqueViolation(node.ID)
			if suppressed {
				require.NoError(t, err, "visibility does not release a stored unique key")
				writeErr := tx.checkNodeWriteConflict(node.ID)
				require.ErrorIs(t, writeErr, ErrConflict)
				require.Contains(t, writeErr.Error(), "changed after transaction start")
				return
			}
			require.Error(t, err)
			var violation *ConstraintViolationError
			if corrupt {
				require.False(t, errors.As(err, &violation))
				require.NotErrorIs(t, err, ErrConflict)
				writeErr := tx.checkNodeWriteConflict(node.ID)
				var failure *CommitFailureError
				require.ErrorAs(t, writeErr, &failure)
				require.Contains(t, writeErr.Error(), err.Error())
				require.NotErrorIs(t, writeErr, ErrConflict)
				require.False(t, errors.As(writeErr, &violation))
			} else {
				require.ErrorAs(t, err, &violation)
				require.Equal(t, ConstraintUnique, violation.Type)
				require.Contains(t, err.Error(), "claimed by concurrent commit")
			}
		})
	}
}
