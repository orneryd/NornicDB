package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestTransactionTemporalAcceptanceLifetimeAndNamespace(t *testing.T) {
	engine := createTestBadgerEngine(t)
	reader, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := reader.Rollback(); err != nil && !errors.Is(err, ErrTransactionClosed) {
			t.Error(err)
		}
	})
	require.NoError(t, reader.SetNamespace("test"))
	visible, err := reader.IsCurrentTemporalNodeInNamespace("test", nil, time.Time{})
	require.NoError(t, err)
	require.False(t, visible)
	node := &Node{ID: "test:local", Labels: []string{"Ordinary"}}
	visible, err = reader.IsCurrentTemporalNodeInNamespace("test", node, time.Time{})
	require.NoError(t, err)
	require.True(t, visible)
	visible, err = reader.IsCurrentTemporalNodeInNamespace("other", &Node{ID: "other:local"}, time.Time{})
	require.ErrorIs(t, err, ErrCrossNamespaceTransaction)
	require.False(t, visible)
	require.NoError(t, reader.Rollback())
	visible, err = reader.IsCurrentTemporalNodeInNamespace("test", node, time.Time{})
	require.ErrorIs(t, err, ErrTransactionClosed)
	require.False(t, visible)
}

func TestTemporalAcceptanceMetadataRecoveryAndReadError(t *testing.T) {
	for _, mode := range []string{"missing_hints", "dangling_hint", "corrupt_body"} {
		t.Run(mode, func(t *testing.T) {
			engine := createTestBadgerEngine(t)
			constraint := Constraint{Name: "recovery_temporal", Type: ConstraintTemporal, Label: "RecoveryFact", Properties: []string{"key", "vf", "vt"}}
			require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(constraint))
			start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
			asOf := start.AddDate(1, 0, 0)
			node := &Node{ID: "test:current", Labels: []string{"RecoveryFact"}, Properties: map[string]interface{}{"key": "one", "vf": start}}
			_, err := engine.CreateNode(node)
			require.NoError(t, err)
			require.NoError(t, engine.RebuildTemporalIndexes(context.Background()))
			descriptor := makeTemporalDescriptor("test", constraint, "one")
			// Simulate missing/stale persisted hints or an unreadable node body.
			// The public acceptance API must recover the former and expose the latter.
			require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
				switch mode {
				case "missing_hints":
					if err := txn.Delete(temporalCurrentKey(descriptor)); err != nil {
						return err
					}
					return txn.Delete(temporalHistoryKey(descriptor, start, node.ID))
				case "dangling_hint":
					return txn.Set(temporalCurrentKey(descriptor), []byte("test:missing"))
				default:
					return txn.Set(nodeKey(node.ID), []byte("{invalid-json"))
				}
			}))
			visible, err := engine.IsCurrentTemporalNodeInNamespace("test", node, asOf)
			if mode == "corrupt_body" {
				require.Error(t, err)
				require.False(t, visible)
			} else {
				require.NoError(t, err)
				require.True(t, visible)
			}
		})
	}
}
