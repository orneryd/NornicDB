package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// countKeysWithPrefix counts the stored keys of one key space.
func countKeysWithPrefix(t *testing.T, b *BadgerEngine, prefix byte) int {
	t.Helper()
	n := 0
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerPrefixIteratorOptions([]byte{prefix}))
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix([]byte{prefix}); it.Next() {
			n++
		}
		return nil
	}))
	return n
}

// TestTemporalConstraintBackfillsBufferedNodes: a temporal constraint created
// over a label whose node the write-behind buffer acknowledged but has not
// applied. Its history index is recorded complete only with that node's entry
// written: the backfill runs after every acknowledged commit, not over them.
func TestTemporalConstraintBackfillsBufferedNodes(t *testing.T) {
	inner, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = inner.Close() })
	engine := NewNamespacedEngine(inner, "test")
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tx, err := inner.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.SetImplicit(true))
	_, err = tx.CreateNode(&Node{ID: "test:period1", Labels: []string{"Period"}, Properties: map[string]any{"key": "k", "from": from, "to": from.Add(24 * time.Hour)}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered(), "the create is acknowledged, not yet applied")

	c := Constraint{Name: "period_history", Type: ConstraintTemporal, EntityType: ConstraintEntityNode, Label: "Period", Properties: []string{"key", "from", "to"}}
	require.NoError(t, ValidateConstraintOnCreationForEngine(engine, c))
	require.NoError(t, engine.GetSchema().AddConstraint(c))
	require.NoError(t, MarkNewTemporalConstraintsForEngine(engine, engine.GetSchema(), []Constraint{c}))
	require.NoError(t, inner.FlushWriteBehind())

	require.Equal(t, 1, countKeysWithPrefix(t, inner, prefixTemporalIndex),
		"the node acknowledged before the constraint has no history entry, yet the index is recorded complete")
}

// TestBetweenSelfHealWritesOnlyWhatIsStored: a between read that falls back
// to the legacy index heals the between index for what it found. A
// relationship deleted after that read, before the heal, keeps no entry.
func TestBetweenSelfHealWritesOnlyWhatIsStored(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := b.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, b.CreateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}))
	require.NoError(t, b.FlushWriteBehind())
	// Drift: the between index lost the relationship; the legacy outgoing
	// index still holds it.
	edge, err := b.GetEdge("test:e")
	require.NoError(t, err)
	require.NoError(t, b.withUpdate(func(txn *badger.Txn) error { return b.deleteEdgeBetweenIndexesInTxn(txn, edge) }))
	require.Zero(t, countKeysWithPrefix(t, b, prefixEdgeBetweenIndex))

	var once sync.Once
	restore := setBetweenSelfHealHook(func() {
		once.Do(func() {
			require.NoError(t, b.DeleteEdge("test:e"))
			require.NoError(t, b.FlushWriteBehind())
		})
	})
	defer restore()
	_, err = b.GetEdgesBetween("test:a", "test:b")
	require.NoError(t, err)
	restore()
	require.NoError(t, b.FlushWriteBehind())

	require.Zero(t, countKeysWithPrefix(t, b, prefixEdgeBetweenIndex), "the heal indexed a relationship deleted before it")
	require.Zero(t, countKeysWithPrefix(t, b, prefixEdgeBetweenHead), "the heal pointed the between head at a relationship deleted before it")
}

// writeAtPause returns a pause hook that starts write at point and waits a
// moment for it: a pass that runs beside writes lets it finish there, a pass
// that runs across no write holds it until the pass ends. wait returns the
// write's outcome.
func writeAtPause(point string, write func() error) (hook func(string), wait func() error) {
	var once sync.Once
	finished := make(chan struct{})
	var err error
	hook = func(got string) {
		if got != point {
			return
		}
		once.Do(func() {
			go func() {
				err = write()
				close(finished)
			}()
			select {
			case <-finished:
			case <-time.After(500 * time.Millisecond):
			}
		})
	}
	wait = func() error {
		select {
		case <-finished:
			return err
		case <-time.After(30 * time.Second):
			return errors.New("the pass never paused, or the write never finished")
		}
	}
	return hook, wait
}

// conflictRetried runs an autocommit write as a driver does: again after a
// retryable conflict. It reports the attempts.
func conflictRetried(b *BadgerEngine, write func(tx *BadgerTransaction) error) (attempts int, err error) {
	for attempts = 1; ; attempts++ {
		_, err = implicitWrite(b, write)
		if !errors.Is(err, ErrConflict) || attempts == 5 {
			return attempts, err
		}
	}
}

// TestTemporalRebuildRunsAcrossNoWrite: RebuildTemporalIndexes replaces the
// temporal indexes from the stored nodes. A node updated after the rebuild
// read it keeps only the history entry of its current interval: the update
// is not applied between the rebuild's read and its write of the entry, and
// a transaction that staged it during the rebuild commits only on retry.
func TestTemporalRebuildRunsAcrossNoWrite(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		t.Run(fmt.Sprintf("write-behind %v", writeBehind), func(t *testing.T) {
			inner := openBackfillTestEngine(t, t.TempDir(), writeBehind)
			engine := NewNamespacedEngine(inner, "test")
			c := Constraint{Name: "period_history", Type: ConstraintTemporal, EntityType: ConstraintEntityNode, Label: "Period", Properties: []string{"key", "from", "to"}}
			require.NoError(t, engine.GetSchema().AddConstraint(c))
			from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			period := func(start time.Time) *Node {
				return &Node{ID: "p1", Labels: []string{"Period"}, Properties: map[string]any{"key": "k", "from": start, "to": start.Add(24 * time.Hour)}}
			}
			_, err := engine.CreateNode(period(from))
			require.NoError(t, err)
			require.NoError(t, inner.FlushWriteBehind())

			attempts := 0
			hook, wait := writeAtPause(pauseTemporalRebuildNode, func() error {
				moved := period(from.Add(48 * time.Hour))
				moved.ID = "test:p1"
				var err error
				attempts, err = conflictRetried(inner, func(tx *BadgerTransaction) error { return tx.UpdateGraphNode(moved, ClientNodeEdits) })
				if err != nil {
					return err
				}
				return inner.FlushWriteBehind()
			})
			restore := setMaintenancePauseHook(hook)
			defer restore()
			require.NoError(t, inner.RebuildTemporalIndexes(context.Background()))
			restore()
			require.NoError(t, wait())
			require.NoError(t, inner.FlushWriteBehind())

			published := indexKeys(t, inner, prefixTemporalIndex, prefixTemporalHead)
			require.NoError(t, inner.RebuildTemporalIndexes(context.Background()))
			require.Equal(t, indexKeys(t, inner, prefixTemporalIndex, prefixTemporalHead), published,
				"the rebuild indexed a node image an update had replaced")
			require.Equal(t, 2, attempts, "a transaction staged during the rebuild commits only after a retry")
		})
	}
}

// TestIDRepairRunsAcrossNoWrite: RepairIDDictionary points a lost node's
// dictionary entry back at the number its index keys use. A label added to
// the node after the repair identified it is indexed under the number the
// node keeps: the update is not applied between identification and restore,
// and a transaction that staged it against the unrepaired dictionary commits
// only on retry.
func TestIDRepairRunsAcrossNoWrite(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		t.Run(fmt.Sprintf("write-behind %v", writeBehind), func(t *testing.T) {
			dir := t.TempDir()
			seed := openBackfillTestEngine(t, dir, false)
			for _, node := range []*Node{{ID: "test:a", Labels: []string{"Doc"}}, {ID: "test:b", Labels: []string{"Doc", "Source"}}} {
				_, err := seed.CreateNode(node)
				require.NoError(t, err)
			}
			dropDictionaryEntries(t, seed, []NodeID{"test:b"}, nil)
			require.NoError(t, seed.Close())
			b := openBackfillTestEngine(t, dir, writeBehind)

			attempts := 0
			hook, wait := writeAtPause(pauseIDRepairRestore, func() error {
				var err error
				attempts, err = conflictRetried(b, func(tx *BadgerTransaction) error {
					return tx.UpdateGraphNode(&Node{ID: "test:b", Labels: []string{"Doc", "Source", "Fresh"}}, ClientNodeEdits)
				})
				if err != nil {
					return err
				}
				return b.FlushWriteBehind()
			})
			restore := setMaintenancePauseHook(hook)
			defer restore()
			report, err := b.RepairIDDictionary()
			require.NoError(t, err)
			restore()
			require.NoError(t, wait())
			require.NoError(t, b.FlushWriteBehind())

			require.Equal(t, []NodeID{"test:b"}, report.RestoredNodes)
			require.True(t, labelled(t, b, "Fresh", "test:b"), "a label added during the repair is indexed under a number the repair moved the node off")
			require.True(t, labelled(t, b, "Source", "test:b"))
			check, err := b.CheckIDDictionary()
			require.NoError(t, err)
			require.Empty(t, check.Nodes)
			require.Empty(t, check.MismatchedNodes)
			require.Equal(t, 2, attempts, "a transaction staged during the repair commits only after a retry")
		})
	}
}
