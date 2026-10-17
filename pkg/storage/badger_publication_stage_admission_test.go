package storage

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A transaction's afterPublication hook (database quota admission release
// and byte accounting, #1366) runs once the ordered stage has applied every
// commit up to the transaction's own, and before the commit's listeners: the
// stage, then afterPublication, then the listener. A buffered commit runs it
// once at its acknowledgement; its replay doesn't run it again.

type publicationEvents struct {
	mu     sync.Mutex
	events []string
}

func (e *publicationEvents) add(event string) {
	e.mu.Lock()
	e.events = append(e.events, event)
	e.mu.Unlock()
}

func (e *publicationEvents) list() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.events...)
}

func newAdmissionTestEngine(t *testing.T, opts BadgerOptions) (*BadgerEngine, *SchemaManager) {
	t.Helper()
	b, err := NewBadgerEngineWithOptions(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	sm := b.GetSchemaForNamespace("db")
	require.NoError(t, sm.AddUniqueConstraint("unique_l_u", "L", "u"))
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(b, sm))
	return b, sm
}

func TestAfterPublicationFollowsTheStage(t *testing.T) {
	b, sm := newAdmissionTestEngine(t, BadgerOptions{InMemory: true})
	var events publicationEvents
	restore := setPublicationStageHook(func(uint64) { events.add("stage") })
	defer restore()
	b.OnNodeCreated(func(node *Node) {
		if node.ID == "db:admitted" {
			events.add("listener")
		}
	})
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "db:admitted", Labels: []string{"L"}, Properties: map[string]any{"u": "A"}})
	require.NoError(t, err)
	require.NoError(t, tx.CommitContextWithPublication(context.Background(), func() {
		holder, found, _, _ := sm.lookupUniqueConstraintValueForValidation("L", "u", "A")
		require.True(t, found && holder == "db:admitted", "the commit's own publication is applied before afterPublication")
		events.add("afterPublication")
	}))
	require.Equal(t, []string{"stage", "afterPublication", "listener"}, events.list())
}

// While an older commit's publication is held in the stage, a newer
// transaction's afterPublication does not run: the stage passes the older
// commit first, then its own.
func TestAfterPublicationWaitsForEarlierPublications(t *testing.T) {
	b, _ := newAdmissionTestEngine(t, BadgerOptions{InMemory: true})
	_, err := b.CreateNode(&Node{ID: "db:older", Labels: []string{"L"}, Properties: map[string]any{"u": "O1"}})
	require.NoError(t, err)

	var events publicationEvents
	b.OnNodeCreated(func(node *Node) {
		if node.ID == "db:newer" {
			events.add("listener")
		}
	})
	var hookRuns atomic.Int32
	publishHeld(t,
		func() error {
			return b.UpdateNode(&Node{ID: "db:older", Labels: []string{"L"}, Properties: map[string]any{"u": "O2"}})
		},
		func() error {
			tx, err := b.BeginTransaction()
			if err != nil {
				return err
			}
			if _, err := tx.CreateNode(&Node{ID: "db:newer", Labels: []string{"L"}, Properties: map[string]any{"u": "N"}}); err != nil {
				return err
			}
			return tx.CommitContextWithPublication(context.Background(), func() {
				hookRuns.Add(1)
				events.add("afterPublication")
			})
		},
		func() {
			require.Zero(t, hookRuns.Load(), "afterPublication ran while an earlier commit's publication was held")
		})
	require.EqualValues(t, 1, hookRuns.Load())
	require.Equal(t, []string{"afterPublication", "listener"}, events.list())
}

// A buffered (write-behind) commit runs afterPublication once, at its
// acknowledgement; the replay that publishes it later doesn't run it again.
func TestAfterPublicationBufferedCommitRunsOnce(t *testing.T) {
	b, sm := newAdmissionTestEngine(t, BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour})
	var hookRuns atomic.Int32
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	_, err = tx.CreateNode(&Node{ID: "db:buffered", Labels: []string{"L"}, Properties: map[string]any{"u": "B"}})
	require.NoError(t, err)
	require.NoError(t, tx.CommitContextWithPublication(context.Background(), func() { hookRuns.Add(1) }))
	require.EqualValues(t, 1, hookRuns.Load(), "afterPublication runs at the buffered commit's acknowledgement")
	require.NotZero(t, b.writeBehind.PendingOps(), "the commit is buffered")

	require.NoError(t, b.FlushWriteBehind())
	require.EqualValues(t, 1, hookRuns.Load(), "the replay doesn't run afterPublication again")
	holder, found, _, _ := sm.lookupUniqueConstraintValueForValidation("L", "u", "B")
	require.True(t, found && holder == "db:buffered", "the replay published the commit")
}
