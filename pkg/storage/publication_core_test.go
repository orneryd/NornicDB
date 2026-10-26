package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

func TestPublicationCoreHelpsEarlierOutOfOrderRegistration(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.AddUniqueConstraint("unique_u", "L", "u"))
	constraints := capturePublicationConstraintView(schema)
	old := &Node{ID: "db:a", Labels: []string{"L"}, Properties: map[string]any{"u": "A"}}
	written := CopyNode(old)
	written.Properties["u"] = "B"
	taken := &Node{ID: "db:b", Labels: []string{"L"}, Properties: map[string]any{"u": "A"}}
	for _, tc := range []struct {
		name         string
		older, newer []any
	}{
		{"same entity", []any{NodeID("db:n")}, []any{NodeID("db:n")}},
		{"repeated entity dependency", []any{NodeID("db:n"), NodeID("db:n")}, []any{NodeID("db:n"), NodeID("db:n")}},
		// The later entity takes A only after the older entity frees it. Their
		// IDs differ, so only the UNIQUE-value dependency establishes the order.
		{"freed unique value across entities", appendNodePublicationKeys(nil, &constraints, old.ID, old, written), appendNodePublicationKeys(nil, &constraints, taken.ID, nil, taken)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stage publicationStage
			var observed []int
			stage.register(20, &commitPublication{keys: tc.newer, run: func() { observed = append(observed, 20) }})
			stage.register(10, &commitPublication{keys: tc.older, run: func() { observed = append(observed, 10) }})
			stage.apply(20)
			require.Equal(t, []int{10, 20}, observed)
			require.Zero(t, stage.pendingCount())
			require.Empty(t, stage.byKey, "retired dependencies must not retain entity keys")
		})
	}
}

// awaitPublicationCore bounds both positive assertions and cleanup joins.
func awaitPublicationCore(t *testing.T, done <-chan struct{}, label string) bool {
	t.Helper()
	if done == nil {
		return true
	}
	select {
	case <-done:
		return true
	case <-time.After(10 * time.Second):
		t.Errorf("%s did not finish", label)
		return false
	}
}

func TestPublicationCoreDisjointKeysDoNotWait(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.AddUniqueConstraint("unique_u", "L", "u"))
	constraints := capturePublicationConstraintView(schema)
	older := &Node{ID: "db:a", Labels: []string{"L"}, Properties: map[string]any{"u": "A"}}
	newer := CopyNode(older)
	newer.ID = "peer:b"
	for _, tc := range []struct {
		name         string
		older, newer []any
	}{
		{"different entities", []any{NodeID("db:a")}, []any{NodeID("db:b")}},
		{"same unique value in different namespaces", appendNodePublicationKeys(nil, &constraints, older.ID, nil, older), appendNodePublicationKeys(nil, &constraints, newer.ID, nil, newer)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stage publicationStage
			entered, release, olderDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var newerDone chan struct{}
			var releaseOnce sync.Once
			releaseOlder := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(func() {
				releaseOlder()
				awaitPublicationCore(t, olderDone, "older publication cleanup")
				awaitPublicationCore(t, newerDone, "newer publication cleanup")
			})
			stage.register(10, &commitPublication{keys: tc.older, run: func() { close(entered); <-release }})
			stage.register(20, &commitPublication{keys: tc.newer, run: func() {}})
			go func() { stage.apply(10); close(olderDone) }()
			if !awaitPublicationCore(t, entered, "older publication rendezvous") {
				return
			}
			newerDone = make(chan struct{})
			go func() { stage.apply(20); close(newerDone) }()
			if !awaitPublicationCore(t, newerDone, "disjoint publication while older key is held") {
				return
			}
			releaseOlder()
			if !awaitPublicationCore(t, olderDone, "older publication") {
				return
			}
			require.Zero(t, stage.pendingCount())
		})
	}
}

func TestPublicationCoreMetadataCommitDoesNotWaitOnStage(t *testing.T) {
	b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{InMemory: true}))
	if b != nil {
		fixtureowner.Own(t, b)
	}
	require.NoError(t, err)
	entered, release, olderDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var metadataDone chan struct{}
	var olderErr, metadataErr error
	var releaseOnce sync.Once
	releaseOlder := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(func() {
		releaseOlder()
		awaitPublicationCore(t, olderDone, "older commit cleanup")
		awaitPublicationCore(t, metadataDone, "metadata commit cleanup")
	})
	go func() {
		defer close(olderDone)
		olderErr = b.withPublishingUpdate(context.Background(), func(txn *badger.Txn) error { return txn.Set([]byte("core:entity"), []byte("body")) }, func(*commitCacheImages) { close(entered); <-release }, func() []any { return []any{NodeID("db:n")} }, nil)
	}()
	if !awaitPublicationCore(t, entered, "older commit rendezvous") {
		return
	}
	metadataDone = make(chan struct{})
	go func() {
		defer close(metadataDone)
		metadataErr = b.withUpdate(func(txn *badger.Txn) error { return txn.Set([]byte("core:metadata"), []byte("value")) })
	}()
	if !awaitPublicationCore(t, metadataDone, "metadata commit while publication is held") {
		return
	}
	releaseOlder()
	if !awaitPublicationCore(t, olderDone, "older commit") {
		return
	}
	require.NoError(t, olderErr)
	require.NoError(t, metadataErr)
	require.Zero(t, b.db.oracle.stage.pendingCount())
}

func TestPublicationCoreLargeCommitPublishesOnceAndAbortPublishesNothing(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint("abort=", fail), func(t *testing.T) {
			b := openLargeCommitTestEngine(t, t.TempDir())
			var published atomic.Int32
			failure := errors.New("stop large commit")
			if fail {
				defer setLargeCommitBatchHook(func(int) error { return failure })()
			}
			var batches int
			err := b.commitEngineWrite(context.Background(), func(cw *commitWriter) error {
				cw.publish = func(*commitCacheImages) { published.Add(1) }
				cw.publishKeys = []any{NodeID("db:n")}
				payload := make([]byte, 64)
				for i := 0; i < 20000; i++ {
					if e := cw.write(func(txn *badger.Txn) error { return txn.Set([]byte(fmt.Sprintf("core:large:%06d", i)), payload) }); e != nil {
						return e
					}
				}
				if cw.large != nil {
					batches = cw.large.batches
				}
				return nil
			})
			if fail {
				require.ErrorIs(t, err, failure)
				require.Zero(t, published.Load())
			} else {
				require.NoError(t, err)
				require.Positive(t, batches)
				require.Equal(t, int32(1), published.Load())
			}
			require.Zero(t, b.db.oracle.stage.pendingCount())
			requireNoLargeCommitIntent(t, b)
		})
	}
}

func TestPublicationCoreFailedCommitRetiresReservedNotification(t *testing.T) {
	b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{InMemory: true}))
	if b != nil {
		fixtureowner.Own(t, b)
	}
	require.NoError(t, err)
	calls := 0
	b.OnNodeCreated(func(*Node) { calls++ })
	n := b.queueCommitOperations(nil, &Operation{Type: OpCreateNode, Node: &Node{ID: "db:n"}}, notificationAtCommit)
	require.NotNil(t, n)
	// Reserve through the same timestamp owner and then reject the physical
	// write with a conflict. The caller completes its empty entry after abort.
	require.NoError(t, b.withUpdate(func(txn *badger.Txn) error { return txn.Set([]byte("core:conflict"), []byte("A")) }))
	err = b.commitEngineWrite(context.Background(), func(cw *commitWriter) error {
		cw.notification = n
		_, e := cw.txn.Get([]byte("core:conflict"))
		if e != nil {
			return e
		}
		if e = b.withUpdate(func(txn *badger.Txn) error { return txn.Set([]byte("core:conflict"), []byte("B")) }); e != nil {
			return e
		}
		return cw.writeOnce(func(txn *badger.Txn) error { return txn.Set([]byte("core:conflict"), []byte("C")) })
	})
	require.ErrorIs(t, err, badger.ErrConflict)
	require.True(t, n.reserved)
	require.Empty(t, n.ops)
	b.completeCommitNotification(n)
	require.Zero(t, calls)
	require.Nil(t, b.notificationHead)
	require.Zero(t, b.db.oracle.stage.pendingCount())
}
