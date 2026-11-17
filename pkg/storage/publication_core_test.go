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
	constraints, viewErr := capturePublicationConstraintView(t.Context(), schema)
	require.NoError(t, viewErr)
	old := &Node{ID: "db:a", Labels: []string{"L"}, Properties: map[string]any{"u": "A"}}
	written := CopyNode(old)
	written.Properties["u"] = "B"
	taken := &Node{ID: "db:b", Labels: []string{"L"}, Properties: map[string]any{"u": "A"}}
	freedKeys, keyErr := appendNodePublicationKeys(t.Context(), nil, &constraints, old.ID, old, written)
	require.NoError(t, keyErr)
	takenKeys, keyErr := appendNodePublicationKeys(t.Context(), nil, &constraints, taken.ID, nil, taken)
	require.NoError(t, keyErr)
	for _, tc := range []struct {
		name         string
		older, newer []any
	}{
		{"same entity", []any{NodeID("db:n")}, []any{NodeID("db:n")}},
		{"repeated entity dependency", []any{NodeID("db:n"), NodeID("db:n")}, []any{NodeID("db:n"), NodeID("db:n")}},
		// The later entity takes A only after the older entity frees it. Their
		// IDs differ, so only the UNIQUE-value dependency establishes the order.
		{"freed unique value across entities", freedKeys, takenKeys},
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
	constraints, viewErr := capturePublicationConstraintView(t.Context(), schema)
	require.NoError(t, viewErr)
	older := &Node{ID: "db:a", Labels: []string{"L"}, Properties: map[string]any{"u": "A"}}
	newer := CopyNode(older)
	newer.ID = "peer:b"
	olderKeys, keyErr := appendNodePublicationKeys(t.Context(), nil, &constraints, older.ID, nil, older)
	require.NoError(t, keyErr)
	newerKeys, keyErr := appendNodePublicationKeys(t.Context(), nil, &constraints, newer.ID, nil, newer)
	require.NoError(t, keyErr)
	for _, tc := range []struct {
		name         string
		older, newer []any
	}{
		{"different entities", []any{NodeID("db:a")}, []any{NodeID("db:b")}},
		{"same unique value in different namespaces", olderKeys, newerKeys},
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
		olderErr = b.withPublishingUpdate(context.Background(), func(txn *badger.Txn) error { return txn.Set([]byte("core:entity"), []byte("body")) }, func(*commitCacheImages) { close(entered); <-release }, func(context.Context) ([]any, error) { return []any{NodeID("db:n")}, nil }, nil, nil)
	}()
	if !awaitPublicationCore(t, entered, "older commit rendezvous") {
		return
	}
	metadataDone = make(chan struct{})
	go func() {
		defer close(metadataDone)
		metadataErr = b.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error { return txn.Set([]byte("core:metadata"), []byte("value")) })
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
					if e := cw.write(cw.ctx, func(txn *badger.Txn) error { return txn.Set([]byte(fmt.Sprintf("core:large:%06d", i)), payload) }); e != nil {
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
	require.NoError(t, b.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error { return txn.Set([]byte("core:conflict"), []byte("A")) }))
	err = b.commitEngineWrite(context.Background(), func(cw *commitWriter) error {
		cw.notification = n
		_, e := cw.txn.Get([]byte("core:conflict"))
		if e != nil {
			return e
		}
		if e = b.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error { return txn.Set([]byte("core:conflict"), []byte("B")) }); e != nil {
			return e
		}
		return (func(txn *badger.Txn) error { return txn.Set([]byte("core:conflict"), []byte("C")) })(cw.txn)
	})
	require.ErrorIs(t, err, badger.ErrConflict)
	require.True(t, n.reserved)
	require.Empty(t, n.ops)
	b.completeCommitNotification(n)
	require.Zero(t, calls)
	require.Nil(t, b.notificationHead)
	require.Zero(t, b.db.oracle.stage.pendingCount())
}

// appendNodePublicationKeys emits one entity key and a fact for every before/
// after/label change of it. The stage orders all of them on the entity, so one
// entity is one queue entry and one dependency.
func TestPublicationCoreOrdersEntityFactsAsOneDependency(t *testing.T) {
	id := NodeID("db:n")
	old := &Node{ID: id, Labels: []string{"L", "M"}}
	written := CopyNode(old)
	keys, keyErr := appendNodePublicationKeys(t.Context(), nil, nil, id, old, written)
	require.NoError(t, keyErr)
	require.Equal(t, id, keys[0], "the entity key precedes its facts")
	var facts int
	for _, key := range keys {
		if _, ok := key.(AffectedFact); ok {
			facts++
		}
	}
	require.GreaterOrEqual(t, facts, 4, "the fixture needs multiple before/after/label facts")
	for _, tc := range []struct {
		name  string
		older []any
	}{
		{"entity key and its facts", keys},
		{"facts only", keys[1:]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stage publicationStage
			var observed []int
			stage.register(10, &commitPublication{keys: tc.older, run: func() { observed = append(observed, 10) }})
			stage.register(20, &commitPublication{keys: []any{id}, run: func() { observed = append(observed, 20) }})
			require.Len(t, stage.byKey, 1, "every fact about one entity must share the entity's queue")
			queue, exists := stage.byKey[id]
			require.True(t, exists)
			require.Equal(t, uint64(10), queue.first)
			require.Equal(t, []uint64{20}, queue.rest)
			stage.apply(20)
			require.Equal(t, []int{10, 20}, observed)
			require.Zero(t, stage.pendingCount())
			require.Empty(t, stage.byKey)
		})
	}
}

// A commit can publish facts for an entity whose own ID key it does not carry:
// a node delete publishes its cascaded edges as DeletedEdgeFacts. The stage
// orders those facts on the entity they name, so an earlier fact-only producer
// still orders before a later publication of that entity's own key.
func TestPublicationCoreFactOnlyProducerOrdersWithEntityKey(t *testing.T) {
	const (
		node = NodeID("db:n")
		edge = EdgeID("db:e")
	)
	for _, tc := range []struct {
		name         string
		older, newer []any
	}{
		{"node fact before node key", []any{AffectedFact{NodeID: node, Label: "L", Before: true}}, []any{node}},
		{"edge fact before edge key", []any{AffectedFact{EdgeID: edge, StartNode: node, EndNode: "db:m", Type: "T", Deleted: true, Before: true}}, []any{edge}},
		{"differently shaped facts of one entity", []any{AffectedFact{NodeID: node, Label: "L"}}, []any{AffectedFact{NodeID: node, Label: "M", Before: true}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stage publicationStage
			var observed []int
			stage.register(20, &commitPublication{keys: tc.newer, run: func() { observed = append(observed, 20) }})
			stage.register(10, &commitPublication{keys: tc.older, run: func() { observed = append(observed, 10) }})
			require.Len(t, stage.byKey, 1)
			stage.apply(20)
			require.Equal(t, []int{10, 20}, observed)
			require.Zero(t, stage.pendingCount())
			require.Empty(t, stage.byKey)
		})
	}
}

// Facts of different entities stay independent: a held publication of one
// entity's facts must not block another entity's publication.
func TestPublicationCoreDisjointFactEntitiesDoNotWait(t *testing.T) {
	held, other := NodeID("db:a"), NodeID("db:b")
	var stage publicationStage
	entered, release, heldDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var otherDone chan struct{}
	var releaseOnce sync.Once
	releaseHeld := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(func() {
		releaseHeld()
		awaitPublicationCore(t, heldDone, "held publication cleanup")
		awaitPublicationCore(t, otherDone, "other publication cleanup")
	})
	stage.register(10, &commitPublication{keys: []any{
		AffectedFact{NodeID: held, Label: "L", Before: true},
		AffectedFact{NodeID: held, Deleted: true},
	}, run: func() { close(entered); <-release }})
	stage.register(20, &commitPublication{keys: []any{other, AffectedFact{NodeID: other, Label: "M", Before: true}}, run: func() {}})
	go func() { stage.apply(10); close(heldDone) }()
	if !awaitPublicationCore(t, entered, "held publication rendezvous") {
		return
	}
	otherDone = make(chan struct{})
	go func() { stage.apply(20); close(otherDone) }()
	if !awaitPublicationCore(t, otherDone, "disjoint fact publication while the other entity is held") {
		return
	}
	releaseHeld()
	if !awaitPublicationCore(t, heldDone, "held publication") {
		return
	}
	require.Zero(t, stage.pendingCount())
}

// The stage must not replace the facts the commit published: the cache
// projections still visit the original AffectedFacts of pub.keys.
func TestPublicationCoreKeepsFactKeysForVisitors(t *testing.T) {
	fact := AffectedFact{EdgeID: "db:e", StartNode: "db:a", EndNode: "db:b", Type: "T", Deleted: true, Before: true}
	published := []any{
		NodeID("db:n"),
		AffectedFact{NodeID: "db:n", Label: "L", Before: true},
		EdgeID("db:e"),
		fact,
	}
	var want []AffectedFact
	publicationKeys(published).VisitAffectedFacts(func(visited AffectedFact) { want = append(want, visited) })
	// Entity IDs and facts both visit as facts: the visitors keep both.
	require.Len(t, want, 4)
	require.Equal(t, fact, want[3])
	pub := &commitPublication{keys: published, run: func() {}}
	var stage publicationStage
	stage.register(10, pub)
	stage.apply(10)
	require.Equal(t, published, pub.keys, "the stage must not rewrite the published keys")
	var got []AffectedFact
	publicationKeys(pub.keys).VisitAffectedFacts(func(visited AffectedFact) { got = append(got, visited) })
	require.Equal(t, want, got, "cache projections must still see the original facts")
	require.Zero(t, stage.pendingCount())
	require.Empty(t, stage.byKey)
}
