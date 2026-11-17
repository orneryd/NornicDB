package storage

import (
	"context"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
)

// TestPhysicalCacheProjectionCapacityGrowthPreservesFacts stages more key
// families than the facts slice holds at any earlier capacity boundary and
// requires the exact facts in the exact order. Doubling may only enlarge the
// slice: it never drops, reorders or re-resolves a fact.
func TestPhysicalCacheProjectionCapacityGrowthPreservesFacts(t *testing.T) {
	fixture := newProjectionFixture(t)
	const rows = 300
	var want []AffectedFact
	var projection physicalCacheProjection
	for row := 0; row < rows; row++ {
		id := NodeID(fmt.Sprintf("test:growth-%d", row))
		// The key mix one labeled node write stages: the body, the label index
		// entry, an embedding chunk, and two families that install no cached
		// body and so emit no fact.
		for _, key := range [][]byte{
			nodeKey(id),
			labelIndexKey("Person", fixture.aNum),
			embeddingKey(id, 0),
			{prefixMVCCNode, 0x01},
			{prefixTemporalHead, 0x01},
		} {
			projection.add(fixture.engine, key, nil)
		}
		want = append(want,
			AffectedFact{NodeID: id},
			AffectedFact{NodeID: "test:a", Label: "Person"},
			AffectedFact{NodeID: id},
		)
	}
	require.False(t, projection.unknown, "every staged key belongs to a classified family")
	require.True(t, projection.scope().known)
	var got []AffectedFact
	projection.VisitAffectedFacts(func(fact AffectedFact) { got = append(got, fact) })
	require.Equal(t, want, got)
}

// TestPhysicalCacheProjectionCapacityGrowthKeepsUnknownInvalidation keeps the
// invalidation contract under the growth path: a malformed key of a classified
// family's own prefix still marks the scope unknown and emits nothing, the
// scope stays unknown for the whole projection, and classified keys after it
// still record their facts.
func TestPhysicalCacheProjectionCapacityGrowthKeepsUnknownInvalidation(t *testing.T) {
	fixture := newProjectionFixture(t)
	// A label index key of the classified prefix whose numeric-ID separator is
	// not where the family requires it.
	malformed := append([]byte{prefixLabelIndex}, []byte("Person")...)
	malformed = append(malformed, 9, 9, 9, 9, 9, 9, 9, 9, 9)

	var projection physicalCacheProjection
	for row := 0; row < 300; row++ {
		if row == 150 {
			projection.add(fixture.engine, malformed, nil)
			require.True(t, projection.unknown, "a same-prefix malformed key invalidates the scope")
			require.False(t, projection.scope().known)
			require.Len(t, projection.facts, 150, "an unclassified key emits no fact")
		}
		projection.add(fixture.engine, nodeKey(NodeID(fmt.Sprintf("test:unknown-%d", row))), nil)
	}
	require.True(t, projection.unknown)
	require.False(t, projection.scope().known)
	require.Len(t, projection.facts, 300, "a classified key still records its fact after an unknown one")
	require.Equal(t, "test:unknown-0", projection.facts[0].id)
	require.Equal(t, "test:unknown-299", projection.facts[299].id)
}

// TestEntityWriteLockCapacityGrowthPreservesHolds takes more entity write locks
// than the inline array and the index threshold hold, and requires every take:
// the in-order holding, the findable key of every held entity after each
// growth, one hold for a repeated lock, and a rollback that releases exactly
// what the growth carried.
func TestEntityWriteLockCapacityGrowthPreservesHolds(t *testing.T) {
	engine := newTestEngine(t)
	ctx := t.Context()
	const nodes = 64
	ids := make([]NodeID, 0, nodes)
	for row := 0; row < nodes; row++ {
		id := NodeID(fmt.Sprintf("test:lock-growth-%d", row))
		_, err := engine.CreateNode(ctx, &Node{ID: id, Labels: []string{"LockGrowth"}})
		require.NoError(t, err)
		ids = append(ids, id)
	}

	tx, err := engine.BeginTransaction(ctx, OrdinaryBegin)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("test"))
	for at, id := range ids {
		latest, err := tx.LockNodeForWrite(ctx, id)
		require.NoError(t, err)
		require.Nil(t, latest)
		require.Len(t, tx.entityLocks.held, at+1)
		require.Equal(t, string(id), tx.entityLocks.held[at].request.key.entityID, "the holding keeps its order")
		for _, taken := range ids[:at+1] {
			require.NotNil(t, tx.entityLocks.find(entityWriteLockKey(bufferedNodeEntity, string(taken))),
				"lock %s is lost after a growth", taken)
		}
	}
	// A repeated lock reuses the hold: it neither adds a second entry nor
	// moves the entity's position.
	latest, err := tx.LockNodeForWrite(ctx, ids[0])
	require.NoError(t, err)
	require.Nil(t, latest)
	require.Len(t, tx.entityLocks.held, nodes)
	require.Equal(t, string(ids[0]), tx.entityLocks.held[0].request.key.entityID)

	require.NoError(t, tx.Rollback())
	require.Empty(t, tx.entityLocks.held)
	// The rolled-back transaction released every hold the growth copied, so a
	// peer takes them all instead of waiting for one.
	peerCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	peer, err := engine.BeginTransaction(ctx, OrdinaryBegin)
	require.NoError(t, err)
	defer func() { _ = peer.Rollback() }()
	require.NoError(t, peer.SetNamespace("test"))
	for _, id := range ids {
		if _, err := peer.LockNodeForWrite(peerCtx, id); err != nil {
			t.Fatalf("lock %s after the holder rolled back: %v", id, err)
		}
	}
}

// TestEntityWriteLockCapacityGrowthKeepsLatestReceipts grows the holding after
// an entity was locked as last committed and requires that receipt to survive
// every copy: the entity reads as last committed, a plain hold does not, and
// the index the growth keeps still finds both.
func TestEntityWriteLockCapacityGrowthKeepsLatestReceipts(t *testing.T) {
	engine := newTestEngine(t)
	ctx := t.Context()
	const nodes = 48
	ids := make([]NodeID, 0, nodes)
	for row := 0; row < nodes; row++ {
		id := NodeID(fmt.Sprintf("test:latest-growth-%d", row))
		_, err := engine.CreateNode(ctx, &Node{ID: id, Labels: []string{"LockGrowth"}, Properties: map[string]interface{}{"v": int64(1)}})
		require.NoError(t, err)
		ids = append(ids, id)
	}

	tx, err := engine.BeginTransaction(ctx, OrdinaryBegin)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("test"))
	// A commit after this transaction's snapshot makes the lock a
	// last-committed read whose receipt the growth copies must carry.
	stale := ids[0]
	stored, err := engine.GetStoredNode(ctx, stale)
	require.NoError(t, err)
	stored.Properties["v"] = int64(2)
	require.NoError(t, engine.UpdateNode(ctx, NodeUpdateRequest{After: stored, Policy: ClientNodeEdits}))
	latest, err := tx.LockNodeForWrite(ctx, stale)
	require.NoError(t, err)
	require.NotNil(t, latest, "the entity was committed after the snapshot")
	require.True(t, tx.readsLatestLocked(bufferedNodeEntity, string(stale)))

	for _, id := range ids[1:] {
		_, err := tx.LockNodeForWrite(ctx, id)
		require.NoError(t, err)
		require.True(t, tx.readsLatestLocked(bufferedNodeEntity, string(stale)),
			"the last-committed receipt is lost after a growth")
	}
	require.False(t, tx.readsLatestLocked(bufferedNodeEntity, string(ids[1])),
		"a hold without a commit after the snapshot reads the snapshot")
	require.Equal(t, 1, tx.entityLocks.latest)
	require.Len(t, tx.entityLocks.held, nodes)
}

// TestEntityWriteLockIndexSeparatesHeldIdentities requires the compact index a
// held lock is found through to keep the identity its canonical key carries:
// the entity kind and the namespace-full entity ID. A node and a relationship
// of one ID, and one local ID taken in two namespaces, are different holds; an
// ID without its namespace prefix is no hold at all; and a latest receipt
// stays with the hold that carries it.
func TestEntityWriteLockIndexSeparatesHeldIdentities(t *testing.T) {
	var locks entityWriteLocks
	take := func(kind bufferedEntityKind, id string) *entityWriteLock {
		return locks.add(uniqueConstraintLockRequest{key: entityWriteLockKey(kind, id)})
	}
	// Lock past the index threshold before the controls, so the controls join
	// a built index and the index's own build path carries the fillers.
	for at := 0; at <= entityWriteLockIndexAfter; at++ {
		take(bufferedNodeEntity, fmt.Sprintf("db1:filler-%d", at))
	}
	node := take(bufferedNodeEntity, "db1:n1")
	edge := take(bufferedEdgeEntity, "db1:n1")
	peer := take(bufferedNodeEntity, "db2:n1")
	require.NotNil(t, locks.index, "the controls join the built index")

	held := func(kind bufferedEntityKind, id string) *entityWriteLock {
		t.Helper()
		return locks.find(entityWriteLockKey(kind, id))
	}
	require.Same(t, node, held(bufferedNodeEntity, "db1:n1"), "the node hold is found by its identity")
	require.Same(t, edge, held(bufferedEdgeEntity, "db1:n1"), "a relationship of the node's ID is another hold")
	require.Same(t, peer, held(bufferedNodeEntity, "db2:n1"), "the local ID taken in another namespace is another hold")
	require.Equal(t, entityWriteLockKey(bufferedNodeEntity, "db1:n1"), node.request.key,
		"the hold keeps its canonical request key")
	require.Len(t, locks.index, len(locks.held), "every hold is indexed once")

	edge.latest = true
	require.Same(t, edge, held(bufferedEdgeEntity, "db1:n1"))
	require.True(t, edge.latest, "the hold the index selects carries its receipt")
	require.False(t, node.latest, "the node hold of the same ID keeps its own receipt")

	require.Nil(t, locks.find(entityWriteLockKey(bufferedNodeEntity, "n1")), "the index keeps the namespace prefix")
	require.Nil(t, locks.find(entityWriteLockKey(bufferedEdgeEntity, "db2:n1")), "an untaken relationship identity is not found")
	require.Nil(t, locks.find(entityWriteLockKey(bufferedNodeEntity, "db1:absent")), "an untaken node identity is not found")
}

// TestEntityWriteLockIndexGrowthKeepsNodeAndEdgeHolds grows one transaction's
// accumulator past the inline array and the index threshold with node and
// relationship holds, then requires each take to stay findable under its own
// entity kind and a rollback to release every hold the growth carried, in the
// order the transaction took them.
func TestEntityWriteLockIndexGrowthKeepsNodeAndEdgeHolds(t *testing.T) {
	engine := newTestEngine(t)
	ctx := t.Context()
	const rows = 6
	ids := make([]NodeID, 0, rows)
	edgeIDs := make([]EdgeID, 0, rows-1)
	for row := 0; row < rows; row++ {
		id := NodeID(fmt.Sprintf("test:identity-%d", row))
		_, err := engine.CreateNode(ctx, &Node{ID: id, Labels: []string{"Identity"}})
		require.NoError(t, err)
		ids = append(ids, id)
	}
	for row := 0; row+1 < rows; row++ {
		edgeID := EdgeID(fmt.Sprintf("test:identity-edge-%d", row))
		require.NoError(t, engine.CreateEdge(ctx, &Edge{ID: edgeID, StartNode: ids[row], EndNode: ids[row+1], Type: "NEXT"}))
		edgeIDs = append(edgeIDs, edgeID)
	}

	tx, err := engine.BeginTransaction(ctx, OrdinaryBegin)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("test"))
	for row := 0; row < rows; row++ {
		latest, err := tx.LockNodeForWrite(ctx, ids[row])
		require.NoError(t, err)
		require.Nil(t, latest)
	}
	for _, edgeID := range edgeIDs {
		latest, err := tx.LockEdgeForWrite(ctx, edgeID)
		require.NoError(t, err)
		require.Nil(t, latest)
	}
	require.NotNil(t, tx.entityLocks.index, "the growth built the index")
	require.Len(t, tx.entityLocks.held, rows+len(edgeIDs))

	for _, id := range ids {
		held := tx.entityLocks.find(entityWriteLockKey(bufferedNodeEntity, string(id)))
		require.NotNil(t, held, "node %s is found after the growth", id)
		require.Equal(t, uint8(nodeWriteLockKind), held.request.key.entityKind)
	}
	for at, edgeID := range edgeIDs {
		held := tx.entityLocks.find(entityWriteLockKey(bufferedEdgeEntity, string(edgeID)))
		require.NotNil(t, held, "relationship %s is found after the growth", edgeID)
		require.Equal(t, uint8(edgeWriteLockKind), held.request.key.entityKind)
		require.NotSame(t, tx.entityLocks.find(entityWriteLockKey(bufferedNodeEntity, string(ids[at]))), held,
			"the node %s and the relationship %s are different holds", ids[at], edgeID)
	}
	require.Nil(t, tx.entityLocks.find(entityWriteLockKey(bufferedEdgeEntity, string(ids[len(ids)-1]))),
		"the last node has no relationship hold")

	taken := make([]uniqueConstraintLockKey, len(tx.entityLocks.held))
	for at := range tx.entityLocks.held {
		taken[at] = tx.entityLocks.held[at].request.key
	}
	require.NoError(t, tx.Rollback())

	// The rolled-back transaction released every hold the growth copied, so a
	// peer takes each canonical key again instead of waiting for one.
	peerCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	peer, err := engine.BeginTransaction(ctx, OrdinaryBegin)
	require.NoError(t, err)
	defer func() { _ = peer.Rollback() }()
	require.NoError(t, peer.SetNamespace("test"))
	for at, key := range taken {
		switch key.entityKind {
		case nodeWriteLockKind:
			_, err := peer.LockNodeForWrite(peerCtx, NodeID(key.entityID))
			require.NoError(t, err, "hold %d (%s) was released", at, key.entityID)
		case edgeWriteLockKind:
			_, err := peer.LockEdgeForWrite(peerCtx, EdgeID(key.entityID))
			require.NoError(t, err, "hold %d (%s) was released", at, key.entityID)
		default:
			t.Fatalf("hold %d has entity kind %d", at, key.entityKind)
		}
	}
}

// TestEntityWriteLockIdentityStaysCompact requires the accumulator's index key
// to stay smaller than the canonical lock key it stands in for: the canonical
// key carries every constraint field, so a field added back to the identity
// would return that size to every lock a bulk statement holds.
func TestEntityWriteLockIdentityStaysCompact(t *testing.T) {
	require.True(t, unsafe.Sizeof(entityWriteLockIdentity{}) < unsafe.Sizeof(uniqueConstraintLockKey{}),
		"the indexed identity is smaller than the canonical key it stands in for")
}
