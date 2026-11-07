package storage

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// reservedPublicationPeer starts an implicit transaction that stages write
// and blocks its commit in the stored-entity observer: after its MVCC
// version is reserved and before it is acknowledged into the buffer. The
// returned unblock releases it; the returned wait settles the commit.
func reservedPublicationPeer(t *testing.T, b *BadgerEngine, write func(*BadgerTransaction) error) (unblock func(), wait func() error) {
	t.Helper()
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	unblock = func() { releaseOnce.Do(func() { close(release) }) }
	require.NoError(t, b.InitializeStoredEntityObserver("test", func(StoredEntitySnapshot) (StoredEntityObserver, error) {
		return func(StoredEntityPublication) (func(), error) {
			once.Do(func() { close(entered); <-release })
			return nil, nil
		}, nil
	}, nil))
	older, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = older.Rollback() })
	require.NoError(t, older.SetImplicit(true))
	require.NoError(t, write(older))
	var commitErr error
	go func() { defer close(done); commitErr = older.Commit() }()
	t.Cleanup(func() {
		unblock()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("the peer commit did not settle")
		}
	})
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the peer commit did not reach the observer")
	}
	wait = func() error {
		select {
		case <-done:
			return commitErr
		case <-time.After(10 * time.Second):
			t.Fatal("the peer commit did not finish")
			return nil
		}
	}
	return unblock, wait
}

// reservedPublicationFixture is the graph the matrix starts from: two
// endpoints, and the entity the peer and the stale transaction contend for
// when the stale write is not a creation.
func reservedPublicationFixture(t *testing.T, b *BadgerEngine, op, kind string) {
	t.Helper()
	ctx := context.Background()
	for _, id := range []NodeID{"test:n2", "test:n3"} {
		_, err := b.CreateNode(ctx, &Node{ID: id, Properties: map[string]any{"v": int64(1)}})
		require.NoError(t, err)
	}
	if op != "create" {
		if kind == "node" {
			_, err := b.CreateNode(ctx, reservedProbeNode(1))
			require.NoError(t, err)
		} else {
			require.NoError(t, b.CreateEdge(ctx, reservedProbeEdge("LINK", 1)))
		}
	}
	require.NoError(t, b.FlushWriteBehind(ctx))
	bufferedCreate(t, b, &Node{ID: "test:unrelated", Properties: map[string]any{"v": int64(1)}})
}

func reservedProbeNode(v int64) *Node {
	return &Node{ID: "test:n9", Labels: []string{"Probe"}, Properties: map[string]any{"v": v}}
}

func reservedProbeEdge(kind string, v int64) *Edge {
	return &Edge{ID: "test:r2", StartNode: "test:n2", EndNode: "test:n3", Type: kind, Properties: map[string]any{"v": v}}
}

// requireTypeCountsAgree compares every relationship-type counter with the
// stored relationships and the label counter with the stored probe nodes.
func requireTypeCountsAgree(t *testing.T, b *BadgerEngine) {
	t.Helper()
	ctx := context.Background()
	edges, err := b.AllEdges(ctx)
	require.NoError(t, err)
	byType := map[string]int{"LINK": 0, "MOVED": 0}
	for _, edge := range edges {
		byType[edge.Type]++
	}
	for edgeType, live := range byType {
		typed, err := b.EdgeCountByType(ctx, edgeType)
		require.NoError(t, err)
		require.EqualValues(t, live, typed, "%s relationship count", edgeType)
	}
	nodes, err := b.AllNodes(ctx)
	require.NoError(t, err)
	probes := 0
	for _, node := range nodes {
		for _, label := range node.Labels {
			if label == "Probe" {
				probes++
			}
		}
	}
	count, err := b.NodeCountByLabel(ctx, "Probe")
	require.NoError(t, err)
	require.EqualValues(t, probes, count, "Probe label count")
}

// BEGIN can include a peer's reserved MVCC version without having observed
// the peer's write. Whatever the stale transaction does to that entity
// afterwards, and whether the peer is still buffered or has replayed, the
// stale commit is a conflict: the peer's write stays, and the counters agree
// with the stored entities. Creations, updates and deletions, nodes and
// relationships, implicit and explicit commits.
func TestReservedPublicationConflictMatrix(t *testing.T) {
	for _, op := range []string{"create", "update", "delete"} {
		for _, kind := range []string{"node", "relationship"} {
			for _, implicit := range []bool{false, true} {
				for _, replay := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/implicit=%t/replayed=%t", op, kind, implicit, replay), func(t *testing.T) {
						b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
						ctx := context.Background()
						reservedPublicationFixture(t, b, op, kind)
						// The peer creates the entity, or changes the one both
						// transactions see; a stale deletion contends with a
						// peer that moved the relationship to another type.
						peerType := "LINK"
						if op == "delete" {
							peerType = "MOVED"
						}
						peer := func(tx *BadgerTransaction) error {
							if op == "create" {
								if kind == "node" {
									_, err := tx.CreateNode(t.Context(), reservedProbeNode(1))
									return err
								}
								return tx.CreateEdge(t.Context(), reservedProbeEdge("LINK", 1))
							}
							if kind == "node" {
								return ApplyNodeUpdate(context.Background(), tx, NodeUpdateRequest{After: reservedProbeNode(11)})
							}
							return tx.UpdateEdge(t.Context(), EdgeUpdateRequest{After: reservedProbeEdge(peerType, 11)})
						}
						stale := func(tx *BadgerTransaction) error {
							switch op {
							case "create":
								if kind == "node" {
									_, err := tx.CreateNode(t.Context(), reservedProbeNode(2))
									return err
								}
								return tx.CreateEdge(t.Context(), reservedProbeEdge("LINK", 2))
							case "update":
								if kind == "node" {
									return ApplyNodeUpdate(context.Background(), tx, NodeUpdateRequest{After: reservedProbeNode(22)})
								}
								return tx.UpdateEdge(t.Context(), EdgeUpdateRequest{After: reservedProbeEdge("LINK", 22)})
							default:
								if kind == "node" {
									return tx.DeleteGraphNode(t.Context(), "test:n9", CascadeEdges)
								}
								return tx.DeleteEdge(t.Context(), "test:r2")
							}
						}
						unblock, wait := reservedPublicationPeer(t, b, peer)
						// BEGIN while the peer holds its reserved MVCC version
						// but is not acknowledged into the buffer yet.
						staleTx, err := b.BeginTransaction(t.Context())
						require.NoError(t, err)
						t.Cleanup(func() { _ = staleTx.Rollback() })
						require.NoError(t, staleTx.SetImplicit(implicit))
						require.NoError(t, stale(staleTx), "the stale snapshot does not hold the peer's write")
						unblock()
						require.NoError(t, wait())
						if replay {
							require.NoError(t, b.FlushWriteBehind(ctx))
						}
						require.ErrorIs(t, staleTx.Commit(), ErrConflict, "a write over an entity a peer published after BEGIN is a conflict")
						require.NoError(t, b.FlushWriteBehind(ctx))
						wantV := int64(1)
						if op != "create" {
							wantV = 11
						}
						if kind == "node" {
							stored, err := b.GetNode(ctx, "test:n9")
							require.NoError(t, err)
							require.EqualValues(t, wantV, stored.Properties["v"], "the peer's node is the stored one")
						} else {
							stored, err := b.GetEdge(ctx, "test:r2")
							require.NoError(t, err)
							require.EqualValues(t, wantV, stored.Properties["v"], "the peer's relationship is the stored one")
							require.Equal(t, peerType, stored.Type)
						}
						requireTypeCountsAgree(t, b)
					})
				}
			}
		}
	}
}

// A caller-supplied ID that happens to be UUID-shaped is not a minted ID:
// with the implicit Cypher transaction's commit fast path on, a stale
// creation of such an ID over a peer's publication is still a conflict.
func TestReservedCreationStableUUIDConflicts(t *testing.T) {
	for _, implicit := range []bool{false, true} {
		for _, replay := range []bool{false, true} {
			for _, kind := range []string{"node", "relationship"} {
				t.Run(fmt.Sprintf("%s/implicit=%t/replayed=%t", kind, implicit, replay), func(t *testing.T) {
					b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
					ctx := context.Background()
					reservedPublicationFixture(t, b, "create", kind)
					edge := func(v int64) *Edge {
						return &Edge{ID: "test:550e8400-e29b-41d4-a716-446655440000", StartNode: "test:n2", EndNode: "test:n3", Type: "LINK", Properties: map[string]any{"v": v}}
					}
					node := func(v int64) *Node {
						return &Node{ID: "test:550e8400-e29b-41d4-a716-446655440001", Labels: []string{"Probe"}, Properties: map[string]any{"v": v}}
					}
					create := func(tx *BadgerTransaction, v int64) error {
						if kind == "node" {
							_, err := tx.CreateNode(t.Context(), node(v))
							return err
						}
						return tx.CreateEdge(t.Context(), edge(v))
					}
					unblock, wait := reservedPublicationPeer(t, b, func(tx *BadgerTransaction) error { return create(tx, 1) })
					stale, err := b.BeginTransaction(t.Context())
					require.NoError(t, err)
					t.Cleanup(func() { _ = stale.Rollback() })
					require.NoError(t, stale.SetImplicit(implicit))
					require.NoError(t, stale.SetValidationSetting(SkipCreateCommitHeadReload, true))
					require.NoError(t, create(stale, 2), "the stale snapshot does not hold the entity")
					unblock()
					require.NoError(t, wait())
					if replay {
						require.NoError(t, b.FlushWriteBehind(ctx))
					}
					require.ErrorIs(t, stale.Commit(), ErrConflict, "a creation over an entity a peer published after BEGIN is a conflict")
					require.NoError(t, b.FlushWriteBehind(ctx))
					if kind == "node" {
						stored, err := b.GetNode(ctx, node(0).ID)
						require.NoError(t, err)
						require.EqualValues(t, 1, stored.Properties["v"], "the peer's node is the stored one")
					} else {
						stored, err := b.GetEdge(ctx, edge(0).ID)
						require.NoError(t, err)
						require.EqualValues(t, 1, stored.Properties["v"], "the peer's relationship is the stored one")
					}
					requireTypeCountsAgree(t, b)
				})
			}
		}
	}
}

// Only an ID this transaction minted takes the commit fast path that skips
// the head reload; a caller-supplied ID keeps the head check whatever its
// shape.
func TestMintedCreationKeepsCommitFastPath(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	ctx := context.Background()
	for _, id := range []NodeID{"test:n2", "test:n3"} {
		_, err := b.CreateNode(ctx, &Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, b.FlushWriteBehind(ctx))
	tx, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetValidationSetting(SkipCreateCommitHeadReload, true))
	_, err = tx.CreateMintedNode(t.Context(), &Node{ID: "test:550e8400-e29b-41d4-a716-446655440010"})
	require.NoError(t, err)
	_, err = tx.CreateNode(t.Context(), &Node{ID: "test:550e8400-e29b-41d4-a716-446655440011"})
	require.NoError(t, err)
	require.NoError(t, tx.CreateMintedEdge(t.Context(), &Edge{ID: "test:550e8400-e29b-41d4-a716-446655440012", StartNode: "test:n2", EndNode: "test:n3", Type: "LINK"}))
	require.NoError(t, tx.CreateEdge(t.Context(), &Edge{ID: "test:550e8400-e29b-41d4-a716-446655440013", StartNode: "test:n2", EndNode: "test:n3", Type: "LINK"}))
	fresh := map[string]bool{}
	for _, op := range tx.operations {
		switch op.Type {
		case OpCreateNode:
			fresh[string(op.NodeID)] = op.FreshID
		case OpCreateEdge:
			fresh[string(op.EdgeID)] = op.FreshID
		}
	}
	require.Equal(t, map[string]bool{
		"test:550e8400-e29b-41d4-a716-446655440010": true,
		"test:550e8400-e29b-41d4-a716-446655440011": false,
		"test:550e8400-e29b-41d4-a716-446655440012": true,
		"test:550e8400-e29b-41d4-a716-446655440013": false,
	}, fresh)
	require.NoError(t, tx.Commit())
}

// A frame that removes an entity and creates the same identity again has no
// peer predecessor to conflict with: the creation commits and is counted once.
func TestFrameRemovedIdentityCanBeCreatedAgain(t *testing.T) {
	for _, route := range []string{"relationship", "relationship-cascade"} {
		t.Run(route, func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
			ctx := context.Background()
			reservedPublicationFixture(t, b, "update", "relationship")
			tx, err := b.BeginTransaction(t.Context())
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			if route == "relationship-cascade" {
				require.NoError(t, tx.DeleteGraphNode(t.Context(), "test:n3", CascadeEdges))
				_, err = tx.CreateNode(t.Context(), &Node{ID: "test:n3", Properties: map[string]any{"v": int64(2)}})
				require.NoError(t, err)
			} else {
				require.NoError(t, tx.DeleteEdge(t.Context(), "test:r2"))
			}
			require.NoError(t, tx.CreateEdge(t.Context(), reservedProbeEdge("LINK", 2)))
			require.NoError(t, tx.Commit())
			require.NoError(t, b.FlushWriteBehind(ctx))
			stored, err := b.GetEdge(ctx, "test:r2")
			require.NoError(t, err)
			require.EqualValues(t, 2, stored.Properties["v"])
			requireTypeCountsAgree(t, b)
		})
	}
}

// TestReservedPublicationCutDuringAllocation holds the reservation ledger
// while a stale transaction's BEGIN and a peer's commit both queue on it:
// the BEGIN to take its cut, the peer to reserve its version. The peer's
// sequence must not have advanced while it waits (the sequence and the
// ledger change in one step), and whichever runs first once the ledger is
// released, the stale write over the peer's entity is a conflict, buffered
// or replayed, and the counters agree with the stored relationships.
func TestReservedPublicationCutDuringAllocation(t *testing.T) {
	for _, op := range []string{"create", "update", "delete"} {
		for _, kind := range []string{"node", "relationship"} {
			for _, implicit := range []bool{false, true} {
				for _, replay := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/implicit=%t/replayed=%t", op, kind, implicit, replay), func(t *testing.T) {
						reservedCutDuringAllocationCase(t, op, kind, implicit, replay)
					})
				}
			}
		}
	}
}

func goroutineStackHas(fragment string) bool {
	stack := make([]byte, 1<<20)
	n := runtime.Stack(stack, true)
	return strings.Contains(string(stack[:n]), fragment)
}

func reservedCutDuringAllocationCase(t *testing.T, op, kind string, implicit, replay bool) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	ctx := context.Background()
	reservedPublicationFixture(t, b, op, kind)
	peerType := "LINK"
	if op == "delete" {
		peerType = "MOVED"
	}
	peerWrite := func(tx *BadgerTransaction) error {
		if op == "create" {
			if kind == "node" {
				_, err := tx.CreateNode(ctx, reservedProbeNode(1))
				return err
			}
			return tx.CreateEdge(ctx, reservedProbeEdge("LINK", 1))
		}
		if kind == "node" {
			return ApplyNodeUpdate(ctx, tx, NodeUpdateRequest{After: reservedProbeNode(11)})
		}
		return tx.UpdateEdge(ctx, EdgeUpdateRequest{After: reservedProbeEdge(peerType, 11)})
	}
	staleWrite := func(tx *BadgerTransaction) error {
		switch op {
		case "create":
			if kind == "node" {
				_, err := tx.CreateNode(ctx, reservedProbeNode(2))
				return err
			}
			return tx.CreateEdge(ctx, reservedProbeEdge("LINK", 2))
		case "update":
			if kind == "node" {
				return ApplyNodeUpdate(ctx, tx, NodeUpdateRequest{After: reservedProbeNode(22)})
			}
			return tx.UpdateEdge(ctx, EdgeUpdateRequest{After: reservedProbeEdge("LINK", 22)})
		default:
			if kind == "node" {
				return tx.DeleteGraphNode(ctx, "test:n9", CascadeEdges)
			}
			return tx.DeleteEdge(ctx, "test:r2")
		}
	}

	// The peer pauses in the stored-entity observer, after its version is
	// reserved and before its acknowledgement, so the stale write is staged
	// against a snapshot that does not hold the peer's write.
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	require.NoError(t, b.InitializeStoredEntityObserver("test", func(StoredEntitySnapshot) (StoredEntityObserver, error) {
		return func(StoredEntityPublication) (func(), error) {
			once.Do(func() { close(entered); <-release })
			return nil, nil
		}, nil
	}, nil))
	peer, err := b.BeginTransaction(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Rollback() })
	require.NoError(t, peer.SetImplicit(true))
	require.NoError(t, peerWrite(peer))

	state, err := b.namespaceMVCC("test")
	require.NoError(t, err)
	before := state.seq.Load()

	b.mvccReservations.mu.Lock()
	locked := true
	defer func() {
		if locked {
			b.mvccReservations.mu.Unlock()
		}
	}()
	type begun struct {
		tx  *BadgerTransaction
		err error
	}
	readerDone := make(chan begun, 1)
	go func() {
		tx, err := b.BeginTransaction(ctx)
		readerDone <- begun{tx, err}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for !goroutineStackHas("(*mvccReservations).cutStarted(") {
		require.False(t, time.Now().After(deadline), "BEGIN did not queue on the ledger")
		runtime.Gosched()
	}
	var commitErr error
	go func() { defer close(done); commitErr = peer.Commit() }()
	t.Cleanup(func() {
		unblock()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("the peer commit did not settle")
		}
	})
	for !goroutineStackHas("(*mvccReservations).reserve(") {
		require.False(t, time.Now().After(deadline), "the peer did not queue on the ledger to reserve its version")
		runtime.Gosched()
	}
	// The sequence advances only inside the ledger's lock: a cut cannot find
	// a version in its boundary that the ledger has not been told about.
	require.Equal(t, before, state.seq.Load(), "the namespace sequence must not advance before the ledger holds the version")

	b.mvccReservations.mu.Unlock()
	locked = false
	var reader begun
	select {
	case reader = <-readerDone:
	case <-time.After(10 * time.Second):
		t.Fatal("BEGIN did not finish")
	}
	require.NoError(t, reader.err)
	stale := reader.tx
	t.Cleanup(func() { _ = stale.Rollback() })
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the peer did not reach the observer")
	}
	runtime.GOMAXPROCS(previous)
	peerSeq := state.seq.Load()
	require.Equal(t, before+1, peerSeq)
	boundary := stale.beginSnapshot["test"]
	inCopy := stale.inflightAtBegin.holds("test", MVCCVersion{CommitSequence: peerSeq})
	t.Logf("peer seq=%d boundary seq=%d peer version in cut copy=%v", peerSeq, boundary.CommitSequence, inCopy)
	// Either the cut ran first and the peer's version is newer than its
	// boundary, or the reservation ran first and the version is in the copy.
	require.True(t, boundary.CommitSequence < peerSeq || inCopy, "the peer's version must be newer than the boundary or in the cut's copy")

	require.NoError(t, stale.SetImplicit(implicit))
	require.NoError(t, staleWrite(stale), "the stale snapshot does not hold the peer's write")
	unblock()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the peer commit did not finish")
	}
	require.NoError(t, commitErr)
	if replay {
		require.NoError(t, b.FlushWriteBehind(ctx))
	}
	staleErr := stale.Commit()
	require.ErrorIs(t, staleErr, ErrConflict, "a write over an entity a peer published after BEGIN is a conflict")
	require.NoError(t, b.FlushWriteBehind(ctx))
	wantV := int64(1)
	if op != "create" {
		wantV = 11
	}
	if kind == "node" {
		stored, err := b.GetNode(ctx, "test:n9")
		require.NoError(t, err)
		require.EqualValues(t, wantV, stored.Properties["v"], "the peer's node is the stored one")
	} else {
		stored, err := b.GetEdge(ctx, "test:r2")
		require.NoError(t, err)
		require.EqualValues(t, wantV, stored.Properties["v"], "the peer's relationship is the stored one")
		require.Equal(t, peerType, stored.Type)
	}
	requireTypeCountsAgree(t, b)
}
