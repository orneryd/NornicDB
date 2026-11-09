package storage

// Written by the prune-lockup lane (Claude Opus 5.5), 9 November 2026.
//
// One key's history can outgrow one Badger batch: with history retention on,
// a hot entity keeps archiving versions while lifecycle maintenance is paused.
// A prune then has to delete that chain in bounded, committed chunks, resume
// inside the chain, and decide exactly what one unbounded step would have
// decided (independent audit #56 of #1768; the large chain is the round-3
// reviewer's TestReview1768R3LargeSingleChainPrune).

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// prunedChain is one logical key whose history a test seeded directly.
type prunedChain struct {
	engine *BadgerEngine
	// logical is the key's logical prefix; head its external head key (nil
	// for adjacency, whose head is its newest record).
	logical, head []byte
	// versions are the seeded records, oldest first.
	versions []MVCCVersion
}

// seedPrunedChain creates the entity and seeds n archived records of one
// history representation (kind) at base + i*spacing. With tombstone, the
// entity is then deleted: the node, or the relationship (whose adjacency
// records then end in a tombstone).
func seedPrunedChain(t *testing.T, kind byte, n int, base time.Time, spacing time.Duration, tombstone bool) prunedChain {
	t.Helper()
	ctx := t.Context()
	b := createMVCCBadgerEngine(t)
	start := NodeID("test:chain-a")
	_, err := b.CreateNode(ctx, &Node{ID: start})
	require.NoError(t, err)
	startNum, ok := b.idDict.lookupNodeNumID(start)
	require.True(t, ok)

	chain := prunedChain{engine: b}
	var key func(MVCCVersion) []byte
	var body []byte
	edge := &Edge{ID: "test:chain-e", StartNode: start, EndNode: "test:chain-b", Type: "R"}
	if kind == prefixMVCCNode {
		body, err = encodeMVCCNodeRecord(&Node{ID: start}, false)
		require.NoError(t, err)
		key = func(v MVCCVersion) []byte { return mvccNodeVersionKey(startNum, v) }
		chain.head = mvccNodeHeadKey(startNum)
	} else {
		_, err = b.CreateNode(ctx, &Node{ID: edge.EndNode})
		require.NoError(t, err)
		require.NoError(t, b.CreateEdge(ctx, edge))
		endNum, ok := b.idDict.lookupNodeNumID(edge.EndNode)
		require.True(t, ok)
		edgeNum, ok := b.idDict.lookupEdgeNumID(edge.ID)
		require.True(t, ok)
		switch kind {
		case prefixMVCCEdge:
			body, err = encodeMVCCEdgeRecord(edge, false)
			require.NoError(t, err)
			key = func(v MVCCVersion) []byte { return mvccEdgeVersionKey(edgeNum, v) }
			chain.head = mvccEdgeHeadKey(edgeNum)
		case prefixMVCCOutgoingAdj:
			body = encodeMVCCAdjacencyRecord(false)
			key = func(v MVCCVersion) []byte { return mvccOutgoingAdjacencyKey(startNum, edgeNum, v) }
		case prefixMVCCIncomingAdj:
			body = encodeMVCCAdjacencyRecord(false)
			key = func(v MVCCVersion) []byte { return mvccIncomingAdjacencyKey(endNum, edgeNum, v) }
		default:
			t.Fatalf("kind %x", kind)
		}
	}
	for first := 0; first < n; first += 1000 {
		require.NoError(t, b.withUpdate(ctx, updateRequest{}, func(txn *badger.Txn) error {
			for i := first; i < min(first+1000, n); i++ {
				v := MVCCVersion{CommitTimestamp: base.Add(time.Duration(i) * spacing), CommitSequence: uint64(i + 1)}
				if i == 0 {
					logical, _, err := extractMVCCLogicalKeyAndVersion(key(v))
					if err != nil {
						return err
					}
					chain.logical = logical
				}
				chain.versions = append(chain.versions, v)
				if err := txn.Set(key(v), body); err != nil {
					return err
				}
			}
			return nil
		}))
	}
	if tombstone {
		if kind == prefixMVCCNode {
			require.NoError(t, b.DeleteNode(ctx, start))
		} else {
			require.NoError(t, b.DeleteEdge(ctx, edge.ID))
		}
	}
	return chain
}

// prunedState is what a chain's prune left: which seeded records remain
// (indexes), how many other records remain (written by the entity's own
// operations), the floor (a seeded index, -1 for another version, -2 for
// none), and whether the external head remains.
type prunedState struct {
	seeded []int
	others int
	floor  int
	head   bool
}

// records lists the versions stored under the chain's logical key, oldest
// first, and its floor (ok false when it has none).
func (c prunedChain) records(t *testing.T) (versions []MVCCVersion, floor MVCCVersion, ok bool) {
	t.Helper()
	require.NoError(t, c.engine.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerPrefixIteratorOptions(c.logical))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			logical, v, err := extractMVCCLogicalKeyAndVersion(it.Item().Key())
			if err != nil {
				return err
			}
			if bytes.Equal(logical, c.logical) {
				versions = append(versions, v)
			}
		}
		item, err := txn.Get(mvccPruneFloorKey(c.logical))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		ok = true
		return item.Value(func(val []byte) error {
			floor, err = decodeMVCCSortVersion(val)
			return err
		})
	}))
	return versions, floor, ok
}

func (c prunedChain) state(t *testing.T) prunedState {
	t.Helper()
	index := make(map[MVCCVersion]int, len(c.versions))
	for i, v := range c.versions {
		index[v] = i
	}
	s := prunedState{floor: -2}
	versions, floor, hasFloor := c.records(t)
	for _, v := range versions {
		if i, ok := index[v]; ok {
			s.seeded = append(s.seeded, i)
		} else {
			s.others++
		}
	}
	if hasFloor {
		s.floor = -1
		if i, ok := index[floor]; ok {
			s.floor = i
		}
	}
	if c.head != nil {
		require.NoError(t, c.engine.db.View(func(txn *badger.Txn) error {
			_, err := txn.Get(c.head)
			s.head = err == nil
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}))
	}
	return s
}

// TestMVCCPruneChunksDecideAsOneUnboundedStep prunes the same seeded history
// of each representation once without a bound and once in chunks of three
// records: retention, a registered reader, the retention age, and a
// tombstoned head with and without retention leave the same records, floor
// and head. Every chunk but the last deletes exactly three records and
// leaves the oldest remaining record as the floor.
func TestMVCCPruneChunksDecideAsOneUnboundedStep(t *testing.T) {
	kinds := []struct {
		name string
		kind byte
	}{{"node", prefixMVCCNode}, {"relationship", prefixMVCCEdge}, {"outgoing", prefixMVCCOutgoingAdj}, {"incoming", prefixMVCCIncomingAdj}}
	scenarios := []struct {
		name      string
		opts      MVCCPruneOptions
		spacing   time.Duration
		reader    int // the seeded record a registered reader sees, or -1
		tombstone bool
	}{
		{name: "retention", opts: MVCCPruneOptions{MaxVersionsPerKey: 2}, spacing: time.Second, reader: -1},
		{name: "reader", opts: MVCCPruneOptions{MaxVersionsPerKey: 1}, spacing: time.Second, reader: 4},
		{name: "retention age", opts: MVCCPruneOptions{MaxVersionsPerKey: 1, MinRetentionAge: 30 * time.Minute}, spacing: 5 * time.Minute, reader: -1},
		{name: "tombstone reclaimed", opts: MVCCPruneOptions{}, spacing: time.Second, reader: -1, tombstone: true},
		{name: "tombstone retained", opts: MVCCPruneOptions{MaxVersionsPerKey: 1}, spacing: time.Second, reader: -1, tombstone: true},
	}
	const records, chunk = 11, 3
	for _, k := range kinds {
		for _, sc := range scenarios {
			t.Run(k.name+"/"+sc.name, func(t *testing.T) {
				// Eleven records about an hour old, or for the retention age
				// spanning the last 55 minutes (the newest five protected).
				base := time.Now().UTC().Add(-time.Hour)
				if sc.spacing > time.Second {
					base = time.Now().UTC().Add(-time.Duration(records) * sc.spacing)
				}
				prune := func(c prunedChain, limit int) []int64 {
					var readers mvccPruneReaders
					if sc.reader >= 0 {
						c.engine.SetLifecycleController(&benchReaderController{
							testLifecycleController: &testLifecycleController{registry: &testSnapshotRegistry{}},
							registry:                &benchReaderRegistry{readers: []SnapshotReaderInfo{{ReaderID: "r", Namespace: "test", SnapshotVersion: c.versions[sc.reader]}}},
						})
						readers = c.engine.mvccPruneReaderFloors()
						require.True(t, readers.any)
					}
					var calls []int64
					for len(calls) < 100 {
						deleted, more, err := c.engine.pruneMVCCLogicalKey(t.Context(), c.logical, sc.opts, &readers, limit)
						require.NoError(t, err)
						calls = append(calls, deleted)
						if !more {
							return calls
						}
						require.EqualValues(t, limit, deleted, "a chunk that leaves records deletes exactly its bound")
						if c.head != nil {
							left, floor, ok := c.records(t)
							require.True(t, ok, "a chunk that deleted records wrote the floor")
							require.Equal(t, left[0], floor, "after a chunk the floor is the oldest record left")
						}
					}
					t.Fatal("the chunked prune never finished")
					return nil
				}
				whole := seedPrunedChain(t, k.kind, records, base, sc.spacing, sc.tombstone)
				chunked := seedPrunedChain(t, k.kind, records, base, sc.spacing, sc.tombstone)
				wholeCalls := prune(whole, 0)
				chunkedCalls := prune(chunked, chunk)
				require.Len(t, wholeCalls, 1)
				var total int64
				for _, n := range chunkedCalls {
					total += n
				}
				require.Equal(t, wholeCalls[0], total, "the chunks delete what one step deletes")
				require.Positive(t, total, "the scenario deletes something")
				left := chunked.state(t)
				require.Equal(t, whole.state(t), left)
				t.Logf("deleted %d in %d chunked calls; left %+v", total, len(chunkedCalls), left)
			})
		}
	}
}

// TestMVCCPruneLargeSingleChainCommitsInChunks is the round-3 reviewer's
// chain: 100,000 archived versions of one node, more than one Badger batch
// holds. One unbudgeted prune reduces it to the one retained version in
// committed chunks instead of failing with badger.ErrTxnTooBig.
func TestMVCCPruneLargeSingleChainCommitsInChunks(t *testing.T) {
	const versions = 100000
	chain := seedPrunedChain(t, prefixMVCCNode, versions, time.Now().UTC().Add(-time.Hour), time.Nanosecond, false)
	n, err := chain.engine.PruneMVCCVersions(t.Context(), MVCCPruneOptions{MaxVersionsPerKey: 1})
	t.Logf("single chain: archived=%d deleted=%d chunk=%d error=%v", versions, n, chain.engine.mvccPruneChunk(), err)
	require.NoError(t, err, "a full manual pass must be able to reduce this key's history")
	require.EqualValues(t, versions-1, n)
	require.Equal(t, prunedState{seeded: []int{versions - 1}, floor: versions - 1, head: true}, chain.state(t))
	_, err = chain.engine.GetNode(t.Context(), "test:chain-a")
	require.NoError(t, err)
}

// TestMVCCPruneLongChainResumesAcrossBudgetedCycles: a budgeted cycle that
// runs out while one chain is partly pruned keeps the chunks it committed,
// and the next cycles continue the chain until only the retained version is
// left.
func TestMVCCPruneLongChainResumesAcrossBudgetedCycles(t *testing.T) {
	versions := 3*mvccPruneChunkRecords + 7
	chain := seedPrunedChain(t, prefixMVCCNode, versions, time.Now().UTC().Add(-time.Hour), time.Nanosecond, false)
	engine := chain.engine
	chunk := engine.mvccPruneChunk()
	require.Equal(t, mvccPruneChunkRecords, chunk)

	// The first chunk's commit waits for storage admission. Admission is
	// released until that chunk is stored, then held again, so the cycle's
	// budget ends while a later chunk waits.
	waiting := make(chan struct{}, 1)
	t.Cleanup(setAdmissionWaitObserver(func(owner any, _ context.Context) {
		if owner == &engine.db.commitGate {
			select {
			case waiting <- struct{}{}:
			default:
			}
		}
	}))
	engine.db.commitGate.Lock()
	held := true
	t.Cleanup(func() {
		if held {
			engine.db.commitGate.Unlock()
		}
	})
	type outcome struct {
		n   int64
		err error
	}
	first := make(chan outcome, 1)
	go func() {
		n, err := engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 1, MaxRuntime: 3 * time.Second})
		first <- outcome{n, err}
	}()
	select {
	case <-waiting:
	case <-time.After(10 * time.Second):
		t.Fatal("the first chunk never reached storage admission")
	}
	engine.db.commitGate.Unlock()
	held = false
	for stored := false; !stored; {
		require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
			_, err := txn.Get(mvccPruneFloorKey(chain.logical))
			stored = err == nil
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}))
		if !stored {
			time.Sleep(50 * time.Microsecond)
		}
	}
	engine.db.commitGate.Lock()
	held = true
	var cycle outcome
	select {
	case cycle = <-first:
	case <-time.After(30 * time.Second):
		t.Fatal("the budgeted cycle did not end while its next chunk waited")
	}
	engine.db.commitGate.Unlock()
	held = false
	require.NoError(t, cycle.err)
	require.Positive(t, cycle.n, "the first chunk committed")
	require.Zero(t, cycle.n%int64(chunk), "only whole chunks committed")
	require.Less(t, cycle.n, int64(versions-1), "the budget ended with the chain partly pruned")
	state := chain.state(t)
	require.Len(t, state.seeded, versions-int(cycle.n))
	require.Equal(t, int(cycle.n), state.seeded[0], "the oldest records went first")
	require.Equal(t, int(cycle.n), state.floor, "the floor is the oldest record left")

	total := cycle.n
	for cycles := 1; total < int64(versions-1); cycles++ {
		require.Less(t, cycles, 20, "the chain is not pruned after %d cycles", cycles)
		n, err := engine.PruneMVCCVersions(t.Context(), MVCCPruneOptions{MaxVersionsPerKey: 1, MaxRuntime: 30 * time.Second})
		require.NoError(t, err)
		require.Positive(t, n, "cycle %d made no progress", cycles)
		total += n
	}
	require.EqualValues(t, versions-1, total)
	require.Equal(t, prunedState{seeded: []int{versions - 1}, floor: versions - 1, head: true}, chain.state(t))
	t.Logf("chain of %d: the first cycle committed %d (chunk %d), later cycles the rest", versions, cycle.n, chunk)
}
