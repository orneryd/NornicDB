package storage

// Written by the prune-lockup lane (Claude Opus 5.5), 9 November 2026.
//
// From the Codex review of #1768 (r4511015255): in a large store whose prune
// candidates are sparse, because most histories are still within retention, a
// budgeted cycle can end before its pending batch fills, a costly candidate
// found early can be left too little of the budget (round 5, audit #58), and
// a dense run of long histories can spend the budget being found before its
// first commit (round 6). Work the walk has found must be committed within the cycle
// (unless that key's own work exceeds a whole budget); otherwise each cycle
// rescans the same span, nothing is ever pruned, and the later keyspaces are
// never reached.

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// TestMVCCPruneSparseCandidatesProgressEveryCycle: 50,000 nodes each keep
// their one archived version (retention 1); four nodes spread through the
// store and one relationship have two versions too many. The budget is a fifth
// of the time an unbudgeted pass takes to scan this store, so one cycle scans
// about a fifth of it: the shape of the default 5 s lifecycle cycle over a
// store about five times larger than 5 s of scanning covers, with fewer than
// 64 prunable entities in each cycle's span. Every budgeted cycle must advance
// the committed resume cursor, and the cycles must prune everything,
// including the relationship in the second keyspace.
func TestMVCCPruneSparseCandidatesProgressEveryCycle(t *testing.T) {
	ctx := t.Context()
	engine := createMVCCBadgerEngine(t)
	const total = 50000
	ids := make([]NodeID, total)
	for first := 0; first < total; first += 5000 {
		nodes := make([]*Node, 0, 5000)
		for i := first; i < first+5000; i++ {
			ids[i] = NodeID(fmt.Sprintf("test:sparse-%06d", i))
			nodes = append(nodes, &Node{ID: ids[i], Labels: []string{"Sparse"}})
		}
		require.NoError(t, engine.BulkCreateNodes(ctx, nodes))
	}
	base := time.Now().UTC().Add(-time.Hour)
	archive := func(id NodeID, versions int, offset int) {
		t.Helper()
		num, ok := engine.idDict.lookupNodeNumID(id)
		require.True(t, ok)
		body, err := encodeMVCCNodeRecord(&Node{ID: id, Labels: []string{"Sparse"}}, false)
		require.NoError(t, err)
		require.NoError(t, engine.withUpdate(ctx, updateRequest{}, func(txn *badger.Txn) error {
			for v := 0; v < versions; v++ {
				at := MVCCVersion{CommitTimestamp: base.Add(time.Duration(offset+v) * time.Second), CommitSequence: uint64(offset + v + 1)}
				if err := txn.Set(mvccNodeVersionKey(num, at), body); err != nil {
					return err
				}
			}
			return nil
		}))
	}
	for first := 0; first < total; first += 1000 {
		require.NoError(t, engine.withUpdate(ctx, updateRequest{}, func(txn *badger.Txn) error {
			for i := first; i < first+1000; i++ {
				num, ok := engine.idDict.lookupNodeNumID(ids[i])
				if !ok {
					return fmt.Errorf("no number for %s", ids[i])
				}
				body, err := encodeMVCCNodeRecord(&Node{ID: ids[i], Labels: []string{"Sparse"}}, false)
				if err != nil {
					return err
				}
				if err := txn.Set(mvccNodeVersionKey(num, MVCCVersion{CommitTimestamp: base, CommitSequence: 1}), body); err != nil {
					return err
				}
			}
			return nil
		}))
	}
	opts := MVCCPruneOptions{MaxVersionsPerKey: 1}

	// Calibrate: an unbudgeted pass over the store, with nothing to prune yet.
	started := time.Now()
	n, err := engine.PruneMVCCVersions(ctx, opts)
	require.NoError(t, err)
	require.Zero(t, n)
	scan := time.Since(started)
	budget := scan / 5

	// The sparse candidates: four nodes through the store and a relationship.
	for _, i := range []int{0, total / 3, 2 * total / 3, total - 1} {
		archive(ids[i], 2, 10)
	}
	start, end := NodeID("test:sparse-edge-a"), NodeID("test:sparse-edge-b")
	_, err = engine.CreateNode(ctx, &Node{ID: start})
	require.NoError(t, err)
	_, err = engine.CreateNode(ctx, &Node{ID: end})
	require.NoError(t, err)
	edge := &Edge{ID: "test:sparse-edge", StartNode: start, EndNode: end, Type: "R"}
	require.NoError(t, engine.CreateEdge(ctx, edge))
	edgeNum, ok := engine.idDict.lookupEdgeNumID(edge.ID)
	require.True(t, ok)
	edgeBody, err := encodeMVCCEdgeRecord(edge, false)
	require.NoError(t, err)
	require.NoError(t, engine.withUpdate(ctx, updateRequest{}, func(txn *badger.Txn) error {
		for v := 0; v < 3; v++ {
			at := MVCCVersion{CommitTimestamp: base.Add(time.Duration(v) * time.Second), CommitSequence: uint64(v + 1)}
			if err := txn.Set(mvccEdgeVersionKey(edgeNum, at), edgeBody); err != nil {
				return err
			}
		}
		return nil
	}))
	const want = 4*2 + 2

	budgeted := opts
	budgeted.MaxRuntime = budget
	var pruned int64
	cursor := engine.mvccPruneResume
	cycles, stuck := 0, 0
	for ; cycles < 60 && pruned < want; cycles++ {
		n, err := engine.PruneMVCCVersions(context.Background(), budgeted)
		require.NoError(t, err)
		pruned += n
		next := engine.mvccPruneResume
		if pruned < want && next.kind <= cursor.kind && (next.kind < cursor.kind || bytes.Compare(next.after, cursor.after) <= 0) {
			stuck++
		}
		cursor = next
	}
	perSecond := float64(total) / scan.Seconds()
	t.Logf("unbudgeted scan of %d histories: %s (%.0f histories/s, so a default 5 s cycle scans about %.0f); %d cycles of %s pruned %d of %d prunable versions; %d cycles committed no progress",
		total, scan.Round(time.Millisecond), perSecond, perSecond*5, cycles, budget.Round(time.Millisecond), pruned, want, stuck)
	require.Zero(t, stuck, "budgeted cycles that committed no progress (resume cursor did not move)")
	require.EqualValues(t, want, pruned, "the cycles never pruned the sparse candidates")
}

// TestMVCCPruneCostlySparseCandidateCommitsInItsCycle is the round-5
// reviewer's case (TestReviewR5SparseCandidateCommitShare): one node with
// 256 large archived versions, found first, followed by 50,000 protected
// histories. The budget is twice the candidate's measured delete work, so
// the work fits the cycle; the first budgeted cycle must commit it.
func TestMVCCPruneCostlySparseCandidateCommitsInItsCycle(t *testing.T) {
	b := createMVCCBadgerEngine(t)
	const records, tail = 256, 50000
	base := time.Now().Add(-time.Hour)
	_, err := b.CreateNode(t.Context(), &Node{ID: "test:costly"})
	require.NoError(t, err)
	num, ok := b.idDict.lookupNodeNumID("test:costly")
	require.True(t, ok)
	heavy, err := encodeMVCCNodeRecord(&Node{ID: "test:costly", Properties: map[string]any{"payload": string(bytes.Repeat([]byte{'x'}, 60000))}}, false)
	require.NoError(t, err)
	light, err := encodeMVCCNodeRecord(&Node{ID: "test:costly-tail"}, false)
	require.NoError(t, err)
	logical := append([]byte{prefixMVCCNode}, encodeNumID(num)...)
	for first := 0; first < records; first += 16 {
		require.NoError(t, b.withUpdate(t.Context(), updateRequest{}, func(txn *badger.Txn) error {
			for v := first; v < min(first+16, records); v++ {
				if err := txn.Set(mvccNodeVersionKey(num, MVCCVersion{CommitTimestamp: base.Add(time.Duration(v) * time.Second), CommitSequence: uint64(v + 1)}), heavy); err != nil {
					return err
				}
			}
			return nil
		}))
	}
	for first := 1; first < tail+1; first += 500 {
		require.NoError(t, b.withUpdate(t.Context(), updateRequest{}, func(txn *badger.Txn) error {
			for id := first; id < min(first+500, tail+1); id++ {
				if err := txn.Set(mvccNodeVersionKey(num+uint64(id), MVCCVersion{CommitTimestamp: base, CommitSequence: 1}), light); err != nil {
					return err
				}
			}
			return nil
		}))
	}
	opts := MVCCPruneOptions{MaxVersionsPerKey: 1, MinRetentionAge: time.Minute}
	// The candidate's delete work, measured on the kernel and rolled back.
	var cost time.Duration
	rollback := fmt.Errorf("calibration rollback")
	for i := 0; i < 4; i++ {
		started := time.Now()
		err := b.withUpdate(t.Context(), updateRequest{}, func(txn *badger.Txn) error {
			if _, _, err := b.pruneMVCCKeyspaceInTxn(t.Context(), txn, logical, opts, nil, b.mvccPruneChunk()); err != nil {
				return err
			}
			return rollback
		})
		require.ErrorIs(t, err, rollback)
		if elapsed := time.Since(started); i > 0 && (cost == 0 || elapsed < cost) {
			cost = elapsed
		}
	}
	opts.MaxRuntime = 2 * cost
	n, err := b.PruneMVCCVersions(t.Context(), opts)
	require.NoError(t, err)
	t.Logf("delete work %s, budget %s: the first cycle deleted %d of %d", cost, opts.MaxRuntime, n, records-1)
	require.EqualValues(t, records-1, n, "the candidate's work fits the cycle, so the first cycle commits it")
	require.Equal(t, 1, len(mustVersionsUnder(t, b, logical)))
}

func mustVersionsUnder(t *testing.T, b *BadgerEngine, logical []byte) [][]byte {
	t.Helper()
	var keys [][]byte
	require.NoError(t, b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerPrefixIteratorOptions(logical))
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(logical); it.Next() {
			keys = append(keys, it.Item().KeyCopy(nil))
		}
		return nil
	}))
	return keys
}
