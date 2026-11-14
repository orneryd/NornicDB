package storage

// Written by the prune-lockup lane (Claude Opus 5.5), 9 November 2026.
//
// The progress of a budgeted prune pass, proved against the whole layout
// space instead of one layout at a time (Rule 73 round after round 6 of
// #1768): sparse candidates, one costly candidate, a dense run of long
// histories (the round-6 reviewer's TestReviewR6CandidateRunProgress), a mixed
// layout and a history that needs several steps, each in the node,
// relationship, outgoing and incoming histories. With a budget of twice the
// first key's measured preparation work, cycles must finish within a bounded
// number of passes and delete exactly what an unbudgeted pass deletes. A
// cycle that makes no progress must have spent its runtime budget: admission
// waits and scheduler preemption can exhaust it before any commit, as the
// controlled budget/admission tests separately establish. A timing sample
// cannot guarantee that a later cycle receives the same amount of CPU time.
//
// Progress means committed deletions, never cursor movement: the walk also
// moves the cursor past the keyspaces it finds empty and past every key with
// nothing to delete, without committing anything. The calibration that sizes
// a budget rolls its transaction back, so it measures the first key's scan
// and delete planning without the commit that ends it; twice that sample can
// therefore fall below the first key's own work, and the contract permits
// such a cycle to commit nothing when MaxRuntime is spent (its admission, its
// scanning and its commit gate all end with the budget). That cycle must then
// leave the cursor before the candidate it found, so the next cycle decides
// that candidate again with the whole budget; a cycle that returns earlier
// than its budget without progress remains a defect.

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// pruneLayoutKey is one logical key of a layout: its version records, and
// whether they carry a large body (node and relationship histories only).
type pruneLayoutKey struct {
	records int
	large   bool
}

func pruneLayouts(chunk int) map[string][]pruneLayoutKey {
	repeat := func(n int, k pruneLayoutKey) []pruneLayoutKey {
		out := make([]pruneLayoutKey, n)
		for i := range out {
			out[i] = k
		}
		return out
	}
	protected := pruneLayoutKey{records: 1}
	costly := pruneLayoutKey{records: 256, large: true}
	layouts := map[string][]pruneLayoutKey{}
	layouts["sparse"] = append(append([]pruneLayoutKey{{records: 3}}, repeat(20000, protected)...), pruneLayoutKey{records: 3})
	layouts["costly isolated"] = append([]pruneLayoutKey{costly}, repeat(20000, protected)...)
	layouts["dense long run"] = repeat(63, pruneLayoutKey{records: 4096})
	var mixed []pruneLayoutKey
	for i := 0; i < 3000; i++ {
		switch {
		case i == 1500:
			mixed = append(mixed, costly)
		case i >= 1000 && i < 1020:
			mixed = append(mixed, pruneLayoutKey{records: 5})
		case i%7 == 0:
			mixed = append(mixed, pruneLayoutKey{records: 3})
		default:
			mixed = append(mixed, protected)
		}
	}
	layouts["mixed"] = mixed
	layouts["interrupted chain"] = append([]pruneLayoutKey{{records: 3*chunk + 7}}, repeat(5000, protected)...)
	return layouts
}

func TestMVCCPruneProgressEveryLayout(t *testing.T) {
	kinds := []struct {
		name string
		kind byte
	}{{"node", prefixMVCCNode}, {"relationship", prefixMVCCEdge}, {"outgoing", prefixMVCCOutgoingAdj}, {"incoming", prefixMVCCIncomingAdj}}
	probe := createMVCCBadgerEngine(t)
	layouts := pruneLayouts(probe.mvccPruneChunk())
	for index, k := range kinds {
		for _, name := range []string{"sparse", "costly isolated", "dense long run", "mixed", "interrupted chain"} {
			layout := layouts[name]
			t.Run(k.name+"/"+name, func(t *testing.T) {
				b := createMVCCBadgerEngine(t)
				ctx := t.Context()
				key := func(id uint64, v MVCCVersion) []byte {
					switch k.kind {
					case prefixMVCCNode:
						return mvccNodeVersionKey(id, v)
					case prefixMVCCEdge:
						return mvccEdgeVersionKey(id, v)
					case prefixMVCCOutgoingAdj:
						return mvccOutgoingAdjacencyKey(id, id, v)
					default:
						return mvccIncomingAdjacencyKey(id, id, v)
					}
				}
				body := func(large bool) []byte {
					props := map[string]any{}
					if large {
						props["payload"] = string(bytes.Repeat([]byte{'x'}, 60000))
					}
					switch k.kind {
					case prefixMVCCNode:
						out, err := encodeMVCCNodeRecord(&Node{ID: "test:layout", Properties: props}, false)
						require.NoError(t, err)
						return out
					case prefixMVCCEdge:
						out, err := encodeMVCCEdgeRecord(&Edge{ID: "test:layout", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: props}, false)
						require.NoError(t, err)
						return out
					default:
						return encodeMVCCAdjacencyRecord(false)
					}
				}
				small, large := body(false), body(true)
				base := time.Now().UTC().Add(-time.Hour)
				// Seed in transactions of up to 1,000 records.
				var pending []func(*badger.Txn) error
				var expected int64
				var candidates []uint64
				flushSeed := func() {
					if len(pending) == 0 {
						return
					}
					require.NoError(t, b.withUpdate(ctx, updateRequest{}, func(txn *badger.Txn) error {
						for _, set := range pending {
							if err := set(txn); err != nil {
								return err
							}
						}
						return nil
					}))
					pending = pending[:0]
				}
				for i, lk := range layout {
					id := uint64(i + 1)
					records := lk.records
					if lk.large && (k.kind == prefixMVCCOutgoingAdj || k.kind == prefixMVCCIncomingAdj) {
						// Adjacency records have no body: a costly history
						// there is a long one.
						records = 4096
					}
					value := small
					if lk.large {
						value = large
					}
					for v := 0; v < records; v++ {
						at := MVCCVersion{CommitTimestamp: base.Add(time.Duration(v) * time.Millisecond), CommitSequence: uint64(v + 1)}
						full := key(id, at)
						pending = append(pending, func(txn *badger.Txn) error { return txn.Set(full, value) })
						limit := 1000
						if lk.large {
							limit = 16
						}
						if len(pending) >= limit {
							flushSeed()
						}
					}
					if records > 1 {
						// No external head: the newest record is the head,
						// and retention keeps one more.
						expected += int64(max(0, records-2))
						candidates = append(candidates, id)
					}
				}
				flushSeed()

				// Calibrate the first key this cycle will decide, not a stale
				// sample from before earlier cycles changed Badger's working set.
				// This sizes a nominal budget; elapsed time below verifies whether a
				// no-progress call actually exhausted it.
				opts := MVCCPruneOptions{MaxVersionsPerKey: 1}
				calibration := opts
				rollback := errors.New("calibration rollback")
				logicalOf := func(id uint64) []byte {
					logical, _, err := extractMVCCLogicalKeyAndVersion(key(id, MVCCVersion{CommitTimestamp: base, CommitSequence: 1}))
					require.NoError(t, err)
					return logical
				}
				preparationWorkOf := func(logical []byte) time.Duration {
					started := time.Now()
					require.NoError(t, b.db.View(func(txn *badger.Txn) error {
						it := txn.NewIterator(badgerPrefixIteratorOptions(logical))
						defer it.Close()
						for it.Rewind(); it.ValidForPrefix(logical); it.Next() {
						}
						return nil
					}))
					err := b.withUpdate(ctx, updateRequest{}, func(txn *badger.Txn) error {
						if _, _, err := b.pruneMVCCKeyspaceInTxn(ctx, txn, logical, calibration, nil, b.mvccPruneChunk()); err != nil {
							return err
						}
						return rollback
					})
					require.ErrorIs(t, err, rollback)
					return time.Since(started)
				}
				frontLogical := func(cursor mvccPruneCursor) []byte {
					for _, id := range candidates {
						logical := logicalOf(id)
						if cursor.kind < index || (cursor.kind == index && bytes.Compare(logical, cursor.after) > 0) {
							return logical
						}
					}
					return nil
				}
				var preparationWork, budget time.Duration

				var pruned int64
				cursor := b.mvccPruneResume
				cycles := 0
				expiredWithoutProgress := 0
				for ; pruned < expected; cycles++ {
					logical := frontLogical(cursor)
					require.NotNil(t, logical, "a candidate remains while %d of %d records are deleted", pruned, expected)
					preparationWork = preparationWorkOf(logical)
					budget = max(2*preparationWork, 2*time.Millisecond)
					opts.MaxRuntime = budget
					require.Less(t, cycles, 3000, "%d of %d deleted after %d cycles of %s", pruned, expected, cycles, budget)
					started := time.Now()
					n, err := b.PruneMVCCVersions(context.Background(), opts)
					elapsed := time.Since(started)
					require.NoError(t, err)
					pruned += n
					next := b.mvccPruneResume
					advanced := next.kind > cursor.kind || (next.kind == cursor.kind && bytes.Compare(next.after, cursor.after) > 0)
					if pruned < expected {
						require.True(t, next.kind > cursor.kind || (next.kind == cursor.kind && bytes.Compare(next.after, cursor.after) >= 0), "a spent budget must not move the resume cursor backwards")
					}
					if n == 0 {
						// Only a committed deletion is progress: the walk moves
						// the cursor over empty keyspaces and over every key
						// with nothing to delete, so advanced alone proves
						// nothing. MaxRuntime includes admission, scanning and
						// the commit gate, so a cycle may find the front
						// candidate and end before committing any of it;
						// returning without progress before that budget ends
						// is a defect, and the candidate it found must still
						// stand before the cursor so the next cycle decides it
						// again with the whole budget. Bounded completion and
						// the exact population below then prove every candidate
						// is committed.
						require.GreaterOrEqual(t, elapsed, budget, "cycle %d committed nothing before its budget expired: %d of %d deleted, cursor %d:%x -> %d:%x", cycles+1, pruned, expected, cursor.kind, cursor.after, next.kind, next.after)
						require.Equal(t, logical, frontLogical(next), "cycle %d committed nothing and passed the candidate it found: cursor %d:%x -> %d:%x", cycles+1, cursor.kind, cursor.after, next.kind, next.after)
						expiredWithoutProgress++
						t.Logf("cycle %d committed nothing: %s of %s budget spent, cursor %d:%x -> %d:%x advanced=%v", cycles+1, elapsed.Round(time.Microsecond), budget.Round(time.Microsecond), cursor.kind, cursor.after, next.kind, next.after, advanced)
					}
					cursor = next
				}
				require.Equal(t, expected, pruned, "the cycles delete exactly what an unbudgeted pass deletes")
				n, err := b.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 1})
				require.NoError(t, err)
				require.Zero(t, n, "nothing prunable is left")
				t.Logf("%d keys, %d prunable versions; last preparation sample %s, budget %s: %d cycles, %d spent-budget cycles without progress", len(layout), expected, preparationWork.Round(time.Microsecond), budget.Round(time.Microsecond), cycles, expiredWithoutProgress)
			})
		}
	}
}
