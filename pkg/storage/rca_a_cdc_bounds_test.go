package storage

// Root-cause diagnostic (campaign v1.5.2, lane C, group G-cdc-bounds; run by lane A).
// Test-side only. Retention (PruneChangeEvents, change_capture.go:906-950)
// deletes the oldest change events, i.e. the start of the event-key range.
// changeBoundsIn (change_capture.go:296-315) finds the earliest event with a
// forward it.Seek(prefix), which must step over every deleted version left in
// that range until the first live key. The diagnostic builds that state
// through the real capture and prune paths, then times ChangeBounds, the
// forward seek alone, the reverse seek alone and a forward seek that starts at
// the first live event key (the corrected step).

import (
	"context"
	"fmt"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

func rcaTimeSeek(t *testing.T, b *BadgerEngine, seek []byte, prefix []byte, reverse bool) (time.Duration, uint64) {
	t.Helper()
	var at uint64
	start := time.Now()
	err := b.db.View(func(txn *badger.Txn) error {
		opts := badgerIteratorOptions()
		opts.Prefix = prefix
		opts.Reverse = reverse
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Seek(seek)
		if it.ValidForPrefix(prefix) {
			at, _, _ = changeEventPosition(it.Item().Key())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return time.Since(start), at
}

func rcaCountVersions(b *BadgerEngine, prefix []byte) (deleted, live int64) {
	_ = b.db.View(func(txn *badger.Txn) error {
		opts := badgerIteratorOptions()
		opts.Prefix = prefix
		opts.AllVersions = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if it.Item().IsDeletedOrExpired() {
				deleted++
			} else {
				live++
			}
		}
		return nil
	})
	return deleted, live
}

func TestRCAChangeBoundsWalksDeletedEvents(t *testing.T) {
	ctx := context.Background()
	for _, events := range []int{2000, 30000, 120000} {
		b, err := NewBadgerEngine(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		const ns = "rca"
		b.SetChangeCapture(ns, ChangeCaptureFull)
		for start := 0; start < events; start += 1000 {
			nodes := make([]*Node, 0, 1000)
			for i := start; i < start+1000 && i < events; i++ {
				nodes = append(nodes, &Node{ID: NodeID(fmt.Sprintf("%s:n%08d", ns, i)), Labels: []string{"Item"}, Properties: map[string]any{"i": int64(i)}})
			}
			if err := b.BulkCreateNodes(ctx, nodes); err != nil {
				t.Fatal(err)
			}
		}
		prefix := changeEventPrefix(ns)
		before := time.Now()
		e0, l0, _ := b.ChangeBounds(ns)
		boundsBefore := time.Since(before)
		pruned, err := b.PruneChangeEvents(ctx, ns, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		deleted, live := rcaCountVersions(b, prefix)
		bs := time.Now()
		earliest, last, err := b.ChangeBounds(ns)
		boundsAfter := time.Since(bs)
		fwd, fwdAt := rcaTimeSeek(t, b, prefix, prefix, false)
		rev, _ := rcaTimeSeek(t, b, append(append([]byte(nil), prefix...), 0xFF), prefix, true)
		direct, directAt := rcaTimeSeek(t, b, changeEventKey(ns, earliest, 0), prefix, false)
		t.Logf("RCA_CDC events=%d before-prune ChangeBounds=%s (earliest=%d last=%d) pruned=%d deleted_versions=%d live_versions=%d after-prune ChangeBounds=%s (earliest=%d last=%d err=%v) forwardSeek(prefix)=%s->%d reverseSeek=%s seekFromFirstLiveKey=%s->%d",
			events, boundsBefore, e0, l0, pruned, deleted, live, boundsAfter, earliest, last, err, fwd, fwdAt, rev, direct, directAt)
		_ = b.Close()
	}
}
