package storage

// Root-cause diagnostic (campaign v1.5.2, lane C, group G-cdc-bounds; run by lane A).
// Test-side only. Retention (PruneChangeEvents, change_capture.go:906-950)
// deletes the oldest change events, i.e. the start of the event-key range.
// changeBoundsIn (change_capture.go:296-315) finds the earliest event with a
// forward it.Seek(prefix), which must step over every deleted version left in
// that range until the first live key. The diagnostic builds that state
// directly (deleted events, then a few live ones) and times the forward seek,
// the reverse seek and a seek that starts at the first live key.

import (
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

func rcaTimeSeek(t *testing.T, db *badger.DB, seek []byte, prefix []byte, reverse bool) (time.Duration, uint64) {
	t.Helper()
	var at uint64
	start := time.Now()
	err := db.View(func(txn *badger.Txn) error {
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

func TestRCAChangeBoundsWalksDeletedEvents(t *testing.T) {
	b, err := NewBadgerEngine(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	db := b.DB()
	const ns = "rca"
	prefix := changeEventPrefix(ns)
	body := make([]byte, 200)
	for _, deleted := range []int{0, 30000, 300000} {
		live := 1000
		base := uint64(1_000_000_000) * uint64(deleted+1)
		wb := db.NewWriteBatch()
		for i := 0; i < deleted+live; i++ {
			if err := wb.Set(changeEventKey(ns, base+uint64(i), 0), body); err != nil {
				t.Fatal(err)
			}
		}
		if err := wb.Flush(); err != nil {
			t.Fatal(err)
		}
		del := db.NewWriteBatch()
		for i := 0; i < deleted; i++ {
			if err := del.Delete(changeEventKey(ns, base+uint64(i), 0)); err != nil {
				t.Fatal(err)
			}
		}
		if err := del.Flush(); err != nil {
			t.Fatal(err)
		}
		bs := time.Now()
		earliest, last, err := b.ChangeBounds(ns)
		boundsTime := time.Since(bs)
		fwd, fwdAt := rcaTimeSeek(t, db, prefix, prefix, false)
		rev, _ := rcaTimeSeek(t, db, append(append([]byte(nil), prefix...), 0xFF), prefix, true)
		direct, directAt := rcaTimeSeek(t, db, changeEventKey(ns, base+uint64(deleted), 0), prefix, false)
		t.Logf("RCA_CDC deleted=%d live=%d ChangeBounds=%s (earliest=%d last=%d err=%v) forwardSeek(prefix)=%s->%d reverseSeek=%s seekFromFirstLive=%s->%d",
			deleted, live, boundsTime, earliest, last, err, fwd, fwdAt, rev, direct, directAt)
		// Remove this round's live events too so the next round starts clean
		// apart from the tombstones it leaves (the store keeps growing them).
		clean := db.NewWriteBatch()
		for i := deleted; i < deleted+live; i++ {
			_ = clean.Delete(changeEventKey(ns, base+uint64(i), 0))
		}
		_ = clean.Flush()
	}
	if err := db.Flatten(1); err != nil {
		t.Logf("RCA_CDC flatten err=%v", err)
	}
	_, _, _ = b.ChangeBounds(ns)
	wb := db.NewWriteBatch()
	_ = wb.Set(changeEventKey(ns, 9_000_000_000_000, 0), body)
	_ = wb.Flush()
	bs := time.Now()
	earliest, _, _ := b.ChangeBounds(ns)
	t.Logf("RCA_CDC after Flatten: ChangeBounds=%s earliest=%d", time.Since(bs), earliest)
}
