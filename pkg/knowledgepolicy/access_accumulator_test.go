package knowledgepolicy

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestAccumulator_Disabled(t *testing.T) {
	a := NewAccessAccumulator(false, 0)
	a.IncrementAccess("n1")
	a.IncrementTraversal("n1")
	a.IncrementCustom("n1", "hits", 5)

	val := a.ReadThrough("n1", "accessCount", 0)
	if val != 0 {
		t.Errorf("expected 0 when disabled, got %d", val)
	}
}

func TestAccumulator_SingleGoroutine(t *testing.T) {
	a := NewAccessAccumulator(true, 0)

	for i := 0; i < 100; i++ {
		a.IncrementAccess("n1")
	}
	for i := 0; i < 50; i++ {
		a.IncrementTraversal("n1")
	}

	accessCount := a.ReadThrough("n1", "accessCount", 0)
	if accessCount != 100 {
		t.Errorf("accessCount: expected 100, got %d", accessCount)
	}

	traversalCount := a.ReadThrough("n1", "traversalCount", 0)
	if traversalCount != 50 {
		t.Errorf("traversalCount: expected 50, got %d", traversalCount)
	}
}

func TestAccumulator_ConcurrentAccess(t *testing.T) {
	a := NewAccessAccumulator(true, 0)
	const goroutines = 64
	const increments = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < increments; i++ {
				a.IncrementAccess("n1")
			}
		}()
	}
	wg.Wait()

	total := a.ReadThrough("n1", "accessCount", 0)
	expected := int64(goroutines * increments)
	if total != expected {
		t.Errorf("expected %d, got %d", expected, total)
	}
}

func TestAccumulator_ConcurrentMultiEntity(t *testing.T) {
	a := NewAccessAccumulator(true, 0)
	const goroutines = 32
	const entities = 16
	const increments = 500

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			entityID := fmt.Sprintf("n%d", g%entities)
			for i := 0; i < increments; i++ {
				a.IncrementAccess(entityID)
			}
		}(g)
	}
	wg.Wait()

	for e := 0; e < entities; e++ {
		entityID := fmt.Sprintf("n%d", e)
		count := a.ReadThrough(entityID, "accessCount", 0)
		expected := int64((goroutines / entities) * increments)
		if count != expected {
			t.Errorf("%s: expected %d, got %d", entityID, expected, count)
		}
	}
}

func TestAccumulator_CustomCounters(t *testing.T) {
	a := NewAccessAccumulator(true, 0)
	a.IncrementCustom("n1", "views", 3)
	a.IncrementCustom("n1", "views", 7)

	val := a.ReadThrough("n1", "views", 10)
	if val != 20 {
		t.Errorf("expected 20 (10 persisted + 10 buffered), got %d", val)
	}
}

func TestAccumulator_ReadThroughWithPersisted(t *testing.T) {
	a := NewAccessAccumulator(true, 0)
	a.IncrementAccess("n1")
	a.IncrementAccess("n1")
	a.IncrementAccess("n1")

	val := a.ReadThrough("n1", "accessCount", 100)
	if val != 103 {
		t.Errorf("expected 103 (100 persisted + 3 buffered), got %d", val)
	}
}

func TestAccumulator_DrainAll(t *testing.T) {
	a := NewAccessAccumulator(true, 0)

	for i := 0; i < 50; i++ {
		a.IncrementAccess("n1")
	}
	for i := 0; i < 30; i++ {
		a.IncrementTraversal("n2")
	}

	merged := a.DrainAll()

	if d, ok := merged["n1"]; !ok || d.accessCount != 50 {
		t.Errorf("n1 accessCount: expected 50, got %v", merged["n1"])
	}
	if d, ok := merged["n2"]; !ok || d.traversalCount != 30 {
		t.Errorf("n2 traversalCount: expected 30, got %v", merged["n2"])
	}

	afterDrain := a.ReadThrough("n1", "accessCount", 0)
	if afterDrain != 0 {
		t.Errorf("expected 0 after drain, got %d", afterDrain)
	}
}

func TestAccumulator_ClearEntity(t *testing.T) {
	a := NewAccessAccumulator(true, 0)
	a.IncrementAccess("n1")
	a.IncrementAccess("n2")

	a.ClearEntity("n1")

	n1 := a.ReadThrough("n1", "accessCount", 0)
	n2 := a.ReadThrough("n2", "accessCount", 0)
	if n1 != 0 {
		t.Errorf("expected 0 after clear, got %d", n1)
	}
	if n2 != 1 {
		t.Errorf("expected 1 for uncleared, got %d", n2)
	}
}

// TestAccumulator_Timestamps_MaxWins places each write on its own shard, so
// DrainAll always merges them across shards; the latest must win, not the sum.
func TestAccumulator_Timestamps_MaxWins(t *testing.T) {
	a := newAccessAccumulator(true, 0, 3)
	a.shards[0].setTimestamp("n1", "lastAccessedAt", 100)
	a.shards[1].setTimestamp("n1", "lastAccessedAt", 50)
	a.shards[2].setTimestamp("n1", "lastAccessedAt", 200)

	merged := a.DrainAll()
	if got := merged["n1"].timestamps["lastAccessedAt"]; got != 200 {
		t.Errorf("expected max timestamp 200, got %d", got)
	}
}

// TestAccumulator_CrossShardMergeRules pins one merge rule per kind of value
// wherever buffered values from different shards meet: counters add,
// timestamps keep the latest.
func TestAccumulator_CrossShardMergeRules(t *testing.T) {
	newSpread := func() *AccessAccumulator {
		a := newAccessAccumulator(true, 0, 3)
		// The shard writes below bypass the public methods, so fix the custom
		// keys' kinds the way their first public use would.
		a.claimCustomKind("seenAt", customTimestamp)
		a.claimCustomKind("views", customCounter)
		for i, ts := range []timestamp{100, 50, 200} {
			a.shards[i].setTimestamp("n1", "seenAt", ts)
			a.shards[i].incrementCustom("n1", "views", counter(i+1))
			a.shards[i].recordAccess("n1", ts)
			a.shards[i].recordTraversal("n1", ts)
		}
		return a
	}

	t.Run("ReadThrough", func(t *testing.T) {
		a := newSpread()
		cases := []struct {
			key       string
			persisted int64
			want      int64
		}{
			{"seenAt", 0, 200},
			{"seenAt", 150, 200},
			{"seenAt", 300, 300},
			{"lastAccessedAt", 0, 200},
			{"lastTraversedAt", 300, 300},
			{"views", 10, 16},
			{"accessCount", 10, 13},
			{"traversalCount", 0, 3},
		}
		for _, tc := range cases {
			if got := a.ReadThrough("n1", tc.key, tc.persisted); got != tc.want {
				t.Errorf("ReadThrough(%s, persisted %d) = %d, want %d", tc.key, tc.persisted, got, tc.want)
			}
		}
	})

	t.Run("DrainAll", func(t *testing.T) {
		d := newSpread().DrainAll()["n1"]
		if d == nil {
			t.Fatal("expected a drained delta for n1")
		}
		if d.timestamps["seenAt"] != 200 || d.lastAccessedAt != 200 || d.lastTraversedAt != 200 {
			t.Errorf("timestamps: seenAt=%d lastAccessedAt=%d lastTraversedAt=%d, want 200 each",
				d.timestamps["seenAt"], d.lastAccessedAt, d.lastTraversedAt)
		}
		if d.counters["views"] != 6 || d.accessCount != 3 || d.traversalCount != 3 {
			t.Errorf("counters: views=%d accessCount=%d traversalCount=%d, want 6, 3, 3",
				d.counters["views"], d.accessCount, d.traversalCount)
		}
		if _, ok := d.counters["seenAt"]; ok {
			t.Error("timestamp seenAt was buffered as a counter")
		}
	})

	t.Run("Flush", func(t *testing.T) {
		for _, tc := range []struct {
			stored int64
			want   int64
		}{{0, 200}, {150, 200}, {300, 300}} {
			store := newMockStore()
			if tc.stored != 0 {
				_ = store.PutAccessMeta("n1", &AccessMetaEntry{
					TargetID: "n1",
					Overflow: map[string]interface{}{"seenAt": tc.stored, "views": int64(10)},
				})
			}
			NewAccessFlusher(newSpread(), store, time.Hour).Flush()
			e := store.get("n1")
			if e == nil {
				t.Fatal("expected a stored entry for n1")
			}
			if got := e.Overflow["seenAt"]; got != tc.want {
				t.Errorf("stored seenAt %d: got %v, want %d", tc.stored, got, tc.want)
			}
			wantViews := int64(6)
			if tc.stored != 0 {
				wantViews = 16
			}
			if got := e.Overflow["views"]; got != wantViews {
				t.Errorf("stored views: got %v, want %d", got, wantViews)
			}
			if e.Fixed.LastAccessedAt != 200 || e.Fixed.AccessCount != 3 {
				t.Errorf("stored fixed: lastAccessedAt=%d accessCount=%d, want 200, 3",
					e.Fixed.LastAccessedAt, e.Fixed.AccessCount)
			}
		}
	})
}
