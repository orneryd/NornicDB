package knowledgepolicy

import (
	"testing"
	"time"
)

// TestAccumulator_CustomKeyKindFixedByFirstUse uses one custom key as both a
// counter and a timestamp. Its first use fixes its kind and the other kind's
// write is refused, so ReadThrough, the flushed value and a read after the
// flush all agree: 150 stored + counter 100 = 250 when the counter came first,
// the latest of 150 and timestamp 200 = 200 when the timestamp came first.
func TestAccumulator_CustomKeyKindFixedByFirstUse(t *testing.T) {
	for _, tc := range []struct {
		name           string
		timestampFirst bool
		want           int64
	}{
		{"counter first", false, 250},
		{"timestamp first", true, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newMockStore()
			_ = store.PutAccessMeta("n1", &AccessMetaEntry{TargetID: "n1", Overflow: map[string]interface{}{"x": int64(150)}})
			acc := NewAccessAccumulator(true, 0)
			writes := []func(){
				func() { acc.IncrementCustom("n1", "x", 100) },
				func() { acc.SetTimestamp("n1", "x", 200) },
			}
			if tc.timestampFirst {
				writes[0], writes[1] = writes[1], writes[0]
			}
			for _, write := range writes {
				write()
			}

			if got := acc.ReadThrough("n1", "x", 150); got != tc.want {
				t.Errorf("ReadThrough before flush = %d, want %d", got, tc.want)
			}
			NewAccessFlusher(acc, store, time.Hour).Flush()
			if got := store.get("n1").Overflow["x"]; got != tc.want {
				t.Errorf("flushed value = %v, want %d", got, tc.want)
			}
			if got := acc.ReadThrough("n1", "x", tc.want); got != tc.want {
				t.Errorf("ReadThrough after flush = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestAccumulator_CustomKeyKindRefusalsCounted pins the refusal: the kind is
// per key (across entities), every refused write is counted, and a key never
// used reads as its persisted value.
func TestAccumulator_CustomKeyKindRefusalsCounted(t *testing.T) {
	acc := NewAccessAccumulator(true, 0)
	acc.IncrementCustom("n1", "x", 1)
	acc.SetTimestamp("n1", "x", 5)
	acc.SetTimestamp("n2", "x", 6)
	acc.IncrementCustom("n2", "x", 1)
	acc.SetTimestamp("n1", "y", 5)
	acc.IncrementCustom("n1", "y", 1)
	acc.SetTimestamp("n1", "y", 7)

	if got := acc.RejectedCustomWrites(); got != 3 {
		t.Errorf("RejectedCustomWrites = %d, want 3", got)
	}
	for _, tc := range []struct {
		entity, key     string
		persisted, want int64
	}{
		{"n1", "x", 10, 11},      // counter: 10 + 1
		{"n2", "x", 10, 11},      // counter: 10 + 1
		{"n1", "y", 6, 7},        // timestamp: latest of 6, 5, 7
		{"n1", "unused", 42, 42}, // never used: persisted
	} {
		if got := acc.ReadThrough(tc.entity, tc.key, tc.persisted); got != tc.want {
			t.Errorf("ReadThrough(%s, %s, %d) = %d, want %d", tc.entity, tc.key, tc.persisted, got, tc.want)
		}
	}
}
