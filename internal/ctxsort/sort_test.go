package ctxsort

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"testing"
)

func TestSortCancellation(t *testing.T) {
	for _, stable := range []bool{false, true} {
		t.Run(map[bool]string{false: "unstable", true: "stable"}[stable], func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			values := make([]int, 100000)
			for i := range values {
				values[i] = len(values) - i
			}
			calls := 0
			err := Sort(ctx, values, func(a, b int) int {
				calls++
				if calls == 10 {
					cancel()
				}
				return cmp.Compare(a, b)
			}, stable)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v", err)
			}
			if calls > 256 {
				t.Fatalf("performed %d comparisons after cancellation", calls)
			}
			if err := Sort(t.Context(), values, cmp.Compare[int], stable); err != nil {
				t.Fatal(err)
			}
			if !slices.IsSorted(values) {
				t.Fatal("healthy retry did not sort")
			}
			cancel()
			calls = 0
			if err := Sort(ctx, values, func(a, b int) int { calls++; return cmp.Compare(a, b) }, stable); !errors.Is(err, context.Canceled) || calls != 0 {
				t.Fatalf("pre-canceled: err=%v calls=%d", err, calls)
			}
		})
	}
}

func TestSortStable(t *testing.T) {
	type entry struct{ key, order int }
	values := []entry{{2, 0}, {1, 1}, {2, 2}, {1, 3}}
	if err := Sort(t.Context(), values, func(a, b entry) int { return cmp.Compare(a.key, b.key) }, true); err != nil {
		t.Fatal(err)
	}
	want := []entry{{1, 1}, {1, 3}, {2, 0}, {2, 2}}
	if !slices.Equal(values, want) {
		t.Fatalf("got %v", values)
	}
}

func TestSortPreservesComparatorPanic(t *testing.T) {
	defer func() {
		if p := recover(); p != "comparator failure" {
			t.Fatalf("panic = %v", p)
		}
	}()
	_ = Sort(t.Context(), []int{2, 1}, func(int, int) int { panic("comparator failure") }, false)
}

func BenchmarkSort(b *testing.B) {
	for _, cancellable := range []bool{false, true} {
		b.Run(map[bool]string{false: "background", true: "cancellable"}[cancellable], func(b *testing.B) {
			ctx := context.Background()
			if cancellable {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
			}
			values := make([]int, 1000)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				for i := range values {
					values[i] = len(values) - i
				}
				if err := Sort(ctx, values, cmp.Compare[int], false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
