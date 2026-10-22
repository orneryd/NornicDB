// Package bulk owns the sorts, copies and clones of query-sized data: a
// list or map a query passed or built, the rows, entities or paths it holds,
// or the keys and values derived from them. A standard-library sort or copy
// has no loop at its call site for a cancellation check to reach, so these
// primitives observe the statement's context within the work instead.
//
// A sort observes once per Stride comparisons and stops at the first
// observation that finds the statement canceled; the slice is then left in
// an unspecified order and the primitive returns the context's error. A copy
// or append observes once per CopyStride items; a smaller one is the plain
// built-in, so data that fits in one stride pays nothing.
//
// The cypher package wraps these primitives to record the statement's
// failure (population_bulk.go); its function package uses them directly and
// returns the error, which the evaluator records.
package bulk

import (
	"context"
	"slices"
	"sort"
	"strings"
)

// Stride is the comparisons, or items, between two observations.
const Stride = 1024

// CopyStride is the items a bulk copy moves between observations: one
// memmove of this many pointers takes microseconds.
const CopyStride = 64 * Stride

type cancellation struct{ err error }

// comparisons counts a sort's comparisons and stops the sort at the first
// observation that finds the statement canceled.
type comparisons struct {
	ctx   context.Context
	count int
}

func (c *comparisons) observe() {
	if c.count&(Stride-1) == 0 {
		if err := c.ctx.Err(); err != nil {
			panic(cancellation{err})
		}
	}
	c.count++
}

// recoverSort turns the sort's own cancellation into its error; an unrelated
// comparator panic still reaches the caller.
func recoverSort(err *error) {
	if failure := recover(); failure != nil {
		stopped, ok := failure.(cancellation)
		if !ok {
			panic(failure)
		}
		*err = stopped.err
	}
}

// Slice is sort.Slice.
func Slice[T any](ctx context.Context, data []T, less func(i, j int) bool) (err error) {
	if len(data) < 2 {
		return nil
	}
	defer recoverSort(&err)
	observed := comparisons{ctx: ctx}
	sort.Slice(data, func(i, j int) bool {
		observed.observe()
		return less(i, j)
	})
	return nil
}

// SliceStable is sort.SliceStable.
func SliceStable[T any](ctx context.Context, data []T, less func(i, j int) bool) (err error) {
	if len(data) < 2 {
		return nil
	}
	defer recoverSort(&err)
	observed := comparisons{ctx: ctx}
	sort.SliceStable(data, func(i, j int) bool {
		observed.observe()
		return less(i, j)
	})
	return nil
}

// observedInterface is a sort.Interface whose comparisons observe.
type observedInterface struct {
	sort.Interface
	observed *comparisons
}

func (o observedInterface) Less(i, j int) bool {
	o.observed.observe()
	return o.Interface.Less(i, j)
}

// Stable is sort.Stable, for data that sorts more than one slice in step.
func Stable(ctx context.Context, data sort.Interface) (err error) {
	if data.Len() < 2 {
		return nil
	}
	defer recoverSort(&err)
	observed := comparisons{ctx: ctx}
	sort.Stable(observedInterface{Interface: data, observed: &observed})
	return nil
}

// SortFunc is slices.SortFunc.
func SortFunc[T any](ctx context.Context, data []T, compare func(T, T) int) (err error) {
	if len(data) < 2 {
		return nil
	}
	defer recoverSort(&err)
	observed := comparisons{ctx: ctx}
	slices.SortFunc(data, func(left, right T) int {
		observed.observe()
		return compare(left, right)
	})
	return nil
}

// SortStableFunc is slices.SortStableFunc. Checking only before and after
// sorting would leave a large stable merge running long after its deadline.
func SortStableFunc[T any](ctx context.Context, data []T, compare func(T, T) int) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(data) < 2 {
		return nil
	}
	defer recoverSort(&err)
	observed := comparisons{ctx: ctx}
	slices.SortStableFunc(data, func(left, right T) int {
		observed.observe()
		return compare(left, right)
	})
	return ctx.Err()
}

// Strings is sort.Strings.
func Strings(ctx context.Context, data []string) error {
	return SortFunc(ctx, data, strings.Compare)
}

// Append is append(dst, src...).
func Append[T any](ctx context.Context, dst, src []T) ([]T, error) {
	if len(src) <= CopyStride {
		return append(dst, src...), nil
	}
	dst = slices.Grow(dst, len(src))
	for start := 0; start < len(src); start += CopyStride {
		if err := ctx.Err(); err != nil {
			return dst, err
		}
		dst = append(dst, src[start:min(start+CopyStride, len(src))]...)
	}
	return dst, nil
}

// Copy is copy(dst, src).
func Copy[T any](ctx context.Context, dst, src []T) (int, error) {
	n := min(len(dst), len(src))
	if n <= CopyStride {
		return copy(dst, src), nil
	}
	for start := 0; start < n; start += CopyStride {
		if err := ctx.Err(); err != nil {
			return start, err
		}
		copy(dst[start:min(start+CopyStride, n)], src[start:min(start+CopyStride, n)])
	}
	return n, nil
}

// Clone is slices.Clone.
func Clone[T any](ctx context.Context, src []T) ([]T, error) {
	if src == nil {
		return nil, nil
	}
	return Append(ctx, make([]T, 0, len(src)), src)
}

// CloneMap is maps.Clone.
func CloneMap[K comparable, V any](ctx context.Context, src map[K]V) (map[K]V, error) {
	if src == nil {
		return nil, nil
	}
	out := make(map[K]V, len(src))
	checkpoint := Checkpoint{ctx: ctx}
	for key, value := range src {
		if err := checkpoint.Observe(); err != nil {
			return nil, err
		}
		out[key] = value
	}
	return out, nil
}

// Checkpoint is a loop's observation: Observe checks the statement on the
// first item and then once per Stride, so an item costs an increment. Use it
// in a loop over query-sized data that no batch owner walks, such as the
// inner loop of a nested pass.
type Checkpoint struct {
	ctx   context.Context
	index int
}

// NewCheckpoint observes ctx.
func NewCheckpoint(ctx context.Context) Checkpoint { return Checkpoint{ctx: ctx} }

// Observe returns the statement's error once it is canceled or expired.
func (c *Checkpoint) Observe() error {
	index := c.index
	c.index++
	if index&(Stride-1) != 0 || c.ctx == nil {
		return nil
	}
	return c.ctx.Err()
}
