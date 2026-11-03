// Package ctxsort provides cancellation-aware in-place sorting.
package ctxsort

import (
	"context"
	"slices"
)

type interrupted struct{ err error }

// Sort orders values using compare. Stable preserves the order of equal values.
// Cancellation can leave values partially sorted; callers must publish them only
// after success. Comparator panics are preserved. A nil context is invalid.
func Sort[T any](ctx context.Context, values []T, compare func(T, T) int, stable bool) (err error) {
	if err = ctx.Err(); err != nil {
		return err
	}
	if ctx.Done() == nil {
		if stable {
			slices.SortStableFunc(values, compare)
		} else {
			slices.SortFunc(values, compare)
		}
		return nil
	}
	// The standard sort has no error channel. This private sentinel unwinds only
	// this call; unrelated comparator panics retain their ordinary behavior.
	defer func() {
		if p := recover(); p != nil {
			if stop, ok := p.(interrupted); ok {
				err = stop.err
			} else {
				panic(p)
			}
		}
	}()
	remaining := uint16(0)
	checked := func(a, b T) int {
		if remaining == 0 {
			if err := ctx.Err(); err != nil {
				panic(interrupted{err})
			}
			remaining = 256
		}
		remaining--
		return compare(a, b)
	}
	if stable {
		slices.SortStableFunc(values, checked)
	} else {
		slices.SortFunc(values, checked)
	}
	return ctx.Err()
}
