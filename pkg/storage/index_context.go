package storage

import "context"

// copySliceWithContext is the retained chunked result-copy owner for index
// candidates and schema snapshots. Chunking observes cancellation without
// adding one context call per element.
func copySliceWithContext[T any](ctx context.Context, ids []T) ([]T, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ids == nil {
		return nil, ctx.Err()
	}
	out := make([]T, len(ids))
	for start := 0; start < len(ids); start += 256 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := min(start+256, len(ids))
		copy(out[start:end], ids[start:end])
	}
	return out, ctx.Err()
}

// sortIndexCandidatesWithContext is the one cancellable sort for private
// candidate/key preparations. Stable merging preserves equal-key order. A
// canceled preparation never publishes its partially sorted private slice.
func sortIndexCandidatesWithContext[T any](ctx context.Context, values []T, less func(T, T) bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(values) < 2 {
		return nil
	}
	scratch := make([]T, len(values))
	src, dst := values, scratch
	for width := 1; width < len(values); {
		for start := 0; start < len(values); {
			mid := start + min(width, len(values)-start)
			end := mid + min(width, len(values)-mid)
			i, j := start, mid
			for k := start; k < end; k++ {
				if err := ctx.Err(); err != nil {
					return err
				}
				if i < mid && (j >= end || !less(src[j], src[i])) {
					dst[k] = src[i]
					i++
				} else {
					dst[k] = src[j]
					j++
				}
			}
			start = end
		}
		src, dst = dst, src
		if width >= len(values)-width {
			break
		}
		width *= 2
	}
	// src can be scratch after the last pass; copy in observable chunks.
	for start := 0; start < len(values); start += 256 {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := min(start+256, len(values))
		copy(values[start:end], src[start:end])
	}
	return ctx.Err()
}

// reverseSliceWithContext owns cancellable reversal of already-private result
// order; the MVCC scan keeps its oldest-first record selection unchanged.
func reverseSliceWithContext[T any](ctx context.Context, values []T) error {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		if err := ctx.Err(); err != nil {
			return err
		}
		values[left], values[right] = values[right], values[left]
	}
	return ctx.Err()
}
