package louvain

import (
	"context"
	"sort"
)

// Cancellation is the caller's synchronous work budget. Continue must return
// false when work must stop; Err then describes the cancellation. A nil owner
// retains the existing context-free API and fast paths.
type Cancellation interface {
	Continue() bool
	Err() error
}

// RunWithCancellation runs every phase under the same owner, including local
// moves, colouring, sorting, aggregation and result materialization. The owner
// is never retained by the graph. A cancelled run returns no partial result.
func RunWithCancellation(graph *Graph, options Options, cancellation Cancellation) (Result, error) {
	return cancellable(cancellation, func() Result {
		if cancellation == nil {
			return Run(graph, options)
		}
		// The shared adjacency and degree arrays are read-only. Keep total's
		// per-run reduction local so cancellation does not leave partial state.
		view := *graph
		return run(&view, options, cancellation)
	})
}

// NewGraphWithCancellation applies the owner to optional arc reservation work.
func NewGraphWithCancellation(size int, arcCounts []int, cancellation Cancellation) (*Graph, error) {
	return cancellable(cancellation, func() *Graph { return newGraph(size, arcCounts, cancellation) })
}

// RenumberWithCancellation uses the same first-node numbering as Renumber.
// On cancellation the caller must discard the partially numbered input.
func RenumberWithCancellation(community []int, cancellation Cancellation) (int, error) {
	return cancellable(cancellation, func() int { return renumber(community, cancellation) })
}

// Only this private sentinel unwinds synchronous bulk operations. Unrelated
// panics retain their existing meaning; no background worker outlives a call.
type cancelled struct{}

func check(cancellation Cancellation) {
	if cancellation != nil && !cancellation.Continue() {
		panic(cancelled{})
	}
}

func cancellable[T any](cancellation Cancellation, run func() T) (result T, err error) {
	if cancellation == nil {
		return run(), nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			if _, ok := recovered.(cancelled); !ok {
				panic(recovered)
			}
			err = cancellation.Err()
			if err == nil {
				err = context.Canceled
			}
		}
	}()
	check(cancellation)
	return run(), nil
}

func sortInts(values []int, cancellation Cancellation) {
	if cancellation == nil {
		sort.Ints(values)
		return
	}
	check(cancellation)
	sort.Slice(values, func(i, j int) bool {
		check(cancellation)
		return values[i] < values[j]
	})
}

func copyInts(values []int, cancellation Cancellation) []int {
	if cancellation == nil {
		return append([]int(nil), values...)
	}
	copy := make([]int, len(values))
	for index, value := range values {
		check(cancellation)
		copy[index] = value
	}
	return copy
}

func clearMap[K comparable, V any](values map[K]V, cancellation Cancellation) {
	if cancellation == nil {
		clear(values)
		return
	}
	for key := range values {
		check(cancellation)
		delete(values, key)
	}
}

func clearBools(values []bool, cancellation Cancellation) {
	if cancellation == nil {
		clear(values)
		return
	}
	for index := range values {
		check(cancellation)
		values[index] = false
	}
}
