package cypher

import (
	"context"
	"sort"

	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)

// The bulk owner (package bulk) owns every sort, copy and clone of
// query-sized data in the cypher packages: rows, entities, IDs, paths, the
// candidates, groups and windows built from them, and the lists and maps a
// query passes or builds (TestPopulationBulkCallSites rejects the raw forms
// anywhere else). These wrappers record the statement's failure when the
// owner stops on a cancellation, as every owner records it.

// populationCopyStride is the items a bulk copy moves between observations.
const populationCopyStride = bulk.CopyStride

func recordPopulationFailure(ctx context.Context, err error) error {
	if err != nil {
		recordExpressionFailure(ctx, err)
	}
	return err
}

// sortPopulation is sort.Slice over a population.
func sortPopulation[T any](ctx context.Context, population []T, less func(i, j int) bool) error {
	return recordPopulationFailure(ctx, bulk.Slice(ctx, population, less))
}

// sortPopulationStable is sort.SliceStable over a population.
func sortPopulationStable[T any](ctx context.Context, population []T, less func(i, j int) bool) error {
	return recordPopulationFailure(ctx, bulk.SliceStable(ctx, population, less))
}

// sortPopulationInterfaceStable is sort.Stable over a population held in
// more than one slice (a sort.Interface that swaps them in step).
func sortPopulationInterfaceStable(ctx context.Context, population sort.Interface) error {
	return recordPopulationFailure(ctx, bulk.Stable(ctx, population))
}

// sortPopulationFunc is slices.SortFunc over a population.
func sortPopulationFunc[T any](ctx context.Context, population []T, compare func(T, T) int) error {
	return recordPopulationFailure(ctx, bulk.SortFunc(ctx, population, compare))
}

// stableSortRows is slices.SortStableFunc over a population: it keeps the
// standard library's stable order.
func stableSortRows[T any](ctx context.Context, rows []T, compare func(T, T) int) error {
	return recordPopulationFailure(ctx, bulk.SortStableFunc(ctx, rows, compare))
}

// sortStrings is sort.Strings over query-sized strings (a map's keys, IDs).
func sortStrings(ctx context.Context, values []string) error {
	return recordPopulationFailure(ctx, bulk.Strings(ctx, values))
}

// appendPopulation is append(dst, src...) over a population.
func appendPopulation[T any](ctx context.Context, dst, src []T) ([]T, error) {
	out, err := bulk.Append(ctx, dst, src)
	return out, recordPopulationFailure(ctx, err)
}

// copyPopulation is copy(dst, src) over a population.
func copyPopulation[T any](ctx context.Context, dst, src []T) (int, error) {
	n, err := bulk.Copy(ctx, dst, src)
	return n, recordPopulationFailure(ctx, err)
}

// clonePopulation is slices.Clone over a population.
func clonePopulation[T any](ctx context.Context, src []T) ([]T, error) {
	out, err := bulk.Clone(ctx, src)
	return out, recordPopulationFailure(ctx, err)
}

// clonePopulationMap is maps.Clone over a population keyed by its entities.
func clonePopulationMap[K comparable, V any](ctx context.Context, src map[K]V) (map[K]V, error) {
	out, err := bulk.CloneMap(ctx, src)
	return out, recordPopulationFailure(ctx, err)
}
