package fabric

import "reflect"

// Unit tests supply a policy explicitly. Public Cypher differential tests use
// the actual native value owner, including recursive numeric/temporal identity.
type testRowEquivalence struct{ rows [][]interface{} }

func (index *testRowEquivalence) Duplicate(row []interface{}) bool {
	for _, previous := range index.rows {
		if reflect.DeepEqual(previous, row) {
			return true
		}
	}
	index.rows = append(index.rows, append([]interface{}{}, row...))
	return false
}

func newTestRowEquivalence() RowEquivalence { return &testRowEquivalence{} }
func newTestFabricExecutor(catalog *Catalog, local *LocalFragmentExecutor, remote *RemoteFragmentExecutor) *FabricExecutor {
	return NewFabricExecutor(catalog, local, remote, newTestRowEquivalence)
}
func newTestDistinctRowIterator(base RowIterator) RowIterator {
	return NewDistinctRowIterator(base, newTestRowEquivalence())
}
func deduplicateTestRows(rows [][]interface{}) [][]interface{} {
	return deduplicateRows(rows, newTestRowEquivalence())
}
func executeTestApplyInMemoryProjection(input *ResultStream, query string) (*ResultStream, bool) {
	return newTestFabricExecutor(nil, nil, nil).executeApplyInMemoryProjection(input, query)
}
