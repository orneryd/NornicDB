package cypher

import "github.com/orneryd/nornicdb/pkg/fabric"

// Fabric uses the same explicit equivalence contract as native DISTINCT,
// grouping and collections, recursively including numeric and temporal values.
type fabricRowEquivalence struct{ values cypherValueIndex }

func newFabricRowEquivalence() fabric.RowEquivalence { return &fabricRowEquivalence{} }

func (index *fabricRowEquivalence) Duplicate(row []interface{}) bool {
	_, duplicate := index.values.addTupleByContract(row, cypherEquivalenceContract)
	return duplicate
}
