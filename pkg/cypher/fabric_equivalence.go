package cypher

import "github.com/orneryd/nornicdb/pkg/fabric"

// Fabric uses the canonical native tuple index, including collision equality.
type fabricRowEquivalence struct{ values cypherValueIndex }

func newFabricRowEquivalence() fabric.RowEquivalence { return &fabricRowEquivalence{} }

func (index *fabricRowEquivalence) Duplicate(row []interface{}) bool {
	_, duplicate := index.values.addTuple(row)
	return duplicate
}
