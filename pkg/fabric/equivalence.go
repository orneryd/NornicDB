package fabric

// RowEquivalence is supplied by the Cypher value owner. Fabric has no separate
// hashing or equality policy: UNION and physical DISTINCT use this contract.
type RowEquivalence interface {
	Duplicate([]interface{}) bool
}

type RowEquivalenceFactory func() RowEquivalence

func deduplicateRows(rows [][]interface{}, seen RowEquivalence) [][]interface{} {
	result := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		if !seen.Duplicate(row) {
			result = append(result, row)
		}
	}
	return result
}
