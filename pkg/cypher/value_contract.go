package cypher

// The zero value is equivalence: DISTINCT, grouping, UNION and collection
// set/frequency indexes use it. Value lookup is a separate, explicit contract;
// in particular equal civil datetimes can be different overlap occurrences.
type cypherComparisonContract uint8

const (
	cypherEquivalenceContract cypherComparisonContract = iota
	cypherValueEqualityContract
)

func (contract cypherComparisonContract) temporalKey(value interface{}) (temporalValueKey, bool) {
	if contract == cypherValueEqualityContract {
		return temporalEqualityParts(value)
	}
	return temporalEquivalenceParts(value)
}
