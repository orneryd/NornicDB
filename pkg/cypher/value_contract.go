package cypher

// Every key, comparison, hash and index operation requires its caller's
// contract. Zero is not a contract. Equal civil datetimes can be different
// overlap occurrences under equivalence.
type cypherComparisonContract uint8

const (
	cypherEquivalenceContract cypherComparisonContract = iota + 1
	cypherValueEqualityContract
)

// Java-object removal sets have a separate numeric representation boundary;
// temporal identity still uses the same explicit equality/equivalence owner.
type cypherNumericComparisonPolicy uint8

const (
	cypherNumericValues cypherNumericComparisonPolicy = iota
	cypherNumericKinds
)
