package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// The allocation ladder: one statement, two populations of the same-type
// relationships the constrained value does not select. A relationship property
// lookup reads the one relationship its value selects, so the statement's
// allocations stay flat across the ladder; enumerating the type reads and
// filters every unrelated relationship and cannot.
const (
	relationshipEqualityAllocationSmallPopulation = 8
	relationshipEqualityAllocationLargePopulation = 512
	// AllocsPerRun's mean over a few runs, after one warm query, so the
	// transaction's and the index's own construction is not part of the count.
	relationshipEqualityAllocationRuns = 3
	// The bound is generous because one allocation count approximates one
	// statement's work: a flat access stays inside the ratio plus the slack,
	// while reading 504 more relationships does not.
	relationshipEqualityAllocationGrowthRatio = 1.25
	relationshipEqualityAllocationBaseSlack   = 256
)

// TestRelationshipPropertyEqualityAllocationsStayFlatAcrossTheType states the
// release's cost as a population ladder instead of a duration: the same public
// statement, on each parser, in auto-commit and inside an explicit read
// transaction, over 8 and over 512 unrelated relationships of the type. Each
// population must return the identical one row, and the larger population may
// not allocate meaningfully more than the smaller one.
func TestRelationshipPropertyEqualityAllocationsStayFlatAcrossTheType(t *testing.T) {
	expected := [][]interface{}{{"r289999", "n73", "n128561"}}
	for _, parser := range relationshipEqualityParsers {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			for _, transaction := range []struct {
				name     string
				explicit bool
			}{
				{"auto-commit", false},
				{"explicit transaction", true},
			} {
				t.Run(transaction.name, func(t *testing.T) {
					small := measureRelationshipEqualityAllocations(t, transaction.explicit, relationshipEqualityAllocationSmallPopulation, expected)
					large := measureRelationshipEqualityAllocations(t, transaction.explicit, relationshipEqualityAllocationLargePopulation, expected)
					t.Logf("%d unrelated relationships: %.1f allocations per statement; %d: %.1f",
						relationshipEqualityAllocationSmallPopulation, small,
						relationshipEqualityAllocationLargePopulation, large)
					require.LessOrEqual(t, large, small*relationshipEqualityAllocationGrowthRatio+relationshipEqualityAllocationBaseSlack,
						"reading the one relationship the value selects must not allocate with the type's unrelated population")
				})
			}
		})
	}
}

// measureRelationshipEqualityAllocations builds one fixture of population
// unrelated same-type relationships and returns the statement's mean allocations
// per call with the query cache disabled, measured after one warm query.
func measureRelationshipEqualityAllocations(t *testing.T, explicit bool, population int, expected [][]interface{}) float64 {
	t.Helper()
	fixture := newRelationshipEqualityAccessFixture(t, relationshipEqualityFixtureOptions{unrelatedRelationships: population})
	ctx := context.Background()
	if explicit {
		ctx = beginRelationshipEqualityTransaction(t, fixture.exec)
	}
	var rows [][]interface{}
	read := func() {
		result, err := fixture.exec.Execute(ctx, relationshipEqualityAccessQuery, map[string]interface{}{"id": "r289999"})
		require.NoError(t, err)
		rows = result.Rows
	}
	read()
	allocations := testing.AllocsPerRun(relationshipEqualityAllocationRuns, read)
	require.Equal(t, expected, rows, "each population must answer the identical one row")
	return allocations
}
