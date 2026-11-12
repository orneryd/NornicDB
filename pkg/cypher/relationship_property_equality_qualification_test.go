package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// relationshipEqualityQualificationPopulations are the unrelated relationship
// populations the release statement must answer identically at: the public
// fixture's own small population, and the 4096 and 32768 populations this
// qualification adds. The fixture, its declaration and the statement are the
// existing owners; this adds only the population ladder.
var relationshipEqualityQualificationPopulations = []int{128, 4096, 32768}

// TestRelationshipPropertyEqualityQualificationPublicResultAcrossPopulations
// states the release statement's determinism as a population ladder through the
// public Execute path the release used: on each parser, at each representative
// population, the constrained relationship property equality match returns the
// same single row, and a value the declaration does not hold returns no row
// instead of one of the type's unrelated relationships.
func TestRelationshipPropertyEqualityQualificationPublicResultAcrossPopulations(t *testing.T) {
	for _, parser := range relationshipEqualityParsers {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			for _, population := range relationshipEqualityQualificationPopulations {
				t.Run(fmt.Sprintf("unrelated-%d", population), func(t *testing.T) {
					fixture := newRelationshipEqualityAccessFixture(t, relationshipEqualityFixtureOptions{unrelatedRelationships: population})
					ctx := context.Background()
					result, err := fixture.exec.Execute(ctx, relationshipEqualityAccessQuery, map[string]interface{}{"id": "r289999"})
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{"r289999", "n73", "n128561"}}, result.Rows)

					absent, err := fixture.exec.Execute(ctx, relationshipEqualityAccessQuery, map[string]interface{}{"id": "r289999-absent"})
					require.NoError(t, err)
					require.Empty(t, absent.Rows)
				})
			}
		})
	}
}
