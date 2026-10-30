package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCorrelatedCallPreservesOuterEntityProjection(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	_, err := executor.Execute(ctx, `
		CREATE (:Company {name: 'Acme'}), (:Company {name: 'Bolt'}),
		       (:Person {name: 'Ada'}), (:Person {name: 'Lin'}), (:Person {name: 'Max'})
	`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, `
		MATCH (company:Company), (person:Person)
		WHERE (company.name = 'Acme' AND person.name IN ['Ada', 'Lin'])
		   OR (company.name = 'Bolt' AND person.name = 'Max')
		CREATE (person)-[:WORKS_AT]->(company)
	`, nil)
	require.NoError(t, err)

	result, err := executor.Execute(ctx, `
		MATCH (company:Company)
		CALL {
			WITH company
			MATCH (person:Person)-[:WORKS_AT]->(company)
			RETURN count(person) AS count
		}
		RETURN company.name AS company, count
		ORDER BY company
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"Acme", int64(2)}, {"Bolt", int64(1)}}, result.Rows)
}
