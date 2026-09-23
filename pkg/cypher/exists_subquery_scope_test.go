package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnwindExistsSubqueryCorrelatesOuterBindings(t *testing.T) {
	executor := setupTestExecutor(t)
	executeBehaviorQuery(t, executor, `
		CREATE (a:Workload {name: 'alpha'}),
		       (b:Workload {name: 'beta'}),
		       (f:Function {name: 'handler'}),
		       (a)-[:HAS_FUNCTION]->(f)
	`)

	exists := executeBehaviorQuery(t, executor, `
		UNWIND ['alpha', 'beta'] AS wanted
		MATCH (workload:Workload {name: wanted})
		WHERE EXISTS { MATCH (workload)-[:HAS_FUNCTION]->(:Function) }
		RETURN workload.name
	`)
	require.Equal(t, [][]interface{}{{"alpha"}}, exists.Rows)

	notExists := executeBehaviorQuery(t, executor, `
		UNWIND ['alpha', 'beta'] AS wanted
		MATCH (workload:Workload {name: wanted})
		WHERE NOT EXISTS { MATCH (workload)-[:HAS_FUNCTION]->(:Function) }
		RETURN workload.name
	`)
	require.Equal(t, [][]interface{}{{"beta"}}, notExists.Rows)
}

func TestExistsSubqueryCarriesCorrelationThroughAggregation(t *testing.T) {
	executor := setupTestExecutor(t)
	executeBehaviorQuery(t, executor, `
		CREATE (a:A)-[:R]->(:B),
		       (a)-[:R]->(:C),
		       (a)-[:R]->(:D)
	`)

	result := executeBehaviorQuery(t, executor, `
		MATCH (n)
		WHERE EXISTS {
			MATCH (n)-->(m)
			WITH n, count(*) AS degree
			WHERE degree = 3
			RETURN true
		}
		RETURN labels(n)
	`)
	require.Equal(t, [][]interface{}{{[]interface{}{"A"}}}, result.Rows)
}

func TestExistsSubqueryRejectsUpdatingClauses(t *testing.T) {
	executor := setupTestExecutor(t)
	_, err := executor.Execute(context.Background(), `
		MATCH (n)
		WHERE EXISTS { MATCH (n)-->(m) SET m.value = 'invalid' }
		RETURN n
	`, nil)
	require.Error(t, err)
	var semanticError *SemanticError
	require.True(t, errors.As(err, &semanticError))
	require.Equal(t, "InvalidClauseComposition", semanticError.Detail)
}

func TestExistsSubqueryAppliesPatternPropertiesAndIdentityPredicates(t *testing.T) {
	executor := setupTestExecutor(t)
	executeBehaviorQuery(t, executor, `
		CREATE (api:Workload {name: 'api'}),
		       (database:Workload {name: 'database'}),
		       (api)-[:DEPENDS_ON]->(database)
	`)

	matching := executeBehaviorQuery(t, executor, `
		MATCH (workload:Workload {name: 'api'})
		WHERE EXISTS {
			MATCH (workload)-[:DEPENDS_ON]->(other:Workload {name: 'database'})
			WHERE other <> workload
		}
		RETURN workload.name
	`)
	require.Equal(t, [][]interface{}{{"api"}}, matching.Rows)

	nonMatching := executeBehaviorQuery(t, executor, `
		MATCH (workload:Workload {name: 'api'})
		WHERE EXISTS {
			MATCH (workload)-[:DEPENDS_ON]->(other:Workload {name: 'cache'})
			WHERE other <> workload
		}
		RETURN workload.name
	`)
	require.Empty(t, nonMatching.Rows)
}
