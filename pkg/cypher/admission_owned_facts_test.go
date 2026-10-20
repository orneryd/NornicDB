package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestAdmissionOwnedFactsPublic(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	samples := []struct {
		query string
		rows  [][]interface{}
	}{
		{"WITH [1,2] AS xs RETURN [x /* native declaration */ IN xs | CASE WHEN x=1 THEN 'x IN xs' ELSE 'other' END] AS result", [][]interface{}{{[]interface{}{"x IN xs", "other"}}}},
		{"WITH 9 AS x CALL { WITH [2,3] AS values RETURN [x IN values | CASE WHEN x=2 THEN x ELSE 0 END] AS result } RETURN x,result", [][]interface{}{{int64(9), []interface{}{int64(2), int64(0)}}}},
		{"WITH [1,2] AS xs RETURN reduce(total=0,x IN xs | total + CASE WHEN x=1 THEN 10 ELSE x END) AS result", [][]interface{}{{int64(12)}}},
		{"WITH 3 AS x RETURN COUNT { WITH x RETURN x } AS result", [][]interface{}{{int64(1)}}},
		{"WITH 3 AS x RETURN COLLECT { WITH x CALL { RETURN 2 AS y } RETURN CASE WHEN y=2 THEN x ELSE y END AS value } AS result", [][]interface{}{{[]interface{}{int64(3)}}}},
		{"WITH true AS unused OPTIONAL CALL { WITH true AS value WHERE false RETURN value } RETURN value", [][]interface{}{{nil}}},
		{"WITH 7 AS x CALL (x) { UNWIND [1,2] AS row WITH x AS value FINISH } RETURN x", [][]interface{}{{int64(7)}}},
	}
	ctx := context.Background()
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, composite := range []bool{false, true} {
			for _, explicit := range []bool{false, true} {
				for index, sample := range samples {
					t.Run(fmt.Sprintf("case_%d/norm_%v/composite_%v/tx_%v", index, normalize, composite, explicit), func(t *testing.T) {
						executor := newFabricCallRowsExecutor(t)
						query := sample.query
						if composite {
							executor = fabricCallRowsCompositeExecutor(t, executor)
							query = "USE comp.one " + query
						}
						if explicit {
							_, err := executor.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							defer executor.Execute(ctx, "ROLLBACK", nil)
						}
						for repeat := 0; repeat < 2; repeat++ {
							result, err := executor.Execute(ctx, query, nil)
							require.NoError(t, err, query)
							require.Equal(t, sample.rows, result.Rows, query)
						}
					})
				}
			}
		}
	}
}

func TestCompiledReadFootprintNativeParity(t *testing.T) {
	for _, expression := range []string{"", "n.age > 30", "n.name", "n.age + m.age > 4", "coalesce(n.age,m.age)"} {
		references, complete := compiledReadFootprint(expression)
		require.True(t, complete, expression)
		require.ElementsMatch(t, semanticExpressionReferences(expression), references, expression)
	}
	for _, text := range []string{"RETURN n.name LIMIT 10", "RETURN a.name,b.name LIMIT 20"} {
		references, complete := compiledClauseReadFootprint(pipelineClause{kind: pipelineClauseReturn, text: text})
		require.True(t, complete, text)
		require.ElementsMatch(t, semanticExpressionReferences(text), references, text)
	}
	for _, expression := range []string{"RETURN n.id AS v", "WITH n RETURN n.id", "RETURN [x IN values | x]", "{k:v IN values | k:v}"} {
		_, complete := compiledReadFootprint(expression)
		require.False(t, complete, expression)
	}
}

func TestStatementAdmissionParserBoundary(t *testing.T) {
	for _, query := range []string{
		"MATCH (n:Person) RETURN n.name LIMIT 10",
		"MATCH (n:Person) WHERE n.age > 30 RETURN count(n)",
		"MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name,b.name LIMIT 20",
		admissionScalingStatement(116525, 123),
	} {
		input := newSemanticStatementInput(query, nil)
		require.Nil(t, input.syntax(), query[:min(len(query), 100)])
		clauses, ok := input.pipeline(true)
		require.True(t, ok)
		for _, clause := range clauses {
			require.True(t, clause.noLocalScopes)
			view := (staticTypeScope{}).withClauseInput(clause)
			require.Nil(t, view.native)
			require.Empty(t, view.nativeScopes(clause.text))
		}
	}
	for _, query := range []string{
		"WITH 1 AS x OPTIONAL CALL (x) { RETURN x } RETURN x",
		"USE comp.one RETURN 1 AS x",
		"RETURN [x IN [1] | CASE WHEN x=1 THEN x END] AS value",
		"RETURN allReduce(s=0,x IN [1] | s+x,s>0) AS value",
	} {
		input := newSemanticStatementInput(query, nil)
		require.NotNil(t, input.syntax(), query)
		require.NoError(t, input.syntax().Err, query)
	}
}

func TestStatementAdmissionPredicateAndCommandPopulation(t *testing.T) {
	for _, query := range []string{
		"MATCH (n WHERE all(x IN [1] WHERE x=1)) RETURN n",
		"MATCH (n {id:head([x IN [1] WHERE x=1 | x])}) RETURN n",
		"MATCH (n)-[r {w:head([x IN [1] WHERE x=1 | x])}]->(m) RETURN m",
		"MATCH ((n)-[r]->(m) WHERE all(x IN [r] WHERE x.w=1)){1,2} RETURN count(*)",
		"MATCH ((n {id:head([x IN [1] | x])})-[r]->(m)){1,2} RETURN count(*)",
	} {
		input := newSemanticStatementInput(query, nil)
		require.NotNil(t, input.syntax(), query)
		if input.syntax().Err != nil {
			var syntax *fabric.QuerySyntaxError
			require.ErrorAs(t, input.syntax().Err, &syntax)
			require.True(t, syntax.PatternLowering, query)
			lowered, rewrite, err := lowerPatternAdmission(query, false)
			require.NoError(t, err)
			require.NotNil(t, rewrite, query)
			require.NoError(t, fabric.ReadQueryStructure(lowered).Err, query)
		}
	}
	for _, query := range []string{"SHOW FUNCTIONS", "SHOW PROCEDURES", "CREATE INDEX admission84 FOR (n:AdmissionPredicate) ON (n.id)", "DROP INDEX admission84 IF EXISTS", "TERMINATE TRANSACTIONS 'absent'", "not a statement"} {
		input := newSemanticStatementInput(query, nil)
		require.Nil(t, input.syntax(), query)
	}
	// Prepared Fabric admission supplies the command descriptor explicitly.
	for _, query := range []string{"SHOW FUNCTIONS", "CREATE INDEX admission84 FOR (n:AdmissionPredicate) ON (n.id)", "TERMINATE TRANSACTIONS 'absent'"} {
		syntax := fabric.ReadQueryStructure(query)
		require.NoError(t, syntax.Err, query)
		input := newSemanticStatementInput(query, syntax)
		require.Same(t, syntax, input.syntax())
		clauses, ok := input.pipeline(true)
		require.True(t, ok)
		require.Equal(t, pipelineClauseCommand, clauses[0].kind)
	}
}

func TestAdmissionPatternPredicatePublic(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, composite := range []bool{false, true} {
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("norm_%v/composite_%v/tx_%v", normalize, composite, explicit), func(t *testing.T) {
					executor := newFabricCallRowsExecutor(t)
					prefix := ""
					if composite {
						executor = fabricCallRowsCompositeExecutor(t, executor)
						prefix = "USE comp.one "
					}
					ctx := context.Background()
					_, err := executor.Execute(ctx, prefix+"CREATE (a:AdmissionPredicate {id:1})-[:P {w:1}]->(b:AdmissionPredicate {id:2})", nil)
					require.NoError(t, err)
					if explicit {
						_, err = executor.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer executor.Execute(ctx, "ROLLBACK", nil)
					}
					for _, query := range []string{
						"MATCH (n:AdmissionPredicate WHERE all(x IN [1] WHERE CASE WHEN x=1 THEN true ELSE false END)) RETURN count(*) AS value",
						"MATCH (n:AdmissionPredicate)-[r:P WHERE any(x IN [1] WHERE x=r.w)]->(m) RETURN count(*) AS value",
						"MATCH (n:AdmissionPredicate {id:head([x IN [1,2] WHERE CASE WHEN x=1 THEN true ELSE false END | x])}) RETURN count(*) AS value",
						"MATCH (n:AdmissionPredicate)-[r:P {w:head([x IN [1] WHERE x=1 | x])}]->(m) RETURN count(*) AS value",
						"MATCH ((n:AdmissionPredicate)-[r:P]->(m) WHERE all(x IN [r] WHERE x.w=1)){1,2} RETURN count(*) AS value",
					} {
						for repeat := 0; repeat < 2; repeat++ {
							result, err := executor.Execute(ctx, prefix+query, nil)
							require.NoError(t, err, query)
							want := int64(1)
							if strings.HasPrefix(query, "MATCH (n:AdmissionPredicate WHERE") {
								want = 2
							}
							require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
						}
					}
				})
			}
		}
	}
}

func TestImplicitPhysicalProjectionNativeParity(t *testing.T) {
	for _, expression := range []string{"n.name", "n.age + 1", "true", "null", "'name,part'", "coalesce(n.name,'unknown')"} {
		projection := fabric.ProjectionSyntax(expression)
		require.NotNil(t, projection, expression)
		require.Len(t, projection.Items, 1)
		expr, column := parseProjectionExprAlias(expression)
		require.Equal(t, projection.Items[0].Expression, expr)
		require.Equal(t, projection.Items[0].Column, column)
		require.False(t, projection.Items[0].HasAlias)
	}
}

func TestAdmissionOwnedPaginationPublic(t *testing.T) {
	executor := newFabricCallRowsExecutor(t)
	ctx := context.Background()
	for _, sample := range []struct {
		query string
		rows  [][]interface{}
	}{
		{"CALL { RETURN 1 AS v LIMIT 1 UNION RETURN 2 AS v LIMIT 0 } RETURN v", [][]interface{}{{int64(1)}}},
		{"RETURN COUNT { WITH 1 AS v RETURN v LIMIT 1 } AS value", [][]interface{}{{int64(1)}}},
		{"RETURN COLLECT { WITH 1 AS v RETURN v LIMIT 0 } AS value", [][]interface{}{{[]interface{}{}}}},
	} {
		result, err := executor.Execute(ctx, sample.query, nil)
		require.NoError(t, err, sample.query)
		require.Equal(t, sample.rows, result.Rows, sample.query)
	}
	for _, query := range []string{
		"CALL { WITH 1 AS n RETURN n LIMIT n } RETURN n",
		"CALL { RETURN 1 AS v LIMIT -1 } RETURN v",
		"RETURN COUNT { RETURN 1 AS v LIMIT v } AS value",
	} {
		_, err := executor.Execute(ctx, query, nil)
		require.Error(t, err, query)
	}
}
