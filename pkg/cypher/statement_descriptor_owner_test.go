package cypher

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStatementDescriptorSharedAdmissionAndExecution(t *testing.T) {
	for _, query := range []string{
		"MATCH (n:Person) RETURN n.name LIMIT 10",
		"MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name,b.name LIMIT 20",
		"WITH 1 AS x OPTIONAL CALL (x) { RETURN x AS y } RETURN x,y",
		"RETURN [x IN [1] | CASE WHEN x=1 THEN x END] AS value",
		"USE comp.one RETURN 1 AS value",
	} {
		input := newSemanticStatementInput(query, nil)
		clauses, ok := input.pipeline(true)
		require.True(t, ok, query)
		descriptor := pipelineStatementFor(query)
		require.Equal(t, descriptor.native, input.native)
		require.Same(t, &descriptor.clauses[0], &clauses[0])
		physical, supported := cachedPipelineClauses(query, true)
		if supported {
			require.Equal(t, clauses, physical)
			physical[0].text = "modified invocation copy"
			require.NotEqual(t, physical[0].text, descriptor.clauses[0].text)
		}
	}
}

func TestPhysicalReadFactsDoNotRecompilePerExecution(t *testing.T) {
	query := "RETURN person.name LIMIT 10"
	references, complete := compiledClauseReadFootprint(pipelineClause{kind: pipelineClauseReturn, text: query})
	require.True(t, complete)
	require.Equal(t, []string{"person.name"}, references)
	allocations := testing.AllocsPerRun(20, func() {
		references, complete = compiledClauseReadFootprint(pipelineClause{kind: pipelineClauseReturn, text: query})
	})
	require.True(t, complete)
	require.Equal(t, float64(0), allocations)
	require.Equal(t, []string{"person.name"}, references)
}

// Declarations and physical reads consume one compiled operand. The old alias
// reader compiled it on every visit despite the shared statement descriptor.
func TestProjectionOperandFactsDoNotRecompile(t *testing.T) {
	expression := "admission97_person.name"
	expr, alias := parseProjectionExprAlias(expression)
	require.Equal(t, expression, expr)
	require.Equal(t, expression, alias)
	allocations := testing.AllocsPerRun(20, func() {
		expr, alias = parseProjectionExprAlias(expression)
	})
	require.Equal(t, float64(0), allocations)
	require.Equal(t, expression, expr)
	require.Equal(t, expression, alias)
}
