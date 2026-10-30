package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Regression test for #1109: a pattern comprehension whose WHERE uses IN was
// rejected at validation with "pattern expressions are only valid as
// predicates in WHERE" because the comprehension was misclassified as a list
// comprehension (it contained " IN ").
func TestPatternComprehensionWithInInWhere(t *testing.T) {
	exec, ctx := newUnitExecutor(t)

	_, err := exec.Execute(ctx, `
		CREATE (c:Company {id: 1})<-[:COMPANY]-(:Assignment {role: 'owner'}),
		       (c)<-[:COMPANY]-(:Assignment {role: 'accountant'})
	`, nil)
	require.NoError(t, err)

	// IN with a list literal.
	result, err := exec.Execute(ctx, `
		MATCH (c:Company {id: 1})
		RETURN [(c)<-[:COMPANY]-(a:Assignment) WHERE a.role IN ['owner', 'spouse'] | a.role]
	`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, []interface{}{[]interface{}{"owner"}}, result.Rows[0])

	// IN with a parameter list.
	result, err = exec.Execute(ctx, `
		MATCH (c:Company {id: 1})
		RETURN [(c)<-[:COMPANY]-(a:Assignment) WHERE a.role IN $roles | a.role]
	`, map[string]interface{}{"roles": []interface{}{"owner", "spouse"}})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, []interface{}{[]interface{}{"owner"}}, result.Rows[0])
}

func TestPatternComprehensionWithNotInWhere(t *testing.T) {
	exec, ctx := newUnitExecutor(t)

	_, err := exec.Execute(ctx, `
		CREATE (c:Company {id: 1})<-[:COMPANY]-(:Assignment {role: 'owner'}),
		       (c)<-[:COMPANY]-(:Assignment {role: 'accountant'})
	`, nil)
	require.NoError(t, err)

	// NOT x IN [...] inside the WHERE stays a pattern comprehension.
	result, err := exec.Execute(ctx, `
		MATCH (c:Company {id: 1})
		RETURN [(c)<-[:COMPANY]-(a:Assignment) WHERE NOT a.role IN ['spouse'] | a.role]
	`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, []interface{}{[]interface{}{"owner", "accountant"}}, result.Rows[0])
}

func TestNestedListComprehensionInsidePatternComprehensionProjection(t *testing.T) {
	exec, ctx := newUnitExecutor(t)

	_, err := exec.Execute(ctx, `
		CREATE (c:Company {id: 1})<-[:COMPANY]-(:Assignment {role: 'owner'})
	`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
		MATCH (c:Company {id: 1})
		RETURN [(c)<-[:COMPANY]-(a) | [r IN [a.role] WHERE r IN ['owner'] | r]]
	`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, []interface{}{[]interface{}{[]interface{}{"owner"}}}, result.Rows[0])
}
