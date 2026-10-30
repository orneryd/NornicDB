package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuantifierNumericPredicatesRejectStaticNonNumericLists(t *testing.T) {
	exec, _ := newTestExecutor(t)
	for _, function := range []string{"all", "any", "none", "single"} {
		for _, list := range []string{"['Clara']", "[false, true]", "['Clara', 'Bob', 'Dave', 'Alice']"} {
			query := "RETURN " + function + "(x IN " + list + " WHERE x % 2 = 0) AS result"
			_, err := exec.Execute(context.Background(), query, nil)
			require.Error(t, err, query)
			semantic, ok := err.(*SemanticError)
			require.True(t, ok, "%s: %T", query, err)
			require.Equal(t, "InvalidArgumentType", semantic.Detail)
		}
	}
}

func TestQuantifierNumericPredicatesAcceptStaticNumericLists(t *testing.T) {
	exec, _ := newTestExecutor(t)
	result, err := exec.Execute(context.Background(), "RETURN none(x IN [1, 3, 5] WHERE x % 2 = 0) AS result", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{true}}, result.Rows)
}

func TestQuantifierValidationIgnoresTextAndComments(t *testing.T) {
	require.NoError(t, validateStaticQuantifierTypes("RETURN any(x IN ['a'] WHERE x /* gap */ IS NOT NULL) AS result"))
	require.NoError(t, validateStaticQuantifierTypes("RETURN any(x IN ['a'] WHERE x = 'x * 2') AS result"))
	for _, query := range []string{
		"RETURN any /* gap */ (x IN ['a'] WHERE x * 2 > 0) AS result",
		"RETURN any(x IN /* gap */ ['a'] WHERE x * 2 > 0) AS result",
		"RETURN any(x /* gap */ IN ['a'] WHERE x * 2 > 0) AS result",
		"RETURN any(x IN ['a'] /* gap */ WHERE x * 2 > 0) AS result",
		"RETURN any(x IN ['/* gap */'] WHERE x * 2 > 0) AS result",
		"RETURN any(x IN ['a'] WHERE x /* gap */ * 2 > 0) AS result",
		"RETURN any(x IN ['a'] WHERE 2 * /* gap */ x > 0) AS result",
		"RETURN any(x /* IN */ IN ['a'] WHERE x * 2 > 0) AS result",
		"RETURN any(x IN ['a'] /* WHERE */ WHERE x * 2 > 0) AS result",
		"RETURN any(x IN ['a'] /* ) */ WHERE x * 2 > 0) AS result",
		"RETURN any(x IN ['a'] WHERE x /* ) */ * 2 > 0) AS result",
	} {
		require.ErrorContains(t, validateStaticQuantifierTypes(query), "numeric operator", query)
	}
	exec, _ := newTestExecutor(t)
	_, err := exec.Execute(context.Background(), "RETURN any /* gap */ (x IN ['a'] WHERE x * 2 > 0) AS result", nil)
	require.Error(t, err)
	semantic, ok := err.(*SemanticError)
	require.True(t, ok, "%T: %v", err, err)
	require.Equal(t, "InvalidArgumentType", semantic.Detail)
	_, err = exec.Execute(context.Background(), "RETURN any(x IN /* gap */ ['a'] WHERE x * 2 > 0) AS result", nil)
	require.Error(t, err)
	semantic, ok = err.(*SemanticError)
	require.True(t, ok, "%T: %v", err, err)
	require.Equal(t, "InvalidArgumentType", semantic.Detail)
	_, err = exec.Execute(context.Background(), "RETURN any(x /* gap */ IN ['a'] WHERE x * 2 > 0) AS result", nil)
	require.Error(t, err)
	semantic, ok = err.(*SemanticError)
	require.True(t, ok, "%T: %v", err, err)
	require.Equal(t, "InvalidArgumentType", semantic.Detail)
	result, err := exec.Execute(context.Background(), "RETURN any(x IN ['a'] WHERE x /* gap */ IS NOT NULL) AS result", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{true}}, result.Rows)
	for _, query := range []string{
		"RETURN 'none(x IN [false] WHERE x % 2 = 0)' AS text",
		"RETURN 1 AS value // none(x IN [false] WHERE x % 2 = 0)",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err, query)
	}
}

func BenchmarkStaticQuantifierValidation(b *testing.B) {
	for _, tc := range []struct {
		name  string
		query string
	}{
		{"plain", "RETURN any(x IN ['a'] WHERE x IS NOT NULL) AS result"},
		{"comment-gap", "RETURN any(x IN ['a'] WHERE x /* gap */ IS NOT NULL) AS result"},
		{"list-comment-gap", "RETURN any(x IN /* gap */ [1] WHERE x > 0) AS result"},
		{"variable-comment-gap", "RETURN any(x /* gap */ IN [1] WHERE x > 0) AS result"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				if err := validateStaticQuantifierTypes(tc.query); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
