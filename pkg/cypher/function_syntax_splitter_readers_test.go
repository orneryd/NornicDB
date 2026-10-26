package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFunctionSyntaxExpressionSplittersUseSharedGroups(t *testing.T) {
	for _, sample := range []struct {
		text string
		want []string
	}{
		{"sum(n.a) + sum(n.b)", []string{"sum(n.a) ", "+", " sum(n.b)"}},
		{"toUpper(')') + 2", []string{"toUpper(')') ", "+", " 2"}},
		{"'a+b' + 2", []string{"'a+b' ", "+", " 2"}},
		{"x /* ) + */ + 2", []string{"x /* ) + */ ", "+", " 2"}},
		{"[1+2][0] - 1", []string{"[1+2][0] ", "-", " 1"}},
		{"-1+2", []string{"-1", "+", "2"}},
		{"1+-2", []string{"1", "+", "-2"}},
	} {
		t.Run("arithmetic/"+sample.text, func(t *testing.T) {
			require.Equal(t, sample.want, splitArithmeticExpression(sample.text))
		})
	}
	for _, sample := range []struct {
		text string
		want []string
	}{
		{"a,b", []string{"a", "b"}},
		{"toUpper(')'), b", []string{"toUpper(')')", " b"}},
		{"'a,b', c", []string{"'a,b'", " c"}},
		{"n.`a,b` DESC, n.name", []string{"n.`a,b` DESC", " n.name"}},
		{"[1,2], {a:1,b:2}, c", []string{"[1,2]", " {a:1,b:2}", " c"}},
		{"a /* , ) */, b", []string{"a /* , ) */", " b"}},
		{"a,,b,", []string{"a", "", "b"}},
	} {
		t.Run("comma/"+sample.text, func(t *testing.T) {
			require.Equal(t, sample.want, splitOutsideParens(sample.text, ','))
		})
	}
}

func TestFunctionSyntaxFinalOrderPublicReaders(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	for _, normalize := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("normalize=%t/explicit=%t", normalize, explicit), func(t *testing.T) {
				config.SetCypherQueryNormalizationEnabled(normalize)
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "order_readers"))
				ctx := context.Background()
				_, err := exec.Execute(ctx, "CREATE (:Lex {`a,b`:1}), (:Lex {`a,b`:2})", nil)
				require.NoError(t, err)
				if explicit {
					_, err = exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
				}
				result, err := exec.Execute(ctx, "MATCH (n:Lex) RETURN n.`a,b` AS v ORDER BY n.`a,b` DESC", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(2)}, {int64(1)}}, result.Rows)
			})
		}
	}
}
