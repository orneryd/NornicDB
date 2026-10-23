package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestQualifiedCommentReadersIgnoreCommentContents(t *testing.T) {
	for _, comment := range []struct {
		name   string
		text   string
		syntax string
	}{
		{"block", "/* range('bad') toUpper(1) bogus AND z ] . { ' */", "RETURN /* ] . { ' */ 1 AS v"},
		{"line", "// range('bad') toUpper(1) bogus AND z ] . { '\n", "RETURN // ] . { '\n 1 AS v"},
	} {
		t.Run(comment.name, func(t *testing.T) {
			t.Run("static arguments", func(t *testing.T) {
				count := 0
				err := forEachStaticFunctionArgument(comment.text+" toUpper('ok') "+comment.text, func(_ staticArgumentType, expression string, _ int) error {
					count++
					require.Equal(t, "'ok'", expression)
					return nil
				})
				require.NoError(t, err)
				require.Equal(t, 1, count)
			})
			t.Run("semantic references", func(t *testing.T) {
				require.Equal(t, []string{"x"}, semanticExpressionReferences(comment.text+" x "+comment.text))
			})
			t.Run("free variables", func(t *testing.T) {
				require.Equal(t, []string{"x"}, expressionFreeVariables(comment.text+" x "+comment.text))
			})
			t.Run("syntax preflight", func(t *testing.T) {
				executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "comment_readers"))
				require.NoError(t, executor.validateSyntaxNornic(comment.syntax))
			})
		})
	}
}
