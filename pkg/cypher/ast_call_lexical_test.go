package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestASTCallUsesFunctionSyntax(t *testing.T) {
	for _, query := range []string{
		"CALL apoc /* ')' */ . demo('it\\'s)', {x:[1,2]}) YIELD value, count AS n",
		"CALL apoc // ')'\n . demo('it\\'s)', {x:[1,2]}) YIELD value, count AS n",
	} {
		call := (&ASTBuilder{}).parseCall(query)
		require.Equal(t, "apoc.demo", call.Procedure)
		require.Equal(t, "'it\\'s)', {x:[1,2]}", call.RawArgs)
		require.Equal(t, []string{"value", "count AS n"}, call.Yield)
	}
}
