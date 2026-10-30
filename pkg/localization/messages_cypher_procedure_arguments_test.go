package localization

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The procedure argument readers' two messages: a null the procedure needs,
// and a value of another type.
func TestCypherProcedureArgumentMessages(t *testing.T) {
	null := CypherProceduresArgumentNull("db.index.vector.embed", "text")
	require.Equal(t, MessageCypherProceduresArgumentNull, null.ID)
	require.Equal(t, "db.index.vector.embed: argument text is null", null.Fallback)
	require.Equal(t, map[string]any{"Procedure": "db.index.vector.embed", "Argument": "text"}, null.Data)

	typed := CypherProceduresArgumentType("apoc.cypher.run", "params", "MAP", "STRING")
	require.Equal(t, MessageCypherProceduresArgumentType, typed.ID)
	require.Equal(t, "apoc.cypher.run: argument params must be MAP, not STRING", typed.Fallback)
	require.Equal(t, map[string]any{"Procedure": "apoc.cypher.run", "Argument": "params", "Expected": "MAP", "Actual": "STRING"}, typed.Data)
}
