package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCallSubqueryWriteAndIdentifierChecks(t *testing.T) {
	require.True(t, callSubqueryQueryIsWrite("MATCH (n) CREATE (m)"))
	require.True(t, callSubqueryQueryIsWrite("MATCH (n) SET n.x = 1"))
	require.False(t, callSubqueryQueryIsWrite("MATCH (n) RETURN n"))

	require.True(t, isSimpleIdentifier("name_1"))
	require.True(t, isSimpleIdentifier("`quoted`"))
	require.False(t, isSimpleIdentifier("1bad"))
	require.False(t, isSimpleIdentifier("bad-char"))
}
