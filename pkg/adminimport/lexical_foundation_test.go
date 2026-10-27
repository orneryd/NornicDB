package adminimport

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSplitCypherStatementsLexicalFoundation(t *testing.T) {
	first := "CREATE CONSTRAINT c FOR (n:X) REQUIRE n.k IN ['a;b', 'c']"
	second := "CREATE INDEX `a``;b` FOR (n:X) ON (n.k)"
	require.Equal(t, []string{first, second, ""}, splitCypherStatements("# wrapper\n// header\n"+first+";\n"+second+";"))
	require.Equal(t, []string{"RETURN 'line\n# literal;data'", "RETURN 2"}, splitCypherStatements("RETURN 'line\n# literal;data';RETURN 2"))
}
