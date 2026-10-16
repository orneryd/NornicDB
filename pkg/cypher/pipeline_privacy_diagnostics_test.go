package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPipelineDiagnosticStructuralIdentity(t *testing.T) {
	shapes := []string{
		"MATCH (n:Person) WHERE n.email = 'private-literal' RETURN n.email AS contact",
		"MATCH (n:Person) WHERE n.phone = 'private-literal' RETURN n.phone AS contact",
		"MATCH (n:Person) WHERE n.email = 'private-literal' RETURN n.email AS address",
		"MATCH (person:Person) WHERE person.email = 'private-literal' RETURN person.email AS contact",
		"UNWIND $items AS item RETURN item.email AS contact",
		"MATCH ()-[edge:KNOWS]->() RETURN edge.email AS contact",
	}
	seen := map[string]bool{}
	for _, query := range shapes {
		redacted := redactDiagnosticQuery(query)
		require.NotContains(t, redacted, "private-literal")
		hash := StatementShapeHash(redacted)
		require.False(t, seen[hash], "diagnostic collapsed shape: %s", redacted)
		seen[hash] = true
		require.True(t, strings.Contains(redacted, ".email") || strings.Contains(redacted, ".phone"), "property names must survive without grammar parsing: %s", redacted)
	}
}

func TestPipelineDiagnosticLexicalSecrecy(t *testing.T) {
	for _, query := range []string{
		"MATCH (n:Person) RETURN n.email /* private-comment */ AS contact // private-comment",
		"RETURN 'private-literal // still literal' AS value",
		"RETURN \"private-literal /* still literal */\" AS value",
		"MATCH (n {pw: privateWord.Secret}) RETURN n",
		"MATCH (n {pw: `privateWord`}) RETURN n",
		"RETURN ['private-literal', 123456789, 0xabcdef] AS value",
		"RETURN 'private-literal",
		"RETURN (] // private-comment",
		"RETURN 1 /* private-comment",
		strings.Repeat("privateWord", 600),
	} {
		for _, redacted := range []string{redactDiagnosticQuery(query)} {
			for _, secret := range []string{"private-literal", "private-comment", "privateWord", "Secret", "123456789", "abcdef"} {
				require.NotContains(t, redacted, secret, "query: %s", query)
			}
		}
	}
	public := RedactLiterals("MATCH (n:Person) WHERE n.email = 'private-literal' /* private-comment */ RETURN n // private-comment")
	require.NotContains(t, public, "private-literal")
	require.NotContains(t, public, "private-comment")
	a := redactDiagnosticQuery("MATCH (n:Person) RETURN n.email = 'one' AS result // comment")
	b := redactDiagnosticQuery("MATCH (n:Person) RETURN n.email = 'two' AS result /* other */")
	require.Equal(t, StatementShapeHash(a), StatementShapeHash(b))
}
