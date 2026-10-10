package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Every spelling Neo4j 5.26 takes for a uniqueness or key requirement
// (IS [NODE | RELATIONSHIP | REL] UNIQUE | KEY, on one property or a
// parenthesized list) creates the constraint Neo4j creates (recorded on
// Neo4j 5.26 Enterprise); an entity that isn't the pattern's is a
// SyntaxError. On main all but three were "invalid CREATE CONSTRAINT
// syntax".
func TestConstraintRequirementSpellings(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "constraint_requirement"))
	ctx := context.Background()
	for statement, wantType := range map[string]string{
		"CREATE CONSTRAINT zq1 FOR (n:Zq) REQUIRE n.k IS NODE KEY":                   "NODE_KEY",
		"CREATE CONSTRAINT zq2 FOR ()-[r:ZqR]-() REQUIRE r.k IS RELATIONSHIP KEY":    "RELATIONSHIP_KEY",
		"CREATE CONSTRAINT zq3 FOR (n:Zq2) REQUIRE (n.a, n.b) IS NODE KEY":           "NODE_KEY",
		"CREATE CONSTRAINT zq4 FOR (n:Zq3) REQUIRE n.a IS KEY":                       "NODE_KEY",
		"CREATE CONSTRAINT zq5 FOR ()-[r:ZqS]-() REQUIRE r.a IS KEY":                 "RELATIONSHIP_KEY",
		"CREATE CONSTRAINT zq8 FOR ()-[r:ZqU]-() REQUIRE r.a IS RELATIONSHIP UNIQUE": "RELATIONSHIP_UNIQUENESS",
		"CREATE CONSTRAINT zq9 FOR (n:Zq5) REQUIRE n.a IS NODE UNIQUE":               "UNIQUENESS",
		"CREATE CONSTRAINT zq10 FOR ()-[r:ZqV]-() REQUIRE r.a IS REL UNIQUE":         "RELATIONSHIP_UNIQUENESS",
		"CREATE CONSTRAINT zq11 FOR ()-[r:ZqW]-() REQUIRE (r.a) IS REL KEY":          "RELATIONSHIP_KEY",
		"CREATE CONSTRAINT zq12 FOR (n:Zq6) REQUIRE n.a IS UNIQUE":                   "UNIQUENESS",
		"CREATE CONSTRAINT zq13 FOR (n:Zq7) REQUIRE n.a IS NODE KEY OPTIONS {}":      "NODE_KEY",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
		name := strings.Fields(statement)[2]
		result, err := exec.Execute(ctx, "SHOW CONSTRAINTS YIELD name, type WHERE name = $name RETURN type", map[string]interface{}{"name": name})
		require.NoError(t, err, statement)
		require.Equal(t, [][]interface{}{{wantType}}, result.Rows, statement)
	}
	for _, statement := range []string{
		"CREATE CONSTRAINT zq6 FOR (n:Zq4) REQUIRE n.a IS REL KEY",
		"CREATE CONSTRAINT zq7 FOR ()-[r:ZqT]-() REQUIRE r.a IS NODE KEY",
		"CREATE CONSTRAINT zq14 FOR (n:Zq8) REQUIRE n.a IS RELATIONSHIP UNIQUE",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.Error(t, err, statement)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}

	// Statements that aren't a FOR … REQUIRE uniqueness or key requirement
	// are left as they are.
	for _, statement := range []string{
		"CREATE CONSTRAINT x FOR (n:L) REQUIRE n.a IS NOT NULL",
		"CREATE CONSTRAINT x FOR (n:L) REQUIRE n.a IS :: INTEGER",
		"CREATE INDEX x FOR (n:L) ON (n.a)",
		"CREATE CONSTRAINT x FOR n:L REQUIRE n.a IS UNIQUE",
		"CREATE CONSTRAINT x FOR [r:R] REQUIRE r.a IS UNIQUE",
	} {
		rewritten, err := exec.canonicalConstraintRequirement(statement)
		require.NoError(t, err, statement)
		require.Equal(t, statement, rewritten, statement)
	}
}
