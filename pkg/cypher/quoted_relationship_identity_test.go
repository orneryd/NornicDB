package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A relationship type is the identity it was created with: a backtick-quoted
// type may hold '|', ':' or a doubled backtick (one backtick), and it stays one
// type, not an alternation. MATCH, COUNT { } and EXISTS { } must read that
// identity, on both parsers.
func TestQuotedRelationshipTypeCountAndExists(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		for _, typeName := range []struct {
			written string // the type as quoted in Cypher
			decoded string // its name, as stored
		}{
			{"A|B", "A|B"},
			{"a b", "a b"},
			{" ", " "},
			{"a:b", "a:b"},
			{"a``b", "a`b"},
		} {
			t.Run(typeName.written, func(t *testing.T) {
				ex := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "quoted_names"))
				ctx := context.Background()
				_, err := ex.Execute(ctx, "CREATE (:ReviewStart)-[:`"+typeName.written+"`]->(:ReviewEnd)", nil)
				require.NoError(t, err)
				// A direct MATCH reads the declaration: the control.
				match, err := ex.Execute(ctx, "MATCH (:ReviewStart)-[r:`"+typeName.written+"`]->(:ReviewEnd) RETURN count(r)", nil)
				require.NoError(t, err)
				require.Equal(t, [][]any{{int64(1)}}, match.Rows)
				stored, err := ex.Execute(ctx, "MATCH (:ReviewStart)-[r:`"+typeName.written+"`]->(:ReviewEnd) RETURN type(r)", nil)
				require.NoError(t, err)
				require.Equal(t, [][]any{{typeName.decoded}}, stored.Rows)
				// COUNT and EXISTS read the same type from the bound node.
				outgoing, err := ex.Execute(ctx, "MATCH (n:ReviewStart) RETURN COUNT { (n)-[:`"+typeName.written+"`]->() } AS c, EXISTS { (n)-[:`"+typeName.written+"`]->() } AS e", nil)
				require.NoError(t, err)
				require.Equal(t, [][]any{{int64(1), true}}, outgoing.Rows)
				// The incoming direction reads it too.
				incoming, err := ex.Execute(ctx, "MATCH (n:ReviewEnd) RETURN COUNT { (n)<-[:`"+typeName.written+"`]-() } AS c, EXISTS { (n)<-[:`"+typeName.written+"`]-() } AS e", nil)
				require.NoError(t, err)
				require.Equal(t, [][]any{{int64(1), true}}, incoming.Rows)
			})
		}
	})
}

// An unquoted | in a relationship type is an alternation, while a quoted one is
// part of the type's name: both readings hold through MATCH and through the
// COUNT / EXISTS shortcuts, in both directions.
func TestQuotedRelationshipAlternationAndIdentity(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		ex := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "quoted_alternation"))
		ctx := context.Background()
		_, err := ex.Execute(ctx, "CREATE (s:ReviewStart), (e:ReviewEnd), (s)-[:A]->(e), (s)-[:B]->(e), "+
			"(s)-[:`A|B`]->(e), (s)-[:`a b`]->(e)", nil)
		require.NoError(t, err)
		for _, tc := range []struct {
			name   string
			inside string // the hop's inside, as written
			want   int64
		}{
			{"ordinary alternation", "[:A|B]", 2},
			{"quoted identity", "[:`A|B`]", 1},
			{"alternation with a quoted member", "[:A|`a b`]", 2},
			{"alternation of three", "[:A|B|`a b`]", 3},
		} {
			t.Run(tc.name, func(t *testing.T) {
				for _, check := range []struct {
					query string
					want  [][]any
				}{
					{"MATCH (:ReviewStart)-[r" + tc.inside[1:] + "->(:ReviewEnd) RETURN count(r)", [][]any{{tc.want}}},
					{"MATCH (:ReviewEnd)<-[r" + tc.inside[1:] + "-(:ReviewStart) RETURN count(r)", [][]any{{tc.want}}},
					{"MATCH (n:ReviewStart) RETURN COUNT { (n)-" + tc.inside + "->() } AS c, EXISTS { (n)-" + tc.inside + "->() } AS e", [][]any{{tc.want, true}}},
					{"MATCH (n:ReviewEnd) RETURN COUNT { (n)<-" + tc.inside + "-() } AS c, EXISTS { (n)<-" + tc.inside + "-() } AS e", [][]any{{tc.want, true}}},
				} {
					result, err := ex.Execute(ctx, check.query, nil)
					require.NoError(t, err, check.query)
					require.Equal(t, check.want, result.Rows, check.query)
				}
			})
		}
	})
}

func TestComputedRelationshipTypeRetainsDecodedIdentity(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "computed_identity"))
		_, err := exec.Execute(t.Context(), "CREATE (s:ComputedStart)-[:`A|B`]->(e), (s)-[:A]->(e), (s)-[:B]->(e)", nil)
		require.NoError(t, err)
		result, err := exec.Execute(t.Context(), "MATCH (n:ComputedStart) RETURN COUNT { (n)-[:$($type)]->() } AS c, EXISTS { (n)-[:$($type)]->() } AS e", map[string]any{"type": "A|B"})
		require.NoError(t, err)
		require.Equal(t, [][]any{{int64(1), true}}, result.Rows)
	})
}

// Neo4j 5.26 allows the deprecated alternate-type separator without a
// relationship binding. Colon conjunctions remain syntax errors on both routes.
func TestDegreeSubqueryRelationshipSeparatorAdmission(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "separator_admission"))
		_, err := exec.Execute(t.Context(), "CREATE (s:SeparatorStart)-[:A]->(e), (s)-[:B]->(e)", nil)
		require.NoError(t, err)
		for _, q := range []string{
			"MATCH (:SeparatorStart)-[:A|:B]->() RETURN count(*) AS c",
			"MATCH (n:SeparatorStart) RETURN COUNT { (n)-[:A|:B]->() } AS c",
		} {
			res, err := exec.Execute(t.Context(), q, nil)
			require.NoError(t, err, q)
			require.Equal(t, [][]any{{int64(2)}}, res.Rows, q)
		}
		for _, q := range []string{
			"MATCH (:SeparatorStart)-[:A:B]->() RETURN count(*) AS c",
			"MATCH (n:SeparatorStart) RETURN COUNT { (n)-[:A:B]->() } AS c",
			"CREATE FULLTEXT INDEX invalid_separator FOR ()-[r:A|:B]-() ON EACH [r.name]",
			"CREATE FULLTEXT INDEX invalid_separator FOR (n:A|:B) ON EACH [n.name]",
			"CREATE FULLTEXT INDEX invalid_separator FOR (n:A B) ON EACH [n.name]",
		} {
			_, err := exec.Execute(t.Context(), q, nil)
			require.Error(t, err, q)
			requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
		}
		indexes, err := exec.Execute(t.Context(), "SHOW INDEXES YIELD name RETURN name", nil)
		require.NoError(t, err)
		require.NotContains(t, indexes.Rows, []any{"invalid_separator"}, "a rejected fulltext declaration must not install an index")
	})
}

func TestSingleSymbolicNameAdmission(t *testing.T) {
	for _, tc := range []struct {
		source string
		value  string
		valid  bool
	}{
		{" ordinary ", "ordinary", true},
		{" café_规则 ", "café_规则", true},
		{"`a``b`", "a`b", true},
		{"`a|b`", "a|b", true},
		{"` `", " ", true},
		{"``", "", true}, // Empty-name admission belongs to the caller.
		{"a b", "", false},
		{"a.b", "", false},
		{"a-b", "", false},
		{"1bad", "", false},
		{"$bad", "", false},
		{"`unterminated", "", false},
		{"", "", false},
	} {
		t.Run(tc.source, func(t *testing.T) {
			value, valid := isOneSymbolicName(tc.source)
			require.Equal(t, tc.valid, valid)
			require.Equal(t, tc.value, value)
		})
	}
}
