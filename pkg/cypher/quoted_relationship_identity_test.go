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
	forEachParser(t, func(t *testing.T) {
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
	forEachParser(t, func(t *testing.T) {
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
