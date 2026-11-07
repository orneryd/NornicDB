package cypher

import (
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// labels(), keys() and properties() are calls whatever separates the function
// name, its argument and its closing paren: whitespace or a comment, as the
// grammar admits between any two tokens. deletedEntityReadsIn reads those gaps
// the statement's way, so a deleted entity's labels, or a deleted
// relationship's keys and properties(), stay an error (#907).
func TestDeletedEntityFunctionReadGapsMatchNeo4j(t *testing.T) {
	for _, testCase := range []struct {
		expression string
		want       []deletedEntityRead
	}{
		// The written gaps are already separated calls.
		{"RETURN labels(n)", []deletedEntityRead{{variable: "n"}}},
		{"RETURN labels ( n )", []deletedEntityRead{{variable: "n"}}},
		{"RETURN keys( r )", []deletedEntityRead{{variable: "r", relationshipOnly: true}}},
		// A comment is one of those gaps.
		{"RETURN labels /* gap */ (n)", []deletedEntityRead{{variable: "n"}}},
		{"RETURN labels(/* gap */ n /* gap */)", []deletedEntityRead{{variable: "n"}}},
		{"RETURN labels( // gap\n n)", []deletedEntityRead{{variable: "n"}}},
		{"RETURN properties /* gap */ ( /* gap */ r /* gap */ )", []deletedEntityRead{{variable: "r", relationshipOnly: true}}},
		{"RETURN size(labels /* gap */ (n))", []deletedEntityRead{{variable: "n"}}},
		// A property read's dot takes the same gap.
		{"RETURN n /* gap */ .p", []deletedEntityRead{{variable: "n"}}},
		// Unchanged distinctions: quoted string text is data, a
		// qualified call's namespace is not a row variable, and a call of
		// another function reads nothing by itself.
		{"RETURN 'labels /* gap */ (n)'", nil},
		{"RETURN labels(' n ')", nil},
		{"RETURN db.labels()", nil},
		{"RETURN size(n)", nil},
		{"RETURN range(1..2)", nil},
	} {
		t.Run(testCase.expression, func(t *testing.T) {
			require.Equal(t, testCase.want, deletedEntityReadsIn(testCase.expression))
		})
	}
}

// The public statement reaches that read: after a DELETE, labels /* gap */ (n)
// and labels(/* gap */ n /* gap */) are EntityNotFound like labels(n), while
// what Neo4j answers without an error — a deleted node's keys() and
// properties(), a relationship's type(), quoted text — is unchanged.
func TestDeletedEntityFunctionReadGapsReachEntityNotFound(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "deleted_read_gaps"))
		ctx := t.Context()
		_, err := exec.Execute(ctx, "CREATE (:G {id: 1, v: 1})-[:GR {w: 1}]->(:G {id: 2})", nil)
		require.NoError(t, err)
		inRolledBackTransaction := func(query string) (*ExecuteResult, error) {
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			defer func() {
				_, err := exec.Execute(ctx, "ROLLBACK", nil)
				require.NoError(t, err)
			}()
			return exec.Execute(ctx, query, nil)
		}

		for _, query := range []string{
			// The control: a written gap already errors.
			"MATCH (n:G {id: 1}) DELETE n RETURN labels(n) AS v",
			"MATCH (n:G {id: 1}) DELETE n RETURN labels /* gap */ (n) AS v",
			"MATCH (n:G {id: 1}) DELETE n RETURN labels(/* gap */ n /* gap */) AS v",
			"MATCH (n:G {id: 1}) DELETE n RETURN labels( // gap\n n) AS v",
			"MATCH (n:G {id: 1}) DELETE n WITH n RETURN size(labels /* gap */ (n)) AS v",
			"MATCH (:G {id: 1})-[r:GR]->() DELETE r RETURN keys /* gap */ (r) AS v",
			"MATCH (:G {id: 1})-[r:GR]->() DELETE r RETURN properties(/* gap */ r /* gap */) AS v",
			"MATCH (n:G {id: 1}) DELETE n RETURN n /* gap */ .v AS v",
		} {
			t.Run(query, func(t *testing.T) {
				_, err := inRolledBackTransaction(query)
				require.Error(t, err)
				code, message := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.ClientError.Statement.EntityNotFound", code, "%s: %v", message, err)
			})
		}

		for _, testCase := range []struct {
			query string
			want  interface{}
		}{
			// A deleted node's keys and properties are empty, not an error.
			{"MATCH (n:G {id: 1}) DELETE n RETURN keys /* gap */ (n) AS v", []interface{}{}},
			{"MATCH (n:G {id: 1}) DELETE n RETURN properties(/* gap */ n /* gap */) AS v", map[string]interface{}{}},
			// A deleted relationship keeps its type.
			{"MATCH (:G {id: 1})-[r:GR]->() DELETE r RETURN type(r) AS v", "GR"},
			// Quoted text is data, not a read.
			{"MATCH (n:G {id: 1}) DELETE n RETURN 'labels /* gap */ (n)' AS v", "labels /* gap */ (n)"},
		} {
			t.Run(testCase.query, func(t *testing.T) {
				result, err := inRolledBackTransaction(testCase.query)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{testCase.want}}, result.Rows)
			})
		}
	})
}
