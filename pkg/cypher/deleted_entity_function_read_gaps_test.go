package cypher

import (
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// labels(), keys() and properties() are calls whatever separates the function
// name, its argument and its closing paren: whitespace or a comment, as the
// grammar admits between any two tokens. The read is decided from the entity
// each call receives, so a deleted entity's labels, or a deleted
// relationship's keys and properties(), stay an error however the call is
// written (#907), and what reads no entity value — quoted text, a qualified
// call's namespace, another function's own argument — stays out of it.
func TestDeletedEntityFunctionReadGapsMatchNeo4j(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		probe := deletedExpressionReadsFixture(t)
		for _, query := range []string{
			// The written gaps are already separated calls.
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN labels(n) AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN labels ( n ) AS v",
			// A comment is one of those gaps.
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN labels /* gap */ (n) AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN labels(/* gap */ n /* gap */) AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN labels( // gap\n n) AS v",
			"MATCH (:EIDel {p: 1})-[r:EIR]->() DELETE r RETURN properties /* gap */ ( /* gap */ r /* gap */ ) AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN size(labels /* gap */ (n)) AS v",
			// A property read's dot takes the same gap.
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN n /* gap */ .p AS v",
			// A backtick-quoted name is the same entity and the same call.
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN `labels`(n) AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN labels(`n`) AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN `n`.p AS v",
			"MATCH (:EIDel {p: 1})-[r:EIR]->() DELETE r RETURN keys( r ) AS v",
			"MATCH (:EIDel {p: 1})-[r:EIR]->() DELETE r RETURN keys /* gap */ (r) AS v",
			"MATCH (:EIDel {p: 1})-[r:EIR]->() DELETE r RETURN properties(/* gap */ r /* gap */) AS v",
		} {
			t.Run(query, func(t *testing.T) {
				_, err := probe(query)
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
			{"MATCH (n:EIDel {p: 1}) DELETE n RETURN keys /* gap */ (n) AS v", []interface{}{}},
			{"MATCH (n:EIDel {p: 1}) DELETE n RETURN properties(/* gap */ n /* gap */) AS v", map[string]interface{}{}},
			// Quoted text is data, not a read.
			{"MATCH (n:EIDel {p: 1}) DELETE n RETURN 'labels /* gap */ (n)' AS v", "labels /* gap */ (n)"},
			// Another call's own argument reads nothing by itself.
			{"MATCH (n:EIDel {p: 1}) DELETE n RETURN range(1, 2) AS v", []interface{}{int64(1), int64(2)}},
		} {
			t.Run(testCase.query, func(t *testing.T) {
				result, err := probe(testCase.query)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{testCase.want}}, result.Rows)
			})
		}
		t.Run("a call's own error is not a deleted read", func(t *testing.T) {
			for _, query := range []string{
				"MATCH (n:EIDel {p: 1}) DELETE n RETURN labels(' n ') AS v",
				"MATCH (n:EIDel {p: 1}) DELETE n RETURN size(n) AS v",
			} {
				_, err := probe(query)
				require.Error(t, err, query)
				code, _ := nornicerrors.Neo4jStatus(err)
				require.NotEqual(t, "Neo.ClientError.Statement.EntityNotFound", code, "%s: %v", query, err)
			}
		})
		t.Run("a qualified call's namespace is not a row variable", func(t *testing.T) {
			// db.labels() names no row entity, so the call is unresolved for its
			// own reason whether or not the row holds a deleted node.
			for _, query := range []string{
				"MATCH (n:EIDel {p: 1}) DELETE n RETURN db.labels() AS v",
				"MATCH (n:EIDel {p: 1}) RETURN db.labels() AS v",
			} {
				_, err := probe(query)
				require.Error(t, err, query)
				code, message := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, "%s: %v", query, err)
				require.Equal(t, "unknown function: db.labels", message)
			}
		})
	})
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
