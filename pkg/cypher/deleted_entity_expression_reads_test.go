package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// deletedExpressionReadsFixture creates the node and relationship the report's
// expressions are taken from, and returns a probe that runs one statement in a
// transaction it rolls back, so every expression reads the same graph.
func deletedExpressionReadsFixture(t *testing.T) func(query string) (*ExecuteResult, error) {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "deleted_expression_reads"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:EIDel {p: 1})-[:EIR {w: 2}]->(:EIDel {p: 3})", nil)
	require.NoError(t, err)
	return func(query string) (*ExecuteResult, error) {
		_, err := exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		defer func() {
			_, err := exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		}()
		return exec.Execute(ctx, query, nil)
	}
}

// After a DELETE, reading a deleted entity's property or its labels is
// EntityNotFound wherever the read is reached: the report's expressions reach
// it through a parenthesized argument (labels((n))), a value another
// expression returned (labels(head([n]))), a parenthesized base ((n).p), a
// dynamic key (n['p']) and a list comprehension over a list holding the entity
// ([x IN [n] | x.p]) — for nodes and relationships, and through an alias and
// collect()/UNWIND as well (#907). The reader is the entity each expression
// receives, not the shape its text has.
func TestDeletedEntityExpressionReadsReachEntityNotFound(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		probe := deletedExpressionReadsFixture(t)
		for _, query := range []string{
			// The report's expressions: a node.
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN labels((n)) AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN labels(head([n])) AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN (n).p AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN n['p'] AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN [x IN [n] | x.p] AS v",
			// The same reads over a relationship.
			"MATCH (:EIDel {p: 1})-[r:EIR]->() DELETE r RETURN (r).w AS v",
			"MATCH (:EIDel {p: 1})-[r:EIR]->() DELETE r RETURN r['w'] AS v",
			// Other values the same reads are reached through.
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN labels([n][0]) AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN labels(coalesce(n)) AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN labels(CASE WHEN true THEN n END) AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN head([n]).p AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN reverse([n])[0].p AS v",
			"MATCH (:EIDel {p: 1})-[r:EIR]->() DELETE r RETURN properties((r)) AS v",
			"MATCH (:EIDel {p: 1})-[r:EIR]->() DELETE r RETURN keys((r)) AS v",
			"MATCH (:EIDel {p: 1})-[r:EIR]->() DELETE r RETURN head([r]).w AS v",
			"MATCH (:EIDel {p: 1})-[r:EIR]->() DELETE r RETURN [x IN [r] | x.w] AS v",
			// A SET value reads the property it assigns.
			"MATCH (n:EIDel {p: 1}) DELETE n SET n.q = n.p RETURN 1 AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n SET n.q = (n).p RETURN 1 AS v",
			// Adding a label to a deleted node reads the labels it lost.
			"MATCH (n:EIDel {p: 1}) DELETE n WITH n, {l: 'Extra'} AS m SET n:$(m.l) RETURN 1 AS v",
			// An alias names the same deleted entity.
			"MATCH (n:EIDel {p: 1}) DELETE n WITH n AS m RETURN (m).p AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n WITH n AS m RETURN labels((m)) AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n WITH n AS m RETURN m['p'] AS v",
			// collect() and UNWIND carry it too.
			"MATCH (n:EIDel {p: 1}) DELETE n WITH collect(n) AS ns RETURN [x IN ns | (x).p] AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n WITH collect(n) AS ns UNWIND ns AS x RETURN labels((x)) AS v",
			// A map the statement built holds the deleted entity: the property
			// read reaches the entity through the map, and reads it the same way
			// it reads it through a variable (m.entity is the deleted node).
			"MATCH (n:EIDel {p: 1}) DELETE n WITH n, {entity: n} AS m RETURN m.entity.p AS v",
			// A null test lets its own operand's read answer; a read the operand
			// computes or nests is not that operand, so it stays the error.
			"MATCH (n:EIDel {p: 1}) DELETE n WITH n, {entity: n} AS m RETURN m.entity.p IS NULL AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN head([n.p]) IS NULL AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN n.p + 1 IS NULL AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN (CASE WHEN true THEN n.p ELSE 1 END) IS NULL AS v",
		} {
			t.Run(query, func(t *testing.T) {
				_, err := probe(query)
				require.Error(t, err)
				code, message := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.ClientError.Statement.EntityNotFound", code, "%s: %v", message, err)
			})
		}
	})
}

// What Neo4j answers for a deleted entity without an error stays as it was:
// the entity itself is empty (its labels and properties, and so n {.*},
// keys(n) and properties(n)), a relationship keeps its type and identity, a
// SET writes nothing, and a property null test is true (#907).
//
// A null test answers for its own single property read only: n.p, (n).p and
// n['p'] ask whether the property is present, and a deleted entity's is not.
// A computed or nested operand (n.p + 1, head([n.p]), the CASE form) reads the
// deleted property as any other expression does, and is the statement error
// TestDeletedEntityExpressionReadsReachEntityNotFound asserts.
func TestDeletedEntityExpressionReadControlsMatchNeo4j(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		probe := deletedExpressionReadsFixture(t)
		for _, testCase := range []struct {
			query string
			want  interface{}
		}{
			{"MATCH (n:EIDel {p: 1}) DELETE n RETURN n {.*} AS v", map[string]interface{}{}},
			{"MATCH (n:EIDel {p: 1}) DELETE n RETURN keys((n)) AS v", []interface{}{}},
			{"MATCH (n:EIDel {p: 1}) DELETE n RETURN properties((n)) AS v", map[string]interface{}{}},
			{"MATCH (n:EIDel {p: 1}) DELETE n RETURN id((n)) IS NOT NULL AS v", true},
			{"MATCH (:EIDel {p: 1})-[r:EIR]->() DELETE r RETURN type((r)) AS v", "EIR"},
			{"MATCH (:EIDel {p: 1})-[r:EIR]->() DELETE r RETURN id((r)) IS NOT NULL AS v", true},
			// A property null test reads no value.
			{"MATCH (n:EIDel {p: 1}) DELETE n RETURN n.p IS NULL AS v", true},
			{"MATCH (n:EIDel {p: 1}) DELETE n RETURN n.p IS NOT NULL AS v", false},
			{"MATCH (n:EIDel {p: 1}) DELETE n RETURN (n).p IS NULL AS v", true},
			{"MATCH (n:EIDel {p: 1}) DELETE n RETURN n['p'] IS NULL AS v", true},
			{"MATCH (:EIDel {p: 1})-[r:EIR]->() DELETE r RETURN r.w IS NULL AS v", true},
			{"MATCH (n:EIDel {p: 1}) DELETE n RETURN (n).p IS NOT NULL AS v", false},
			{"MATCH (n:EIDel {p: 1}) DELETE n RETURN n['p'] IS NOT NULL AS v", false},
			{"MATCH (n:EIDel {p: 1}) DELETE n RETURN CASE WHEN n.p IS NULL THEN 1 ELSE 0 END AS v", int64(1)},
			{"MATCH (:EIDel {p: 1})-[r:EIR]->() DELETE r RETURN CASE WHEN r.w IS NOT NULL THEN 1 ELSE 0 END AS v", int64(0)},
			{"MATCH (n:EIDel {p: 1}) DELETE n WITH n WHERE n.p IS NULL RETURN 1 AS v", int64(1)},
			// SET writes nothing to a deleted entity, statically named or not,
			// and the row carries the deleted entity's view afterwards.
			{"MATCH (n:EIDel {p: 1}) DELETE n SET n.q = 1 RETURN 1 AS v", int64(1)},
			{"MATCH (n:EIDel {p: 1}) DELETE n WITH n, {k: 'q'} AS m SET n[m.k] = 1 RETURN 1 AS v", int64(1)},
		} {
			t.Run(testCase.query, func(t *testing.T) {
				result, err := probe(testCase.query)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{testCase.want}}, result.Rows)
			})
		}
		for _, query := range []string{
			// A CASE branch the statement doesn't take reads nothing.
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN CASE WHEN false THEN n.p ELSE 1 END AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN CASE WHEN false THEN (n).p ELSE 1 END AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN CASE WHEN true THEN 1 ELSE labels((n)) END AS v",
		} {
			t.Run(query, func(t *testing.T) {
				result, err := probe(query)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
			})
		}
		for _, query := range []string{
			// An empty comprehension reads nothing.
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN [x IN [] | x.p] AS v",
			"MATCH (n:EIDel {p: 1}) DELETE n RETURN [x IN [] | labels(x)] AS v",
		} {
			t.Run(query, func(t *testing.T) {
				result, err := probe(query)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{[]interface{}{}}}, result.Rows)
			})
		}
		t.Run("a null test filters instead of failing", func(t *testing.T) {
			// n.p IS NOT NULL doesn't hold for an absent property: the row is
			// dropped, and reading it raised nothing.
			result, err := probe("MATCH (n:EIDel {p: 1}) DELETE n WITH n WHERE n.p IS NOT NULL RETURN 1 AS v")
			require.NoError(t, err)
			require.Empty(t, result.Rows)
		})
		t.Run("an empty quantifier reads nothing", func(t *testing.T) {
			result, err := probe("MATCH (n:EIDel {p: 1}) DELETE n RETURN all(x IN [] WHERE x.p = 1) AS v")
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{true}}, result.Rows)
		})
		t.Run("a SET target is a write, not a read", func(t *testing.T) {
			result, err := probe("MATCH (n:EIDel {p: 1}) DELETE n WITH n SET n.q = 1 RETURN 1 AS v")
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
		})
		t.Run("labels of a relationship is the type error it is without a delete", func(t *testing.T) {
			for _, query := range []string{
				"MATCH (:EIDel {p: 1})-[r:EIR]->() DELETE r RETURN labels((r)) AS v",
				"MATCH (:EIDel {p: 1})-[r:EIR]->() RETURN labels((r)) AS v",
				"MATCH (:EIDel {p: 1})-[r:EIR]->() RETURN labels(r) AS v",
			} {
				_, err := probe(query)
				require.Error(t, err, query)
				code, message := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, "%s: %v", query, err)
				require.Equal(t, "Type mismatch: expected Node but was Relationship", message)
			}
		})
		t.Run("a FOREACH over an empty list reads nothing", func(t *testing.T) {
			result, err := probe("MATCH (n:EIDel {p: 1}) DELETE n FOREACH (x IN [] | SET x.q = 1) RETURN 1 AS v")
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
		})
	})
}

// The guard reads the entity a statement actually has: without a DELETE the
// same expressions read the entity's values, and a statement that deletes
// nothing from an empty match reads nothing at all.
func TestDeletedEntityExpressionReadsNeedADeletedEntity(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		probe := deletedExpressionReadsFixture(t)
		for _, testCase := range []struct {
			query string
			want  interface{}
		}{
			{"MATCH (n:EIDel {p: 1}) RETURN labels((n)) AS v", []interface{}{"EIDel"}},
			{"MATCH (n:EIDel {p: 1}) RETURN (n).p AS v", int64(1)},
			{"MATCH (n:EIDel {p: 1}) RETURN n['p'] AS v", int64(1)},
			{"MATCH (n:EIDel {p: 1}) RETURN [x IN [n] | x.p] AS v", []interface{}{int64(1)}},
			{"MATCH (:EIDel {p: 1})-[r:EIR]->() RETURN (r).w AS v", int64(2)},
			{"MATCH (:EIDel {p: 1})-[r:EIR]->() RETURN properties((r)) AS v", map[string]interface{}{"w": int64(2)}},
			{"MATCH (n:EIDel {p: 1}) WITH n AS m RETURN labels((m)) AS v", []interface{}{"EIDel"}},
		} {
			t.Run(testCase.query, func(t *testing.T) {
				result, err := probe(testCase.query)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{testCase.want}}, result.Rows)
			})
		}
		t.Run("a delete that matched nothing reads nothing", func(t *testing.T) {
			for _, query := range []string{
				"MATCH (n:Missing) DELETE n RETURN labels((n)) AS v",
				"MATCH (n:Missing) DELETE n RETURN (n).p AS v",
			} {
				result, err := probe(query)
				require.NoError(t, err, query)
				require.Empty(t, result.Rows, query)
			}
		})
	})
}
