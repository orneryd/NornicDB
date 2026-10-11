package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Built-in procedures read their evaluated arguments: a variable, an
// expression or a parameter is read like a literal, and the call's own
// parentheses end at its argument list, not at the statement's last ")".
// null for an argument the procedure needs is the procedure's failure
// (ProcedureCallFailed), and a value of another type a TypeError.
func TestProcedureEvaluatedArguments(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "procedure_evaluated_arguments"))
	ctx := context.Background()
	run := func(query string, params map[string]interface{}) *ExecuteResult {
		t.Helper()
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err, query)
		return result
	}
	requireCode := func(query string, params map[string]interface{}, code string) {
		t.Helper()
		_, err := exec.Execute(ctx, query, params)
		require.Error(t, err, query)
		got, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, code, got, query)
	}

	// Fulltext: names and lists from variables and expressions.
	run("WITH 'ft_' + 'doc' AS name, ['Doc'] AS labels CALL db.index.fulltext.createNodeIndex(name, labels, [p IN ['title'] | p]) YIELD name AS created RETURN created", nil)
	run("CREATE (:Doc {title: 'graph databases'}), (:Doc {title: 'graph theory'}), (:Doc {title: 'cooking'})", nil)
	result := run("WITH 'ft_doc' AS idx CALL db.index.fulltext.queryNodes(idx, toLower('GRAPH')) YIELD node WHERE size(node.title) > (1) RETURN count(node) AS c", nil)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
	result = run("WITH {limit: 1} AS options CALL db.index.fulltext.queryNodes('ft_doc', 'graph', options) YIELD node RETURN count(node) AS c", nil)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	run("CREATE ()-[:NOTE {text: 'see the graph'}]->()", nil)
	run("CALL db.index.fulltext.createRelationshipIndex($name, $types, ['text'])", map[string]interface{}{"name": "ft_note", "types": []interface{}{"NOTE"}})
	result = run("CALL db.index.fulltext.queryRelationships('ft_note', 'graph') YIELD relationship RETURN count(relationship) AS c", nil)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	run("WITH 'ft_note' AS name CALL db.index.fulltext.drop(name) YIELD name AS dropped RETURN dropped", nil)

	for _, query := range []string{
		"CALL db.index.fulltext.createNodeIndex(null, ['Doc'], ['title'])",
		"CALL db.index.fulltext.createNodeIndex('ft_x', null, ['title'])",
		"CALL db.index.fulltext.createRelationshipIndex('ft_y', ['NOTE'], null)",
		"CALL db.index.fulltext.queryNodes(null, 'graph')",
		"CALL db.index.fulltext.queryNodes('ft_doc', null)",
		"CALL db.index.fulltext.queryRelationships('ft_note', null)",
		"CALL db.index.fulltext.drop(null)",
		"CALL db.index.vector.drop(null)",
	} {
		requireCode(query, nil, "Neo.ClientError.Procedure.ProcedureCallFailed")
	}
	// No index was named "null" by the calls above.
	result = run("SHOW INDEXES YIELD name WHERE name = 'null' RETURN count(*) AS c", nil)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
	for query, params := range map[string]map[string]interface{}{
		"CALL db.index.fulltext.queryNodes($index, 'graph')":                 {"index": int64(1)},
		"CALL db.index.fulltext.createNodeIndex('ft_z', $labels, ['title'])": {"labels": []interface{}{int64(1)}},
		"CALL db.index.fulltext.queryNodes('ft_doc', 'graph', $options)":     {"options": int64(1)},
	} {
		requireCode(query, params, "Neo.ClientError.Statement.TypeError")
	}
}
