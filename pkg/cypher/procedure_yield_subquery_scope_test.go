package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Expected rows are from Neo4j 5.26.30, including validation on empty input.
func TestProcedureYieldWhereSubqueryScope(t *testing.T) {
	ctx := context.Background()
	for _, populated := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "populated"}[populated], func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "yield_scope"))
			_, err := exec.Execute(ctx, "CREATE FULLTEXT INDEX idx FOR (s:Src) ON EACH [s.text]", nil)
			require.NoError(t, err)
			if populated {
				_, err = exec.Execute(ctx, "CREATE (s:Src {id:'yes', document_id:'d', text:'boiler'})-[:LINK]->(:Doc {id:'d'}), (:Src {id:'no', document_id:'missing', text:'boiler'})", nil)
				require.NoError(t, err)
			}
			for _, tc := range []struct {
				name, yield, predicate, result string
			}{
				{"exists", "node, score", "EXISTS { MATCH (:Doc {id: node.document_id}) }", "node.id"},
				{"count", "node, score", "COUNT { MATCH (:Doc {id: node.document_id}) } > 0", "node.id"},
				{"collect", "node, score", "size(COLLECT { MATCH (d:Doc {id: node.document_id}) RETURN d.id }) > 0", "node.id"},
				{"alias", "node AS n", "EXISTS { MATCH (n)-->() }", "n.id"},
				{"comprehension", "node", "size([(node)-->(d) | d.id]) > 0", "node.id"},
				{"identity", "node", "EXISTS { WITH node AS node MATCH (node)-->() }", "node.id"},
				{"nested", "node", "EXISTS { MATCH (d:Doc {id: node.document_id}) WHERE COUNT { MATCH (node)-->(d) } > 0 }", "node.id"},
			} {
				for _, prefix := range []string{"", "WITH 1 AS input "} {
					t.Run(tc.name+"/"+prefix, func(t *testing.T) {
						query := prefix + "CALL db.index.fulltext.queryNodes('idx', $query) YIELD " + tc.yield + " WHERE " + tc.predicate + " RETURN " + tc.result + " AS id ORDER BY id LIMIT $limit"
						result, err := exec.Execute(ctx, query, map[string]interface{}{"query": "boiler", "limit": int64(10)})
						require.NoError(t, err)
						if populated {
							require.Equal(t, [][]interface{}{{"yes"}}, result.Rows)
						} else {
							require.Empty(t, result.Rows)
						}
					})
				}
			}
			for _, suffix := range []string{"", " YIELD *", " YIELD node RETURN node.id"} {
				_, err := exec.Execute(ctx, "CALL db.index.fulltext.queryNodes('idx', COLLECT { MATCH (:Doc {id: missing.document_id}) RETURN 'boiler' AS q }[0])"+suffix, nil)
				require.Error(t, err)
				require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError")
			}
			_, err = exec.Execute(ctx, "CALL db.index.fulltext.queryNodes('idx', COLLECT { RETURN node.document_id AS q }[0]) YIELD node RETURN node.id", nil)
			require.Error(t, err)
			require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError")
			for _, predicate := range []string{
				"EXISTS { WITH 1 AS node RETURN node }",
				"EXISTS { UNWIND [1] AS node RETURN node }",
				"EXISTS { RETURN 1 AS node }",
				"COUNT { MATCH (:Doc {id: missing.document_id}) } > 0",
				"size(COLLECT { MATCH (d:Doc {id: missing.document_id}) RETURN d.id }) > 0",
				"EXISTS { MATCH (:Doc {id: score}) }",
			} {
				_, err := exec.Execute(ctx, "CALL db.index.fulltext.queryNodes('idx', 'boiler') YIELD node WHERE "+predicate+" RETURN node.id", nil)
				require.Error(t, err, predicate)
				require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError", predicate)
			}
			// Neo4j rejects standalone filtering without a following clause.
			_, err = exec.Execute(ctx, "CALL db.labels() YIELD label WHERE EXISTS { MATCH (x) WHERE label IN labels(x) }", nil)
			require.Error(t, err)
			require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError")
			for _, query := range []string{
				"CALL db.labels() YIELD label WHERE EXISTS { MATCH (x) WHERE label IN labels(x) } RETURN label ORDER BY label",

				"WITH 'Doc' AS wanted CALL db.labels() YIELD label WHERE EXISTS { MATCH (x) WHERE label IN labels(x) AND label = wanted } RETURN label",
			} {
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, query)
				if !populated {
					require.Empty(t, result.Rows)
				} else if query[0:4] == "WITH" {
					require.Equal(t, [][]interface{}{{"Doc"}}, result.Rows)
				} else {
					require.ElementsMatch(t, [][]interface{}{{"Doc"}, {"Src"}}, result.Rows)
				}
			}
		})
	}
}

func TestProcedureYieldWhereEligibilityStates(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "yield_states"))
	_, err := exec.Execute(ctx, "CREATE FULLTEXT INDEX src_text FOR (n:Src) ON EACH [n.text]", nil)
	require.NoError(t, err)
	cases := []struct {
		name, query string
		archived    bool
	}{
		{"combined", `CALL db.index.fulltext.queryNodes('src_text', $query) YIELD node, score
WHERE ((node:Src AND (NOT EXISTS { MATCH (:Doc {id: node.document_id})-[:MEMBER_OF]->(:Collection) } OR EXISTS { MATCH (:Doc {id: node.document_id})-[:MEMBER_OF]->(unarchived_in:Collection) WHERE NOT coalesce(unarchived_in.archived, false) })) OR (NOT node:Src AND (false OR coalesce(node.current, true)))) AND (NOT node:Summary OR (coalesce(node.current, false) AND
      ((node.owner_kind = "collection" AND EXISTS {
        MATCH (summary_collection:Collection {id: node.owner_id})
        WHERE NOT coalesce(summary_collection.archived, false) })
       OR (node.owner_kind = "document" AND EXISTS {
        MATCH (summary_document:Doc {id: node.owner_id})
        MATCH (summary_version:Version {id: summary_document.current_version_id,
                                          document_id: summary_document.id})
        WHERE summary_version.current_source_id IN node.source_ids
          AND (NOT EXISTS { MATCH (summary_document)-[:MEMBER_OF]->(:Collection) } OR EXISTS { MATCH (summary_document)-[:MEMBER_OF]->(unarchived_in:Collection) WHERE NOT coalesce(unarchived_in.archived, false) }) }))))
RETURN node, score ORDER BY score DESC, node.id LIMIT $limit`, false},

		{"unfiltered", `CALL db.index.fulltext.queryNodes('src_text', $query) YIELD node, score
RETURN node.id AS id, score ORDER BY score DESC, node.id LIMIT $limit`, true},

		{"label", `CALL db.index.fulltext.queryNodes('src_text', $query) YIELD node, score
WHERE node:Src
RETURN node.id AS id, score ORDER BY score DESC, node.id LIMIT $limit`, true},

		{"archive", `CALL db.index.fulltext.queryNodes('src_text', $query) YIELD node, score
WHERE (NOT EXISTS { MATCH (:Doc {id: node.document_id})-[:MEMBER_OF]->(:Collection) } OR EXISTS { MATCH (:Doc {id: node.document_id})-[:MEMBER_OF]->(unarchived_in:Collection) WHERE NOT coalesce(unarchived_in.archived, false) })
RETURN node.id AS id, score ORDER BY score DESC, node.id LIMIT $limit`, false},

		{"summary", `CALL db.index.fulltext.queryNodes('src_text', $query) YIELD node, score
WHERE (NOT node:Summary OR (coalesce(node.current, false) AND
      ((node.owner_kind = "collection" AND EXISTS {
        MATCH (summary_collection:Collection {id: node.owner_id})
        WHERE NOT coalesce(summary_collection.archived, false) })
       OR (node.owner_kind = "document" AND EXISTS {
        MATCH (summary_document:Doc {id: node.owner_id})
        MATCH (summary_version:Version {id: summary_document.current_version_id,
                                          document_id: summary_document.id})
        WHERE summary_version.current_source_id IN node.source_ids
          AND (NOT EXISTS { MATCH (summary_document)-[:MEMBER_OF]->(:Collection) } OR EXISTS { MATCH (summary_document)-[:MEMBER_OF]->(unarchived_in:Collection) WHERE NOT coalesce(unarchived_in.archived, false) }) }))))
RETURN node.id AS id, score ORDER BY score DESC, node.id LIMIT $limit`, true},

		{"correlated_exists", `CALL db.index.fulltext.queryNodes('src_text', $query) YIELD node, score
WHERE EXISTS { MATCH (:Doc {id: node.document_id}) }
RETURN node.id AS id, score ORDER BY score DESC, node.id LIMIT $limit`, true},

		{"uncorrelated_exists", `CALL db.index.fulltext.queryNodes('src_text', $query) YIELD node, score
WHERE EXISTS { MATCH (:Doc) }
RETURN node.id AS id, score ORDER BY score DESC, node.id LIMIT $limit`, true},

		{"with_full_predicate", `CALL db.index.fulltext.queryNodes('src_text', $query) YIELD node, score
WITH node, score WHERE ((node:Src AND (NOT EXISTS { MATCH (:Doc {id: node.document_id})-[:MEMBER_OF]->(:Collection) } OR EXISTS { MATCH (:Doc {id: node.document_id})-[:MEMBER_OF]->(unarchived_in:Collection) WHERE NOT coalesce(unarchived_in.archived, false) })) OR (NOT node:Src AND (false OR coalesce(node.current, true)))) AND (NOT node:Summary OR (coalesce(node.current, false) AND
      ((node.owner_kind = "collection" AND EXISTS {
        MATCH (summary_collection:Collection {id: node.owner_id})
        WHERE NOT coalesce(summary_collection.archived, false) })
       OR (node.owner_kind = "document" AND EXISTS {
        MATCH (summary_document:Doc {id: node.owner_id})
        MATCH (summary_version:Version {id: summary_document.current_version_id,
                                          document_id: summary_document.id})
        WHERE summary_version.current_source_id IN node.source_ids
          AND (NOT EXISTS { MATCH (summary_document)-[:MEMBER_OF]->(:Collection) } OR EXISTS { MATCH (summary_document)-[:MEMBER_OF]->(unarchived_in:Collection) WHERE NOT coalesce(unarchived_in.archived, false) }) }))))
RETURN node.id AS id, score ORDER BY score DESC, node.id LIMIT $limit`, false},

		{"match_full_predicate", `MATCH (node:Src) WHERE ((node:Src AND (NOT EXISTS { MATCH (:Doc {id: node.document_id})-[:MEMBER_OF]->(:Collection) } OR EXISTS { MATCH (:Doc {id: node.document_id})-[:MEMBER_OF]->(unarchived_in:Collection) WHERE NOT coalesce(unarchived_in.archived, false) })) OR (NOT node:Src AND (false OR coalesce(node.current, true)))) AND (NOT node:Summary OR (coalesce(node.current, false) AND
      ((node.owner_kind = "collection" AND EXISTS {
        MATCH (summary_collection:Collection {id: node.owner_id})
        WHERE NOT coalesce(summary_collection.archived, false) })
       OR (node.owner_kind = "document" AND EXISTS {
        MATCH (summary_document:Doc {id: node.owner_id})
        MATCH (summary_version:Version {id: summary_document.current_version_id,
                                          document_id: summary_document.id})
        WHERE summary_version.current_source_id IN node.source_ids
          AND (NOT EXISTS { MATCH (summary_document)-[:MEMBER_OF]->(:Collection) } OR EXISTS { MATCH (summary_document)-[:MEMBER_OF]->(unarchived_in:Collection) WHERE NOT coalesce(unarchived_in.archived, false) }) })))) RETURN node.id AS id ORDER BY id`, false},
	}
	for _, state := range []string{"empty", "archived", "active"} {
		if state == "archived" {
			_, err = exec.Execute(ctx, "CREATE (d:GraphNode:Doc {id:'d',current_version_id:'v'}), (:Version {id:'v',document_id:'d',current_source_id:'s'}), (:Src {id:'s',document_id:'d',version_id:'v',text:'boiler invoice'}), (c:Collection {id:'archive',archived:true}), (d)-[:MEMBER_OF]->(c)", nil)
			require.NoError(t, err)
		} else if state == "active" {
			_, err = exec.Execute(ctx, "MATCH (c:Collection {id:'archive'}) SET c.archived=false", nil)
			require.NoError(t, err)
		}
		for _, tc := range cases {
			t.Run(state+"/"+tc.name, func(t *testing.T) {
				result, err := exec.Execute(ctx, tc.query, map[string]interface{}{"query": "boiler", "limit": int64(10)})
				require.NoError(t, err)
				if state == "empty" || (state == "archived" && !tc.archived) {
					require.Empty(t, result.Rows)
					return
				}
				require.Len(t, result.Rows, 1)
				if tc.name == "combined" {
					node, ok := result.Rows[0][0].(*storage.Node)
					require.True(t, ok)
					require.Equal(t, "s", node.Properties["id"])
				} else {
					require.Equal(t, "s", result.Rows[0][0])
				}
				if len(result.Rows[0]) > 1 {
					require.Greater(t, result.Rows[0][1].(float64), float64(0))
				}
			})
		}
	}
}

// Clause declarations use the same protected outer scope for every expression
// subquery and every source of the outer variable, not just procedure outputs.
func TestExpressionSubqueryBindingsKeepOuterScope(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "subquery_bindings"))
	ctx := context.Background()
	for _, keyword := range []string{"EXISTS", "COUNT", "COLLECT"} {
		for _, body := range []string{"WITH 2 AS n RETURN n", "UNWIND [2] AS n RETURN n", "RETURN 2 AS n"} {
			query := "WITH 1 AS n RETURN " + keyword + " { " + body + " } AS v"
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err, query)
			require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError", query)
		}
	}
	for query, want := range map[string]interface{}{
		"WITH 1 AS n RETURN EXISTS { WITH n RETURN n } AS v":                true,
		"WITH 1 AS n RETURN COUNT { WITH n AS n RETURN n } AS v":            int64(1),
		"WITH 1 AS n RETURN COLLECT { WITH DISTINCT n AS n RETURN n } AS v": []interface{}{int64(1)},
		"WITH 1 AS n WITH 2 AS n RETURN n AS v":                             int64(2),
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
}
