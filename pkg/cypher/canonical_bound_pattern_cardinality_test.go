package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Author: #1640 lane (Codex). Pattern operands keep the admitted identity and
// grammar cardinality. Group predicates read one iteration; their projected
// bindings and variable-length relationships are lists, including on no rows.
func TestCanonicalBoundPatternCardinality(t *testing.T) {
	for _, sample := range []struct{ name, query string }{
		{"dynamic-length", "WITH 'RoleR' AS t MATCH (s:RoleN {id:1})-[rs:$(t)*1..2]->(e) RETURN count(*) AS c"},
		{"length-list-use", "MATCH (s:RoleN {id:1})-[rs:RoleR*1..2]->(e) WHERE all(r IN rs WHERE type(r) = 'RoleR') RETURN count(*) AS c"},
		{"group-list-use", "MATCH (s:RoleN {id:1})((x)-[rs:RoleR]->(y))+(e) WHERE all(r IN rs WHERE type(r) = 'RoleR') RETURN count(*) AS c"},
		{"group-iteration-use", "MATCH (s:RoleN {id:1})((x)-[rs:RoleR]->(y) WHERE type(rs) = 'RoleR')+(e) RETURN count(*) AS c"},
		{"group-nested-reuse", "MATCH (s:RoleN {id:1})((x)-[rs:RoleR WHERE EXISTS { MATCH (a)-[rs]->(b) }]->(y))+(e) RETURN count(*) AS c"},
		{"quantified-relationship", "MATCH (s:RoleN {id:1})-[rs:RoleR]->+(e) WHERE all(r IN rs WHERE type(r) = 'RoleR') RETURN count(*) AS c"},
	} {
		for _, prefix := range []string{"", "UNWIND [] AS __role_input ", "UNWIND [0] AS __role_input "} {
			for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
				t.Run(sample.name+"/"+prefix+"/"+mode, func(t *testing.T) {
					executor, ctx := newUnitExecutor(t)
					_, err := executor.Execute(ctx, "CREATE (:RoleN {id:1})-[:RoleR]->(:RoleN {id:2})-[:RoleR]->(:RoleN {id:3})", nil)
					require.NoError(t, err)
					query := mode + prefix + sample.query
					result, err := executor.Execute(ctx, query, nil)
					require.NoError(t, err, query)
					require.Equal(t, []string{"c"}, result.Columns, query)
					if mode == "EXPLAIN " {
						require.Empty(t, result.Rows, query)
						return
					}
					want := int64(2)
					if strings.Contains(prefix, "[]") {
						want = 0
					}
					require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
				})
			}
		}
	}
}
