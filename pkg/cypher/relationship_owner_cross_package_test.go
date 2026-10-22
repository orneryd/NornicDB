package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A text-only routing fast path may decline a query, but it must never route
// away from the real label because a value or comment resembles another label.
func TestRelationshipOwnerCrossPackageRoutingFacts(t *testing.T) {
	for _, query := range []string{
		"MATCH (n:Wanted) RETURN ' :Decoy ' AS x",
		"MATCH (n:Wanted {text: ' :Decoy '}) RETURN n",
		"MATCH (n:Wanted) RETURN n // :Decoy",
		"MATCH (n:Wanted) /* :Decoy */ RETURN n",
	} {
		t.Run(query, func(t *testing.T) {
			router := storage.NewQueryAnalyzer()
			router.SetLabelRouting("Wanted", []string{"wanted"})
			router.SetLabelRouting("Decoy", []string{"decoy"})
			info := router.AnalyzeQuery(query)
			targets := router.RouteQuery(info, []string{"wanted", "decoy"})
			require.Contains(t, targets, "wanted", "query-derived facts cannot discard the real constituent: %#v", info)
			require.NotContains(t, info.Labels, "DECOY", "a literal or comment is not a schema token")
		})
	}
}

// REQUIRE blocks are NornicDB's existing extension, not a Neo4j statement.
// Their pattern literals must retain the same identifier facts as Execute's
// matching grammar; COUNT and NOT EXISTS must not decode them independently.
func TestRelationshipOwnerCrossPackageContractFacts(t *testing.T) {
	quote := func(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }
	for _, tc := range []struct {
		name, relName, relToken, labelName, labelToken string
	}{
		{"plain-control", "R", "R", "End", "End"},
		{"quoted-pipe", "A|B", "`A|B`", "End|One", "`End|One`"},
		{"escaped", "A`B", "`A``B`", "End`One", "`End``One`"},
		{"unicode", "关系类型", "关系类型", "节点", "节点"},
		{"dynamic-looking-literal", "$(R)", "`$(R)`", "$(L)", "`$(L)`"},
		{"closing-brace-literal", "A}B", "`A}B`", "End}One", "`End}One`"},
		{"closing-bracket-literal", "A]B", "`A]B`", "End]One", "`End]One`"},
	} {
		for _, explicit := range []bool{false, true} {
			mode := "autocommit"
			if explicit {
				mode = "explicit"
			}
			for _, matchKeyword := range []string{"", "MATCH "} {
				t.Run(tc.name+"/"+mode+"/"+matchKeyword, func(t *testing.T) {
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "owner_cross_package"))
					ctx := context.Background()
					_, err := exec.Execute(ctx, "CREATE (:Start)-[:"+quote(tc.relName)+"]->(:"+quote(tc.labelName)+")", nil)
					require.NoError(t, err)
					pattern := "(n)-[:" + tc.relToken + "]->(:" + tc.labelToken + ")"
					result, err := exec.Execute(ctx, "MATCH (n:Start) MATCH "+pattern+" RETURN count(*) AS n", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows, "establish that matching owns this exact literal")
					if explicit {
						_, err = exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) }()
					}
					query := "CREATE CONSTRAINT owner_cross_package FOR (n:Start) REQUIRE { COUNT { " + matchKeyword + pattern + " } >= 1\n NOT EXISTS { " + matchKeyword + "(n)-[:" + tc.relToken + "]->(:Absent) } }"
					_, err = exec.Execute(ctx, query, nil)
					require.NoError(t, err, query)
					if explicit {
						_, err = exec.Execute(ctx, "COMMIT", nil)
						require.NoError(t, err)
					}
					_, err = exec.Execute(ctx, "MATCH (n:Start) SET n.ok = 1", nil)
					require.NoError(t, err, "a valid write must still satisfy the compiled contract")
					_, err = exec.Execute(ctx, "MATCH (n:Start) CREATE (n)-[:"+quote(tc.relName)+"]->(:Absent)", nil)
					require.ErrorContains(t, err, "constraint contract", "NOT EXISTS must use the same literal type at write time")
				})
			}
		}
	}
}

func TestRelationshipOwnerCrossPackageContractPropertyKeys(t *testing.T) {
	for _, key := range []string{"plain", "a|b", "a`b", "属性", "$(p)", "a]b"} {
		t.Run(key, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "owner_contract_keys"))
			ctx := context.Background()
			quoted := "`" + strings.ReplaceAll(key, "`", "``") + "`"
			_, err := exec.Execute(ctx, "CREATE (:Start {"+quoted+": 1})", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "CREATE CONSTRAINT owner_key FOR (n:Start) REQUIRE { n."+quoted+" IN [1, 2] }", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "MATCH (n:Start) SET n."+quoted+" = 2", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "MATCH (n:Start) SET n."+quoted+" = 3", nil)
			require.ErrorContains(t, err, "constraint contract")
		})
	}
}
