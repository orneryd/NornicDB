package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRelationshipOwnerRemainingConsumers(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "autocommit", true: "explicit"}[explicit], func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "review_remaining"))
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (a:ReviewA)-[:`A|B`]->(), (a)-[:`A``B`]->(), (a)-[:A]->(), (a)-[:B]->()", nil)
			require.NoError(t, err)
			if explicit {
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			for _, query := range []string{
				"MATCH (a:ReviewA) RETURN COUNT { (a)-[:`A|B`]->() } AS c",
				"MATCH (a:ReviewA) RETURN COUNT { (a)-[:`A``B`]->() } AS c",
				"MATCH (a:ReviewA) RETURN COUNT { (a)-[:`A|B`]->(b WHERE true) } AS c",
				"MATCH (a:ReviewA)-[:`A|B`]->() RETURN count(*) AS c",
			} {
				result, err := exec.Execute(ctx, query, nil)
				if err != nil {
					t.Errorf("%s: %v", query, err)
					continue
				}
				t.Logf("%s => %v", query, result.Rows)
				assert.Equal(t, [][]interface{}{{int64(1)}}, result.Rows, query)
			}
			if explicit {
				_, err = exec.Execute(ctx, "ROLLBACK", nil)
				require.NoError(t, err)
			}
		})
	}
}

func TestRelationshipOwnerLookupQuotedVariable(t *testing.T) {
	for _, variable := range []string{"`r r`", "rélation"} {
		t.Run(variable, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "review_lookup"))
			_, err := exec.Execute(context.Background(), "DROP INDEX "+storage.DefaultRelationshipLookupIndexName+" IF EXISTS", nil)
			require.NoError(t, err)
			query := "CREATE LOOKUP INDEX review_lookup FOR ()-[" + variable + "]-() ON EACH type(" + variable + ")"
			_, err = exec.Execute(context.Background(), query, nil)
			require.NoError(t, err, query)
		})
	}
}

func TestRelationshipOwnerDynamicRangeParameters(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "review_range_params"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:ReviewRange)-[:R]->()-[:S]->()", nil)
	require.NoError(t, err)
	for _, value := range []interface{}{"", "NOPE"} {
		for _, prefix := range []string{"", "PROFILE "} {
			query := prefix + "MATCH (:ReviewRange)-[r:$($t)*1..2]->() RETURN count(r) AS c"
			result, err := exec.Execute(ctx, query, map[string]interface{}{"t": value})
			if err != nil {
				t.Errorf("t=%#v %s => %v", value, query, err)
				continue
			}
			assert.Equal(t, [][]interface{}{{int64(2)}}, result.Rows, "t=%#v %s", value, query)
		}
	}
}

func TestRelationshipOwnerArrowNormalizer(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "review_arrow"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:SlashA)-[:$('R\\\\')]->(:SlashB)-[:S]->(:SlashC)", nil)
	require.NoError(t, err)
	for _, explicit := range []bool{false, true} {
		if explicit {
			_, err = exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
		}
		for _, query := range []string{
			"MATCH (:SlashA)-[:$('R\\\\')]->()-->(:SlashMissing) RETURN count(*) AS c",
			"MATCH (:SlashA)-[:$('R\\\\')]->()-[:S]->(:SlashMissing) RETURN count(*) AS c",
		} {
			result, err := exec.Execute(ctx, query, nil)
			if err != nil {
				t.Errorf("explicit=%v %s => %v", explicit, query, err)
				continue
			}
			assert.Equal(t, [][]interface{}{{int64(0)}}, result.Rows, "explicit=%v %s", explicit, query)
		}
		if explicit {
			_, err = exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		}
	}
}

func TestRelationshipOwnerKeywordVariable(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "autocommit", true: "explicit"}[explicit], func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "keyword_variable"))
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE ()-[:KeywordType]->()", nil)
			require.NoError(t, err)
			if explicit {
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			for _, variable := range []string{"is", "`is`", "`where`", "`all`"} {
				query := "MATCH ()-[" + variable + "]->() RETURN " + symbolicNameValue(variable)
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, query)
				require.Len(t, result.Rows, 1, query)
				require.Len(t, result.Rows[0], 1)
				edge, ok := result.Rows[0][0].(*storage.Edge)
				require.True(t, ok, "query=%s result=%#v", query, result.Rows)
				require.Equal(t, "KeywordType", edge.Type)
			}
			if explicit {
				_, err = exec.Execute(ctx, "ROLLBACK", nil)
				require.NoError(t, err)
			}
		})
	}
}

func TestRelationshipOwnerISKeywordType(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "keyword_type"))
		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (:KeywordIS)-[:IS]->()", nil)
		require.NoError(t, err)
		for _, declaration := range []string{"IS IS", "is IS", "IS IS*1..1"} {
			query := "MATCH (:KeywordIS)-[" + declaration + "]->() RETURN count(*) AS c"
			got := relationshipPatternOracleRun(ctx, exec, query, explicit)
			require.Equal(t, []string{"c", "1"}, got, "explicit=%v %s", explicit, query)
		}
	}
}

func TestRelationshipOwnerStretchExpressionHeaders(t *testing.T) {
	var reads stretchTokens
	addStretchExpressionReads("COUNT { ()-[:R {k: n.value}]->() } + size([(n) WHERE n:L | n])", &reads)
	require.True(t, reads.anyNode)
	require.True(t, reads.anyRelationship)
	require.NotContains(t, reads.labels, "R", "a relationship type is never a node-label expression")
	require.Contains(t, reads.labels, "L")
	require.Contains(t, reads.keys, "value", "property expressions remain visible to dependency analysis")
}

func TestRelationshipOwnerFulltextTypeNames(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, tc := range []struct {
			types string
			names []interface{}
		}{
			{"`A|B`", []interface{}{"A|B"}},
			{"`A``B`", []interface{}{"A`B"}},
			{"`关系`", []interface{}{"关系"}},
			{"`A|B`|`C:D`", []interface{}{"A|B", "C:D"}},
		} {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "fulltext_owner"))
			ctx := context.Background()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			_, err := exec.Execute(ctx, "CREATE FULLTEXT INDEX owner_ft FOR ()-[`r r`:"+tc.types+"]-() ON EACH [`r r`.text]", nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, "SHOW INDEXES YIELD name, labelsOrTypes WHERE name = 'owner_ft' RETURN labelsOrTypes", nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			require.Len(t, result.Rows[0], 1)
			require.Equal(t, cypherShellPlainValue(tc.names), cypherShellPlainValue(result.Rows[0][0]))
			if explicit {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		}
	}
}

func TestRelationshipOwnerMalformedFrameDiagnostics(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "malformed_frame"))
	for _, query := range []string{"MATCH (n)-[r]]->(m) RETURN n", "MATCH (n))-[r]->(m) RETURN n", "MATCH (n {x: 1}})-[r]->(m) RETURN n"} {
		for repeat := 0; repeat < 2; repeat++ {
			_, err := exec.Execute(context.Background(), query, nil)
			require.ErrorContains(t, err, "syntax error", query)
		}
	}
}

func TestRelationshipOwnerExpressionListBoundary(t *testing.T) {
	for _, query := range []string{
		"CREATE FULLTEXT INDEX ix FOR ()-[in:A|B]-() ON EACH [in.text]",
		"MATCH ()-[r:$([x IN ['R'] | x])]->() RETURN r",
	} {
		require.NoError(t, validateListComprehensionVariables(query), query)
	}
	for _, query := range []string{
		"MATCH ()-[in:A|B {xs: [n.x IN [1] | n.x]}]->() RETURN in",
		"MATCH ()-[r:$([n.x IN ['R'] | n.x])]->() RETURN r",
		"RETURN (1) - [n.x IN [1] | n.x] - (2)",
		"RETURN -[n.x IN [1] | n.x]",
	} {
		require.ErrorContains(t, validateListComprehensionVariables(query), "iterator must be a variable", query)
	}
}

func TestRelationshipOwnerGroupedUnionSyntax(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "owner_union_syntax"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (a:OwnerUnionA:OwnerUnionB)-[:R]->(), (a)-[:S]->()", nil)
	require.NoError(t, err)
	for _, types := range []string{"R|S", "(R|S)", "((R|S))"} {
		result, err := exec.Execute(ctx, "MATCH (a:OwnerUnionA:OwnerUnionB)-[:"+types+"]->() RETURN count(*) AS n", nil)
		require.NoError(t, err, types)
		require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows, types)
	}
	for _, types := range []string{"R&S", "!R", "%"} {
		_, err := exec.Execute(ctx, "MATCH (a:OwnerUnionA:OwnerUnionB)-[:"+types+"]->() RETURN count(*) AS n", nil)
		require.Error(t, err, types)
	}
}
