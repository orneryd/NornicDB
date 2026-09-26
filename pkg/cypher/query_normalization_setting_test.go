package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestQueryNormalizationSettingKeepsClassification: permission and routing
// decisions (read / write / schema / admin, cacheability, write detection)
// read the canonical text whatever NORNICDB_CYPHER_QUERY_NORMALIZATION says,
// so a comment or unusual spacing never changes them.
func TestQueryNormalizationSettingKeepsClassification(t *testing.T) {
	analyzer := NewQueryAnalyzer(100)
	for _, statement := range []string{
		"/* c */ CREATE (n:X)",
		"MATCH (n)\n\tSET\tn.x = 1",
		"// note\nMERGE (n:K {id: 1})",
		"MATCH (n)\nREMOVE\tn.x",
		"MATCH (n) /* x */ DETACH\n DELETE n",
		"MATCH (n) // delete later\nRETURN n",
		"MATCH (n) RETURN n /* CREATE */",
		"/* c */ CREATE INDEX idx_x FOR (n:X) ON (n.p)",
		"// c\nSHOW TRANSACTIONS",
		"MATCH (n)\nRETURN rand() AS r",
	} {
		canonical, _ := canonicalizeQueryText(statement)
		want := QueryPermissionRequirements(canonical)
		wantWrite := looksLikeWriteQuery(canonical)
		wantCacheable := isCacheableReadQuery(canonical)
		wantInfo := *analyzer.Analyze(canonical)
		for _, enabled := range []bool{true, false} {
			restore := config.WithCypherQueryNormalizationDisabled()
			config.SetCypherQueryNormalizationEnabled(enabled)
			analyzer.ClearCache()
			require.Equal(t, want, QueryPermissionRequirements(statement), "%q enabled=%v", statement, enabled)
			require.Equal(t, wantWrite, looksLikeWriteQuery(statement), "%q enabled=%v", statement, enabled)
			require.Equal(t, wantCacheable, isCacheableReadQuery(statement), "%q enabled=%v", statement, enabled)
			info := analyzer.Analyze(statement)
			require.Equal(t, wantInfo.IsWriteQuery, info.IsWriteQuery, "%q enabled=%v", statement, enabled)
			require.Equal(t, wantInfo.IsReadOnly, info.IsReadOnly, "%q enabled=%v", statement, enabled)
			restore()
		}
	}
	require.True(t, QueryPermissionRequirements("/* c */ CREATE (n:X)").Write)
	require.False(t, QueryPermissionRequirements("MATCH (n) // delete later\nRETURN n").Write)
	require.True(t, looksLikeWriteQuery("MATCH (n)\n\tSET\tn.x = 1"))

	canonical := "MATCH (n:Person {name: $name}) WHERE n.age > 30 RETURN n.name AS name"
	require.Zero(t, testing.AllocsPerRun(100, func() { _ = classificationText(canonical) }))
}

// TestQueryNormalizationOffKeepsNormalizedStatements: with
// NORNICDB_CYPHER_QUERY_NORMALIZATION off, a statement sent in normalized
// form gives the same result as with it on, since only the rewrite is
// skipped.
func TestQueryNormalizationOffKeepsNormalizedStatements(t *testing.T) {
	statements := []string{
		"CREATE (:QN {id: 1, name: 'a b', tags: ['x', 'y']})-[:R {w: 2}]->(:QN {id: 2, name: 'c'})",
		"MATCH (n:QN) RETURN n.id AS id, n.name AS name ORDER BY id",
		"MATCH (a:QN)-[r:R]->(b) RETURN a.id AS a, r.w AS w, b.id AS b",
		"MATCH (n:QN) WHERE n.name STARTS WITH 'a' RETURN count(n) AS c",
		"MATCH (n:QN) WITH n ORDER BY n.id DESC LIMIT 1 RETURN n.id AS id",
		"UNWIND [3, 1, 2] AS x RETURN x ORDER BY x DESC",
		"UNWIND [1, 2, 3] AS x WITH x WHERE x > 1 RETURN collect(x) AS xs",
		"MATCH (n:QN) RETURN n.id AS id, size(n.tags) AS tags ORDER BY id",
		"RETURN 'http://x.test/a' AS url, 'a  b' AS spaced",
		"MATCH (n:QN {id: 1}) SET n.seen = true RETURN n.seen AS seen",
		"MATCH (n:QN) RETURN n.id AS id, EXISTS { (n)-[:R]->() } AS out ORDER BY id",
		"MATCH (n:QN) RETURN n.id AS id, COUNT { (n)--() } AS degree ORDER BY id",
		"CALL db.labels() YIELD label RETURN label ORDER BY label",
		"MERGE (n:QN {id: 3}) ON CREATE SET n.created = true RETURN n.created AS created",
		"MATCH (n:QN {id: 3}) DETACH DELETE n RETURN count(*) AS c",
		"RETURN 1 / 0 AS boom",
	}
	type outcome struct {
		columns []string
		rows    [][]interface{}
		err     string
	}
	run := func(enabled bool) []outcome {
		restore := config.WithCypherQueryNormalizationDisabled()
		defer restore()
		config.SetCypherQueryNormalizationEnabled(enabled)
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "qn"))
		ctx := context.Background()
		var out []outcome
		for _, statement := range statements {
			canonical, rewrite := canonicalizeQueryText(statement)
			require.Nil(t, rewrite, "the corpus is normalized: %q", statement)
			require.Equal(t, statement, canonical)
			result, err := exec.Execute(ctx, statement, nil)
			if err != nil {
				out = append(out, outcome{err: err.Error()})
				continue
			}
			out = append(out, outcome{columns: result.Columns, rows: result.Rows})
		}
		return out
	}
	on, off := run(true), run(false)
	for i := range statements {
		require.Equal(t, on[i], off[i], statements[i])
	}
	require.NotEmpty(t, on[len(on)-1].err, "RETURN 1 / 0 fails either way")
}
