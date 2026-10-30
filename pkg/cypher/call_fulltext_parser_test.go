package cypher

import (
	"context"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFulltextParser_Regression_GraphitiNodeShape reproduces the bug from
// the field report: db.index.fulltext.queryNodes must intersect the
// group_id field-scoped clause with the parenthesized default-field
// clause, not discard the parenthesized term.
func TestFulltextParser_Regression_GraphitiNodeShape(t *testing.T) {
	ctx := context.Background()
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "ft_regression")
	exec := NewStorageExecutor(eng)

	_, err := exec.Execute(ctx,
		"CREATE FULLTEXT INDEX node_name_and_summary IF NOT EXISTS FOR (n:Entity) ON EACH [n.name, n.summary, n.group_id]",
		nil)
	require.NoError(t, err)

	for _, q := range []string{
		`CREATE (:Entity {uuid:'n1', group_id:'ft_repro', name:'CloudTrail', summary:'AWS CloudTrail audit logging'})`,
		`CREATE (:Entity {uuid:'n2', group_id:'ft_repro', name:'Ladybug',    summary:'embedded graph driver'})`,
		`CREATE (:Entity {uuid:'n3', group_id:'ft_repro', name:'Redis',      summary:'in-memory data store'})`,
	} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, "seed: %s", q)
	}

	run := func(t *testing.T, query string) []string {
		t.Helper()
		res, err := exec.Execute(ctx,
			`CALL db.index.fulltext.queryNodes("node_name_and_summary", $q) YIELD node, score RETURN node.name AS name`,
			map[string]interface{}{"q": query})
		require.NoError(t, err)
		out := make([]string, 0, len(res.Rows))
		for _, row := range res.Rows {
			if s, ok := row[0].(string); ok {
				out = append(out, s)
			}
		}
		sort.Strings(out)
		return out
	}

	// Behavior table from the bug report.
	assert.Equal(t, []string{"CloudTrail"}, run(t, `group_id:"ft_repro" AND (CloudTrail)`),
		"expected only CloudTrail — field-scoped clause must intersect with parenthesized default-field term")
	assert.Equal(t, []string{"Ladybug"}, run(t, `group_id:"ft_repro" AND (Ladybug)`))
	assert.Equal(t, []string{}, run(t, `group_id:"ft_repro" AND (zzznope)`),
		"nonsense term must produce empty result, not the field-scoped set")

	// Sanity checks.
	assert.Equal(t, []string{"CloudTrail"}, run(t, "CloudTrail"))
	assert.Equal(t, []string{"CloudTrail", "Redis"}, run(t, `group_id:"ft_repro" AND (CloudTrail OR Redis)`))
	assert.Equal(t, []string{"CloudTrail", "Redis"}, run(t, `group_id:"ft_repro" AND -Ladybug`))
}

// TestFulltextParser_Regression_GraphitiEdgeShape covers the parallel path
// for db.index.fulltext.queryRelationships against a Graphiti edge_name_
// and_fact index.
func TestFulltextParser_Regression_GraphitiEdgeShape(t *testing.T) {
	ctx := context.Background()
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "ft_regression_edge")
	exec := NewStorageExecutor(eng)

	_, err := exec.Execute(ctx,
		"CREATE FULLTEXT INDEX edge_name_and_fact IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON EACH [e.name, e.fact, e.group_id]",
		nil)
	require.NoError(t, err)

	for _, q := range []string{
		`CREATE (:Entity {uuid:'a'})`,
		`CREATE (:Entity {uuid:'b'})`,
		`CREATE (:Entity {uuid:'c'})`,
		`MATCH (a:Entity {uuid:'a'}), (b:Entity {uuid:'b'})
		 CREATE (a)-[:RELATES_TO {uuid:'r1', group_id:'ft_repro', name:'CloudTrail', fact:'CloudTrail audit logging'}]->(b)`,
		`MATCH (a:Entity {uuid:'b'}), (b:Entity {uuid:'c'})
		 CREATE (a)-[:RELATES_TO {uuid:'r2', group_id:'ft_repro', name:'Ladybug', fact:'embedded graph driver'}]->(b)`,
	} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, "seed: %s", q)
	}

	run := func(t *testing.T, query string) []string {
		t.Helper()
		res, err := exec.Execute(ctx,
			`CALL db.index.fulltext.queryRelationships("edge_name_and_fact", $q) YIELD relationship, score RETURN relationship.name AS name`,
			map[string]interface{}{"q": query})
		require.NoError(t, err)
		out := make([]string, 0, len(res.Rows))
		for _, row := range res.Rows {
			if s, ok := row[0].(string); ok {
				out = append(out, s)
			}
		}
		sort.Strings(out)
		return out
	}

	assert.Equal(t, []string{"CloudTrail"}, run(t, `group_id:"ft_repro" AND (CloudTrail)`))
	assert.Equal(t, []string{"Ladybug"}, run(t, `group_id:"ft_repro" AND (Ladybug)`))
	assert.Equal(t, []string{}, run(t, `group_id:"ft_repro" AND (zzznope)`))
}

// TestFulltextParser_Regression_BackslashEscape verifies the secondary issue
// noted in the report: backslash-escaped ASCII letters should tokenize to
// the same terms as unescaped input, matching Lucene's escape rules.
func TestFulltextParser_Regression_BackslashEscape(t *testing.T) {
	ctx := context.Background()
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "ft_regression_escape")
	exec := NewStorageExecutor(eng)

	_, err := exec.Execute(ctx,
		"CREATE FULLTEXT INDEX node_name_and_summary IF NOT EXISTS FOR (n:Entity) ON EACH [n.name, n.summary]",
		nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx,
		`CREATE (:Entity {uuid:'n1', name:'CloudTrail', summary:'AWS audit logging'})`, nil)
	require.NoError(t, err)

	run := func(t *testing.T, query string) []string {
		t.Helper()
		res, err := exec.Execute(ctx,
			`CALL db.index.fulltext.queryNodes("node_name_and_summary", $q) YIELD node, score RETURN node.name AS name`,
			map[string]interface{}{"q": query})
		require.NoError(t, err)
		out := make([]string, 0, len(res.Rows))
		for _, row := range res.Rows {
			if s, ok := row[0].(string); ok {
				out = append(out, s)
			}
		}
		return out
	}

	assert.Equal(t, []string{"CloudTrail"}, run(t, `CloudTrail`))
	assert.Equal(t, []string{"CloudTrail"}, run(t, `Cloud\Trail`),
		"\\T should decode to literal T; escaped and unescaped terms must match the same docs")
}
