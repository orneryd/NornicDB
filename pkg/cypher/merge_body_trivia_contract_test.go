package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestMergeBodyTriviaSharedProducts(t *testing.T) {
	restore := config.WithCypherQueryNormalizationDisabled()
	defer restore()
	for _, gap := range []string{" ", "/*c*/", " //c\n"} {
		exec, ctx := newUnitExecutor(t)
		query := "MERGE" + gap + "(n:LocalMergeBody {id:1}) ON CREATE SET n.v=7 ON MATCH SET n.v=n.v+1,n:Seen RETURN n.v AS k"
		first, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{int64(7)}}, first.Rows, query)
		require.Equal(t, 1, first.Stats.NodesCreated)
		second, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{int64(8)}}, second.Rows, query)
		require.Zero(t, second.Stats.NodesCreated)
		count, err := exec.Execute(ctx, "MATCH (n:LocalMergeBody) RETURN count(n) AS k", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, count.Rows)
		items, keys, ok := mergeStatementSetItems(ctx, query, "LocalMergeBody")
		require.True(t, ok)
		require.Len(t, items, 3)
		require.Equal(t, "n.v=7", items[0].text)
		require.Equal(t, "n.v=n.v+1", items[1].text)
		require.Equal(t, "n:Seen", items[2].text)
		require.Equal(t, map[string][]string{"id": {"1"}}, keys)
		clauses, ok := splitPipelineClauses(ctx, query)
		require.True(t, ok)
		labels, dynamic := statementSetLabels(ctx, clauses)
		require.Equal(t, []string{"Seen"}, labels["n"])
		require.Empty(t, dynamic)
		relationship := "MERGE" + gap + "(a)-[r:LocalMergeRel {id:row.id}]->(b)"
		template := exec.pipelineMergeTemplateFor(originalBraceView(relationship))
		require.True(t, template.usable, relationship)
		require.Equal(t, "LocalMergeRel", template.shape.relType)
		require.Len(t, template.properties, 1)
		batch, ok := parseRelationshipBatchMergeClause(originalBraceView(relationship), "row")
		require.True(t, ok, relationship)
		require.Equal(t, "a", batch.startVar)
		require.Equal(t, "b", batch.endVar)
		require.Equal(t, "r", batch.relVar)
		require.Equal(t, "LocalMergeRel", batch.relType)
		require.Equal(t, map[string]string{"id": "id"}, batch.rowFieldRefs)
		chain, ok := parseUnwindMergeRelationshipClause(originalBraceView("MERGE" + gap + "(a)-[r:LocalMergeRel]->(b)"))
		require.True(t, ok)
		require.Equal(t, "a", chain.fromVar)
		require.Equal(t, "b", chain.toVar)
	}
}

func TestMergeBodyTriviaTypedSource(t *testing.T) {
	for _, gap := range []string{" ", "/*c*/", " //c\n"} {
		query := "MERGE" + gap + `(\u006e:LocalMergeBody {id:'\u0061'}) ON CREATE SET n.v='\u0062'`
		source := cyphertext.NormalizeSource(query)
		merge := NewASTBuilder().parseMerge(source)
		require.Equal(t, `(\u006e:LocalMergeBody {id:'\u0061'})`, merge.Pattern.RawText)
		require.Equal(t, "n", merge.Pattern.Nodes[0].Variable)
		require.Equal(t, `id:'\u0061'`, merge.Pattern.Nodes[0].RawProps)
		require.Equal(t, "a", merge.Pattern.Nodes[0].Properties["id"].Literal)
		require.Len(t, merge.OnCreate, 1)
		require.Equal(t, `'\u0062'`, merge.OnCreate[0].RawValue)
		require.Equal(t, "b", merge.OnCreate[0].Value.Literal)
	}
}
