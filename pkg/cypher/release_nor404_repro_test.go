package cypher

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReleaseNOR404DropKeepsOtherVectorIndex(t *testing.T) {
	e, _ := newTestExecutor(t)
	ctx := t.Context()
	for _, q := range []string{
		"CREATE VECTOR INDEX doc_v FOR (n:Doc) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`:3, `vector.similarity_function`:'cosine'}}",
		"CREATE VECTOR INDEX note_v FOR (n:Note) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`:3, `vector.similarity_function`:'cosine'}}",
		"CREATE (:Doc {id:'doc'}), (:Note {id:'note'})",
		"MATCH (n:Doc) CALL db.create.setNodeVectorProperty(n,'emb',[1.0,0.0,0.0])",
		"MATCH (n:Note) CALL db.create.setNodeVectorProperty(n,'emb',[1.0,0.0,0.0])"} {
		_, err := e.Execute(ctx, q, nil)
		require.NoError(t, err, q)
	}
	q := "CALL db.index.vector.queryNodes('note_v',5,[1.0,0.0,0.0]) YIELD node RETURN node.id"
	before, err := e.Execute(ctx, q, nil)
	require.NoError(t, err)
	require.Len(t, before.Rows, 1, "populated control")
	_, err = e.Execute(ctx, "DROP INDEX doc_v", nil)
	require.NoError(t, err)
	after, err := e.Execute(ctx, q, nil)
	require.NoError(t, err)
	require.Equal(t, before.Rows, after.Rows, "dropping doc index must preserve note index")
	_, err = e.Execute(ctx, "CREATE VECTOR INDEX doc_v FOR (n:Doc) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`:3, `vector.similarity_function`:'cosine'}}", nil)
	require.NoError(t, err)
	recreated, err := e.Execute(ctx, "CALL db.index.vector.queryNodes('doc_v',5,[1.0,0.0,0.0]) YIELD node RETURN node.id", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"doc"}}, recreated.Rows, "recreated index must populate stored nodes")
}
