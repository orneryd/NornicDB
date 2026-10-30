package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// db.index.vector.drop / db.index.fulltext.drop must actually drop the index,
// fail for unknown names, and only drop an index of their own kind (#528).
func TestIndexDropProceduresDropTheIndex(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	run := func(q string) ([][]interface{}, error) {
		res, err := exec.Execute(ctx, q, nil)
		if err != nil {
			return nil, err
		}
		return res.Rows, nil
	}
	names := func() []string {
		rows, err := run("SHOW INDEXES YIELD name WHERE name IN ['vidx', 'ftidx'] RETURN name ORDER BY name")
		require.NoError(t, err)
		out := []string{}
		for _, row := range rows {
			out = append(out, row[0].(string))
		}
		return out
	}

	_, err := run("CREATE VECTOR INDEX vidx FOR (n:V) ON (n.e) OPTIONS { indexConfig: { `vector.dimensions`: 3, `vector.similarity_function`: 'cosine' } }")
	require.NoError(t, err)
	_, err = run("CREATE FULLTEXT INDEX ftidx FOR (n:F) ON EACH [n.t]")
	require.NoError(t, err)
	require.Equal(t, []string{"ftidx", "vidx"}, names())

	// Wrong kind: neither procedure drops the other kind's index.
	_, err = run("CALL db.index.vector.drop('ftidx')")
	require.Error(t, err)
	_, err = run("CALL db.index.fulltext.drop('vidx')")
	require.Error(t, err)
	require.Equal(t, []string{"ftidx", "vidx"}, names())

	rows, err := run("CALL db.index.vector.drop('vidx')")
	require.NoError(t, err)
	assert.Equal(t, [][]interface{}{{"vidx", true}}, rows)
	rows, err = run("CALL db.index.fulltext.drop('ftidx')")
	require.NoError(t, err)
	assert.Equal(t, [][]interface{}{{"ftidx", true}}, rows)
	assert.Equal(t, []string{}, names())

	for _, q := range []string{
		"CALL db.index.vector.drop('does_not_exist')",
		"CALL db.index.fulltext.drop('does_not_exist')",
		"CALL db.index.vector.drop('vidx')",
	} {
		_, err = run(q)
		require.Error(t, err, q)
		assert.Contains(t, err.Error(), "IndexDropFailed", q)
	}

	// The name is free again: re-creating with other settings works.
	_, err = run("CREATE VECTOR INDEX vidx FOR (n:V) ON (n.e) OPTIONS { indexConfig: { `vector.dimensions`: 8, `vector.similarity_function`: 'cosine' } }")
	require.NoError(t, err)
	assert.Equal(t, []string{"vidx"}, names())
}
