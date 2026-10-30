package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ON EACH [...] is fulltext-only syntax; CREATE [RANGE] INDEX must reject it
// instead of creating an index on a property named "p]" (#526).
func TestCreateIndexRejectsOnEach(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()

	for _, q := range []string{
		"CREATE INDEX eachidx FOR (n:L) ON EACH [n.p]",
		"CREATE RANGE INDEX eachidx FOR (n:L) ON EACH [n.p]",
		"CREATE RANGE INDEX eachidx2 FOR (n:L) ON EACH [n.q, n.r]",
		"CREATE INDEX eachidx3 FOR (n:L) ON [n.p]",
		"CREATE INDEX junkidx FOR (n:L) ON (n.p])",
		"CREATE VECTOR INDEX eachvec FOR (n:L) ON EACH [n.e] OPTIONS { indexConfig: { `vector.dimensions`: 3, `vector.similarity_function`: 'cosine' } }",
	} {
		_, err := exec.Execute(ctx, q, nil)
		require.Error(t, err, q)
	}
	res, err := exec.Execute(ctx, "SHOW INDEXES YIELD name WHERE name STARTS WITH 'each' OR name = 'junkidx' RETURN name", nil)
	require.NoError(t, err)
	assert.Empty(t, res.Rows)

	// Valid forms still work, including backtick-quoted names and the bare form.
	for _, q := range []string{
		"CREATE INDEX okidx FOR (n:L) ON (n.p)",
		"CREATE RANGE INDEX okidx2 FOR (n:L) ON (n.`full name`)",
		"CREATE INDEX okidx3 FOR (n:L2) ON n.q",
		"CREATE FULLTEXT INDEX okft FOR (n:L) ON EACH [n.t]",
	} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, q)
	}
	res, err = exec.Execute(ctx, "SHOW INDEXES YIELD name, properties WHERE name STARTS WITH 'ok' RETURN name, properties", nil)
	require.NoError(t, err)
	got := map[string]interface{}{}
	for _, row := range res.Rows {
		got[row[0].(string)] = row[1]
	}
	assert.Equal(t, map[string]interface{}{
		"okft":   []string{"t"},
		"okidx":  []string{"p"},
		"okidx2": []string{"full name"},
		"okidx3": []string{"q"},
	}, got)
}
