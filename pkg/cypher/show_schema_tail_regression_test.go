package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShowSchemaYieldWhereReturn(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	for _, statement := range []string{
		"CREATE INDEX zeta FOR (n:Z) ON (n.z)",
		"CREATE INDEX alpha FOR (n:A) ON (n.a)",
		"CREATE CONSTRAINT kz FOR (n:K) REQUIRE n.z IS UNIQUE",
		"CREATE CONSTRAINT ka FOR (n:K) REQUIRE n.a IS UNIQUE",
	} {
		_, err := executor.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	for _, testCase := range []struct {
		query   string
		columns []string
		rows    [][]interface{}
	}{
		{"SHOW INDEXES YIELD name, type WHERE type = 'RANGE' RETURN name ORDER BY name", []string{"name"}, [][]interface{}{{"alpha"}, {"ka"}, {"kz"}, {"zeta"}}},
		{"SHOW INDEXES YIELD name, type WHERE type = 'RANGE' RETURN collect(name) AS names", []string{"names"}, [][]interface{}{{[]interface{}{"alpha", "ka", "kz", "zeta"}}}},
		{"SHOW INDEXES YIELD name, type WHERE type = 'RANGE' RETURN count(*) AS c", []string{"c"}, [][]interface{}{{int64(4)}}},
		{"SHOW INDEXES YIELD name, labelsOrTypes WHERE name = 'alpha' RETURN labelsOrTypes", []string{"labelsOrTypes"}, [][]interface{}{{[]string{"A"}}}},
		{"SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name DESC", []string{"name"}, [][]interface{}{{"kz"}, {"ka"}}},
		{"SHOW INDEXES YIELD name AS indexName WHERE name = 'alpha' RETURN indexName AS title", []string{"title"}, [][]interface{}{{"alpha"}}},
		// The four range indexes and the two token lookup indexes (#530).
		{"SHOW INDEXES YIELD * RETURN count(*) AS total", []string{"total"}, [][]interface{}{{int64(6)}}},
		{"SHOW CONSTRAINTS YIELD name ORDER BY name LIMIT 1", []string{"name"}, [][]interface{}{{"ka"}}},
		{"SHOW CONSTRAINTS YIELD name WHERE name = 'missing' RETURN count(*) AS total", []string{"total"}, [][]interface{}{{int64(0)}}},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			result, err := executor.Execute(ctx, testCase.query, nil)
			require.NoError(t, err)
			require.Equal(t, testCase.columns, result.Columns)
			if testCase.columns[0] == "names" {
				require.Len(t, result.Rows, 1)
				require.Len(t, result.Rows[0], 1)
				require.ElementsMatch(t, testCase.rows[0][0], result.Rows[0][0])
				return
			}
			require.Equal(t, testCase.rows, result.Rows)
		})
	}
	_, err := executor.Execute(ctx, "SHOW INDEXES YIELD nonexistent RETURN nonexistent", nil)
	require.Error(t, err)
	// Neo4j's SHOW grammar: RETURN needs YIELD, WITH isn't allowed, YIELD's
	// WHERE comes after its ORDER BY / SKIP / LIMIT, which take literals.
	for _, statement := range []string{
		"SHOW INDEXES RETURN count(*) AS total",
		"SHOW INDEXES WHERE name = 'alpha' RETURN name",
		"SHOW INDEXES YIELD name WITH name RETURN name",
		"SHOW INDEXES YIELD name WHERE name = 'alpha' ORDER BY name",
		"SHOW INDEXES YIELD name LIMIT 1 + 1 RETURN name",
	} {
		_, err := executor.Execute(ctx, statement, nil)
		require.Error(t, err, statement)
	}
	_, err = executor.Execute(ctx, "DROP CONSTRAINT ka", nil)
	require.NoError(t, err)
	result, err := executor.Execute(ctx, "SHOW INDEXES YIELD name WHERE name = 'ka' RETURN count(*) AS total", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}
