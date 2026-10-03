package cypher

import (
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestShowBuiltInProcedureSignatureDescriptions(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	result, err := executor.Execute(ctx, "SHOW PROCEDURES YIELD name, argumentDescription, returnDescription WHERE name = 'db.index.fulltext.queryNodes' RETURN size(argumentDescription) AS arguments, size(returnDescription) AS returns, [argument IN argumentDescription | argument.name] AS names", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(3), int64(2), []interface{}{"indexName", "queryString", "options"}}}, result.Rows)
}

func TestShowBuiltInProcedureRichDescriptions(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	result, err := executor.Execute(ctx, "SHOW PROCEDURES YIELD name, argumentDescription, returnDescription WHERE name = 'db.index.fulltext.queryNodes' RETURN argumentDescription, returnDescription", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{
		[]interface{}{
			map[string]interface{}{"name": "indexName", "type": "STRING", "description": "The name of the full-text index.", "isDeprecated": false},
			map[string]interface{}{"name": "queryString", "type": "STRING", "description": "The string to find approximate matches for.", "isDeprecated": false},
			map[string]interface{}{"name": "options", "type": "MAP", "description": "{skip :: INTEGER, limit :: INTEGER, analyzer :: STRING}", "default": "DefaultParameterValue{value={}, type=MAP}", "isDeprecated": false},
		},
		[]interface{}{
			map[string]interface{}{"name": "node", "type": "NODE", "description": "A node which contains a property similar to the query string.", "isDeprecated": false},
			map[string]interface{}{"name": "score", "type": "FLOAT", "description": "The score measuring how similar the node property is to the query string.", "isDeprecated": false},
		},
	}}, result.Rows)
}

func TestShowBuiltInProcedureFlags(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	result, err := executor.Execute(ctx, "SHOW PROCEDURES YIELD name, worksOnSystem, admin, rolesExecution, rolesBoostedExecution, isDeprecated, deprecatedBy, option WHERE name = 'db.index.fulltext.queryNodes' RETURN worksOnSystem, admin, rolesExecution, rolesBoostedExecution, isDeprecated, deprecatedBy, option", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{true, false, nil, nil, false, nil, map[string]interface{}{"deprecated": false}}}, result.Rows)
}

func TestShowFulltextRelationshipProcedureMetadata(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	result, err := executor.Execute(ctx, "SHOW PROCEDURES YIELD name, signature, argumentDescription, returnDescription, worksOnSystem WHERE name = 'db.index.fulltext.queryRelationships' RETURN signature, argumentDescription, returnDescription, worksOnSystem", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{
		"db.index.fulltext.queryRelationships(indexName :: STRING, queryString :: STRING, options = {} :: MAP) :: (relationship :: RELATIONSHIP, score :: FLOAT)",
		[]interface{}{
			map[string]interface{}{"name": "indexName", "type": "STRING", "description": "The name of the full-text index.", "isDeprecated": false},
			map[string]interface{}{"name": "queryString", "type": "STRING", "description": "The string to find approximate matches for.", "isDeprecated": false},
			map[string]interface{}{"name": "options", "type": "MAP", "description": "{skip :: INTEGER, limit :: INTEGER, analyzer :: STRING}", "default": "DefaultParameterValue{value={}, type=MAP}", "isDeprecated": false},
		},
		[]interface{}{
			map[string]interface{}{"name": "relationship", "type": "RELATIONSHIP", "description": "A relationship which contains a property similar to the query string.", "isDeprecated": false},
			map[string]interface{}{"name": "score", "type": "FLOAT", "description": "The score measuring how similar the relationship property is to the query string.", "isDeprecated": false},
		},
		true,
	}}, result.Rows)
}

func TestShowRangeFunctionArgumentDescriptions(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	result, err := executor.Execute(ctx, "SHOW FUNCTIONS YIELD name, signature, argumentDescription WHERE name = 'range' RETURN signature, [argument IN argumentDescription | argument.description] AS descriptions ORDER BY signature", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"range(start :: INTEGER, end :: INTEGER) :: LIST<INTEGER>", []interface{}{"The start value of the range.", "The end value of the range."}},
		{"range(start :: INTEGER, end :: INTEGER, step :: INTEGER) :: LIST<INTEGER>", []interface{}{"The start value of the range.", "The end value of the range.", "The size of the increment (default value: 1)."}},
	}, result.Rows)
}

func TestResidualShowSchemaNonBooleanPredicates(t *testing.T) {
	for _, populated := range []bool{false, true} {
		exec, ctx := newUnitExecutor(t)
		name := "empty"
		if populated {
			name = "populated"
			_, err := exec.Execute(ctx, "CREATE INDEX named FOR (n:P) ON (n.k)", nil)
			require.NoError(t, err)
		}
		for _, query := range []string{
			"SHOW INDEXES YIELD name WHERE 1 RETURN name",
			"SHOW INDEXES YIELD name WHERE name + 1 RETURN name",
		} {
			t.Run(name+"/"+query, func(t *testing.T) {
				_, err := exec.Execute(ctx, query, nil)
				require.Error(t, err, query)
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
			})
		}
	}
}

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
