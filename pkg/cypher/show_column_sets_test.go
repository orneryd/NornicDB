package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestShowColumnSetsMatchNeo4j covers #690: SHOW FUNCTIONS / PROCEDURES /
// INDEXES / CONSTRAINTS list Neo4j 5.26's default columns, YIELD * and
// YIELD <column> reach the full set, and SHOW … WHERE keeps the defaults.
func TestShowColumnSetsMatchNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	for query, want := range map[string][]string{
		"SHOW FUNCTIONS":                              {"name", "category", "description"},
		"SHOW FUNCTIONS YIELD *":                      {"name", "category", "description", "signature", "isBuiltIn", "argumentDescription", "returnDescription", "aggregating", "rolesExecution", "rolesBoostedExecution", "isDeprecated", "deprecatedBy"},
		"SHOW PROCEDURES":                             {"name", "description", "mode", "worksOnSystem"},
		"SHOW PROCEDURES YIELD *":                     {"name", "description", "mode", "worksOnSystem", "signature", "argumentDescription", "returnDescription", "admin", "rolesExecution", "rolesBoostedExecution", "isDeprecated", "deprecatedBy", "option"},
		"SHOW INDEXES":                                {"id", "name", "state", "populationPercent", "type", "entityType", "labelsOrTypes", "properties", "indexProvider", "owningConstraint", "lastRead", "readCount"},
		"SHOW INDEXES YIELD *":                        {"id", "name", "state", "populationPercent", "type", "entityType", "labelsOrTypes", "properties", "indexProvider", "owningConstraint", "lastRead", "readCount", "trackedSince", "options", "failureMessage", "createStatement"},
		"SHOW CONSTRAINTS":                            {"id", "name", "type", "entityType", "labelsOrTypes", "properties", "ownedIndex", "propertyType"},
		"SHOW CONSTRAINTS YIELD *":                    {"id", "name", "type", "entityType", "labelsOrTypes", "properties", "ownedIndex", "propertyType", "options", "createStatement", "direction", "maxCount", "sourceLabel", "targetLabel", "policyMode"},
		"SHOW FUNCTIONS YIELD name, category LIMIT 1": {"name", "category"},
		"SHOW FUNCTIONS WHERE name = 'toUpper'":       {"name", "category", "description"},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Columns, query)
	}

	// substring has two signatures, listed as two rows (as Neo4j does).
	result, err := exec.Execute(ctx, "SHOW FUNCTIONS YIELD name, category, isBuiltIn, argumentDescription, returnDescription WHERE name = 'substring' RETURN category, isBuiltIn, argumentDescription, returnDescription ORDER BY size(argumentDescription)", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 2)
	require.Equal(t, "String", result.Rows[1][0])
	require.Equal(t, true, result.Rows[1][1])
	require.Equal(t, []interface{}{
		map[string]interface{}{"name": "original", "type": "STRING", "description": "", "isDeprecated": false},
		map[string]interface{}{"name": "start", "type": "INTEGER", "description": "", "isDeprecated": false},
	}, result.Rows[0][2])
	require.Equal(t, []interface{}{
		map[string]interface{}{"name": "original", "type": "STRING", "description": "", "isDeprecated": false},
		map[string]interface{}{"name": "start", "type": "INTEGER", "description": "", "isDeprecated": false},
		map[string]interface{}{"name": "length", "type": "INTEGER", "description": "", "isDeprecated": false},
	}, result.Rows[1][2])
	require.Equal(t, "STRING", result.Rows[1][3])
}

// TestShowListingsAreOrderedByName: Neo4j lists every SHOW command by name,
// so indexes and constraints come in name order, not creation order.
func TestShowListingsAreOrderedByName(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	for _, statement := range []string{
		"CREATE INDEX zz_o FOR (n:OZ) ON (n.a)",
		"CREATE INDEX mm_o FOR (n:OZ) ON (n.b)",
		"CREATE INDEX aa_o FOR (n:OZ) ON (n.c)",
		"CREATE CONSTRAINT zc_o FOR (n:OC) REQUIRE n.a IS UNIQUE",
		"CREATE CONSTRAINT ac_o FOR (n:OC) REQUIRE n.b IS UNIQUE",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	for _, query := range []string{"SHOW INDEXES", "SHOW CONSTRAINTS YIELD *", "SHOW FUNCTIONS", "SHOW PROCEDURES YIELD name", "SHOW SETTINGS"} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		column := -1
		for i, name := range result.Columns {
			if name == "name" {
				column = i
			}
		}
		require.GreaterOrEqual(t, column, 0, query)
		require.NotEmpty(t, result.Rows, query)
		for i := 1; i < len(result.Rows); i++ {
			require.LessOrEqual(t, result.Rows[i-1][column].(string), result.Rows[i][column].(string), query)
		}
	}
	result, err := exec.Execute(ctx, "SHOW INDEXES YIELD name WHERE name ENDS WITH '_o'", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"aa_o"}, {"ac_o"}, {"mm_o"}, {"zc_o"}, {"zz_o"}}, result.Rows)
}

// TestFulltextWildcardSkipsRelationshipsWithoutIndexedProperties: as for
// nodes and in Neo4j, a relationship with none of the index's properties has
// no document, so the match-all wildcard doesn't return it (#547).
func TestFulltextWildcardSkipsRelationshipsWithoutIndexedProperties(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	for _, query := range []string{
		"CREATE FULLTEXT INDEX fw_rel FOR ()-[r:FWR]-() ON EACH [r.d]",
		"CREATE (a:FW {d: 'hello'})-[:FWR {d: 'text'}]->(b:FW), (a)-[:FWR {x: 1}]->(b)",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
	result, err := exec.Execute(ctx, "CALL db.index.fulltext.queryRelationships('fw_rel', '*') YIELD relationship RETURN relationship.d AS d", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"text"}}, result.Rows)
}

// TestShowReturnStarIsTheYieldedColumns: RETURN * after a SHOW's YIELD lists
// the yielded columns in YIELD order, through a WHERE or ORDER BY too, as in
// Neo4j 5.26.30.
func TestShowReturnStarIsTheYieldedColumns(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	full := []string{"name", "category", "description", "signature", "isBuiltIn", "argumentDescription", "returnDescription", "aggregating", "rolesExecution", "rolesBoostedExecution", "isDeprecated", "deprecatedBy"}
	for query, want := range map[string][]string{
		"SHOW FUNCTIONS YIELD name, category RETURN * LIMIT 1":              {"name", "category"},
		"SHOW FUNCTIONS YIELD name AS n, category WHERE n = 'abs' RETURN *": {"n", "category"},
		"SHOW FUNCTIONS YIELD * WHERE name = 'abs' RETURN *":                full,
		"SHOW FUNCTIONS YIELD * ORDER BY name LIMIT 1 RETURN *":             full,
		"SHOW FUNCTIONS YIELD category RETURN DISTINCT * ORDER BY category": {"category"},
		"SHOW FUNCTIONS YIELD name, category RETURN *, 1 AS one LIMIT 1":    {"name", "category", "one"},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Columns, query)
	}
}
