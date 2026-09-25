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

	result, err := exec.Execute(ctx, "SHOW FUNCTIONS YIELD name, category, isBuiltIn, argumentDescription, returnDescription WHERE name = 'substring' RETURN category, isBuiltIn, argumentDescription, returnDescription", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "String", result.Rows[0][0])
	require.Equal(t, true, result.Rows[0][1])
	require.Equal(t, []interface{}{
		map[string]interface{}{"name": "original", "type": "STRING", "description": "", "isDeprecated": false},
		map[string]interface{}{"name": "start", "type": "INTEGER", "description": "", "isDeprecated": false},
		map[string]interface{}{"name": "length", "type": "INTEGER", "description": "", "isDeprecated": false},
	}, result.Rows[0][2])
	require.Equal(t, "STRING", result.Rows[0][3])
}
