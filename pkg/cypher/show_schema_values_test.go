package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SHOW INDEXES / SHOW CONSTRAINTS values as Neo4j 5.26.30 reports them
// (#530), and createStatement recreating every index and constraint.
func TestShowSchemaValuesMatchNeo4j(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	for _, ddl := range []string{
		"CREATE INDEX s_range FOR (n:S) ON (n.a)",
		"CREATE INDEX s_comp FOR (n:S) ON (n.a, n.b)",
		"CREATE INDEX s_rel FOR ()-[r:SR]-() ON (r.a)",
		"CREATE FULLTEXT INDEX s_ft FOR (n:S) ON EACH [n.t, n.u]",
		"CREATE VECTOR INDEX s_vec FOR (n:S) ON (n.v) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}",
		"CREATE CONSTRAINT s_uniq FOR (n:SC) REQUIRE n.y IS UNIQUE",
		"CREATE CONSTRAINT s_relu FOR ()-[r:SR]-() REQUIRE r.k IS UNIQUE",
		"CREATE CONSTRAINT s_nn FOR (n:SC) REQUIRE n.z IS NOT NULL",
		"CREATE CONSTRAINT s_relnn FOR ()-[r:SR]-() REQUIRE r.z IS NOT NULL",
		"CREATE CONSTRAINT s_key FOR (n:SC) REQUIRE (n.k1, n.k2) IS NODE KEY",
		"CREATE CONSTRAINT s_relkey FOR ()-[r:SR]-() REQUIRE (r.k1, r.k2) IS RELATIONSHIP KEY",
		"CREATE CONSTRAINT s_type FOR (n:SC) REQUIRE n.w IS :: INTEGER",
		"CREATE CONSTRAINT s_temporal FOR (n:ST) REQUIRE (n.key, n.from, n.to) IS TEMPORAL NO OVERLAP",
		"CREATE CONSTRAINT s_domain FOR (n:SD) REQUIRE n.status IN ['a', 'it''s', 3]",
		"CREATE CONSTRAINT s_card FOR ()-[r:SCARD]->() REQUIRE MAX COUNT 2",
		"CREATE CONSTRAINT s_cardin FOR ()<-[r:SCARD2]-() REQUIRE MAX COUNT 5",
		"CREATE CONSTRAINT s_policy FOR (:SA)-[r:SP]->(:SB) REQUIRE ALLOWED",
	} {
		_, err := exec.Execute(ctx, ddl, nil)
		require.NoError(t, err, ddl)
	}

	rows := func(query string) map[string]map[string]interface{} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		out := map[string]map[string]interface{}{}
		for _, row := range result.Rows {
			values := map[string]interface{}{}
			for i, column := range result.Columns {
				values[column] = row[i]
			}
			out[fmt.Sprint(values["name"])] = values
		}
		return out
	}
	indexes := rows("SHOW INDEXES YIELD *")
	constraints := rows("SHOW CONSTRAINTS YIELD *")

	range1 := map[string]interface{}{"indexConfig": map[string]interface{}{}, "indexProvider": "range-1.0"}
	for name, want := range map[string]map[string]interface{}{
		"s_range": {"type": "RANGE", "indexProvider": "range-1.0", "options": range1, "createStatement": "CREATE RANGE INDEX `s_range` FOR (n:`S`) ON (n.`a`)"},
		"s_comp":  {"type": "RANGE", "createStatement": "CREATE RANGE INDEX `s_comp` FOR (n:`S`) ON (n.`a`, n.`b`)"},
		"s_rel":   {"type": "RANGE", "entityType": "RELATIONSHIP", "createStatement": "CREATE RANGE INDEX `s_rel` FOR ()-[r:`SR`]-() ON (r.`a`)"},
		"s_ft":    {"type": "FULLTEXT", "indexProvider": "fulltext-1.0", "createStatement": "CREATE FULLTEXT INDEX `s_ft` FOR (n:`S`) ON EACH [n.`t`, n.`u`]"},
		"s_vec": {"type": "VECTOR", "indexProvider": "vector-2.0",
			"options":         map[string]interface{}{"indexConfig": map[string]interface{}{"vector.dimensions": int64(4), "vector.similarity_function": "COSINE"}, "indexProvider": "vector-2.0"},
			"createStatement": "CREATE VECTOR INDEX `s_vec` FOR (n:`S`) ON (n.`v`) OPTIONS {indexConfig: {`vector.dimensions`: 4,`vector.similarity_function`: 'COSINE'}}"},
		// An index a constraint owns has the constraint's name and statement.
		"s_uniq": {"owningConstraint": "s_uniq", "createStatement": "CREATE CONSTRAINT `s_uniq` FOR (n:`SC`) REQUIRE (n.`y`) IS UNIQUE"},
		"s_relu": {"owningConstraint": "s_relu", "entityType": "RELATIONSHIP", "createStatement": "CREATE CONSTRAINT `s_relu` FOR ()-[r:`SR`]-() REQUIRE (r.`k`) IS UNIQUE"},
	} {
		got, ok := indexes[name]
		require.True(t, ok, "index %s listed", name)
		for column, value := range want {
			assert.Equal(t, value, got[column], "%s %s", name, column)
		}
		assert.Equal(t, "", got["failureMessage"], name)
		assert.Nil(t, got["readCount"], name)
		assert.Nil(t, got["lastRead"], name)
	}
	_, suffixed := indexes["s_relu_index"]
	assert.False(t, suffixed, "a relationship constraint's index has the constraint's name")

	for name, want := range map[string]map[string]interface{}{
		"s_uniq":     {"type": "UNIQUENESS", "options": range1, "createStatement": "CREATE CONSTRAINT `s_uniq` FOR (n:`SC`) REQUIRE (n.`y`) IS UNIQUE"},
		"s_relu":     {"type": "RELATIONSHIP_UNIQUENESS", "ownedIndex": "s_relu", "createStatement": "CREATE CONSTRAINT `s_relu` FOR ()-[r:`SR`]-() REQUIRE (r.`k`) IS UNIQUE"},
		"s_nn":       {"type": "NODE_PROPERTY_EXISTENCE", "options": nil, "createStatement": "CREATE CONSTRAINT `s_nn` FOR (n:`SC`) REQUIRE (n.`z`) IS NOT NULL"},
		"s_relnn":    {"type": "RELATIONSHIP_PROPERTY_EXISTENCE", "createStatement": "CREATE CONSTRAINT `s_relnn` FOR ()-[r:`SR`]-() REQUIRE (r.`z`) IS NOT NULL"},
		"s_key":      {"type": "NODE_KEY", "createStatement": "CREATE CONSTRAINT `s_key` FOR (n:`SC`) REQUIRE (n.`k1`, n.`k2`) IS NODE KEY"},
		"s_relkey":   {"type": "RELATIONSHIP_KEY", "createStatement": "CREATE CONSTRAINT `s_relkey` FOR ()-[r:`SR`]-() REQUIRE (r.`k1`, r.`k2`) IS RELATIONSHIP KEY"},
		"s_type":     {"type": "NODE_PROPERTY_TYPE", "propertyType": "INTEGER", "createStatement": "CREATE CONSTRAINT `s_type` FOR (n:`SC`) REQUIRE (n.`w`) IS :: INTEGER"},
		"s_temporal": {"type": "TEMPORAL_NO_OVERLAP", "createStatement": "CREATE CONSTRAINT `s_temporal` FOR (n:`ST`) REQUIRE (n.`key`, n.`from`, n.`to`) IS TEMPORAL NO OVERLAP"},
		"s_domain":   {"type": "DOMAIN", "createStatement": "CREATE CONSTRAINT `s_domain` FOR (n:`SD`) REQUIRE n.`status` IN ['a', 'it\\'s', 3]"},
		"s_card":     {"type": "CARDINALITY", "createStatement": "CREATE CONSTRAINT `s_card` FOR ()-[r:`SCARD`]->() REQUIRE MAX COUNT 2"},
		"s_cardin":   {"type": "CARDINALITY", "createStatement": "CREATE CONSTRAINT `s_cardin` FOR ()<-[r:`SCARD2`]-() REQUIRE MAX COUNT 5"},
		"s_policy":   {"type": "RELATIONSHIP_POLICY", "createStatement": "CREATE CONSTRAINT `s_policy` FOR (:`SA`)-[r:`SP`]->(:`SB`) REQUIRE ALLOWED"},
	} {
		got, ok := constraints[name]
		require.True(t, ok, "constraint %s listed", name)
		for column, value := range want {
			assert.Equal(t, value, got[column], "%s %s", name, column)
		}
	}

	// Every createStatement recreates what it describes.
	for name := range constraints {
		_, err := exec.Execute(ctx, "DROP CONSTRAINT `"+name+"`", nil)
		require.NoError(t, err, name)
	}
	for name, row := range indexes {
		if row["owningConstraint"] == nil {
			_, err := exec.Execute(ctx, "DROP INDEX `"+name+"`", nil)
			require.NoError(t, err, name)
		}
	}
	require.Empty(t, rows("SHOW INDEXES YIELD *"))
	require.Empty(t, rows("SHOW CONSTRAINTS YIELD *"))
	for name, row := range constraints {
		_, err := exec.Execute(ctx, row["createStatement"].(string), nil)
		require.NoError(t, err, "%s: %v", name, row["createStatement"])
	}
	for name, row := range indexes {
		if row["owningConstraint"] == nil {
			_, err := exec.Execute(ctx, row["createStatement"].(string), nil)
			require.NoError(t, err, "%s: %v", name, row["createStatement"])
		}
	}
	recreatedIndexes := rows("SHOW INDEXES YIELD *")
	recreatedConstraints := rows("SHOW CONSTRAINTS YIELD *")
	for _, listing := range []struct {
		before, after map[string]map[string]interface{}
		columns       []string
	}{
		{indexes, recreatedIndexes, []string{"type", "entityType", "labelsOrTypes", "properties", "indexProvider", "owningConstraint", "options", "createStatement"}},
		{constraints, recreatedConstraints, []string{"type", "entityType", "labelsOrTypes", "properties", "ownedIndex", "propertyType", "options", "createStatement", "direction", "maxCount", "sourceLabel", "targetLabel", "policyMode"}},
	} {
		require.Len(t, listing.after, len(listing.before))
		for name, before := range listing.before {
			after, ok := listing.after[name]
			require.True(t, ok, "%s recreated", name)
			for _, column := range listing.columns {
				assert.Equal(t, before[column], after[column], "%s %s", name, column)
			}
		}
	}
}

func TestShowDatabasesValues(t *testing.T) {
	column := func(row []interface{}, name string) interface{} {
		for i, c := range showDatabasesColumns {
			if c == name {
				return row[i]
			}
		}
		t.Fatalf("no column %s", name)
		return nil
	}
	createdAt := processStartTime.Add(-time.Hour)
	row := showDatabaseRow(nil, "db", "standard", "online", true, createdAt)
	assert.Equal(t, int64(1), column(row, "currentPrimariesCount"))
	assert.Equal(t, int64(0), column(row, "currentSecondariesCount"))
	assert.Equal(t, int64(0), column(row, "replicationLag"))
	assert.Equal(t, map[string]interface{}{}, column(row, "options"))
	assert.Equal(t, createdAt.UTC(), column(row, "creationTime"))
	assert.Equal(t, processStartTime.UTC(), column(row, "lastStartTime"), "a database created before the process started starts with it")
	assert.Nil(t, column(row, "databaseID"))
	assert.Nil(t, column(row, "store"))

	later := processStartTime.Add(time.Hour)
	row = showDatabaseRow(nil, "db", "standard", "online", true, later)
	assert.Equal(t, later.UTC(), column(row, "lastStartTime"), "a database created later starts when it is created")

	row = showDatabaseRow(nil, "db", "standard", "online", true, time.Time{})
	assert.Nil(t, column(row, "creationTime"), "an unknown creation time is null")
	assert.Nil(t, column(row, "lastStartTime"))
	assert.True(t, showDatabaseCreatedAt(nil, "db").IsZero())
}

// A database has Neo4j's two token lookup indexes: listed, droppable,
// recreatable, one per entity type (#530).
func TestLookupIndexesAsNeo4j(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	lookups := func() [][]interface{} {
		result, err := exec.Execute(ctx, "SHOW LOOKUP INDEXES YIELD name, type, entityType, labelsOrTypes, properties, indexProvider, options, createStatement", nil)
		require.NoError(t, err)
		return result.Rows
	}
	options := map[string]interface{}{"indexConfig": map[string]interface{}{}, "indexProvider": "token-lookup-1.0"}
	require.Equal(t, [][]interface{}{
		{"index_343aff4e", "LOOKUP", "NODE", nil, nil, "token-lookup-1.0", options, "CREATE LOOKUP INDEX `index_343aff4e` FOR (n) ON EACH labels(n)"},
		{"index_f7700477", "LOOKUP", "RELATIONSHIP", nil, nil, "token-lookup-1.0", options, "CREATE LOOKUP INDEX `index_f7700477` FOR ()-[r]-() ON EACH type(r)"},
	}, lookups())

	for statement, code := range map[string]string{
		"CREATE LOOKUP INDEX other FOR (n) ON EACH labels(n)":              "Neo.ClientError.Schema.IndexAlreadyExists",
		"CREATE LOOKUP INDEX other FOR ()-[r]-() ON EACH type(r)":          "Neo.ClientError.Schema.IndexAlreadyExists",
		"CREATE LOOKUP INDEX FOR (n) ON EACH type(n)":                      "Neo.ClientError.Statement.SyntaxError",
		"CREATE LOOKUP INDEX FOR (n) ON EACH labels(m)":                    "Neo.ClientError.Statement.SyntaxError",
		"CREATE LOOKUP INDEX index_343aff4e FOR ()-[r]-() ON EACH type(r)": "Neo.ClientError.Schema.IndexAlreadyExists",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.Error(t, err, statement)
		assert.Contains(t, err.Error(), code, statement)
	}
	_, err := exec.Execute(ctx, "CREATE LOOKUP INDEX other_rel IF NOT EXISTS FOR ()-[r]-() ON EACH type(r)", nil)
	require.NoError(t, err)
	require.Len(t, lookups(), 2)

	// Dropped, the row is gone and label / type scans answer the same.
	_, err = exec.Execute(ctx, "CREATE (:LK)-[:LR]->(:LK)", nil)
	require.NoError(t, err)
	for _, name := range []string{"index_343aff4e", "index_f7700477"} {
		_, err = exec.Execute(ctx, "DROP INDEX "+name, nil)
		require.NoError(t, err, name)
	}
	require.Empty(t, lookups())
	result, err := exec.Execute(ctx, "MATCH (n:LK)-[r:LR]->() RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Rows[0][0])

	// Recreated, by name or with the default name.
	_, err = exec.Execute(ctx, "CREATE LOOKUP INDEX my_rel_lookup FOR ()-[r]-() ON EACH type(r)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE LOOKUP INDEX IF NOT EXISTS FOR (n) ON EACH labels(n)", nil)
	require.NoError(t, err)
	names := []interface{}{}
	for _, row := range lookups() {
		names = append(names, row[0])
	}
	require.Equal(t, []interface{}{"index_343aff4e", "my_rel_lookup"}, names)
	_, err = exec.Execute(ctx, "CREATE INDEX s FOR (n:L) ON (n.a)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "DROP INDEX my_rel_lookup", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE LOOKUP INDEX s FOR ()-[r]-() ON EACH type(r)", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Neo.ClientError.Schema.IndexWithNameAlreadyExists")
	assert.Contains(t, err.Error(), "There already exists an index called 's'.")
}
