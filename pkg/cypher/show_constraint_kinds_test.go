package cypher

import (
	"context"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// SHOW [kind] CONSTRAINTS lists the constraints of the kind, as Neo4j does
// (Cypher 5: 5.26 Enterprise; Cypher 25: the kinds Neo4j 2026.09 takes,
// PROPERTY UNIQUENESS among them, with the same constraints). A kind a
// version doesn't take is a SyntaxError ("ERR"). On main every kind but ALL
// was "unsupported query type".
func TestShowConstraintKinds(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "show_constraint_kinds"))
	ctx := context.Background()
	for _, statement := range []string{
		"CREATE CONSTRAINT zk_nu FOR (n:ZkA) REQUIRE n.u IS UNIQUE",
		"CREATE CONSTRAINT zk_ru FOR ()-[r:ZkR]-() REQUIRE r.u IS UNIQUE",
		"CREATE CONSTRAINT zk_ne FOR (n:ZkA) REQUIRE n.e IS NOT NULL",
		"CREATE CONSTRAINT zk_re FOR ()-[r:ZkR]-() REQUIRE r.e IS NOT NULL",
		"CREATE CONSTRAINT zk_nk FOR (n:ZkB) REQUIRE (n.k) IS NODE KEY",
		"CREATE CONSTRAINT zk_rk FOR ()-[r:ZkS]-() REQUIRE (r.k) IS RELATIONSHIP KEY",
		"CREATE CONSTRAINT zk_nt FOR (n:ZkA) REQUIRE n.t IS :: INTEGER",
		"CREATE CONSTRAINT zk_rt FOR ()-[r:ZkR]-() REQUIRE r.t IS :: STRING",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	for _, tc := range []struct {
		kind              string
		cypher5, cypher25 interface{}
	}{
		{"", []string{"zk_ne", "zk_nk", "zk_nt", "zk_nu", "zk_re", "zk_rk", "zk_rt", "zk_ru"}, []string{"zk_ne", "zk_nk", "zk_nt", "zk_nu", "zk_re", "zk_rk", "zk_rt", "zk_ru"}},
		{"ALL", []string{"zk_ne", "zk_nk", "zk_nt", "zk_nu", "zk_re", "zk_rk", "zk_rt", "zk_ru"}, []string{"zk_ne", "zk_nk", "zk_nt", "zk_nu", "zk_re", "zk_rk", "zk_rt", "zk_ru"}},
		{"UNIQUE", []string{"zk_nu", "zk_ru"}, []string{"zk_nu", "zk_ru"}},
		{"NODE UNIQUE", []string{"zk_nu"}, []string{"zk_nu"}},
		{"RELATIONSHIP UNIQUE", []string{"zk_ru"}, []string{"zk_ru"}},
		{"REL UNIQUE", []string{"zk_ru"}, []string{"zk_ru"}},
		{"UNIQUENESS", []string{"zk_nu", "zk_ru"}, []string{"zk_nu", "zk_ru"}},
		{"NODE UNIQUENESS", []string{"zk_nu"}, []string{"zk_nu"}},
		{"RELATIONSHIP UNIQUENESS", []string{"zk_ru"}, []string{"zk_ru"}},
		{"REL UNIQUENESS", []string{"zk_ru"}, []string{"zk_ru"}},
		{"PROPERTY UNIQUENESS", "ERR", []string{"zk_nu", "zk_ru"}},
		{"NODE PROPERTY UNIQUENESS", "ERR", []string{"zk_nu"}},
		{"RELATIONSHIP PROPERTY UNIQUENESS", "ERR", []string{"zk_ru"}},
		{"REL PROPERTY UNIQUENESS", "ERR", []string{"zk_ru"}},
		{"EXISTENCE", []string{"zk_ne", "zk_re"}, []string{"zk_ne", "zk_re"}},
		{"EXIST", []string{"zk_ne", "zk_re"}, []string{"zk_ne", "zk_re"}},
		{"NODE EXISTENCE", []string{"zk_ne"}, []string{"zk_ne"}},
		{"NODE EXIST", []string{"zk_ne"}, []string{"zk_ne"}},
		{"RELATIONSHIP EXISTENCE", []string{"zk_re"}, []string{"zk_re"}},
		{"REL EXISTENCE", []string{"zk_re"}, []string{"zk_re"}},
		{"REL EXIST", []string{"zk_re"}, []string{"zk_re"}},
		{"PROPERTY EXISTENCE", []string{"zk_ne", "zk_re"}, []string{"zk_ne", "zk_re"}},
		{"NODE PROPERTY EXISTENCE", []string{"zk_ne"}, []string{"zk_ne"}},
		{"RELATIONSHIP PROPERTY EXISTENCE", []string{"zk_re"}, []string{"zk_re"}},
		{"REL PROPERTY EXISTENCE", []string{"zk_re"}, []string{"zk_re"}},
		{"PROPERTY EXIST", []string{"zk_ne", "zk_re"}, []string{"zk_ne", "zk_re"}},
		{"KEY", []string{"zk_nk", "zk_rk"}, []string{"zk_nk", "zk_rk"}},
		{"NODE KEY", []string{"zk_nk"}, []string{"zk_nk"}},
		{"RELATIONSHIP KEY", []string{"zk_rk"}, []string{"zk_rk"}},
		{"REL KEY", []string{"zk_rk"}, []string{"zk_rk"}},
		{"PROPERTY TYPE", []string{"zk_nt", "zk_rt"}, []string{"zk_nt", "zk_rt"}},
		{"NODE PROPERTY TYPE", []string{"zk_nt"}, []string{"zk_nt"}},
		{"RELATIONSHIP PROPERTY TYPE", []string{"zk_rt"}, []string{"zk_rt"}},
		{"REL PROPERTY TYPE", []string{"zk_rt"}, []string{"zk_rt"}},
	} {
		for prefix, want := range map[string]interface{}{"CYPHER 5 ": tc.cypher5, "CYPHER 25 ": tc.cypher25} {
			query := prefix + "SHOW " + tc.kind + " CONSTRAINTS YIELD name RETURN collect(name) AS n"
			result, err := exec.Execute(ctx, query, nil)
			if want == "ERR" {
				require.Error(t, err, query)
				requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
				continue
			}
			require.NoError(t, err, query)
			names := toStringSlice(result.Rows[0][0])
			sort.Strings(names)
			if len(want.([]string)) == 0 {
				require.Empty(t, names, query)
				continue
			}
			require.Equal(t, want, names, query)
		}
	}
}

// A Cypher 25 statement lists constraints as Neo4j 2026.09 does: property
// uniqueness type names, enforcedLabel (a default column) and
// classification, and options without an indexProvider. A Cypher 5
// statement keeps Neo4j 5.26's listing. A kind keeps each constraint's id.
func TestShowConstraintsByVersion(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "show_constraints_version"))
	ctx := context.Background()
	for _, statement := range []string{
		"CREATE CONSTRAINT zk_nu FOR (n:ZkA) REQUIRE n.u IS UNIQUE",
		"CREATE CONSTRAINT zk_ru FOR ()-[r:ZkR]-() REQUIRE r.u IS UNIQUE",
		"CREATE CONSTRAINT zk_nk FOR (n:ZkB) REQUIRE (n.k) IS NODE KEY",
		"CREATE CONSTRAINT zk_nt FOR (n:ZkA) REQUIRE n.t IS :: INTEGER",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	rows := func(query string) [][]interface{} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result.Rows
	}
	require.Equal(t, [][]interface{}{
		{"zk_nu", "NODE_PROPERTY_UNIQUENESS", nil, "undesignated", map[string]interface{}{"indexConfig": map[string]interface{}{}}},
		{"zk_ru", "RELATIONSHIP_PROPERTY_UNIQUENESS", nil, "undesignated", map[string]interface{}{"indexConfig": map[string]interface{}{}}},
	}, rows("CYPHER 25 SHOW UNIQUENESS CONSTRAINTS YIELD name, type, enforcedLabel, classification, options RETURN name, type, enforcedLabel, classification, options ORDER BY name"))
	require.Equal(t, [][]interface{}{
		{"zk_nu", "UNIQUENESS", map[string]interface{}{"indexConfig": map[string]interface{}{}, "indexProvider": "range-1.0"}},
		{"zk_ru", "RELATIONSHIP_UNIQUENESS", map[string]interface{}{"indexConfig": map[string]interface{}{}, "indexProvider": "range-1.0"}},
	}, rows("CYPHER 5 SHOW UNIQUENESS CONSTRAINTS YIELD name, type, options RETURN name, type, options ORDER BY name"))
	_, err := exec.Execute(ctx, "CYPHER 5 SHOW CONSTRAINTS YIELD enforcedLabel RETURN *", nil)
	require.Error(t, err)

	result, err := exec.Execute(ctx, "CYPHER 25 SHOW CONSTRAINTS", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"id", "name", "type", "entityType", "labelsOrTypes", "properties", "enforcedLabel", "ownedIndex", "propertyType"}, result.Columns)
	result, err = exec.Execute(ctx, "CYPHER 5 SHOW CONSTRAINTS", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"id", "name", "type", "entityType", "labelsOrTypes", "properties", "ownedIndex", "propertyType"}, result.Columns)

	ids := map[interface{}]interface{}{}
	for _, row := range rows("SHOW CONSTRAINTS YIELD id, name RETURN id, name") {
		ids[row[1]] = row[0]
	}
	for _, kind := range []string{"KEY", "NODE PROPERTY TYPE", "REL UNIQUE"} {
		for _, row := range rows("SHOW " + kind + " CONSTRAINTS YIELD id, name RETURN id, name") {
			require.Equal(t, ids[row[1]], row[0], kind)
		}
	}
}

// Words before CONSTRAINTS that make no kind are Neo4j's SyntaxError in
// both versions (Neo4j 5.26 Enterprise and 2026.09); a SHOW command with
// any other word there isn't a SHOW … CONSTRAINTS command.
func TestShowConstraintKindErrors(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "show_constraint_kind_errors"))
	ctx := context.Background()
	for _, kind := range []string{"PROPERTY KEY", "TYPE", "NODE TYPE", "NODE", "RELATIONSHIP", "ALL NODE", "PROPERTY", "UNIQUE KEY", "NODE PROPERTY", "NODE ALL", "REL NODE UNIQUE"} {
		for _, prefix := range []string{"CYPHER 5 ", "CYPHER 25 "} {
			_, err := exec.Execute(ctx, prefix+"SHOW "+kind+" CONSTRAINTS", nil)
			require.Error(t, err, prefix+kind)
			requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
		}
	}
	for _, command := range []string{"SHOW PROCEDURES YIELD name, constraint", "SHOW INDEXES", "TERMINATE TRANSACTIONS 'x'", "SHOW"} {
		_, isConstraints, err := showConstraintKindOf(command, false)
		require.NoError(t, err, command)
		require.False(t, isConstraints, command)
	}
	_, err := exec.executeShowConstraints(ctx, "SHOW PROPERTY KEY CONSTRAINTS")
	require.Error(t, err)
}
