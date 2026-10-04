package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCanonicalUnwindMergeValue_CoversTypeBranches(t *testing.T) {
	cases := []struct {
		name string
		in   interface{}
		typ  string
	}{
		{name: "string", in: "x", typ: "string"},
		{name: "bool", in: true, typ: "bool"},
		{name: "int", in: int(1), typ: "int"},
		{name: "int8", in: int8(1), typ: "int8"},
		{name: "int16", in: int16(1), typ: "int16"},
		{name: "int32", in: int32(1), typ: "int32"},
		{name: "int64", in: int64(1), typ: "int64"},
		{name: "uint", in: uint(1), typ: "uint"},
		{name: "uint8", in: uint8(1), typ: "uint8"},
		{name: "uint16", in: uint16(1), typ: "uint16"},
		{name: "uint32", in: uint32(1), typ: "uint32"},
		{name: "uint64", in: uint64(1), typ: "uint64"},
		{name: "float32", in: float32(1.25), typ: "float32"},
		{name: "float64", in: 1.25, typ: "float64"},
		{name: "nil", in: nil, typ: "nil"},
		{name: "typed slice fallback", in: []int{1, 2}, typ: "[]int"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, ok := canonicalUnwindMergeValue(tc.in).(map[string]interface{})
			require.True(t, ok)
			require.Equal(t, tc.typ, out["type"])
		})
	}

	mapOut, ok := canonicalUnwindMergeValue(map[string]interface{}{"b": 2, "a": 1}).(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "map", mapOut["type"])
	entries, ok := mapOut["entries"].([]interface{})
	require.True(t, ok)
	require.Len(t, entries, 2)
	firstKV, ok := entries[0].([]interface{})
	require.True(t, ok)
	require.Equal(t, "a", firstKV[0])

	listOut, ok := canonicalUnwindMergeValue([]interface{}{1, "x"}).(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "list", listOut["type"])
}

func TestClauseHelpers_ParseAndRewrite(t *testing.T) {
	require.True(t, isSimplePropertyReference("n.prop"))
	require.True(t, isSimplePropertyReference("a.b.c"))
	require.False(t, isSimplePropertyReference("n..bad"))
	require.False(t, isSimplePropertyReference(""))

	in := "MATCH (a:Person) MATCH (b:Person) WHERE a.id = b.id RETURN a"
	out := rewriteTopLevelMultiMatchToCartesianMatch(in)
	require.Equal(t, "MATCH (a:Person), (b:Person) WHERE a.id = b.id RETURN a", out)
	require.Equal(t, "MATCH (a:Person) RETURN a", rewriteTopLevelMultiMatchToCartesianMatch("MATCH (a:Person) RETURN a"))

	require.True(t, canApplySetBasedUnwindRewrite("UNWIND $rows AS row MATCH (n) RETURN count(n) AS c", []interface{}{1, 2, 3}))
	require.False(t, canApplySetBasedUnwindRewrite("", []interface{}{1}))
	require.False(t, canApplySetBasedUnwindRewrite("UNWIND $rows AS row MATCH (n) RETURN count(n) AS c", []interface{}{1, 1}))
	require.False(t, canApplySetBasedUnwindRewrite("UNWIND $rows AS row MATCH (n) RETURN count(n) AS c", []interface{}{map[string]interface{}{"k": "v"}}))
	require.False(t, canApplySetBasedUnwindRewrite("UNWIND $rows AS row MATCH (n) SET n.x = 1 RETURN count(n) AS c", []interface{}{1}))
}

func TestParseSimpleCountReturnAndRelClause(t *testing.T) {
	alias, ok := parseSimpleCountReturn("RETURN count(n) AS total", "n")
	require.True(t, ok)
	require.Equal(t, "total", alias)

	alias, ok = parseSimpleCountReturn("", "n")
	require.True(t, ok)
	require.Empty(t, alias)

	_, ok = parseSimpleCountReturn("RETURN count(m) AS total", "n")
	require.False(t, ok)
	alias, ok = parseSimpleCountReturn("RETURN count(n)", "n")
	require.True(t, ok)
	require.Equal(t, "count(n)", alias)

	rel, ok := parseUnwindMergeRelationshipClause("MERGE (a)-[r:KNOWS]->(b)")
	require.True(t, ok)
	require.Equal(t, "a", rel.fromVar)
	require.Equal(t, "b", rel.toVar)
	require.Equal(t, "r", rel.relVar)
	require.Equal(t, "KNOWS", rel.relType)

	rel, ok = parseUnwindMergeRelationshipClause("MERGE (a)-[:KNOWS]->(b)")
	require.True(t, ok)
	require.Empty(t, rel.relVar)

	_, ok = parseUnwindMergeRelationshipClause("MERGE (a)<-[:KNOWS]-(b)")
	require.False(t, ok)
	_, ok = parseUnwindMergeRelationshipClause("MATCH (a)-[:KNOWS]->(b)")
	require.False(t, ok)
}
