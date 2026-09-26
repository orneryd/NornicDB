package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestValueTypeNamesComeFromOneTable: each style's name for a value comes
// from the one classification (#657).
func TestValueTypeNamesComeFromOneTable(t *testing.T) {
	for _, tt := range []struct {
		value                             interface{}
		cypher, runtime, typeSystem, apoc string
	}{
		{nil, "Null", "NoValue", "NULL", "NULL"},
		{true, "Boolean", "Boolean", "BOOLEAN", "BOOLEAN"},
		{int64(1), "Integer", "Long", "INTEGER", "INTEGER"},
		{1.5, "Float", "Double", "FLOAT", "FLOAT"},
		{"x", "String", "String", "STRING", "STRING"},
		{map[string]interface{}{"a": 1}, "Map", "Map", "MAP", "MAP"},
		{[]interface{}{int64(1), "a"}, "List", "List", "LIST", "LIST"},
		{&storage.Node{}, "Node", "NodeIdReference", "NODE", "NODE"},
		{&storage.Edge{}, "Relationship", "RelationshipReference", "RELATIONSHIP", "RELATIONSHIP"},
		{&PathResult{}, "Path", "Path", "PATH", "PATH"},
		{map[string]interface{}{"_pathResult": true}, "Path", "Path", "PATH", "PATH"},
		{CypherDate{Time: time.Now()}, "Date", "Date", "DATE", "DATE"},
		{CypherDateTime{Time: time.Now()}, "DateTime", "DateTime", "ZONED DATETIME", "DATE_TIME"},
		{CypherLocalDateTime{Time: time.Now()}, "LocalDateTime", "LocalDateTime", "LOCAL DATETIME", "LOCAL_DATE_TIME"},
		{&CypherDuration{}, "Duration", "Duration", "DURATION", "DURATION"},
	} {
		require.Equal(t, tt.cypher, cypherTypeName(tt.value), "%#v", tt.value)
		require.Equal(t, tt.runtime, neo4jValueTypeName(tt.value), "%#v", tt.value)
		require.Equal(t, tt.typeSystem, cypherTypeSystemName(tt.value), "%#v", tt.value)
		require.Equal(t, tt.apoc, apocValueTypeName(tt.value), "%#v", tt.value)
	}
	require.Equal(t, "LongArray", neo4jValueTypeName([]int64{1, 2}))
	require.Equal(t, "StringArray", neo4jValueTypeName([]string{"a"}))
	require.Equal(t, "LongArray[1, 2]", neo4jValueRepr([]int64{1, 2}))
}

// TestTypeErrorMessagesNameCypherTypes: errors name a value's Cypher type,
// never its Go type (int64, []interface {}, …).
func TestTypeErrorMessagesNameCypherTypes(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "typenames"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:TypeNames {a: 1})", nil)
	require.NoError(t, err)
	for query, want := range map[string]string{
		"MATCH (n:TypeNames) WITH n, 1 AS v SET n = v":         "got type Integer",
		"MATCH (n:TypeNames) WITH n, [1, 'a'] AS v SET n += v": "got type List",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), want, query)
		require.NotContains(t, err.Error(), "int64", query)
		require.NotContains(t, err.Error(), "interface", query)
	}
}
