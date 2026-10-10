package cypher

import (
	"context"
	"errors"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// The VECTOR / UUID functions on every path (#907). Statements carry Neo4j
// 2026.09's answers from the differential sweep (cases_types.jsonl); the
// direct calls cover what the compile-time checks keep a statement from
// reaching (argument counts, argument types).
func TestVectorAndUUIDFunctionPaths(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "vector_paths"))
	ctx := context.Background()
	for query, want := range map[string]interface{}{
		"RETURN toIntegerList(vector([1.5, 2.7, -3.2], 3, INTEGER))":                              []interface{}{int64(1), int64(2), int64(-3)},
		"RETURN toFloatList(vector('[1.5, 2]', 2, FLOAT))":                                        []interface{}{1.5, 2.0},
		"RETURN vector_distance(vector([1, 2], 2, FLOAT), vector([2, 4], 2, FLOAT), HAMMING)":     2.0,
		"RETURN vector_distance(vector([3, 1], 2, INTEGER), vector([1, 2], 2, INTEGER), HAMMING)": 2.0,
		"RETURN uuid() IS :: UUID":                  true,
		"RETURN size(toString(uuid()))":             int64(36),
		"RETURN substring(toString(uuid()), 14, 1)": "7",
		"RETURN uuid.mostSignificantBits(uuid('00000000-0000-0001-0000-000000000002'))":                      int64(1),
		"RETURN uuid.leastSignificantBits(uuid(1, 2))":                                                       int64(2),
		"RETURN uuid.leastSignificantBits(null)":                                                             nil,
		"RETURN uuid('550E8400-E29B-41D4-A716-446655440000') = uuid('550e8400-e29b-41d4-a716-446655440000')": true,
		"RETURN uuid(null)": nil,
	} {
		result, err := exec.Execute(ctx, "CYPHER 25 "+query+" AS x", nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}

	// Direct calls: argument expressions evaluate to the values below.
	failed := errors.New("evaluation failed")
	values := map[string]interface{}{"v": CypherVector{Type: VectorInteger64, Ints: []int64{1, 2}}, "s": "x", "n": nil, "i": int64(1), "big": int64(5000)}
	call := cypherfn.Context{Eval: func(expr string) (interface{}, error) {
		if expr == "fail" {
			return nil, failed
		}
		return values[expr], nil
	}}
	for name, run := range map[string]func() (interface{}, error){
		"vector count":           func() (interface{}, error) { return fnVector(call, []string{"v", "i"}) },
		"vector eval":            func() (interface{}, error) { return fnVector(call, []string{"fail", "i", "'INTEGER'"}) },
		"vector dimension type":  func() (interface{}, error) { return fnVector(call, []string{"v", "s", "'INTEGER'"}) },
		"vector dimension range": func() (interface{}, error) { return fnVector(call, []string{"v", "big", "'INTEGER'"}) },
		"dimension count count":  func() (interface{}, error) { return fnVectorDimensionCount(call, nil) },
		"dimension count eval":   func() (interface{}, error) { return fnVectorDimensionCount(call, []string{"fail"}) },
		"dimension count type":   func() (interface{}, error) { return fnVectorDimensionCount(call, []string{"s"}) },
		"distance count":         func() (interface{}, error) { return fnVectorDistance(call, []string{"v"}) },
		"distance eval":          func() (interface{}, error) { return fnVectorDistance(call, []string{"fail", "v", "'COSINE'"}) },
		"distance a":             func() (interface{}, error) { return fnVectorDistance(call, []string{"s", "v", "'COSINE'"}) },
		"distance b":             func() (interface{}, error) { return fnVectorDistance(call, []string{"v", "s", "'COSINE'"}) },
		"norm count":             func() (interface{}, error) { return fnVectorNorm(call, []string{"v"}) },
		"norm eval":              func() (interface{}, error) { return fnVectorNorm(call, []string{"fail", "'EUCLIDEAN'"}) },
		"norm type":              func() (interface{}, error) { return fnVectorNorm(call, []string{"s", "'EUCLIDEAN'"}) },
		"uuid count":             func() (interface{}, error) { return fnUUID(call, []string{"i", "i", "i"}) },
		"uuid eval":              func() (interface{}, error) { return fnUUID(call, []string{"fail"}) },
		"uuid text type":         func() (interface{}, error) { return fnUUID(call, []string{"i"}) },
		"uuid msb type":          func() (interface{}, error) { return fnUUID(call, []string{"s", "i"}) },
		"uuid lsb type":          func() (interface{}, error) { return fnUUID(call, []string{"i", "s"}) },
		"half count":             func() (interface{}, error) { return fnUUIDHalf(false)(call, nil) },
		"half eval":              func() (interface{}, error) { return fnUUIDHalf(true)(call, []string{"fail"}) },
		"half type":              func() (interface{}, error) { return fnUUIDHalf(false)(call, []string{"s"}) },
	} {
		_, err := run()
		require.Error(t, err, name)
	}
	_, err := fnUUIDHalf(false)(call, []string{"s"})
	require.ErrorContains(t, err, "uuid.leastSignificantBits", "each half names itself")
	count, err := fnVectorDimensionCount(cypherfn.Context{Eval: func(string) (interface{}, error) {
		return &CypherVector{Type: VectorFloat32, Floats: []float64{1, 2, 3}}, nil
	}}, []string{"p"})
	require.NoError(t, err)
	require.Equal(t, int64(3), count, "a vector given by pointer")

	items, err := vectorItems("[1, , 2]")
	require.NoError(t, err)
	require.Equal(t, []interface{}{1.0, 2.0}, items, "an empty item in the text is skipped")
	_, err = vectorItems("[1, x]")
	require.Error(t, err)
	_, err = vectorItems(true)
	require.Error(t, err)
	vector, err := newCypherVector([]interface{}{float32(1.5)}, VectorFloat64)
	require.NoError(t, err)
	require.Equal(t, []float64{1.5}, vector.Floats, "a stored float32 coordinate")
	_, err = newCypherVector([]interface{}{"a"}, VectorInteger64)
	require.Error(t, err)
	require.Equal(t, 0, compareVectors(CypherVector{Type: VectorFloat64, Floats: []float64{1.5}}, CypherVector{Type: VectorFloat64, Floats: []float64{1.5}}))

	_, ok := parseCypherUUID("550e8400-e29b-41d4-a716-44665544-000")
	require.False(t, ok, "a fifth hyphen")
	var decodedVector CypherVector
	require.Error(t, msgpack.Unmarshal([]byte{0xc7, 9, 48, 0, 1}, &decodedVector), "truncated vector")
	var decodedUUID CypherUUID
	require.Error(t, msgpack.Unmarshal([]byte{0xd8, 49, 1, 2}, &decodedUUID), "truncated UUID")

	// The rewrite leaves a namespaced name and an unclosed call alone, and
	// takes only a bare name as a coordinate type.
	for _, query := range []string{"RETURN gds.vector([1], 1, INTEGER)", "RETURN vector([1], 1, INTEGER"} {
		rewritten, _, _ := desugarLabelExpressions(query)
		require.NotContains(t, rewritten, vectorFunction, query)
	}
	require.False(t, isBareName("INT-8"))
}
