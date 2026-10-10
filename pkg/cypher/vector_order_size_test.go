package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// VECTOR and UUID in ORDER BY, size() in every clause, vectors read by
// vector.similarity.* (#907). Every expected value is Neo4j 2026.09's
// (sweep cases 900047, 900048, 900054, 900105, 900115, 900117, 900123,
// 900125).
func TestVectorAndUUIDOrderAndSize(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "vector_order"))
	ctx := context.Background()
	l := func(values ...interface{}) []interface{} { return values }
	for query, want := range map[string][][]interface{}{
		"UNWIND [vector([2, 2], 2, INTEGER), vector([1, 2], 2, INTEGER)] AS v RETURN toIntegerList(v) AS l ORDER BY v": {
			{l(int64(1), int64(2))}, {l(int64(2), int64(2))}},
		"UNWIND [vector([1, 2], 2, INTEGER), vector([1], 1, INTEGER)] AS v RETURN toIntegerList(v) AS l ORDER BY v": {
			{l(int64(1))}, {l(int64(1), int64(2))}},
		"UNWIND [vector([2], 1, INTEGER), vector([1, 2], 2, INTEGER), vector([1, 1, 1], 3, INTEGER)] AS v RETURN toIntegerList(v) AS l ORDER BY v": {
			{l(int64(2))}, {l(int64(1), int64(2))}, {l(int64(1), int64(1), int64(1))}},
		"UNWIND [vector([1.0], 1, FLOAT), vector([1.0], 1, FLOAT32)] AS v RETURN valueType(v) AS t ORDER BY v": {
			{"VECTOR<FLOAT32 NOT NULL>(1) NOT NULL"}, {"VECTOR<FLOAT NOT NULL>(1) NOT NULL"}},
		"UNWIND [uuid(1, 3), uuid(1, 2), uuid(-1, 0)] AS u RETURN toString(u) AS s ORDER BY u": {
			{"00000000-0000-0001-0000-000000000002"}, {"00000000-0000-0001-0000-000000000003"}, {"ffffffff-ffff-ffff-0000-000000000000"}},
		"UNWIND [uuid(-1, 0), uuid(1, 0), uuid(0, -1), uuid(0, 1)] AS u RETURN toString(u) AS s ORDER BY u": {
			{"00000000-0000-0000-0000-000000000001"}, {"00000000-0000-0000-ffff-ffffffffffff"}, {"00000000-0000-0001-0000-000000000000"}, {"ffffffff-ffff-ffff-0000-000000000000"}},
		"RETURN vector.similarity.cosine(vector([1, 0], 2, INTEGER), [1.0, 0.0]) AS s":                    {{1.0}},
		"RETURN vector.similarity.euclidean(vector([1, 0], 2, INTEGER), vector([1, 0], 2, INTEGER)) AS s": {{1.0}},
		"WITH vector([1, 2], 2, INTEGER) AS v RETURN size(v) AS s":                                        {{int64(2)}},
		"UNWIND [vector([1, 2], 2, INTEGER)] AS v WITH v WHERE size(v) = 2 RETURN count(*) AS c":          {{int64(1)}},
		"WITH [vector([1, 2, 3], 3, FLOAT)] AS vs RETURN [v IN vs | size(v)] AS s":                        {{l(int64(3))}},
	} {
		result, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}
}
