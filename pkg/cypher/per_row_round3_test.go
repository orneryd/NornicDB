package cypher

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// These literal expectations were obtained independently from Neo4j 5.26.30.
func TestPerRowRound3Neo4jRegressions(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		want        [][]interface{}
	}{
		{"node_identity", "CREATE (n:N {v:1}) WITH n MATCH (m:N) SET n.v=2 RETURN n=m AS eq,n IN [m] AS mem,CASE n WHEN m THEN 1 ELSE 0 END AS c", [][]interface{}{{true, true, int64(1)}}},
		{"edge_identity", "CREATE ()-[r:R {v:1}]->() WITH r MATCH ()-[s:R]->() SET r.v=2 RETURN r=s AS eq,r IN [s] AS mem,CASE r WHEN s THEN 1 ELSE 0 END AS c", [][]interface{}{{true, true, int64(1)}}},
		{"pattern_or", "CREATE (n:N)-[:R]->(:N) WITH n,0 AS x RETURN abs(CASE WHEN size([(n)-->(m) | 1/x])=0 OR x=0 THEN 1 ELSE 0 END) AS v", [][]interface{}{{int64(1)}}},
		{"null_substring", "UNWIND [null,null] AS x RETURN substring(x,-1) AS v", [][]interface{}{{nil}, {nil}}},
		{"null_substring_property", "CREATE (a:SubA)-[:SUB]->(:SubB) WITH a MATCH (a)-[:SUB]->(b) RETURN substring(a.missing,-1) AS v", [][]interface{}{{nil}}},
		{"integer_equality", "UNWIND [0,1] AS x RETURN x+1=1 AS v", [][]interface{}{{true}, {false}}},
		{"labels_owner", "CREATE (n:LabelsR3) RETURN labels(n) AS v", [][]interface{}{{[]interface{}{"LabelsR3"}}}},
		{"type_owner", "CREATE ()-[r:TYPE_R3]->() RETURN type(r) AS v", [][]interface{}{{"TYPE_R3"}}},
	} {
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/text=%v", tc.name, text), func(t *testing.T) {
				rowValueProgramsDisabled.Store(text)
				defer rowValueProgramsDisabled.Store(false)
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r3"))
				result, err := exec.Execute(context.Background(), tc.query, nil)
				require.NoError(t, err)
				require.Equal(t, tc.want, result.Rows)
			})
		}
	}
}

// Neo4j's exact message is compared separately from the Bolt status prefix
// that NornicDB includes when formatting a SemanticError for API callers.
func requireRound3Neo4jError(t *testing.T, err error, want string) {
	t.Helper()
	require.Error(t, err)
	var semantic *SemanticError
	if errors.As(err, &semantic) {
		require.Equal(t, want, semantic.Message)
	} else {
		require.Equal(t, want, err.Error())
	}
}

func TestPerRowRound3ReachedErrors(t *testing.T) {
	for _, tc := range []struct{ query, message string }{
		{"UNWIND ['a','a'] AS x RETURN substring(x,-1) AS v", "Cannot handle negative start index nor negative length"},
	} {
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/text=%v", tc.query, text), func(t *testing.T) {
				rowValueProgramsDisabled.Store(text)
				defer rowValueProgramsDisabled.Store(false)
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r3errors"))
				_, err := exec.Execute(context.Background(), tc.query, nil)
				requireRound3Neo4jError(t, err, tc.message)
			})
		}
	}
}

func TestPerRowRound3TypedEqualityAllocations(t *testing.T) {
	for _, operator := range []string{"=", "<>", "!="} {
		value := int64(4096)
		want := operator == "="
		allocations := testing.AllocsPerRun(100, func() {
			value++
			require.Equal(t, want, compareCypherPredicateValue(value, value, operator))
		})
		require.Zero(t, allocations, operator)
	}
}

// An indexed IN must use the same identity equality as the literal Neo4j
// snapshot cases above, even though graph pointers are comparable Go values.
func TestPerRowRound3IndexedEntityEquality(t *testing.T) {
	for _, snapshots := range [][2]interface{}{
		{&storage.Node{ID: "same", Properties: map[string]interface{}{"v": int64(2)}}, &storage.Node{ID: "same", Properties: map[string]interface{}{"v": int64(1)}}},
		{&storage.Edge{ID: "same", Properties: map[string]interface{}{"v": int64(2)}}, &storage.Edge{ID: "same", Properties: map[string]interface{}{"v": int64(1)}}},
	} {
		exec := &StorageExecutor{}
		predicate := exec.makeCompiledParamMembershipPredicate(func(binding, map[string]interface{}) (interface{}, bool) {
			return snapshots[0], true
		}, "matches")
		require.Equal(t, truthTrue, predicate(nil, map[string]interface{}{"matches": []interface{}{snapshots[1]}}))
	}
}
