package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The reference oracle's equality column controls predicates. Its independent
// equivalence column controls DISTINCT/grouping, never candidate join keys.
// Cover the public read and write paths for the entire generated population,
// including the nested property values supported by NornicDB's storage engine.
func TestCartesianTemporalContracts(t *testing.T) {
	for _, programs := range []bool{true, false} {
		t.Run(fmt.Sprintf("programs=%v", programs), func(t *testing.T) {
			rowValueProgramsDisabled.Store(!programs)
			defer rowValueProgramsDisabled.Store(false)
			testCartesianTemporalContractCases(t)
		})
	}
}

func testCartesianTemporalContractCases(t *testing.T) {
	for _, tc := range temporalContractCases(t) {
		t.Run(tc.Name, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporal-join")
			exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
			ctx := context.Background()
			values, err := exec.Execute(ctx, "RETURN "+tc.A+" AS a,"+tc.B+" AS b", nil)
			require.NoError(t, err)
			for i, label := range []string{"TemporalLeft", "TemporalRight", "TemporalThird"} {
				value := values.Rows[0][i%2]
				_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(label), Labels: []string{label}, Properties: map[string]interface{}{"t": value}})
				require.NoError(t, err)
			}
			// A projection is the unoptimized predicate control. The hash join
			// must not discard its true result before WHERE is evaluated.
			control, err := exec.Execute(ctx, "MATCH (a:TemporalLeft), (b:TemporalRight) RETURN a.t = b.t AS eq", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tc.Equality}}, control.Rows)
			want := int64(0)
			if tc.Equality == true {
				want = 1
			}
			// coalesce prevents Neo4j 5.26.30's own equality-join extraction,
			// which otherwise drops overlap rows despite projected equality.
			for _, predicate := range []string{"coalesce(a.t = b.t,false)", "a.t = b.t", "b.t = a.t"} {
				result, err := exec.Execute(ctx, "MATCH (a:TemporalLeft), (b:TemporalRight) WHERE "+predicate+" RETURN count(*) AS n", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{want}}, result.Rows, predicate)
			}
			// The third equality closes a cycle: index construction, lookups
			// in both expansion directions and the final constraint check
			// must all select the same equality contract before CREATE.
			write, err := exec.Execute(ctx, "MATCH (a:TemporalLeft), (b:TemporalRight), (c:TemporalThird) WHERE a.t = b.t AND c.t = b.t AND a.t = c.t CREATE (a)-[:TEMPORAL_JOIN]->(c) RETURN count(*) AS n", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{want}}, write.Rows)
			persisted, err := exec.Execute(ctx, "MATCH (:TemporalLeft)-[r:TEMPORAL_JOIN]->(:TemporalThird) RETURN count(r) AS n", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{want}}, persisted.Rows)
			// Exercise the legacy pushdown owner as well as the public
			// pipeline's indexed join; both use cartesianValueKey.
			if tc.Equality == true {
				left, err := store.GetNode("TemporalLeft")
				require.NoError(t, err)
				right, err := store.GetNode("TemporalRight")
				require.NoError(t, err)
				matches := []struct {
					variable string
					nodes    []*storage.Node
				}{{"a", []*storage.Node{left}}, {"b", []*storage.Node{right}}}
				predicateContext, statement := expressionTestContext(ctx, "a.t = b.t")
				filtered := exec.applyCartesianWherePushdown(statement, predicateContext, matches, "a.t = b.t")
				require.Len(t, filtered[0].nodes, 1)
				require.Len(t, filtered[1].nodes, 1)
			}
		})
	}
}
