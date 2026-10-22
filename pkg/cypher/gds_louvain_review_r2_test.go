package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestReviewR2Differential(t *testing.T) {
	path := os.Getenv("NORNICDB_LOUVAIN_REFERENCE_CASES")
	if path == "" {
		t.Skip("set NORNICDB_LOUVAIN_REFERENCE_CASES to run the live GDS comparison")
	}
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var cases []struct {
		Name        string
		N           int
		Edges       [][]*float64
		Orientation string
		Seeds       []*int64
		Config      map[string]interface{}
		Expected    []json.RawMessage
	}
	require.NoError(t, json.Unmarshal(data, &cases))
	failures := 0
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			graphProjections = make(map[string]*GraphProjection)
			engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "reviewr2")
			exec := NewStorageExecutor(engine)
			for i := 0; i < c.N; i++ {
				props := map[string]interface{}{}
				if c.Seeds != nil && c.Seeds[i] != nil {
					props["seed"] = *c.Seeds[i]
				}
				_, err := engine.CreateNode(gdsNode(fmt.Sprintf("n%03d", i), "N", props))
				require.NoError(t, err)
			}
			for i, e := range c.Edges {
				props := map[string]interface{}{}
				if e[2] != nil {
					props["w"] = *e[2]
				}
				require.NoError(t, engine.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e%04d", i)), StartNode: storage.NodeID(fmt.Sprintf("n%03d", int(*e[0]))), EndNode: storage.NodeID(fmt.Sprintf("n%03d", int(*e[1]))), Type: "R", Properties: props}))
			}
			np := "'N'"
			if c.Seeds != nil {
				np = "{N:{properties:{seed:{defaultValue:null}}}}"
			}
			rp := fmt.Sprintf("{R:{orientation:'%s',properties:{w:{defaultValue:null}}}}", c.Orientation)
			if len(c.Edges) == 0 {
				rp = "'*'"
			}
			_, err := exec.Execute(context.Background(), "CALL gds.graph.project('review',"+np+","+rp+")", nil)
			require.NoError(t, err)
			for _, k := range []string{"concurrency", "maxLevels", "maxIterations", "minCommunitySize"} {
				if v, ok := c.Config[k].(float64); ok {
					c.Config[k] = int64(v)
				}
			}
			result, err := exec.callGdsLouvainStream(context.Background(), []interface{}{"review", c.Config})
			require.NoError(t, err)
			var got [][]interface{}
			for i, r := range result.Rows {
				got = append(got, []interface{}{int64(i), r[1], r[2]})
			}
			actual, err := json.Marshal(got)
			require.NoError(t, err)
			expected, err := json.Marshal(c.Expected)
			require.NoError(t, err)
			if string(actual) != string(expected) {
				failures++
				t.Errorf("expected %s, got %s", expected, actual)
			}
		})
	}
	t.Logf("Compared %d cases; differing cases=%d", len(cases), failures)
}

func TestReviewR2SeedTypes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		value     interface{}
		wantError bool
	}{
		{"integer_list", []int64{10, 11}, true},
		{"float_list", []float64{10, 11}, true},
		{"negative_one", int64(-1), true},
		{"negative_two", int64(-2), true},
		{"long_min_missing", int64(-9223372036854775808), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			graphProjections = make(map[string]*GraphProjection)
			other := tc.value
			if _, ok := tc.value.(int64); ok {
				other = int64(2)
			}
			exec, run := newGDSEngineGraph(t, "seedtype", gdsNode("n0", "N", map[string]interface{}{"seed": tc.value}), gdsNode("n1", "N", map[string]interface{}{"seed": other}))
			_, err := run("MATCH (n:N) CREATE (n)-[:R]->(n)")
			require.NoError(t, err)
			_, err = run("CALL gds.graph.project('review',{N:{properties:'seed'}},{R:{orientation:'UNDIRECTED'}})")
			require.NoError(t, err)
			result, err := exec.callGdsLouvainStream(context.Background(), []interface{}{"review", map[string]interface{}{"seedProperty": "seed"}})
			t.Logf("rows=%+v error=%v", result, err)
			if tc.wantError {
				require.Error(t, err)
				if tc.name == "integer_list" || tc.name == "float_list" {
					require.ErrorContains(t, err, "does not comprise exclusively of long values")
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestReviewR2DirectedCycle(t *testing.T) {
	for _, tc := range []struct {
		orientation string
		want        []int64
	}{
		{"NATURAL", []int64{0, 0, 1}},
		{"REVERSE", []int64{0, 1, 0}},
		{"UNDIRECTED", []int64{0, 0, 0}},
	} {
		t.Run(tc.orientation, func(t *testing.T) {
			graphProjections = make(map[string]*GraphProjection)
			exec, run := newGDSEngineGraph(t, "cycle", gdsNode("n0", "N", nil), gdsNode("n1", "N", nil), gdsNode("n2", "N", nil))
			for _, ends := range [][2]string{{"n0", "n1"}, {"n1", "n2"}, {"n2", "n0"}} {
				_, err := run(fmt.Sprintf("MATCH (a),(b) WHERE id(a)='%s' AND id(b)='%s' CREATE (a)-[:R]->(b)", ends[0], ends[1]))
				require.NoError(t, err)
			}
			_, err := run(fmt.Sprintf("CALL gds.graph.project('review','N',{R:{orientation:'%s'}})", tc.orientation))
			require.NoError(t, err)
			result, err := exec.callGdsLouvainStream(context.Background(), []interface{}{"review", map[string]interface{}{"consecutiveIds": true, "concurrency": int64(1)}})
			require.NoError(t, err)
			var got []int64
			for _, row := range result.Rows {
				got = append(got, row[1].(int64))
			}
			t.Logf("GDS %v NornicDB %v", tc.want, got)
			require.Equal(t, tc.want, got)
		})
	}
}
