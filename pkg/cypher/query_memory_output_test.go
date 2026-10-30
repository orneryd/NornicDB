package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func queryMemoryOutputFixture(t *testing.T) (*StorageExecutor, *storage.BadgerEngine) {
	be, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })
	schema := be.GetSchemaForNamespace("review")
	for _, edge := range []bool{false, true} {
		name, scope := "doc", knowledgepolicy.ScopeNode
		if edge {
			name, scope = "link", knowledgepolicy.ScopeEdge
		}
		require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: name, Scope: scope, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600 * 24 * 365, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
		binding := knowledgepolicy.DecayProfileBinding{Name: name, ProfileRef: name, PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: "secret", HalfLifeSeconds: 1}}}
		if edge {
			binding.IsEdge = true
			binding.TargetEdgeType = "LINK"
		} else {
			binding.TargetLabels = []string{"Doc"}
		}
		require.NoError(t, schema.CreateDecayProfileBinding(binding))
	}
	be.SetDecayEnabled(true)
	old := time.Now().Add(-time.Hour)
	for _, id := range []storage.NodeID{"review:a", "review:b"} {
		_, err = be.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"secret": "node-hidden", "title": string(id)}, CreatedAt: old, UpdatedAt: old})
		require.NoError(t, err)
	}
	for i := 0; i < 2; i++ {
		require.NoError(t, be.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("review:r%d", i)), StartNode: "review:a", EndNode: "review:b", Type: "LINK", Properties: map[string]interface{}{"secret": "edge-hidden", "weight": int64(7)}, CreatedAt: old, UpdatedAt: old}))
	}
	return NewStorageExecutor(storage.NewNamespacedEngine(be, "review"), QueryCachePolicy{MaxEntries: 0, TTL: 0}), be
}
func queryMemoryOutputCheck(t *testing.T, value interface{}) {
	t.Helper()
	switch v := value.(type) {
	case *storage.Node:
		if v != nil {
			queryMemoryOutputCheck(t, *v)
		}
		return
	case storage.Node:
		require.NotContains(t, v.Properties, "secret")
		if containsString(v.Labels, "Doc") {
			require.NotEmpty(t, v.Properties["title"])
		}
		return
	case *storage.Edge:
		if v != nil {
			queryMemoryOutputCheck(t, *v)
		}
		return
	case storage.Edge:
		require.NotContains(t, v.Properties, "secret")
		if v.Type == "LINK" {
			require.Equal(t, int64(7), v.Properties["weight"])
		}
		return
	case storage.RemoteEntityValue, storage.RemotePathValue:
		return
	case PathResult:
		queryMemoryOutputCheck(t, v.Nodes)
		queryMemoryOutputCheck(t, v.Relationships)
		return
	case *PathResult:
		if v != nil {
			queryMemoryOutputCheck(t, *v)
		}
		return
	case map[string]interface{}:
		for _, x := range v {
			queryMemoryOutputCheck(t, x)
		}
		return
	}
	if value != nil {
		v := reflect.ValueOf(value)
		if v.Kind() == reflect.Slice || v.Kind() == reflect.Array {
			for i := 0; i < v.Len(); i++ {
				queryMemoryOutputCheck(t, v.Index(i).Interface())
			}
		}
		if v.Kind() == reflect.Map {
			entries := v.MapRange()
			for entries.Next() {
				queryMemoryOutputCheck(t, entries.Value().Interface())
			}
		}
	}
}
func TestQueryMemoryOutputStreamedCarrierVisibility(t *testing.T) {
	for _, query := range []string{
		"MATCH (a:Doc)-[r:LINK]->(b) RETURN a,r",
		"MATCH ()-[r:LINK]->() RETURN r",
		"MATCH p=(:Doc)-[:LINK]->() RETURN p",
		"MATCH p=(a:Doc)-[r:LINK]->(b) RETURN {n:a,r:r,p:p}",
		"MATCH p=(a:Doc)-[r:LINK]->(b) RETURN [a,r,p]",
		"MATCH (a:Doc)-[r:LINK]->(b) WITH a AS n, r AS rel RETURN n,rel",
	} {
		t.Run(query, func(t *testing.T) {
			exec, _ := queryMemoryOutputFixture(t)
			result, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 2)
			t.Run("materialized", func(t *testing.T) { queryMemoryOutputCheck(t, result.Rows) })
			t.Run("streamed", func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				run := executeStreamed(ctx, exec, query, 1)
				defer func() { cancel(); <-run.stream.Done() }()
				require.True(t, run.stream.streamed(), "%v", run.errIfDone())
				for {
					rows, done := run.stream.Next(1)
					queryMemoryOutputCheck(t, rows)
					if done {
						break
					}
				}
				<-run.stream.Done()
				require.NoError(t, run.err)
			})
		})
	}
}
func TestQueryMemoryOutputCarrierRepresentations(t *testing.T) {
	exec, be := queryMemoryOutputFixture(t)
	n, err := be.GetQueryNode("review:a")
	require.NoError(t, err)
	r, err := be.GetEdge("review:r0")
	require.NoError(t, err)
	p := PathResult{Nodes: []*storage.Node{n}, Relationships: []*storage.Edge{r}, Length: 1}
	for name, value := range map[string]interface{}{
		"node_pointer": n, "node_value": *n, "edge_pointer": r, "edge_value": *r,
		"path_pointer": &p, "path_value": p,
		"node_pointer_list": []*storage.Node{n}, "node_value_list": []storage.Node{*n},
		"edge_pointer_list": []*storage.Edge{r}, "edge_value_list": []storage.Edge{*r},
		"path_pointer_list": []*PathResult{&p}, "path_value_list": []PathResult{p},
		"map": map[string]interface{}{"n": n, "p": p}, "map_list": []map[string]interface{}{{"n": n, "p": p}},
		"list":       []interface{}{n, r, p},
		"node_array": [1]storage.Node{*n}, "edge_array": [1]storage.Edge{*r},
		"path_array": [1]PathResult{p}, "typed_map": map[string]storage.Node{"node": *n},
		"nested": []interface{}{map[string]interface{}{"entities": []interface{}{*n, *r, []PathResult{p}}}},
	} {
		t.Run(name, func(t *testing.T) {
			result, err := exec.Execute(context.Background(), "RETURN $v", map[string]interface{}{"v": value})
			require.NoError(t, err)
			queryMemoryOutputCheck(t, result.Rows)
			require.Equal(t, "node-hidden", n.Properties["secret"], "parameter/mutation owner remains complete")
			require.Equal(t, "edge-hidden", r.Properties["secret"], "parameter/mutation owner remains complete")
		})
	}
}
func TestQueryMemoryOutputScoredStreamRetention(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			be, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
			require.NoError(t, err)
			defer be.Close()
			be.SetDecayEnabled(enabled)
			for _, id := range []storage.NodeID{"review:a", "review:b"} {
				_, err = be.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": string(id)}})
				require.NoError(t, err)
			}
			for i := 0; i < 16384; i++ {
				require.NoError(t, be.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("review:r%06d", i)), StartNode: "review:a", EndNode: "review:b", Type: "LINK", Properties: map[string]interface{}{"secret": strings.Repeat(fmt.Sprintf("%08d", i), 512)}}))
			}
			exec := NewStorageExecutor(storage.NewNamespacedEngine(be, "review"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			run := executeStreamed(ctx, exec, "MATCH ()-[r:LINK]->() RETURN r.secret", 1)
			defer func() { cancel(); <-run.stream.Done() }()
			require.True(t, run.stream.streamed())
			rows := 0
			for rows < 8192 {
				batch, done := run.stream.Next(128)
				rows += len(batch)
				require.False(t, done)
			}
			runtime.GC()
			runtime.ReadMemStats(&after)
			live := int64(after.HeapAlloc) - int64(before.HeapAlloc)
			t.Logf("enabled=%v consumed=%d retained=%d", enabled, rows, live)
			require.Less(t, live, int64(16<<20), "scalar stream must not retain consumed relationship bodies")
		})
	}
}
