package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCreateSingleNodeNamedPathPublic(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			old := config.GetParserType()
			config.SetParserType(parser)
			defer config.SetParserType(old)
			for _, route := range []string{"native", "fabric"} {
				for _, name := range []string{"p", "limit", "skip"} {
					for _, props := range []string{"", " {v:7}"} {
						t.Run(route+"/"+name+props, func(t *testing.T) {
							e := newFabricCallRowsExecutor(t)
							prefix := ""
							if route == "fabric" {
								prefix = "USE comp.one "
							}
							q := fmt.Sprintf("%sCREATE %s = (n:SingleNodePath%s) RETURN %s AS path, n AS node, length(%s) AS hops, size(nodes(%s)) AS ns, size(relationships(%s)) AS rs", prefix, name, props, name, name, name, name)
							r, err := e.Execute(t.Context(), q, nil)
							require.NoError(t, err)
							require.Len(t, r.Rows, 1)
							require.Len(t, r.Rows[0], 5)
							require.Equal(t, []interface{}{int64(0), int64(1), int64(0)}, r.Rows[0][2:])
							require.NotNil(t, r.Stats)
							require.Equal(t, 1, r.Stats.NodesCreated)
							require.Zero(t, r.Stats.RelationshipsCreated)
							require.Equal(t, 1, r.Stats.LabelsAdded)
							path, ok := r.Rows[0][0].(map[string]interface{})
							require.True(t, ok)
							raw, ok := path["_pathResult"].(PathResult)
							require.True(t, ok)
							require.Len(t, raw.Nodes, 1)
							require.Nil(t, raw.Relationships)
							require.Zero(t, raw.Length)
							node, ok := r.Rows[0][1].(*storage.Node)
							require.True(t, ok)
							require.Equal(t, node.ID, raw.Nodes[0].ID)
							require.Equal(t, []string{"SingleNodePath"}, node.Labels)
							if props == "" {
								require.Empty(t, node.Properties)
							} else {
								require.Equal(t, int64(7), node.Properties["v"])
								require.Equal(t, 1, r.Stats.PropertiesSet)
							}
							read, err := e.Execute(t.Context(), prefix+"MATCH (n:SingleNodePath) RETURN n", nil)
							require.NoError(t, err)
							require.Len(t, read.Rows, 1)
							stored, ok := read.Rows[0][0].(*storage.Node)
							require.True(t, ok)
							require.Equal(t, node.ID, stored.ID)
							require.Equal(t, node.Labels, stored.Labels)
							require.Equal(t, node.Properties, stored.Properties)
						})
					}
				}
			}
		})
	}
}
func TestCreateSingleNodeNamedPathPlanningIdentity(t *testing.T) {
	e := newFabricCallRowsExecutor(t)
	plan := acquireCreatePlan()
	defer plan.release()
	nodes := map[string]*storage.Node{}
	edges := map[string]*storage.Edge{}
	paths, err := e.planCreatePatterns(t.Context(), "p=(n:SingleNodePath), q=(a:PathStart)-[r:LINK]->(b:PathEnd)", nodes, edges, plan)
	require.NoError(t, err)
	require.Len(t, plan.nodes, 3)
	require.Len(t, plan.edges, 1)
	require.Same(t, nodes["n"], paths["p"].Nodes[0])
	require.Same(t, plan.nodes[0], paths["p"].Nodes[0])
	require.Len(t, paths["q"].Nodes, 2)
	require.Len(t, paths["q"].Relationships, 1)
	require.Equal(t, 1, paths["q"].Length)
}
func TestCreateSingleNodeNamedPathAtomicFailure(t *testing.T) {
	for _, route := range []string{"native", "fabric"} {
		t.Run(route, func(t *testing.T) {
			e := newFabricCallRowsExecutor(t)
			prefix := ""
			if route == "fabric" {
				prefix = "USE comp.one "
			}
			_, err := e.Execute(t.Context(), prefix+"CREATE p=(n:SingleNodePathAtomic), (:SingleNodePathAtomic {v:1/0}) RETURN p", nil)
			require.Error(t, err)
			r, err := e.Execute(t.Context(), prefix+"MATCH (n:SingleNodePathAtomic) RETURN count(n)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, r.Rows)
		})
	}
}

func TestCreateSingleNodeNamedPathPipelineAndMixed(t *testing.T) {
	for _, route := range []string{"native", "fabric"} {
		t.Run(route, func(t *testing.T) {
			e := newFabricCallRowsExecutor(t)
			prefix := ""
			if route == "fabric" {
				prefix = "USE comp.one "
			}
			r, err := e.Execute(t.Context(), prefix+"UNWIND [1,2] AS i CREATE p=(n:SingleNodePipeline {v:i}) WITH p,n,i RETURN i,length(p),nodes(p)[0].v,size(relationships(p)) ORDER BY i", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1), int64(0), int64(1), int64(0)}, {int64(2), int64(0), int64(2), int64(0)}}, r.Rows)
			require.Equal(t, 2, r.Stats.NodesCreated)
			require.Zero(t, r.Stats.RelationshipsCreated)
			r, err = e.Execute(t.Context(), prefix+"CREATE p=(n:SingleNodeMixed), q=(a:MixedStart)-[r:LINK]->(b:MixedEnd) RETURN length(p),length(q),size(nodes(p)),size(nodes(q)),size(relationships(q))", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0), int64(1), int64(1), int64(2), int64(1)}}, r.Rows)
			require.Equal(t, 3, r.Stats.NodesCreated)
			require.Equal(t, 1, r.Stats.RelationshipsCreated)
			require.Equal(t, 3, r.Stats.LabelsAdded)
		})
	}
}
func TestCreateUnnamedNodeRetainsNoPathBinding(t *testing.T) {
	e := newFabricCallRowsExecutor(t)
	plan := acquireCreatePlan()
	defer plan.release()
	nodes := map[string]*storage.Node{}
	paths, err := e.planCreatePatterns(t.Context(), "(n:UnnamedControl)", nodes, map[string]*storage.Edge{}, plan)
	require.NoError(t, err)
	require.Nil(t, paths)
	require.Len(t, plan.nodes, 1)
	require.Same(t, nodes["n"], plan.nodes[0])
}
