package cypher

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPR1682TraversalRetainedPathsOwnTheirSlices(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		ctx := context.Background()
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "paths")
		exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
		for _, name := range []string{"root", "a", "b", "shared", "leaf"} {
			node := &storage.Node{ID: storage.NodeID(name), Properties: map[string]any{"name": name}}
			if name == "root" {
				node.Labels = []string{"PathRoot"}
			}
			_, err := store.CreateNode(ctx, node)
			require.NoError(t, err)
		}
		for _, pair := range [][2]string{{"root", "a"}, {"root", "b"}, {"a", "shared"}, {"b", "shared"}, {"shared", "leaf"}, {"shared", "root"}} {
			err := store.CreateEdge(ctx, &storage.Edge{ID: storage.EdgeID(strings.Join(pair[:], "-")), StartNode: storage.NodeID(pair[0]), EndNode: storage.NodeID(pair[1]), Type: "R"})
			require.NoError(t, err)
		}
		want := []string{"root", "root/a", "root/a/shared", "root/a/shared/leaf", "root/a/shared/root", "root/b", "root/b/shared", "root/b/shared/leaf", "root/b/shared/root"}
		root, err := store.GetNode(ctx, "root")
		require.NoError(t, err)
		state := exec.newTraversalContext(ctx, root, &RelationshipPattern{Direction: "outgoing", MinHops: 0, MaxHops: 3})
		nodes := make([]*storage.Node, 1, 8)
		nodes[0] = root
		edges := make([]*storage.Edge, 0, 8)
		paths := exec.findPaths(state, root, nodes, edges, 0, nil, nil)
		state.memory.close()
		require.Empty(t, state.usedEdges, "completed sibling edges must not remain in active-path state")
		// Even caller-owned spare capacity must never alias retained paths.
		clear(nodes[:cap(nodes)])
		clear(edges[:cap(edges)])
		got := make([]string, 0, len(paths))
		for _, path := range paths {
			require.Len(t, path.Relationships, path.Length)
			require.Len(t, path.Nodes, path.Length+1)
			names := make([]string, len(path.Nodes))
			for i, node := range path.Nodes {
				require.NotNil(t, node)
				names[i] = node.Properties["name"].(string)
				if i > 0 {
					require.Equal(t, names[i-1]+"-"+names[i], string(path.Relationships[i-1].ID))
				}
			}
			got = append(got, strings.Join(names, "/"))
		}
		sort.Strings(got)
		require.Equal(t, want, got)
		// The public variable-length path consumer must preserve every sibling,
		// including zero-hop paths and revisiting a node through distinct edges.
		result, err := exec.Execute(ctx, "MATCH p=(s:PathRoot)-[:R*0..3]->(n) RETURN [v IN nodes(p) | v.name] AS names", nil)
		require.NoError(t, err)
		got = got[:0]
		for _, row := range result.Rows {
			got = append(got, strings.ReplaceAll(strings.Trim(fmt.Sprint(row[0]), "[]"), " ", "/"))
		}
		sort.Strings(got)
		require.Equal(t, want, got)
	})
}

func TestPR1682AnonymousPathWhereBindsOuterEntities(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(newTestMemoryEngine(t))
		node := &storage.Node{ID: "outer", Properties: map[string]any{"name": "outer"}}
		edge := &storage.Edge{ID: "outer-edge", Properties: map[string]any{"weight": int64(7)}}
		path := PathResult{Nodes: []*storage.Node{node}}
		for _, check := range []struct {
			where string
			want  int
		}{
			{where: "outer.name = 'outer' AND rel.weight = 7", want: 1},
			{where: "outer.name = 'other' OR rel.weight = 8", want: 0},
		} {
			got := exec.filterPathsByWhere(context.Background(), []PathResult{path}, &TraversalMatch{}, check.where, map[string]any{"outer": node, "rel": edge})
			require.Len(t, got, check.want)
		}
	})
}

func TestPR1682TraversalResultOrderAndUnwind(t *testing.T) {
	for _, scenario := range []struct {
		name       string
		limit      int
		cancelAt   int
		repeatable bool
		want       []string
	}{
		{name: "ordered siblings", cancelAt: -1, want: []string{"root", "a", "b", "c"}},
		{name: "limit within child", limit: 2, cancelAt: -1, want: []string{"root", "a"}},
		{name: "cancel after accepted child", cancelAt: 2, want: []string{"root", "a"}},
		{name: "repeatable cycle", cancelAt: -1, repeatable: true, want: []string{"root", "root", "root", "root"}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			root := &storage.Node{ID: "root"}
			engine := &cancellationTraversalEngine{
				Engine: newTestMemoryEngine(t), nodes: map[storage.NodeID]*storage.Node{"root": root},
				cancel: cancel, cancelAt: scenario.cancelAt,
			}
			if scenario.repeatable {
				engine.edges = []*storage.Edge{{ID: "loop", StartNode: "root", EndNode: "root", Type: "R"}}
			} else {
				for _, id := range []storage.NodeID{"a", "b", "c"} {
					engine.nodes[id] = &storage.Node{ID: id}
					engine.edges = append(engine.edges, &storage.Edge{ID: storage.EdgeID(id), StartNode: "root", EndNode: id, Type: "R"})
				}
			}
			exec := NewStorageExecutor(engine)
			state := exec.newTraversalContext(ctx, root, &RelationshipPattern{Direction: "outgoing", MinHops: 0, MaxHops: 3})
			state.limit, state.repeatable = scenario.limit, scenario.repeatable
			paths := exec.findPaths(state, root, []*storage.Node{root}, nil, 0, nil, nil)
			state.memory.close()
			got := make([]string, len(paths))
			for i, path := range paths {
				got[i] = string(path.Nodes[len(path.Nodes)-1].ID)
				require.Len(t, path.Nodes, path.Length+1)
				require.Len(t, path.Relationships, path.Length)
				if scenario.repeatable {
					require.Equal(t, i, path.Length)
				}
			}
			require.Equal(t, scenario.want, got, "DFS order and previously accepted paths survive unwind")
			require.Empty(t, state.usedEdges)
			if scenario.cancelAt > 0 {
				require.ErrorIs(t, ctx.Err(), context.Canceled)
			} else {
				require.NoError(t, ctx.Err())
			}
		})
	}
}
