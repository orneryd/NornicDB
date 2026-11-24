package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestAdminImportFullPreservesGraphAfterReopen(t *testing.T) {
	fixture := t.TempDir()
	nodes := filepath.Join(fixture, "nodes.csv")
	edges := filepath.Join(fixture, "edges.csv")
	require.NoError(t, os.WriteFile(nodes, []byte(":ID,:LABEL\na,Start;Other\nb,End\nc,\n"), 0600))
	require.NoError(t, os.WriteFile(edges, []byte(":START_ID,:END_ID,:TYPE,:ID,n:int\na,b,LINK,r1,1\na,b,LINK,r2,2\na,a,LINK,r3,3\nc,a,LINK,r4,4\n"), 0600))

	for _, existing := range []bool{false, true} {
		name := "fresh store"
		if existing {
			name = "existing store"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store")
			ctx := t.Context()
			var previousHead storage.MVCCHead
			if existing {
				base, err := storage.NewBadgerEngine(path)
				require.NoError(t, err)
				_, err = base.CreateNode(ctx, &storage.Node{ID: "other:sentinel", Labels: []string{"Sentinel"}})
				require.NoError(t, err)
				_, err = base.CreateNode(ctx, &storage.Node{ID: "target:a", Labels: []string{"Previous"}})
				require.NoError(t, err)
				require.NoError(t, base.DeleteNode(ctx, "target:a"))
				previousHead, err = base.GetNodeCurrentHead(ctx, "target:a")
				require.NoError(t, err)
				require.NoError(t, base.Close())
			}

			require.NoError(t, runCapacityAdminCommand(t, path, "database", "import", "full", "target",
				"--nodes", nodes, "--relationships", edges, "--chunk-size", "1", "--build-indexes=false"))
			base, err := storage.NewBadgerEngine(path)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			graph := storage.NewNamespacedEngine(base, "target")
			if existing {
				_, err = base.GetNode(ctx, "other:sentinel")
				require.NoError(t, err)
				head, err := base.GetNodeCurrentHead(ctx, "target:a")
				require.NoError(t, err)
				require.Equal(t, previousHead.FloorVersion, head.FloorVersion)
				require.Positive(t, head.Version.Compare(previousHead.Version))
			}

			count, err := graph.NodeCount(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 3, count)
			count, err = graph.EdgeCount(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 4, count)
			count, err = base.EdgeCountByTypeInNamespace(ctx, "target", "LINK")
			require.NoError(t, err)
			require.EqualValues(t, 4, count)
			count, err = base.EdgeCountByStartLabelInNamespace(ctx, "target", "Start", "LINK")
			require.NoError(t, err)
			require.EqualValues(t, 3, count)
			count, err = base.EdgeCountByEndLabelInNamespace(ctx, "target", "End", "LINK")
			require.NoError(t, err)
			require.EqualValues(t, 2, count)

			outgoing, err := graph.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Node: "a", Direction: storage.AdjacentOutgoing})
			require.NoError(t, err)
			require.Len(t, outgoing, 3)
			incoming, err := graph.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Node: "b", Direction: storage.AdjacentIncoming})
			require.NoError(t, err)
			require.Len(t, incoming, 2)
			pair, err := graph.GetEdgeBetween(ctx, "a", "b", "LINK")
			require.NoError(t, err)
			require.Contains(t, []storage.EdgeID{"r1", "r2"}, pair.ID)
			require.EqualValues(t, map[storage.EdgeID]int{"r1": 1, "r2": 2}[pair.ID], pair.Properties["n"])

			node, err := graph.GetNode(ctx, "a")
			require.NoError(t, err)
			node.Labels = []string{"Moved"}
			require.NoError(t, graph.UpdateNode(ctx, storage.NodeUpdateRequest{After: node}))
			count, err = base.EdgeCountByStartLabelInNamespace(ctx, "target", "Moved", "LINK")
			require.NoError(t, err)
			require.EqualValues(t, 3, count)
		})
	}
}

func TestAdminImportFullRejectsCollidingQualifiedIDs(t *testing.T) {
	for _, fixture := range []struct {
		name, nodes, edges, chunk string
		wantNodes, wantEdges      int64
	}{
		{"node in a later chunk", ":ID\na\ntarget:a\n", "", "1", 1, 0},
		{"node in the same chunk", ":ID\na\ntarget:a\n", "", "2", 0, 0},
		{"relationship in a later chunk", ":ID\na\nb\n", ":START_ID,:END_ID,:TYPE,:ID\na,b,LINK,r\na,b,LINK,target:r\n", "1", 2, 1},
		{"relationship in the same chunk", ":ID\na\nb\n", ":START_ID,:END_ID,:TYPE,:ID\na,b,LINK,r\na,b,LINK,target:r\n", "2", 2, 0},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			dir := t.TempDir()
			nodes := filepath.Join(dir, "nodes.csv")
			require.NoError(t, os.WriteFile(nodes, []byte(fixture.nodes), 0600))
			args := []string{"database", "import", "full", "target", "--nodes", nodes, "--chunk-size", fixture.chunk, "--build-indexes=false"}
			if fixture.edges != "" {
				edges := filepath.Join(dir, "edges.csv")
				require.NoError(t, os.WriteFile(edges, []byte(fixture.edges), 0600))
				args = append(args, "--relationships", edges)
			}
			path := filepath.Join(dir, "store")
			require.ErrorIs(t, runCapacityAdminCommand(t, path, args...), storage.ErrAlreadyExists)

			base, err := storage.NewBadgerEngine(path)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			graph := storage.NewNamespacedEngine(base, "target")
			nodesImported, err := graph.NodeCount(t.Context())
			require.NoError(t, err)
			require.Equal(t, fixture.wantNodes, nodesImported)
			edgesImported, err := graph.EdgeCount(t.Context())
			require.NoError(t, err)
			require.Equal(t, fixture.wantEdges, edgesImported)
		})
	}
}
