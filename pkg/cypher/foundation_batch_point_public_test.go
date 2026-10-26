package cypher

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Author: Lane #1644 (Codex). Nornic2's batch/point invariant is observed
// through a real public procedure consumer of FunctionStorage and Execute.
func TestFoundationPublicCompositeBatchPoint(t *testing.T) {
	base := enginePopulationBase(t)
	first := storage.NewNamespacedEngine(base, "first")
	second := storage.NewNamespacedEngine(base, "second")
	for _, seed := range []struct {
		engine storage.Engine
		id     storage.NodeID
		value  string
	}{
		{first, "shared", "first"}, {second, "shared", "second"}, {first, "one", "first-only"}, {second, "two", "second-only"},
	} {
		_, err := seed.engine.CreateNode(&storage.Node{ID: seed.id, Labels: []string{"BatchPoint"}, Properties: map[string]interface{}{"value": seed.value}})
		require.NoError(t, err)
	}
	composite := storage.NewCompositeEngine(map[string]storage.Engine{"a": first, "b": second}, map[string]string{"a": "first", "b": "second"}, map[string]string{"a": "read", "b": "read"})
	owner := NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"))
	for _, word := range []string{"", "T", "W", "TW", "WT"} {
		t.Run("wrappers="+word, func(t *testing.T) {
			var view storage.Engine = composite
			for _, token := range word {
				if token == 'T' {
					view = storage.NewTracedEngine(view)
				} else {
					view = foundationWAL(t, view)
				}
			}
			const name = "foundation.batchPoint"
			cleanupAdmissionProcedure(t, name)
			require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: name, Mode: ProcedureModeRead, Returns: []ProcedureColumn{{Name: "mismatches", Type: "INTEGER"}, {Name: "checked", Type: "INTEGER"}, {Name: "pointSource", Type: "STRING"}, {Name: "batchSource", Type: "STRING"}}}, func(ctx context.Context, _ *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
				issued, err := NewStorageExecutor(view).FunctionStorage("")
				if err != nil {
					return nil, err
				}
				point, err := issued.GetNode("shared")
				if err != nil {
					return nil, err
				}
				single, err := issued.BatchGetNodes([]storage.NodeID{"shared"})
				if err != nil {
					return nil, err
				}
				batchSource := ""
				if single["shared"] != nil {
					batchSource = single["shared"].Database
				}
				ids := []storage.NodeID{"shared", "one", "two", "missing", storage.NodeID(storage.SourceNodeElementID("first", "shared")), storage.NodeID(storage.SourceNodeElementID("second", "shared"))}
				type image struct {
					ID         storage.NodeID
					Database   string
					Labels     []string
					Properties map[string]interface{}
				}
				images := func(nodes map[storage.NodeID]*storage.Node) map[storage.NodeID]image {
					out := map[storage.NodeID]image{}
					for id, n := range nodes {
						if n != nil {
							out[id] = image{n.ID, n.Database, n.Labels, n.Properties}
						}
					}
					return out
				}
				var mismatches, checked int64
				for mask := 0; mask < 1<<len(ids); mask++ {
					for _, reverse := range []bool{false, true} {
						var request []storage.NodeID
						for i, id := range ids {
							if mask&(1<<i) != 0 {
								request = append(request, id)
							}
						}
						if len(request) > 0 {
							request = append(request, request[0])
						}
						if reverse {
							for l, r := 0, len(request)-1; l < r; l, r = l+1, r-1 {
								request[l], request[r] = request[r], request[l]
							}
						}
						want := map[storage.NodeID]*storage.Node{}
						for _, id := range request {
							node, err := issued.GetNode(id)
							if errors.Is(err, storage.ErrNotFound) {
								continue
							}
							if err != nil {
								return nil, err
							}
							want[id] = node
						}
						for _, contextual := range []bool{false, true} {
							var got map[storage.NodeID]*storage.Node
							if contextual {
								got, err = storage.BatchGetNodesWithContext(ctx, issued, request)
							} else {
								got, err = issued.BatchGetNodes(request)
							}
							if err != nil {
								return nil, err
							}
							checked++
							if !reflect.DeepEqual(images(want), images(got)) {
								mismatches++
							}
						}
					}
				}
				return &ExecuteResult{Columns: []string{"mismatches", "checked", "pointSource", "batchSource"}, Rows: [][]interface{}{{mismatches, checked, point.Database, batchSource}}, Stats: &QueryStats{}}, nil
			}))
			result, err := owner.Execute(context.Background(), "CALL "+name+"() YIELD mismatches,checked,pointSource,batchSource RETURN mismatches,checked,pointSource,batchSource", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0), int64(256), "first", "first"}}, result.Rows, "every generated ID set must preserve the point node, source and absence")
		})
	}
}
