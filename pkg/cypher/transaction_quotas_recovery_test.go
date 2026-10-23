package cypher

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTransactionQuotaRejectionLeavesWALIndexesAndSearchUnchanged(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, kind := range []string{"nodes", "edges", "bytes", "rate"} {
			t.Run(fmt.Sprintf("explicit=%v/%s", explicit, kind), func(t *testing.T) {
				ctx := context.Background()
				dir := t.TempDir()
				base := newTestMemoryEngine(t)
				wal, err := storage.NewWAL(dir, nil)
				require.NoError(t, err)
				logged := storage.NewWALEngine(base, wal)
				manager, err := multidb.NewDatabaseManager(logged, nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = manager.Close(); _ = logged.Close() })
				require.NoError(t, manager.CreateDatabase("quota"))
				limits := &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 1}}
				query := "UNWIND [1,2] AS id CREATE (:Quota {id:id,content:'rejectedtoken'})"
				switch kind {
				case "edges":
					limits = &multidb.Limits{Storage: multidb.StorageLimits{MaxEdges: 1}}
					query = "CREATE (a:Quota {content:'rejectedtoken'})-[:R]->(b:Quota)-[:R]->(c:Quota)"
				case "bytes":
					limits = &multidb.Limits{Storage: multidb.StorageLimits{MaxBytes: 1}}
				case "rate":
					limits = &multidb.Limits{Rate: multidb.RateLimits{MaxWritesPerSecond: 1}}
				}

				engine, err := manager.GetStorage("quota")
				require.NoError(t, err)
				svc := search.NewServiceWithDimensions(engine, 3)
				require.NoError(t, svc.BuildIndexes(ctx))
				var published atomic.Int64
				base.OnNodeCreated(func(n *storage.Node) {
					if strings.HasPrefix(string(n.ID), "quota:") {
						published.Add(1)
						local := storage.CopyNode(n)
						local.ID = storage.NodeID(strings.TrimPrefix(string(n.ID), "quota:"))
						require.NoError(t, svc.IndexNode(local))
					}
				})
				base.OnEdgeCreated(func(*storage.Edge) { published.Add(1) })
				_, err = engine.CreateNode(&storage.Node{ID: "positive", Labels: []string{"Control"}, Properties: map[string]any{"content": "retainedtoken"}, ChunkEmbeddings: [][]float32{{1, 0, 0}}})
				require.NoError(t, err)
				positive, err := svc.Search(ctx, "retainedtoken", nil, search.DefaultSearchOptions())
				require.NoError(t, err)
				require.NotEmpty(t, positive.Results)
				published.Store(0)
				require.NoError(t, manager.SetDatabaseLimits("quota", limits))
				exec := NewStorageExecutor(engine)
				if explicit {
					_, err = exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				_, err = exec.Execute(ctx, query, nil)
				if explicit {
					require.NoError(t, err)
					_, err = exec.Execute(ctx, "COMMIT", nil)
				}
				require.Error(t, err)
				require.Zero(t, published.Load(), "rejected writes must not emit indexing events")
				verify := func(engine storage.Engine, svc *search.Service) {
					nodes, err := engine.NodeCount()
					require.NoError(t, err)
					require.EqualValues(t, 1, nodes)
					edges, err := engine.EdgeCount()
					require.NoError(t, err)
					require.Zero(t, edges)
					result, err := NewStorageExecutor(engine).Execute(ctx, "MATCH (n:Quota) RETURN count(n)", nil)
					require.NoError(t, err)
					require.EqualValues(t, 0, result.Rows[0][0])
					response, err := svc.Search(ctx, "rejectedtoken", nil, search.DefaultSearchOptions())
					require.NoError(t, err)
					require.Empty(t, response.Results)
					positive, err := svc.Search(ctx, "retainedtoken", []float32{1, 0, 0}, search.DefaultSearchOptions())
					require.NoError(t, err)
					require.NotEmpty(t, positive.Results)
					for _, hit := range positive.Results {
						require.Equal(t, "positive", string(hit.ID))
					}
				}
				verify(engine, svc)
				require.NoError(t, wal.Close())
				recovered, err := storage.RecoverFromWAL(dir, "")
				require.NoError(t, err)
				t.Cleanup(func() { _ = recovered.Close() })
				recoveredEngine := storage.NewNamespacedEngine(recovered, "quota")
				recoveredSearch := search.NewServiceWithDimensions(recoveredEngine, 3)
				require.NoError(t, recoveredSearch.BuildIndexes(ctx))
				verify(recoveredEngine, recoveredSearch)
			})
		}
	}
}
