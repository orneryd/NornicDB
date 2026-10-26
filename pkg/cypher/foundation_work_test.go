package cypher

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFoundationPublicRowBindingWork(t *testing.T) {
	for _, schedule := range []struct {
		name, query string
		temporal    bool
	}{
		{"rows", "MATCH (n:FoundationCost) RETURN n.v", false},
		{"count", "MATCH (n:FoundationCost) RETURN count(n)", false},
		{"temporal", "MATCH (n:FoundationCost) RETURN n.v", true},
		{"endpoints", "MATCH (a:FoundationCost)-[:FoundationCostEdge]->(b) RETURN b.v", false},
	} {
		query := schedule.query
		t.Run(schedule.name, func(t *testing.T) {
			var previous [2]uint64
			var previousSites map[string]uint64
			for _, rows := range []int{1, 256} {
				base := enginePopulationBase(t)
				tenant := storage.NewNamespacedEngine(base, "tenant")
				for i := 0; i < rows; i++ {
					_, err := tenant.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("cost%d", i)), Labels: []string{"FoundationCost"}, Properties: map[string]interface{}{"v": int64(i)}})
					require.NoError(t, err)
				}
				for i := 0; i < rows; i++ {
					id := storage.NodeID(fmt.Sprintf("cost%d", i))
					require.NoError(t, tenant.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("costEdge%d", i)), StartNode: id, EndNode: id, Type: "FoundationCostEdge"}))
				}
				var view storage.Engine = tenant
				for i := 0; i < 8; i++ {
					view = storage.NewTracedEngine(storage.NewWALEngine(view, nil))
				}
				exec := NewStorageExecutor(view)
				work := &storage.CapabilityWork{Trace: schedule.name == "endpoints"}
				release := storage.ObserveCapabilityWork(work)
				ctx := context.Background()
				if schedule.temporal {
					ctx = WithTemporalViewport(ctx, AsOfTemporalViewport(time.Now().UTC()))
				}
				result, err := exec.Execute(ctx, query, nil)
				release()
				require.NoError(t, err)
				if query == "MATCH (n:FoundationCost) RETURN count(n)" {
					require.Equal(t, int64(rows), result.Rows[0][0])
				} else {
					require.Len(t, result.Rows, rows)
				}
				observed := [2]uint64{work.ProviderVisits.Load(), work.TypeChecks.Load()}
				t.Logf("rows=%d provider-visits=%d type-checks=%d", rows, observed[0], observed[1])
				sites := work.Sites()
				if rows > 1 && observed != previous {
					var keys []string
					for key, count := range sites {
						if count > previousSites[key] {
							keys = append(keys, key)
						}
					}
					sort.Slice(keys, func(i, j int) bool {
						return sites[keys[i]]-previousSites[keys[i]] > sites[keys[j]]-previousSites[keys[j]]
					})
					for i, key := range keys {
						if i == 12 {
							break
						}
						t.Logf("extra-work=%d %s", sites[key]-previousSites[key], key)
					}
				}
				if rows > 1 {
					require.Equal(t, previous, observed, "structural work must be independent of delivered/scanned rows")
				}
				previous = observed
				previousSites = sites
			}
		})
	}
}
