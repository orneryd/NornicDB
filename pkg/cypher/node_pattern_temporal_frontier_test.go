package cypher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Independent review overlay: all assertions enter through Execute.
type reviewA01cTemporal struct {
	storage.Engine
	mu      sync.Mutex
	seen    []time.Time
	failure error
}

func TestNodePatternTemporalTransactionReadOwnership(t *testing.T) {
	for _, mutation := range []string{"peer_commit", "own_writes"} {
		for _, indexed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/index=%t", mutation, indexed), func(t *testing.T) {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporal_snapshot")
				setup := NewStorageExecutor(store)
				execute := func(ex *StorageExecutor, ctx context.Context, statement string) *ExecuteResult {
					result, err := ex.Execute(ctx, statement, nil)
					require.NoError(t, err, statement)
					return result
				}
				plain := context.Background()
				execute(setup, plain, "CREATE CONSTRAINT frontier_temporal FOR (n:Frontier) REQUIRE (n.key,n.vf,n.vt) IS TEMPORAL NO OVERLAP")
				execute(setup, plain, "CREATE (:Frontier {key:'one',id:1,vf:datetime('2020-01-01T00:00:00Z'),vt:datetime('2023-01-01T00:00:00Z')}), (:Frontier {key:'one',id:2,vf:datetime('2023-01-01T00:00:00Z')})")
				if indexed {
					execute(setup, plain, "CREATE INDEX frontier_id FOR (n:Frontier) ON (n.id)")
				}
				ctx := WithTemporalViewport(plain, AsOfTemporalViewport(time.Date(2026, 7, 6, 0, 0, 0, 0, time.UTC)))
				reader := NewStorageExecutor(store)
				execute(reader, ctx, "BEGIN")
				defer func() { _, _ = reader.Execute(ctx, "ROLLBACK", nil) }()
				query := "MATCH (n:Frontier) WHERE n.id>0 RETURN n.id ORDER BY n.id LIMIT 1"
				require.Equal(t, [][]interface{}{{int64(2)}}, execute(reader, ctx, query).Rows)
				writer, writeCtx := setup, plain
				if mutation == "own_writes" {
					writer, writeCtx = reader, ctx
				}
				execute(writer, writeCtx, "MATCH (n:Frontier {id:2}) SET n.vt=datetime('2025-01-01T00:00:00Z')")
				execute(writer, writeCtx, "CREATE (:Frontier {key:'one',id:3,vf:datetime('2025-01-01T00:00:00Z')})")
				want := int64(2)
				if mutation == "own_writes" {
					want = 3
				} else {
					require.Equal(t, [][]interface{}{{int64(3)}}, execute(setup, ctx, query).Rows)
				}
				require.Equal(t, [][]interface{}{{want}}, execute(reader, ctx, query).Rows)
				require.Equal(t, [][]interface{}{{int64(1)}}, execute(reader, ctx, "MATCH (n:Frontier) RETURN count(n)").Rows)
			})
		}
	}
}
func (s *reviewA01cTemporal) IsCurrentTemporalNode(n *storage.Node, asOf time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen = append(s.seen, asOf)
	if s.failure != nil {
		return false, s.failure
	}
	return asOf.IsZero() || n.ID == "n4", nil
}
func TestReviewA01cCompactTemporal(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		for _, explicit := range []bool{false} {
			for _, profile := range []bool{false, true} {
				for _, shape := range []string{"ordered", "stream", "count"} {
					for _, fail := range []bool{false, true} {
						t.Run(fmt.Sprintf("index=%t/tx=%t/profile=%t/%s/error=%t", indexed, explicit, profile, shape, fail), func(t *testing.T) {
							store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "compact_temporal")
							setup := NewStorageExecutor(store)
							_, err := setup.Execute(context.Background(), "CREATE (:Compact {id:1,flag:true}), (:Compact:Wanted {id:2,flag:false}), (:Compact:Wanted {id:3,flag:true}), (:Compact:Wanted {id:4,flag:true})", nil)
							require.NoError(t, err)
							// Use stable public IDs solely to make the temporal storage oracle deterministic.
							nodes, err := store.GetNodesByLabel("Compact")
							require.NoError(t, err)
							for _, node := range nodes {
								old := node.ID
								node.ID = storage.NodeID(fmt.Sprintf("n%d", node.Properties["id"]))
								require.NoError(t, store.DeleteNode(old))
								_, err = store.CreateNode(node)
								require.NoError(t, err)
							}
							if indexed {
								_, err = setup.Execute(context.Background(), "CREATE INDEX compact_temporal_id FOR (n:Compact) ON (n.id)", nil)
								require.NoError(t, err)
							}
							probe := &reviewA01cTemporal{Engine: store}
							sentinel := errors.New("independent temporal failure")
							if fail {
								probe.failure = sentinel
							}
							ex := NewStorageExecutor(probe)
							asOf := time.Date(2024, 7, 6, 12, 34, 56, 789, time.FixedZone("review", 3600))
							ctx := WithTemporalViewport(context.Background(), AsOfTemporalViewport(asOf))
							if explicit {
								_, err = ex.Execute(ctx, "BEGIN", nil)
								require.NoError(t, err)
								defer func() { _, _ = ex.Execute(ctx, "ROLLBACK", nil) }()
							}
							query := "MATCH (n:Compact:Wanted {flag:true}) WHERE n.id>0 RETURN n.id LIMIT 1"
							want := int64(4)
							if shape == "ordered" {
								query = "MATCH (n:Compact:Wanted {flag:true}) WHERE n.id>0 RETURN n.id ORDER BY n.id LIMIT 1"
							}
							if shape == "count" {
								query = "MATCH (n:Compact:Wanted {flag:true}) WHERE n.id>0 RETURN count(n)"
								want = 1
							}
							if profile {
								query = "PROFILE " + query
							}
							got, err := ex.Execute(ctx, query, nil)
							if fail {
								require.ErrorIs(t, err, sentinel)
							} else {
								require.NoError(t, err)
								require.Equal(t, [][]interface{}{{want}}, got.Rows)
							}
							require.NotEmpty(t, probe.seen)
							for _, seen := range probe.seen {
								require.Equal(t, asOf.UTC(), seen)
							}
						})
					}
				}
			}
		}
	}
}

func TestReviewA01cRealTemporalTransactions(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			for _, profile := range []bool{false, true} {
				for _, year := range []int{2021, 2024} {
					for _, shape := range []string{"scan", "ordered", "count"} {
						t.Run(fmt.Sprintf("index=%t/tx=%t/profile=%t/year=%d/%s", indexed, explicit, profile, year, shape), func(t *testing.T) {
							store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "real_temporal")
							setup := NewStorageExecutor(store)
							ctx := context.Background()
							for _, statement := range []string{
								"CREATE CONSTRAINT review_temporal FOR (n:CompactTemporal) REQUIRE (n.key,n.vf,n.vt) IS TEMPORAL NO OVERLAP",
								"CREATE (:CompactTemporal:Wanted {key:'one',id:1,flag:true,vf:datetime('2020-01-01T00:00:00Z'),vt:datetime('2023-01-01T00:00:00Z')})",
								"CREATE (:CompactTemporal:Wanted {key:'one',id:2,flag:true,vf:datetime('2023-01-01T00:00:00Z')})",
							} {
								_, err := setup.Execute(ctx, statement, nil)
								require.NoError(t, err, statement)
							}
							if indexed {
								_, err := setup.Execute(ctx, "CREATE INDEX review_temporal_id FOR (n:CompactTemporal) ON (n.id)", nil)
								require.NoError(t, err)
							}
							asOf := time.Date(year, 7, 6, 12, 34, 56, 789, time.FixedZone("review", 3600))
							ctx = WithTemporalViewport(ctx, AsOfTemporalViewport(asOf))
							ex := NewStorageExecutor(store)
							if explicit {
								_, err := ex.Execute(ctx, "BEGIN", nil)
								require.NoError(t, err)
								defer func() { _, _ = ex.Execute(ctx, "ROLLBACK", nil) }()
							}
							query := "MATCH (n:CompactTemporal:Wanted {flag:true}) RETURN n.id ORDER BY n.id"
							want := int64(1)
							if year == 2024 {
								want = 2
							}
							if shape == "ordered" {
								query = "MATCH (n:CompactTemporal:Wanted {flag:true}) WHERE n.id>0 RETURN n.id ORDER BY n.id LIMIT 1"
							}
							if shape == "count" {
								query = "MATCH (n:CompactTemporal:Wanted {flag:true}) WHERE n.id>0 RETURN count(n)"
								want = 1
							}
							if profile {
								query = "PROFILE " + query
							}
							got, err := ex.Execute(ctx, query, nil)
							require.NoError(t, err, query)
							require.Equal(t, [][]interface{}{{want}}, got.Rows, query)
						})
					}
				}
			}
		}
	}
}
