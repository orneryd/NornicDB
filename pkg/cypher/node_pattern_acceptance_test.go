package cypher

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Only Engine's required methods are forwarded, so this exercises the
// materialized compatibility path rather than a projected/streaming scan.
type nodeAcceptanceMaterializedEngine struct{ storage.Engine }

type nodeAcceptanceTemporalEngine struct {
	storage.Engine
	visible map[storage.NodeID]bool
	failure error
}

func (engine *nodeAcceptanceTemporalEngine) IsCurrentTemporalNode(node *storage.Node, _ time.Time) (bool, error) {
	return engine.visible[node.ID], engine.failure
}

func TestNodePatternAcceptanceAcrossAccessPaths(t *testing.T) {
	for _, access := range []string{"projected", "label_ids", "materialized"} {
		for _, labels := range []string{":R73Label", ":R73Label:R73Wanted"} {
			t.Run(access+labels, func(t *testing.T) {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "acceptance")
				var nodes []*storage.Node
				var ids []storage.NodeID
				for index := 1; index <= 4; index++ {
					labels := []string{"R73Label", "R73Wanted"}
					if index == 1 {
						labels = append(labels, "_internal")
					}
					if index == 2 {
						labels = []string{"R73Label"}
					}
					id := storage.NodeID(fmt.Sprintf("candidate-%d", index))
					_, err := store.CreateNode(&storage.Node{ID: id, Labels: labels, Properties: map[string]interface{}{"id": int64(index), "flag": index == 1 || index == 4}})
					require.NoError(t, err)
					node, err := store.GetNode(id)
					require.NoError(t, err)
					nodes = append(nodes, node)
					ids = append(ids, id)
				}
				var engine storage.Engine
				projected := &collectNodesLabelProbeEngine{Engine: store, projectedNodes: append([]*storage.Node{nil}, nodes...)}
				lookup := &collectNodesIDProbeEngine{Engine: store, labelIDs: ids}
				switch access {
				case "projected":
					engine = projected
				case "label_ids":
					engine = lookup
				default:
					engine = &nodeAcceptanceMaterializedEngine{Engine: store}
				}
				result, err := NewStorageExecutor(engine).Execute(context.Background(), "MATCH (n"+labels+" {flag:true}) RETURN n.id LIMIT 1", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(4)}}, result.Rows)
				if access == "projected" {
					require.Positive(t, projected.projectedLabelCalls)
				}
				if access == "label_ids" && labels == ":R73Label" {
					require.Positive(t, lookup.labelLookupCalls)
				}
			})
		}
	}
}

func TestNodePatternIndexAcceptanceVisibility(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		for _, namespace := range []string{"user", "system"} {
			t.Run(fmt.Sprintf("index=%t/database=%s", indexed, namespace), func(t *testing.T) {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), namespace)
				for index := 1; index <= 3; index++ {
					labels := []string{"R73Label", "R73Wanted"}
					if index == 1 {
						labels = append(labels, "_internal")
					}
					_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", index)), Labels: labels, Properties: map[string]interface{}{"id": int64(index)}})
					require.NoError(t, err)
				}
				query := "MATCH (n:R73Label:R73Wanted) WHERE n.id>0 RETURN n.id ORDER BY n.id LIMIT 1"
				if namespace == "system" {
					// Public graph reads and DDL are rejected before MATCH on this
					// database; they do not exercise either candidate access path.
					_, err := NewStorageExecutor(store).Execute(context.Background(), query, nil)
					requireStatusCode(t, err, "Neo.ClientError.Statement.SemanticError")
					if indexed {
						_, err = NewStorageExecutor(store).Execute(context.Background(), "CREATE INDEX acceptance_idx FOR (n:R73Label) ON (n.id)", nil)
						requireStatusCode(t, err, "Neo.ClientError.Statement.SemanticError")
					}
					return
				}
				if indexed {
					_, err := NewStorageExecutor(store).Execute(context.Background(), "CREATE INDEX acceptance_idx FOR (n:R73Label) ON (n.id)", nil)
					require.NoError(t, err)
				}
				result, err := NewStorageExecutor(store).Execute(context.Background(), query, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
				if namespace == "user" {
					engine := &nodeAcceptanceTemporalEngine{Engine: store, visible: map[storage.NodeID]bool{"n3": true}}
					ctx := WithTemporalViewport(context.Background(), AsOfTemporalViewport(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)))
					result, err = NewStorageExecutor(engine).Execute(ctx, query, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows)
					engine.failure = errors.New("temporal read failed")
					_, err = NewStorageExecutor(engine).Execute(ctx, query, nil)
					require.ErrorIs(t, err, engine.failure)
				}
			})
		}
	}
}
