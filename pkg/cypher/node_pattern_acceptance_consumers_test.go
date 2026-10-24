package cypher

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNodePatternCorrelatedSeekAcceptance(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		for _, fail := range []bool{false, true} {
			for _, predicate := range []string{"n.id=3", "n.id IN $ids", "id(n)=$id", "n.id=1 OR n.id=3"} {
				t.Run(fmt.Sprintf("index=%t/error=%t/%s", indexed, fail, predicate), func(t *testing.T) {
					store := nodePatternSeekFixture(t, indexed)
					probe := &nodeAcceptanceTemporalEngine{Engine: store, visible: map[storage.NodeID]bool{"n3": true}}
					if fail {
						probe.failure = errors.New("correlated candidate temporal read failed")
					}
					ctx := WithTemporalViewport(context.Background(), AsOfTemporalViewport(time.Date(2024, 7, 6, 0, 0, 0, 0, time.UTC)))
					query := "MATCH (n:R73Label:R73Wanted {flag:true}) WHERE " + predicate + " CALL { WITH n RETURN n.id AS found } RETURN found"
					result, err := NewStorageExecutor(probe).Execute(ctx, query, map[string]interface{}{"ids": []interface{}{int64(1), int64(3)}, "id": "n3"})
					if fail {
						require.ErrorIs(t, err, probe.failure)
					} else {
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows)
					}
				})
			}
		}
	}
}

// Fail only after the initial scan has accepted a seed. Reusing that binding
// must preserve the statement's error channel, including streamed consumers.
type nodeAcceptanceReboundErrorEngine struct {
	storage.Engine
	calls   atomic.Int64
	failure error
}

func (engine *nodeAcceptanceReboundErrorEngine) IsCurrentTemporalNode(_ *storage.Node, _ time.Time) (bool, error) {
	if engine.calls.Add(1) > 1 {
		return false, engine.failure
	}
	return true, nil
}

func TestNodePatternReboundTemporalError(t *testing.T) {
	for _, query := range []string{
		"MATCH (n:BoundFrontier) WITH n MATCH (n) RETURN n.id",
		"MATCH (n:BoundFrontier) WITH n MATCH (n)-[:R]->(q) RETURN q.id",
		"MATCH (n:BoundFrontier) WITH n MATCH (n) RETURN count(n)",
		"MATCH (n:BoundFrontier) WITH n OPTIONAL MATCH (n)-[:R]->(q) RETURN n.id, count(q)",
	} {
		t.Run(query, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "rebound_frontier")
			setup := NewStorageExecutor(store)
			_, err := setup.Execute(context.Background(), "CREATE (:BoundFrontier {id:1})-[:R]->(:EndFrontier {id:2})", nil)
			require.NoError(t, err)
			probe := &nodeAcceptanceReboundErrorEngine{Engine: store, failure: errors.New("rebound temporal read failed")}
			ctx := WithTemporalViewport(context.Background(), AsOfTemporalViewport(time.Date(2024, 7, 6, 0, 0, 0, 0, time.UTC)))
			_, err = NewStorageExecutor(probe).Execute(ctx, query, nil)
			require.ErrorIs(t, err, probe.failure)
			require.Greater(t, probe.calls.Load(), int64(1))
		})
	}
}
