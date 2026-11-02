package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// inflightAccessEntityMeta pauses at the existing flusher metadata boundary,
// which runs after DrainAll. Every lookup delegates to the real stored entity;
// the witness neither fabricates a missing-entity error nor intercepts writes.
type inflightAccessEntityMeta struct {
	engine      *BadgerEngine
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	afterLookup bool
}

func (g *inflightAccessEntityMeta) GetEntityMeta(ctx context.Context, id string) (knowledgepolicy.EntityMeta, error) {
	var meta knowledgepolicy.EntityMeta
	var err error
	if g.afterLookup {
		meta, err = g.engine.GetEntityMeta(ctx, id)
	}
	g.once.Do(func() { close(g.entered) })
	<-g.release
	if g.afterLookup {
		return meta, err
	}
	return g.engine.GetEntityMeta(ctx, id)
}

// This component regression covers a batch already owned by the flusher when
// a real node/edge deletion commits. It does not qualify rollback, ID reuse,
// snapshot result materialization, or full DB.Close routing.
func TestAccessRetirementWithInflightDrainedBatch(t *testing.T) {
	for _, kind := range []string{"node", "edge"} {
		for _, afterLookup := range []bool{false, true} {
			phase := "before_lookup"
			if afterLookup {
				phase = "after_lookup"
			}
			t.Run(kind+"/"+phase, func(t *testing.T) {
				engine := newTestEngine(t)
				_, err := engine.CreateNode(t.Context(), &Node{ID: "inflight:node"})
				require.NoError(t, err)
				id := "inflight:node"
				if kind == "edge" {
					_, err = engine.CreateNode(t.Context(), &Node{ID: "inflight:peer"})
					require.NoError(t, err)
					require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "inflight:edge", StartNode: "inflight:node", EndNode: "inflight:peer", Type: "LINK"}))
					id = "inflight:edge"
				}
				acc := knowledgepolicy.NewAccessAccumulator(true, 0)
				engine.SetAccessAccumulator(acc)
				flusher := knowledgepolicy.NewAccessFlusher(acc, engine, time.Hour)
				gate := &inflightAccessEntityMeta{afterLookup: afterLookup, engine: engine, entered: make(chan struct{}), release: make(chan struct{})}
				flusher.SetPropertySuppression(nil, gate, nil)
				callbacks := 0
				flusher.SetSuppressionRecheck(func(context.Context, string, knowledgepolicy.EntityMeta) { callbacks++ })
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(gate.release) }) }
				joined := make(chan struct{})
				launched := false
				// Always release and join the actual worker, including assertion failures.
				// Registered before launch or any assertion while the worker exists.
				t.Cleanup(func() {
					release()
					if launched {
						select {
						case <-joined:
						case <-time.After(5 * time.Second):
							t.Error("access flush worker did not join")
						}
					}
				})
				acc.IncrementAccess(id)
				launched = true
				go func() { defer close(joined); flusher.Flush() }()
				select {
				case <-gate.entered:
				case <-time.After(5 * time.Second):
					t.Fatal("flush did not reach real metadata lookup after draining")
				}
				// The batch is no longer in accumulator shards. ClearEntity at deletion's
				// publication cannot remove it from the flusher's private merged map.
				require.Empty(t, acc.DrainAll())
				if kind == "node" {
					require.NoError(t, engine.DeleteNode(t.Context(), NodeID(id)))
				} else {
					require.NoError(t, engine.DeleteEdge(t.Context(), EdgeID(id)))
				}
				release()
				select {
				case <-joined:
				case <-time.After(5 * time.Second):
					t.Fatal("released access flush worker did not join")
				}
				result, err := engine.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{Mode: knowledgepolicy.AccessMetaReadPoint, EntityID: id})
				require.NoError(t, err)
				assert.Zero(t, callbacks, "retired flush must not publish suppression callbacks")
				assert.Nil(t, result.Entry, "an in-flight access batch must not recreate metadata for a committed deletion")
				assert.NoError(t, flusher.Stop(), "a legitimate committed retirement must not become a retained flusher failure")
				assert.NoError(t, flusher.Stop(), "repeated retirement must preserve healthy outcome")
			})
		}
	}
}
