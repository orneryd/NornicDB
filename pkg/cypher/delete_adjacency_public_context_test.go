package cypher

import (
	"context"
	"fmt"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// pollCancellation cancels the caller at its nth context poll, so a test can
// cancel inside one bulk read of an otherwise racing statement. It is the
// caller's own context ending (context.Canceled), not an injected error: the
// statement reaches the cancellation through its real read path.
type pollCancellation struct {
	context.Context
	cancel   context.CancelFunc
	checks   int
	cancelAt int
}

func (c *pollCancellation) Err() error {
	c.checks++
	if c.checks == c.cancelAt {
		c.cancel()
	}
	return c.Context.Err()
}

// guardIncidentPopulation is the guarded node's degree. A non-DETACH DELETE's
// incidence read visits every one of those relationships, so the cancellation
// point below lies inside it while the statement's fixed work stays far below.
const guardIncidentPopulation = 3 * readProjectionCancellationInterval

// guardIncidentCancellationPoll is past the statement's own fixed work (about
// 520 polls, measured through this exact statement: statement analysis, cache,
// plan and the transaction) and inside the incidence read of
// guardIncidentPopulation relationships.
const guardIncidentCancellationPoll = 1000

// seedGuardedNode creates one Guarded node whose guardIncidentPopulation
// relationships connect it to itself, so its incidence read visits that many
// relationships while the statement's match visits one node.
func seedGuardedNode(t testing.TB, store storage.Engine, id storage.NodeID) {
	t.Helper()
	_, err := store.CreateNode(t.Context(), &storage.Node{ID: id, Labels: []string{"Guarded"}, Properties: map[string]interface{}{"id": string(id)}})
	require.NoError(t, err)
	for i := range guardIncidentPopulation {
		require.NoError(t, store.CreateEdge(t.Context(), &storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("%s-e%d", id, i)),
			StartNode: id,
			EndNode:   id,
			Type:      "R",
		}))
	}
}

// requireCancellationLandsInIncidenceRead proves the cancellation point is past
// the statement's match: the same match, run alone on the warmed executor, polls
// the caller fewer times than the point.
func requireCancellationLandsInIncidenceRead(t *testing.T, exec *StorageExecutor, match string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	polled := &pollCancellation{Context: ctx, cancel: cancel, cancelAt: -1}
	_, err := exec.Execute(polled, match, nil)
	require.NoError(t, err)
	require.Less(t, polled.checks, guardIncidentCancellationPoll, "the statement's match must not reach the incidence cancellation point")
}

// TestPublicDeleteStopsAtCanceledNativeIncidentRead reproduces the public
// DELETE gap over the native implicit transaction: a non-DETACH DELETE of a
// connected node reads the node's relationships through the guard before the
// statement is accepted, and a caller canceled inside that read must fail the
// statement instead of finishing the enumeration. Both parsers route DELETE
// through that guard.
func TestPublicDeleteStopsAtCanceledNativeIncidentRead(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "delete_guard_context")
		seedGuardedNode(t, store, "a")
		exec := NewStorageExecutor(store)
		const query = "MATCH (a:Guarded {id: 'a'}) DELETE a"

		// Healthy control first: it warms the statement cache and shows the
		// residual relationship still fails the statement.
		_, err := exec.Execute(t.Context(), query, nil)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Schema.ConstraintValidationFailed", code)
		requireCancellationLandsInIncidenceRead(t, exec, "MATCH (a:Guarded {id: 'a'}) RETURN a")

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		polled := &pollCancellation{Context: ctx, cancel: cancel, cancelAt: guardIncidentCancellationPoll}
		_, err = exec.Execute(polled, query, nil)
		require.ErrorIs(t, err, context.Canceled, "the guard's incidence read must observe the caller's cancellation")
		require.GreaterOrEqual(t, polled.checks, guardIncidentCancellationPoll)
		require.Less(t, polled.checks, 3*guardIncidentPopulation, "the incidence enumeration stops at the cancellation instead of finishing")

		// Nothing was deleted, and the statement is healthy again afterwards.
		guarded, err := store.GetNodesByLabel(t.Context(), "Guarded")
		require.NoError(t, err)
		require.Len(t, guarded, 1)
		edges, err := store.AllEdges(t.Context())
		require.NoError(t, err)
		require.Len(t, edges, guardIncidentPopulation)
	})
}
