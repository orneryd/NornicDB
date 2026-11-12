package storage

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

// Relationship cardinality is enforced on two read authorities: the engine's
// publishing *badger.Txn, which iterates and decodes the anchor node's raw
// adjacency index, and a BadgerTransaction's staged view, which selects pending
// images over committed adjacency with deleted and rewritten images excluded.
// edgeCardinalityCounter owns everything downstream of that selection. These
// tests prove the owner's population rules, and that both routes reach the same
// decision — and describe it the same way — on the same state.

func TestEdgeCardinalityCounterOwnsPopulationThresholdAndViolation(t *testing.T) {
	outgoing := Constraint{Type: ConstraintCardinality, Label: "OWNS", Direction: "OUTGOING", MaxCount: 2}
	probe := &Edge{ID: "test:new", StartNode: "test:a", EndNode: "test:b", Type: "OWNS"}

	t.Run("outgoing", func(t *testing.T) {
		counter := newEdgeCardinalityCounter(outgoing, probe, "test", "test:new")
		require.Equal(t, NodeID("test:a"), counter.anchor)
		require.True(t, counter.anchored(&Edge{StartNode: "test:a", EndNode: "test:z"}))
		require.False(t, counter.anchored(&Edge{StartNode: "test:z", EndNode: "test:a"}))

		require.False(t, counter.admits("test:new"), "the edge under validation never counts towards itself")
		require.False(t, counter.admits("other:e"), "a scoped count admits only its own namespace")
		require.True(t, counter.admits("test:e1"))

		require.False(t, counter.counts("OTHER"), "only the constrained relationship type counts")
		require.Equal(t, 0, counter.count)
		require.False(t, counter.reached())

		require.True(t, counter.counts("OWNS"))
		require.Equal(t, 1, counter.count)
		require.False(t, counter.reached())
		require.True(t, counter.counts("OWNS"))
		require.True(t, counter.reached(), "two counted images fill a maximum count of two")

		violation := counter.violation()
		require.Equal(t, ConstraintCardinality, violation.Type)
		require.Equal(t, "OWNS", violation.Label)
		require.Nil(t, violation.Properties)
		require.Equal(t,
			"Adding this edge would exceed max outgoing count of 2 for relationship type OWNS on node test:a (current: 2)",
			violation.Message)
		var localized *localization.LocalizedError
		require.True(t, errors.As(violation.Cause, &localized), "the violation keeps the message identity")
		require.Equal(t, localization.MessageStorageValidationCardinalityExceeded, localized.Message.ID)
	})

	t.Run("incoming", func(t *testing.T) {
		incoming := outgoing
		incoming.Direction = "INCOMING"
		counter := newEdgeCardinalityCounter(incoming, probe, "", "")
		require.Equal(t, NodeID("test:b"), counter.anchor)
		require.False(t, counter.anchored(&Edge{StartNode: "test:b", EndNode: "test:z"}))
		require.True(t, counter.anchored(&Edge{StartNode: "test:z", EndNode: "test:b"}))
		require.True(t, counter.admits("other:e"), "an unscoped count admits every namespace")
		require.Equal(t,
			"Adding this edge would exceed max incoming count of 2 for relationship type OWNS on node test:b (current: 0)",
			counter.violation().Message)
	})
}

// cardinalityDifferentialEngine is the node layout every differential case shares.
func cardinalityDifferentialEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	engine := newTestEngine(t)
	for _, id := range []NodeID{"test:a", "test:b", "test:c", "test:d", "test:e", "other:o1", "other:o2"} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Endpoint"}, Properties: map[string]any{}})
		require.NoError(t, err, "CreateNode(%q)", id)
	}
	return engine
}

// stagedCardinalityWrite runs steps in one explicit transaction, reporting the
// failure the caller would see and committing the writes otherwise.
func stagedCardinalityWrite(t *testing.T, engine *BadgerEngine, steps func(tx *BadgerTransaction) error) error {
	t.Helper()
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	if err := steps(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

type cardinalityRouteCase struct {
	name       string
	constraint Constraint
	// committed prepares committed state before the route runs.
	committed func(t *testing.T, engine *BadgerEngine)
	// stagedCommitted prepares committed state for the staged route when the
	// route stages part of the population itself; nil means committed.
	stagedCommitted func(t *testing.T, engine *BadgerEngine)
	// direct is the mutation under validation through the engine's publishing
	// transaction, which counts the raw adjacency index.
	direct func(t *testing.T, engine *BadgerEngine) error
	// staged is the same logical mutation through an explicit transaction,
	// which counts its staged view.
	staged func(t *testing.T, engine *BadgerEngine) error
	// overThreshold marks a case whose population already exceeds the
	// constraint: the publishing route stops at the image that fills it and the
	// staged route counts the whole population.
	overThreshold bool
}

func TestCardinalityRoutesAgree(t *testing.T) {
	owns := func(direction string, maxCount int) Constraint {
		return Constraint{
			Name: "owns", Type: ConstraintCardinality, EntityType: ConstraintEntityRelationship,
			Label: "OWNS", Direction: direction, MaxCount: maxCount,
		}
	}
	image := func(id EdgeID, start, end NodeID, relationType string) *Edge {
		return &Edge{ID: id, StartNode: start, EndNode: end, Type: relationType, Properties: map[string]any{}}
	}
	withRelationType := func(id EdgeID, start, end NodeID, relationType string) *Edge {
		edge := image(id, start, end, relationType)
		edge.Properties["v"] = int64(1)
		return edge
	}
	constrain := func(t *testing.T, engine *BadgerEngine, c Constraint) {
		t.Helper()
		require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(c))
	}
	staged := func(steps func(tx *BadgerTransaction) error) func(*testing.T, *BadgerEngine) error {
		return func(t *testing.T, engine *BadgerEngine) error {
			t.Helper()
			return stagedCardinalityWrite(t, engine, steps)
		}
	}

	outgoingMaxTwo := owns("OUTGOING", 2)
	outgoingMaxOne := owns("OUTGOING", 1)
	incomingMaxOne := owns("INCOMING", 1)
	overLimit := owns("OUTGOING", 1)

	cases := []cardinalityRouteCase{
		{
			name:       "outgoing_healthy",
			constraint: outgoingMaxTwo,
			committed:  func(t *testing.T, engine *BadgerEngine) { constrain(t, engine, outgoingMaxTwo) },
			direct: func(t *testing.T, engine *BadgerEngine) error {
				return engine.CreateEdge(t.Context(), image("test:e1", "test:a", "test:b", "OWNS"))
			},
			staged: staged(func(tx *BadgerTransaction) error {
				return tx.CreateEdge(t.Context(), image("test:e1", "test:a", "test:b", "OWNS"))
			}),
		},
		{
			name:       "outgoing_max_reached",
			constraint: outgoingMaxOne,
			committed: func(t *testing.T, engine *BadgerEngine) {
				require.NoError(t, engine.CreateEdge(t.Context(), image("test:e0", "test:a", "test:b", "OWNS")))
				constrain(t, engine, outgoingMaxOne)
			},
			direct: func(t *testing.T, engine *BadgerEngine) error {
				return engine.CreateEdge(t.Context(), image("test:e1", "test:a", "test:c", "OWNS"))
			},
			staged: staged(func(tx *BadgerTransaction) error {
				return tx.CreateEdge(t.Context(), image("test:e1", "test:a", "test:c", "OWNS"))
			}),
		},
		{
			name:       "incoming_max_reached",
			constraint: incomingMaxOne,
			committed: func(t *testing.T, engine *BadgerEngine) {
				require.NoError(t, engine.CreateEdge(t.Context(), image("test:e0", "test:a", "test:b", "OWNS")))
				constrain(t, engine, incomingMaxOne)
			},
			direct: func(t *testing.T, engine *BadgerEngine) error {
				return engine.CreateEdge(t.Context(), image("test:e1", "test:c", "test:b", "OWNS"))
			},
			staged: staged(func(tx *BadgerTransaction) error {
				return tx.CreateEdge(t.Context(), image("test:e1", "test:c", "test:b", "OWNS"))
			}),
		},
		{
			name:       "direction_isolated",
			constraint: outgoingMaxOne,
			committed: func(t *testing.T, engine *BadgerEngine) {
				require.NoError(t, engine.CreateEdge(t.Context(), image("test:e0", "test:a", "test:b", "OWNS")))
				constrain(t, engine, outgoingMaxOne)
			},
			direct: func(t *testing.T, engine *BadgerEngine) error {
				return engine.CreateEdge(t.Context(), image("test:e1", "test:c", "test:a", "OWNS"))
			},
			staged: staged(func(tx *BadgerTransaction) error {
				return tx.CreateEdge(t.Context(), image("test:e1", "test:c", "test:a", "OWNS"))
			}),
		},
		{
			name:       "namespace_isolated",
			constraint: outgoingMaxOne,
			committed: func(t *testing.T, engine *BadgerEngine) {
				require.NoError(t, engine.CreateEdge(t.Context(), image("other:e0", "other:o1", "other:o2", "OWNS")))
				constrain(t, engine, outgoingMaxOne)
			},
			direct: func(t *testing.T, engine *BadgerEngine) error {
				return engine.CreateEdge(t.Context(), image("test:e1", "test:a", "test:b", "OWNS"))
			},
			staged: staged(func(tx *BadgerTransaction) error {
				return tx.CreateEdge(t.Context(), image("test:e1", "test:a", "test:b", "OWNS"))
			}),
		},
		{
			name:       "own_edge_rewrite",
			constraint: outgoingMaxOne,
			committed: func(t *testing.T, engine *BadgerEngine) {
				require.NoError(t, engine.CreateEdge(t.Context(), image("test:e0", "test:a", "test:b", "OWNS")))
				constrain(t, engine, outgoingMaxOne)
			},
			direct: func(t *testing.T, engine *BadgerEngine) error {
				return engine.UpdateEdge(t.Context(), EdgeUpdateRequest{After: withRelationType("test:e0", "test:a", "test:b", "OWNS")})
			},
			staged: staged(func(tx *BadgerTransaction) error {
				return tx.UpdateEdge(t.Context(), EdgeUpdateRequest{After: withRelationType("test:e0", "test:a", "test:b", "OWNS")})
			}),
		},
		{
			name:       "delete_frees_slot",
			constraint: outgoingMaxOne,
			committed: func(t *testing.T, engine *BadgerEngine) {
				require.NoError(t, engine.CreateEdge(t.Context(), image("test:e0", "test:a", "test:b", "OWNS")))
				constrain(t, engine, outgoingMaxOne)
			},
			direct: func(t *testing.T, engine *BadgerEngine) error {
				require.NoError(t, engine.DeleteEdge(t.Context(), "test:e0"))
				return engine.CreateEdge(t.Context(), image("test:e1", "test:a", "test:c", "OWNS"))
			},
			staged: staged(func(tx *BadgerTransaction) error {
				require.NoError(t, tx.DeleteEdge(t.Context(), "test:e0"))
				return tx.CreateEdge(t.Context(), image("test:e1", "test:a", "test:c", "OWNS"))
			}),
		},
		{
			name:       "retype_frees_slot",
			constraint: outgoingMaxOne,
			committed: func(t *testing.T, engine *BadgerEngine) {
				require.NoError(t, engine.CreateEdge(t.Context(), image("test:e0", "test:a", "test:b", "OWNS")))
				constrain(t, engine, outgoingMaxOne)
			},
			direct: func(t *testing.T, engine *BadgerEngine) error {
				require.NoError(t, engine.UpdateEdge(t.Context(), EdgeUpdateRequest{After: image("test:e0", "test:a", "test:b", "OTHER")}))
				return engine.CreateEdge(t.Context(), image("test:e1", "test:a", "test:c", "OWNS"))
			},
			staged: staged(func(tx *BadgerTransaction) error {
				require.NoError(t, tx.UpdateEdge(t.Context(), EdgeUpdateRequest{After: image("test:e0", "test:a", "test:b", "OTHER")}))
				return tx.CreateEdge(t.Context(), image("test:e1", "test:a", "test:c", "OWNS"))
			}),
		},
		{
			name:       "move_frees_slot",
			constraint: outgoingMaxOne,
			committed: func(t *testing.T, engine *BadgerEngine) {
				require.NoError(t, engine.CreateEdge(t.Context(), image("test:e0", "test:a", "test:b", "OWNS")))
				constrain(t, engine, outgoingMaxOne)
			},
			direct: func(t *testing.T, engine *BadgerEngine) error {
				require.NoError(t, engine.UpdateEdge(t.Context(), EdgeUpdateRequest{After: image("test:e0", "test:b", "test:c", "OWNS")}))
				return engine.CreateEdge(t.Context(), image("test:e1", "test:a", "test:d", "OWNS"))
			},
			staged: staged(func(tx *BadgerTransaction) error {
				require.NoError(t, tx.UpdateEdge(t.Context(), EdgeUpdateRequest{After: image("test:e0", "test:b", "test:c", "OWNS")}))
				return tx.CreateEdge(t.Context(), image("test:e1", "test:a", "test:d", "OWNS"))
			}),
		},
		{
			name:       "pending_combination",
			constraint: outgoingMaxTwo,
			committed: func(t *testing.T, engine *BadgerEngine) {
				// The publishing route sees the population the staged route builds
				// up: one committed image plus the image the transaction stages.
				require.NoError(t, engine.CreateEdge(t.Context(), image("test:e0", "test:a", "test:b", "OWNS")))
				require.NoError(t, engine.CreateEdge(t.Context(), image("test:e1", "test:a", "test:c", "OWNS")))
				constrain(t, engine, outgoingMaxTwo)
			},
			stagedCommitted: func(t *testing.T, engine *BadgerEngine) {
				require.NoError(t, engine.CreateEdge(t.Context(), image("test:e0", "test:a", "test:b", "OWNS")))
				constrain(t, engine, outgoingMaxTwo)
			},
			direct: func(t *testing.T, engine *BadgerEngine) error {
				return engine.CreateEdge(t.Context(), image("test:e2", "test:a", "test:d", "OWNS"))
			},
			staged: staged(func(tx *BadgerTransaction) error {
				require.NoError(t, tx.CreateEdge(t.Context(), image("test:e1", "test:a", "test:c", "OWNS")))
				return tx.CreateEdge(t.Context(), image("test:e2", "test:a", "test:d", "OWNS"))
			}),
		},
		{
			name:       "over_threshold_population",
			constraint: overLimit,
			committed: func(t *testing.T, engine *BadgerEngine) {
				// The constraint arrives over an existing population.
				require.NoError(t, engine.CreateEdge(t.Context(), image("test:e0", "test:a", "test:b", "OWNS")))
				require.NoError(t, engine.CreateEdge(t.Context(), image("test:e1", "test:a", "test:c", "OWNS")))
				require.NoError(t, engine.CreateEdge(t.Context(), image("test:e2", "test:a", "test:d", "OWNS")))
				constrain(t, engine, overLimit)
			},
			direct: func(t *testing.T, engine *BadgerEngine) error {
				return engine.CreateEdge(t.Context(), image("test:e3", "test:a", "test:e", "OWNS"))
			},
			staged: staged(func(tx *BadgerTransaction) error {
				return tx.CreateEdge(t.Context(), image("test:e3", "test:a", "test:e", "OWNS"))
			}),
			overThreshold: true,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			directEngine := cardinalityDifferentialEngine(t)
			testCase.committed(t, directEngine)
			directErr := testCase.direct(t, directEngine)

			stagedEngine := cardinalityDifferentialEngine(t)
			if testCase.stagedCommitted != nil {
				testCase.stagedCommitted(t, stagedEngine)
			} else {
				testCase.committed(t, stagedEngine)
			}
			stagedErr := testCase.staged(t, stagedEngine)

			require.Equal(t, directErr == nil, stagedErr == nil,
				"routes disagree: publishing route %v, staged route %v", directErr, stagedErr)
			if directErr == nil {
				return
			}
			directMessage := cardinalityViolationMessage(t, directErr, testCase.constraint)
			stagedMessage := cardinalityViolationMessage(t, stagedErr, testCase.constraint)
			if testCase.overThreshold {
				// Preserved convention: the publishing route stops at the image that
				// fills the constraint, the staged route counts the whole population.
				// Both report the count each actually enforced.
				require.Equal(t, cardinalityMessagePrefix(directMessage), cardinalityMessagePrefix(stagedMessage))
				require.Equal(t, testCase.constraint.MaxCount, cardinalityCurrent(t, directMessage))
				require.Equal(t, 3, cardinalityCurrent(t, stagedMessage))
				return
			}
			require.Equal(t, directMessage, stagedMessage)
		})
	}
}

// stagedCardinalityViaCurrentAdjacency is the measured alternative to the staged
// route's own selection: it counts the same population through the transaction's
// current-adjacency owner instead of streaming pending and committed images. It
// exists to prove the reuse is decision-equivalent and to measure what its
// materialization costs; production code does not use it.
func stagedCardinalityViaCurrentAdjacency(tx *BadgerTransaction, edge *Edge, c Constraint, namespace string) error {
	counter := newEdgeCardinalityCounter(c, edge, namespace, edge.ID)
	direction := AdjacentOutgoing
	if c.Direction != "OUTGOING" {
		direction = AdjacentIncoming
	}
	images, err := tx.currentDirectedEdgesLocked(context.Background(), AdjacentEdgeRead{Direction: direction, Node: counter.anchor})
	if err != nil {
		return err
	}
	for _, image := range images {
		if counter.admits(image.ID) {
			counter.counts(image.Type)
		}
	}
	if counter.reached() {
		return counter.violation()
	}
	return nil
}

func TestStagedCardinalityMatchesCurrentAdjacencySelection(t *testing.T) {
	image := func(id EdgeID, start, end NodeID, relationType string) *Edge {
		return &Edge{ID: id, StartNode: start, EndNode: end, Type: relationType, Properties: map[string]any{}}
	}
	outgoingMaxTwo := Constraint{Type: ConstraintCardinality, Label: "OWNS", Direction: "OUTGOING", MaxCount: 2}
	probe := image("test:new", "test:a", "test:e", "OWNS")

	overlays := []struct {
		name    string
		pending []*Edge
		deleted []EdgeID
	}{
		{name: "committed_only"},
		{name: "pending_addition", pending: []*Edge{image("test:e2", "test:a", "test:d", "OWNS")}},
		{name: "rewritten_pending", pending: []*Edge{image("test:e1", "test:a", "test:c", "OWNS")}},
		{name: "deleted_committed", deleted: []EdgeID{"test:e1"}},
		{name: "moved_away", pending: []*Edge{image("test:e1", "test:b", "test:c", "OWNS")}},
		{name: "retyped", pending: []*Edge{image("test:e1", "test:a", "test:c", "OTHER")}},
		{name: "namespace_rewritten", pending: []*Edge{image("other:e1", "test:a", "test:c", "OWNS")}},
	}
	for _, overlay := range overlays {
		t.Run(overlay.name, func(t *testing.T) {
			engine := cardinalityDifferentialEngine(t)
			require.NoError(t, engine.CreateEdge(t.Context(), image("test:e0", "test:a", "test:b", "OWNS")))
			require.NoError(t, engine.CreateEdge(t.Context(), image("test:e1", "test:a", "test:c", "OWNS")))
			tx := &BadgerTransaction{
				engine:       engine,
				pendingEdges: make(map[EdgeID]*Edge, len(overlay.pending)),
				deletedEdges: make(map[EdgeID]struct{}, len(overlay.deleted)),
			}
			for _, edge := range overlay.pending {
				tx.pendingEdges[edge.ID] = edge
			}
			for _, id := range overlay.deleted {
				tx.deletedEdges[id] = struct{}{}
			}

			streamed := tx.checkEdgeCardinality(context.Background(), probe, outgoingMaxTwo, "test")
			viaAdjacency := stagedCardinalityViaCurrentAdjacency(tx, probe, outgoingMaxTwo, "test")
			require.Equal(t, streamed == nil, viaAdjacency == nil,
				"staged selection and current-adjacency selection disagree: %v vs %v", streamed, viaAdjacency)
			if streamed == nil {
				return
			}
			require.Equal(t,
				cardinalityViolationMessage(t, streamed, outgoingMaxTwo),
				cardinalityViolationMessage(t, viaAdjacency, outgoingMaxTwo))
		})
	}
}

// cardinalityViolationMessage reports the message of a CARDINALITY violation.
func cardinalityViolationMessage(t *testing.T, err error, c Constraint) string {
	t.Helper()
	var violation *ConstraintViolationError
	require.ErrorAs(t, err, &violation)
	require.Equal(t, ConstraintCardinality, violation.Type)
	require.Equal(t, c.Label, violation.Label)
	return violation.Message
}

// cardinalityMessagePrefix is the violation message without the count it reports.
func cardinalityMessagePrefix(message string) string {
	if start := strings.LastIndex(message, cardinalityCurrentMarker); start >= 0 {
		return message[:start]
	}
	return message
}

const cardinalityCurrentMarker = "(current: "

// cardinalityCurrent is the population a violation reports.
func cardinalityCurrent(t *testing.T, message string) int {
	t.Helper()
	start := strings.LastIndex(message, cardinalityCurrentMarker)
	require.GreaterOrEqual(t, start, 0, "message %q reports no count", message)
	value := strings.TrimSuffix(message[start+len(cardinalityCurrentMarker):], ")")
	current, err := strconv.Atoi(value)
	require.NoError(t, err, "message %q", message)
	return current
}
