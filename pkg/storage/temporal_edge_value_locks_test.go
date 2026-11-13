package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// temporalEdgeLockConstraint is the relationship NO OVERLAP constraint these
// controls register: one grouping column and the (from, to) window.
var temporalEdgeLockConstraint = Constraint{
	Name:       "records_no_overlap",
	Type:       ConstraintTemporal,
	EntityType: ConstraintEntityRelationship,
	Label:      "RECORDS",
	Properties: []string{"subject", "from", "to"},
}

// temporalEdgeLockFixture registers that constraint in the "test" namespace and
// seeds the two endpoint nodes every staged edge uses.
func temporalEdgeLockFixture(t *testing.T) *BadgerEngine {
	t.Helper()
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(temporalEdgeLockConstraint))
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Endpoint"}, Properties: map[string]any{}})
		require.NoError(t, err, "CreateNode(%q)", id)
	}
	return engine
}

// temporalEdgeLockImage is one writer's edge: the fixture's key column, a two
// hour interval from from, and a distinct ID.
func temporalEdgeLockImage(id EdgeID, subject string, from time.Time) *Edge {
	edge := &Edge{ID: id, StartNode: "test:a", EndNode: "test:b", Type: temporalEdgeLockConstraint.Label}
	edge.Properties = map[string]any{
		"subject": subject,
		"from":    from,
		"to":      from.Add(2 * time.Hour),
	}
	return edge
}

// stageTemporalEdgeTransactions begins one transaction per image and stages that
// image, so every commit below starts with its edge already pending.
func stageTemporalEdgeTransactions(t *testing.T, engine *BadgerEngine, images []*Edge) []*BadgerTransaction {
	t.Helper()
	txs := make([]*BadgerTransaction, len(images))
	for i, image := range images {
		tx, err := engine.BeginTransaction(t.Context())
		require.NoError(t, err)
		require.NoError(t, tx.CreateEdge(t.Context(), image), "stage %s", image.ID)
		txs[i] = tx
	}
	return txs
}

// commitTemporalEdgesTogether releases every staged transaction at the same
// instant so their commit windows overlap, and reports each result in order.
func commitTemporalEdgesTogether(txs []*BadgerTransaction) []error {
	errs := make([]error, len(txs))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range txs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = txs[i].Commit()
		}(i)
	}
	close(start)
	wg.Wait()
	return errs
}

// TestTemporalRelationshipValueLockKeyDomain pins the two properties the commit
// serialization depends on: a relationship temporal group projects exactly the
// node group's value, and its key lives in the relationship label domain, never
// the node label domain of the same name.
func TestTemporalRelationshipValueLockKeyDomain(t *testing.T) {
	single := []string{"subject", "from", "to"}
	composite := []string{"subject", "target", "from", "to"}
	for _, tc := range []struct {
		name         string
		properties   []string
		wantProperty string
	}{
		{name: "single", properties: single, wantProperty: "subject"},
		{name: "composite", properties: composite, wantProperty: string(ConstraintTemporal) + ":subject,target"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values := map[string]any{
				"subject": "s1",
				"target":  "t1",
				"from":    "2026-01-01T00:00:00Z",
				"to":      "2026-01-02T00:00:00Z",
			}
			schema := NewSchemaManager()
			relationship := temporalEdgeLockConstraint
			relationship.Properties = tc.properties
			node := relationship
			node.Name = "records_no_overlap_node"
			node.EntityType = ConstraintEntityNode
			require.NoError(t, schema.AddConstraint(relationship))
			require.NoError(t, schema.AddConstraint(node))

			edge := &Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: relationship.Label, Properties: values}
			edgeKeys, err := schema.appendConstraintValueKeys(t.Context(), nil, nil, []*Edge{edge})
			require.NoError(t, err)
			require.Len(t, edgeKeys, 1, "a temporal relationship group reserves exactly one commit key")
			stored := &Node{ID: "test:n1", Labels: []string{node.Label}, Properties: values}
			nodeKeys, err := schema.appendConstraintValueKeys(t.Context(), nil, []*Node{stored}, nil)
			require.NoError(t, err)
			require.Len(t, nodeKeys, 1)

			require.Equal(t, uniqueConstraintLockKey{label: node.Label, property: tc.wantProperty, value: nodeKeys[0].value}, nodeKeys[0])
			require.Equal(t, uniqueConstraintLockKey{label: relationshipConstraintLabel(relationship.Label), property: tc.wantProperty, value: nodeKeys[0].value}, edgeKeys[0],
				"the relationship key is the node group projection in the relationship label domain")
			require.NotEqual(t, nodeKeys[0], edgeKeys[0], "a relationship key never shares the node label domain")
			require.True(t, lockableUniqueConstraintKey(edgeKeys[0]), "the relationship key must be lockable")
		})
	}
}

// TestTemporalRelationshipNoOverlapCommitsSerialize commits overlapping writers
// of one temporal group together, at widths 1 and 4. Every acknowledged writer
// leaves the committed group with overlapping intervals, so exactly one may be
// acknowledged per round and the rest must be rejected by the constraint.
func TestTemporalRelationshipNoOverlapCommitsSerialize(t *testing.T) {
	engine := temporalEdgeLockFixture(t)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	const rounds = 6
	for _, width := range []int{1, 4} {
		t.Run(fmt.Sprintf("width=%d", width), func(t *testing.T) {
			for round := 0; round < rounds; round++ {
				subject := fmt.Sprintf("w%d-r%d", width, round)
				images := make([]*Edge, width)
				for i := range images {
					id := EdgeID(fmt.Sprintf("test:e-w%d-r%d-%d", width, round, i))
					images[i] = temporalEdgeLockImage(id, subject, base)
				}
				errs := commitTemporalEdgesTogether(stageTemporalEdgeTransactions(t, engine, images))
				acknowledged := 0
				for i, err := range errs {
					if err == nil {
						acknowledged++
						continue
					}
					var violation *ConstraintViolationError
					require.ErrorAs(t, err, &violation, "writer %d round %d: %v", i, round, err)
					require.Equal(t, ConstraintTemporal, violation.Type)
					require.Equal(t, temporalEdgeLockConstraint.Label, violation.Label)
				}
				require.Equal(t, 1, acknowledged,
					"round %d width %d: %d overlapping writers of one temporal group were acknowledged", round, width, acknowledged)
			}
		})
	}
}

// TestTemporalRelationshipDisjointGroupsCommitConcurrently stages overlapping
// windows in distinct temporal groups and commits them together: disjoint groups
// must not serialize or reject each other.
func TestTemporalRelationshipDisjointGroupsCommitConcurrently(t *testing.T) {
	engine := temporalEdgeLockFixture(t)
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	const width = 4
	images := make([]*Edge, width)
	for i := range images {
		id := EdgeID(fmt.Sprintf("test:d-%d", i))
		images[i] = temporalEdgeLockImage(id, fmt.Sprintf("group-%d", i), base)
	}
	errs := commitTemporalEdgesTogether(stageTemporalEdgeTransactions(t, engine, images))
	for i, err := range errs {
		require.NoError(t, err, "disjoint group writer %d must commit", i)
	}
}
