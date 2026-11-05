package storage

// Regression reproduction for NOR-374 on release f0074ec55: the canonical
// unique-value publisher retires every committed predecessor claim and then
// reacquires the final claims, so a node that keeps its label and canonical
// value is briefly absent from its UNIQUE claim cache. A concurrent reader
// that later resolves MERGE/commit validation through
// LookupUniqueConstraintValue can observe that absence and treat the value as
// free.
//
// The window is made deterministic without product hooks by blocking the
// publisher inside its retire phase: the node carries a second constrained
// claim (Aux.gate) under a second label, and holding that constraint's read
// lock stops ApplyUniqueValue there. Label order selects which constraint the
// retire loop reaches first, so the Doc-then-Aux subtest must observe the
// retire of the unchanged Doc claim before the publisher blocks. The reversed
// subtest is the control that shows the observation depends only on that
// order, not on timing.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func relatedUnchangedClaimImage(labels []string, counter int64) *Node {
	return &Node{
		ID:     "test:n",
		Labels: append([]string(nil), labels...),
		Properties: map[string]any{
			"id":   "stable",
			"gate": "g",
			"c":    counter,
		},
	}
}

func TestRelatedUnchangedUniqueClaimVisibleDuringPublication(t *testing.T) {
	for _, labels := range [][]string{{"Doc", "Aux"}, {"Aux", "Doc"}} {
		t.Run(strings.Join(labels, "_then_"), func(t *testing.T) {
			engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{synchronous: true})
			sm := engine.GetSchemaForNamespace("test")
			require.NoError(t, sm.AddConstraint(Constraint{
				Name:       "doc_id_unique",
				Type:       ConstraintUnique,
				Label:      "Doc",
				Properties: []string{"id"},
			}))
			require.NoError(t, sm.AddConstraint(Constraint{
				Name:       "aux_gate_unique",
				Type:       ConstraintUnique,
				Label:      "Aux",
				Properties: []string{"gate"},
			}))

			_, err := engine.CreateNode(t.Context(), relatedUnchangedClaimImage(labels, 0))
			require.NoError(t, err)
			// The planner trusts a claim-cache miss only when the cache is
			// complete, so the reproduction uses the rebuilt cache a reader
			// meets in a running database.
			require.NoError(t, RefreshUniqueConstraintValuesForEngine(t.Context(), engine, sm))

			stored, err := engine.GetNode(t.Context(), "test:n")
			require.NoError(t, err)
			require.Equal(t, labels, stored.Labels,
				"the publication pre-image must keep the label order that selects retire order")

			docConstraint := sm.uniqueConstraints[nodePropertyKey{label: "Doc", property: "id"}]
			auxConstraint := sm.uniqueConstraints[nodePropertyKey{label: "Aux", property: "gate"}]
			require.NotNil(t, docConstraint)
			require.NotNil(t, auxConstraint)

			holder, found, constrained, complete, err := sm.LookupUniqueConstraintValue(t.Context(), "Doc", "id", "stable")
			require.NoError(t, err)
			require.True(t, constrained)
			require.True(t, complete, "the reproduction requires the planner-trusted claim cache")
			require.True(t, found, "the created holder must be published")
			require.Equal(t, NodeID("test:n"), holder)

			// Hold the blocker claim's cache: the publisher's retire phase stops
			// there, after it has already reached every claim processed earlier.
			unlock, writerWaiting := holdIndexReadForTesting(&auxConstraint.mu)
			released := false
			release := func() {
				if !released {
					released = true
					unlock()
				}
			}

			updateDone := make(chan struct{})
			var updateErr error
			go func() {
				defer close(updateDone)
				updateErr = engine.UpdateNode(context.Background(), NodeUpdateRequest{
					After:  relatedUnchangedClaimImage(labels, 1),
					Policy: ClientNodeEdits,
				})
			}()
			defer func() {
				release()
				<-updateDone
				require.NoError(t, updateErr)
			}()

			require.Eventually(t, writerWaiting, 5*time.Second, time.Millisecond,
				"the publisher must be parked on the second constraint before observing the claim")
			holder, found, _, complete, err = sm.LookupUniqueConstraintValue(t.Context(), "Doc", "id", "stable")
			require.NoError(t, err)
			require.True(t, complete)
			require.Truef(t, found, "an unchanged claim must stay present while another claim blocks (holder %q)", holder)
			require.Equal(t, NodeID("test:n"), holder)

			release()
			<-updateDone
			require.NoError(t, updateErr)

			holder, found, _, complete, err = sm.LookupUniqueConstraintValue(t.Context(), "Doc", "id", "stable")
			require.NoError(t, err)
			require.True(t, complete)
			require.True(t, found, "the unchanged claim must survive publication")
			require.Equal(t, NodeID("test:n"), holder)

			after, err := engine.GetNode(t.Context(), "test:n")
			require.NoError(t, err)
			require.Equal(t, int64(1), after.Properties["c"])
			require.Equal(t, "stable", after.Properties["id"])
			require.Equal(t, "g", after.Properties["gate"])
		})
	}
}
