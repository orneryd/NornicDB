package storage

// Close-out lane L3 (v1.5.2 verification campaign), items P-N15-147 (NOR-380),
// P-N15-145 (NOR-379) and P-N15-144 (NOR-378). Test-side only.
//
// Each item asks whether a relationship write acknowledged through the
// write-behind buffer (implicit commit, ACK before replay) is seen by the
// relationship constraint checks of a later direct engine write, exactly as a
// synchronous commit or a drained ACK is. Every case runs at three cut points:
//
//	synchronous: explicit commit, no buffering (control)
//	drain:       implicit commit ACKed, then FlushWriteBehind (control)
//	ack:         implicit commit ACKed and still pending (the item's case)
//
// The expected outcome is identical at all three points. A failure whose
// message starts with PRECONDITION means the setup did not reach the state the
// item describes (not a defect verdict); a failure starting with DEFECT is the
// item's disagreement at that point.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var closeoutL3Points = []string{"synchronous", "drain", "ack"}

// closeoutL3Commit stages edit in one transaction and commits it at point.
func closeoutL3Commit(t *testing.T, engine *BadgerEngine, point string, edit func(*BadgerTransaction) error) {
	t.Helper()
	tx, err := engine.BeginTransaction(context.Background(), OrdinaryBegin)
	require.NoError(t, err, "PRECONDITION: begin")
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"), "PRECONDITION: namespace")
	require.NoError(t, tx.SetImplicit(point != "synchronous"), "PRECONDITION: commit mode")
	require.NoError(t, edit(tx), "PRECONDITION: staged edit")
	require.NoError(t, tx.Commit(), "PRECONDITION: commit")
	require.Equal(t, point != "synchronous", tx.AcknowledgedBuffered(), "PRECONDITION: commit route (buffered ACK expected for implicit commits)")
	switch point {
	case "drain":
		require.NoError(t, engine.FlushWriteBehind(context.Background()), "PRECONDITION: drain")
		require.Zero(t, engine.writeBehind.PendingOps(), "PRECONDITION: drained")
	case "ack":
		require.Positive(t, engine.writeBehind.PendingOps(), "PRECONDITION: the ACKed commit must still be pending (not replayed) when the direct write runs")
	}
	t.Logf("point=%s acknowledgedBuffered=%v pendingOps=%d", point, tx.AcknowledgedBuffered(), engine.writeBehind.PendingOps())
}

func closeoutL3Engine(t *testing.T) *BadgerEngine {
	t.Helper()
	// Manual drain: the background replay worker is stopped, so an ACKed
	// commit stays pending until an explicit drain or a write's ordering
	// barrier replays it.
	return newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
}

// P-N15-147 (NOR-380): adjacent relationship ALLOWED / DISALLOWED policy at a
// node label edit after an ACKed relationship create or delete.
func TestCloseoutL3_N15_147_AdjacentPolicyAfterBufferedRelationshipACK(t *testing.T) {
	for _, point := range closeoutL3Points {
		for _, mode := range []string{"ALLOWED", "DISALLOWED"} {
			for _, endpoint := range []string{"source", "target"} {
				for _, mutation := range []string{"edge-create", "edge-delete"} {
					t.Run(point+"/"+mode+"/"+endpoint+"/"+mutation, func(t *testing.T) {
						ctx := context.Background()
						engine := closeoutL3Engine(t)
						startLabel, newLabel := "Allowed", "Blocked"
						if mode == "DISALLOWED" {
							startLabel, newLabel = "Plain", "Bad"
						}
						for _, id := range []NodeID{"test:a", "test:b"} {
							_, err := engine.CreateNode(ctx, &Node{ID: id, Labels: []string{startLabel}})
							require.NoError(t, err, "PRECONDITION: node")
						}
						schema := engine.GetSchemaForNamespace("test")
						if mode == "ALLOWED" {
							require.NoError(t, schema.AddConstraint(Constraint{Name: "allow_aa", Type: ConstraintPolicy, EntityType: ConstraintEntityRelationship, Label: "T", PolicyMode: "ALLOWED", SourceLabel: "Allowed", TargetLabel: "Allowed"}), "PRECONDITION: policy")
						} else {
							require.NoError(t, schema.AddConstraint(Constraint{Name: "deny_bad_source", Type: ConstraintPolicy, EntityType: ConstraintEntityRelationship, Label: "T", PolicyMode: "DISALLOWED", SourceLabel: "Bad", TargetLabel: "Plain"}), "PRECONDITION: policy")
							require.NoError(t, schema.AddConstraint(Constraint{Name: "deny_bad_target", Type: ConstraintPolicy, EntityType: ConstraintEntityRelationship, Label: "T", PolicyMode: "DISALLOWED", SourceLabel: "Plain", TargetLabel: "Bad"}), "PRECONDITION: policy")
						}
						edge := func() *Edge { return &Edge{ID: "test:holder", StartNode: "test:a", EndNode: "test:b", Type: "T"} }
						if mutation == "edge-delete" {
							require.NoError(t, engine.CreateEdge(ctx, edge()), "PRECONDITION: existing relationship")
						}
						closeoutL3Commit(t, engine, point, func(tx *BadgerTransaction) error {
							if mutation == "edge-create" {
								return tx.CreateEdge(ctx, edge())
							}
							return tx.DeleteEdge(ctx, "test:holder")
						})
						relabel := NodeID("test:b")
						if endpoint == "source" {
							relabel = "test:a"
						}
						err := engine.UpdateNode(ctx, NodeUpdateRequest{After: &Node{ID: relabel, Labels: []string{newLabel}}, Policy: ClientNodeEdits})
						t.Logf("relabel %s -> %s after %s at %s: err=%v", relabel, newLabel, mutation, point, err)
						if mutation == "edge-create" {
							require.Error(t, err, "DEFECT: the relabel must be rejected because the ACKed relationship makes it violate the %s policy", mode)
							var violation *ConstraintViolationError
							require.True(t, errors.As(err, &violation), "DEFECT: expected a policy ConstraintViolationError, got %T: %v", err, err)
							require.Equal(t, ConstraintPolicy, violation.Type, "DEFECT: violation type")
						} else {
							require.NoError(t, err, "DEFECT: the ACKed relationship delete removed the policy obligation; the relabel must be accepted")
						}
					})
				}
			}
		}
	}
}

// P-N15-145 (NOR-379): relationship cardinality after an ACKed relationship
// create, delete or endpoint move, then a direct relationship create.
func TestCloseoutL3_N15_145_CardinalityAfterBufferedRelationshipACK(t *testing.T) {
	for _, point := range closeoutL3Points {
		for _, direction := range []string{"OUTGOING", "INCOMING"} {
			for _, mutation := range []string{"create", "delete", "move"} {
				t.Run(point+"/"+direction+"/"+mutation, func(t *testing.T) {
					ctx := context.Background()
					engine := closeoutL3Engine(t)
					for _, id := range []NodeID{"test:hub", "test:a", "test:other"} {
						_, err := engine.CreateNode(ctx, &Node{ID: id})
						require.NoError(t, err, "PRECONDITION: node")
					}
					edge := func(id EdgeID, anchor NodeID) *Edge {
						result := &Edge{ID: id, StartNode: anchor, EndNode: "test:a", Type: "T"}
						if direction == "INCOMING" {
							result.StartNode, result.EndNode = "test:a", anchor
						}
						return result
					}
					if mutation != "create" {
						require.NoError(t, engine.CreateEdge(ctx, edge("test:holder", "test:hub")), "PRECONDITION: existing relationship")
					}
					require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(Constraint{Name: "card_one", Type: ConstraintCardinality, EntityType: ConstraintEntityRelationship, Label: "T", Direction: direction, MaxCount: 1}), "PRECONDITION: cardinality constraint")
					closeoutL3Commit(t, engine, point, func(tx *BadgerTransaction) error {
						switch mutation {
						case "create":
							return tx.CreateEdge(ctx, edge("test:holder", "test:hub"))
						case "delete":
							return tx.DeleteEdge(ctx, "test:holder")
						default:
							return tx.UpdateEdge(ctx, EdgeUpdateRequest{After: edge("test:holder", "test:other")})
						}
					})
					err := engine.CreateEdge(ctx, edge("test:probe", "test:hub"))
					t.Logf("probe create at hub after %s at %s (%s): err=%v", mutation, point, direction, err)
					if mutation == "create" {
						var violation *ConstraintViolationError
						require.True(t, errors.As(err, &violation), "DEFECT: the ACKed relationship fills the hub's cardinality of 1; the probe must be rejected (got %v)", err)
						require.Equal(t, ConstraintCardinality, violation.Type, "DEFECT: violation type")
					} else {
						require.NoError(t, err, "DEFECT: the ACKed %s freed the hub's only slot; the probe must be accepted", mutation)
					}
				})
			}
		}
	}
}

// P-N15-144 (NOR-378): relationship temporal no-overlap after an ACKed
// relationship interval change, then a direct relationship create.
func TestCloseoutL3_N15_144_TemporalAfterBufferedRelationshipACK(t *testing.T) {
	for _, point := range closeoutL3Points {
		t.Run(point, func(t *testing.T) {
			ctx := context.Background()
			engine := closeoutL3Engine(t)
			require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(Constraint{Name: "history", Type: ConstraintTemporal, EntityType: ConstraintEntityRelationship, Label: "T", Properties: []string{"key", "from", "to"}}), "PRECONDITION: temporal constraint")
			for _, id := range []NodeID{"test:a", "test:b"} {
				_, err := engine.CreateNode(ctx, &Node{ID: id})
				require.NoError(t, err, "PRECONDITION: node")
			}
			start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			props := func(key string, offset time.Duration) map[string]any {
				return map[string]any{"key": key, "from": start.Add(offset), "to": start.Add(offset + time.Hour)}
			}
			edge := func(id EdgeID, key string, offset time.Duration) *Edge {
				return &Edge{ID: id, StartNode: "test:a", EndNode: "test:b", Type: "T", Properties: props(key, offset)}
			}
			require.NoError(t, engine.CreateEdge(ctx, edge("test:holder", "before", 0)), "PRECONDITION: existing interval")
			closeoutL3Commit(t, engine, point, func(tx *BadgerTransaction) error {
				return tx.UpdateEdge(ctx, EdgeUpdateRequest{After: edge("test:holder", "changed", 2*time.Hour)})
			})
			overlapErr := engine.CreateEdge(ctx, edge("test:overlap", "changed", 150*time.Minute))
			t.Logf("overlapping create on the ACKed interval at %s: err=%v", point, overlapErr)
			var violation *ConstraintViolationError
			require.True(t, errors.As(overlapErr, &violation), "DEFECT: the new interval overlaps the ACKed one (key=changed); it must be rejected (got %v)", overlapErr)
			require.Equal(t, ConstraintTemporal, violation.Type, "DEFECT: violation type")
			freedErr := engine.CreateEdge(ctx, edge("test:freed", "before", 0))
			t.Logf("create on the superseded interval at %s: err=%v", point, freedErr)
			require.NoError(t, freedErr, "DEFECT: the ACKed update moved the holder off key=before; that interval is free")
		})
	}
}
