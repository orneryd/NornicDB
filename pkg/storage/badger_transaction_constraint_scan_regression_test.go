package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerTransactionConstraintScanErrorsDoNotCommit(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, c := range []Constraint{
		{Name: "unique", Type: ConstraintUnique, Properties: []string{"key"}},
		{Name: "temporal", Type: ConstraintTemporal, Properties: []string{"key", "from", "to"}},
		{Name: "cardinality_outgoing", Type: ConstraintCardinality, Direction: "OUTGOING", MaxCount: 1},
		{Name: "cardinality_incoming", Type: ConstraintCardinality, Direction: "INCOMING", MaxCount: 1},
	} {
		t.Run(c.Name, func(t *testing.T) {
			for _, record := range []struct {
				name  string
				value []byte
			}{
				{"corrupt", []byte("corrupt-record")},
				{"empty", []byte{}},
			} {
				t.Run(record.name, func(t *testing.T) {
					engine := newTestEngine(t)
					for _, id := range []NodeID{"test:source", "test:target"} {
						_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Account"}})
						require.NoError(t, err)
					}
					properties := map[string]interface{}{"key": "duplicate", "from": start, "to": start.Add(time.Hour)}
					require.NoError(t, engine.CreateEdge(&Edge{
						ID: "test:original", StartNode: "test:source", EndNode: "test:target", Type: "REL", Properties: properties,
					}))
					c.Label, c.EntityType = "REL", ConstraintEntityRelationship
					require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(c))
					_, decodeErr := engine.decodeEdgeBodyByID(record.value, "test:original")
					require.Error(t, decodeErr)
					require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
						return txn.Set(edgeKey("test:original"), record.value)
					}))
					engine.cacheDeleteEdge("test:original")

					tx, err := engine.BeginTransaction()
					require.NoError(t, err)
					t.Cleanup(func() { _ = tx.Rollback() })
					require.NoError(t, tx.CreateEdge(&Edge{
						ID: "test:duplicate", StartNode: "test:source", EndNode: "test:target", Type: "REL", Properties: properties,
					}))
					require.ErrorContains(t, tx.Commit(), decodeErr.Error())
					require.NoError(t, engine.withView(func(txn *badger.Txn) error {
						_, err := txn.Get(edgeKey("test:duplicate"))
						require.ErrorIs(t, err, badger.ErrKeyNotFound, "failed commit must not publish a violating relationship")
						return nil
					}))
				})
			}
		})
	}
}

func TestBadgerTransactionPolicyLabelChangeErrorsDoNotCommit(t *testing.T) {
	for _, outgoing := range []bool{true, false} {
		direction := "incoming"
		if outgoing {
			direction = "outgoing"
		}
		t.Run(direction, func(t *testing.T) {
			for _, corruptEndpoint := range []bool{false, true} {
				name := "relationship"
				if corruptEndpoint {
					name = "other_endpoint"
				}
				t.Run(name, func(t *testing.T) {
					engine := newTestEngine(t)
					for _, id := range []NodeID{"test:source", "test:target"} {
						_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Account"}})
						require.NoError(t, err)
					}
					require.NoError(t, engine.CreateEdge(&Edge{
						ID: "test:original", StartNode: "test:source", EndNode: "test:target", Type: "REL",
					}))
					require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(Constraint{
						Name: "policy", Type: ConstraintPolicy, EntityType: ConstraintEntityRelationship, Label: "REL",
						SourceLabel: "Account", TargetLabel: "Account", PolicyMode: "ALLOWED",
					}))
					anchor, other := NodeID("test:target"), NodeID("test:source")
					if outgoing {
						anchor, other = "test:source", "test:target"
					}
					key := edgeKey("test:original")
					_, decodeErr := engine.decodeEdgeBodyByID([]byte("corrupt-record"), "test:original")
					if corruptEndpoint {
						key = nodeKey(other)
						_, decodeErr = engine.decodeNode("test", []byte("corrupt-record"))
					}
					require.Error(t, decodeErr)
					if corruptEndpoint {
						require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
							return txn.Set(key, []byte("corrupt-record"))
						}))
					}

					tx, err := engine.BeginTransaction()
					require.NoError(t, err)
					t.Cleanup(func() { _ = tx.Rollback() })
					require.NoError(t, tx.SetDeferredConstraintValidation(true))
					require.NoError(t, tx.UpdateNode(&Node{ID: anchor, Labels: []string{"Forbidden"}}))
					if !corruptEndpoint {
						// Relabeling also reads adjacency for counters, so inject corruption after staging.
						require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
							return txn.Set(key, []byte("corrupt-record"))
						}))
						engine.cacheDeleteEdge("test:original")
					}
					require.ErrorContains(t, tx.Commit(), decodeErr.Error())
					node, err := engine.GetNode(anchor)
					require.NoError(t, err)
					require.Equal(t, []string{"Account"}, node.Labels, "failed commit must not publish violating labels")
				})
			}
		})
	}
}

func TestBadgerTransactionPolicyEndpointErrorsDoNotCommit(t *testing.T) {
	for _, endpoint := range []NodeID{"test:source", "test:target"} {
		t.Run(string(endpoint), func(t *testing.T) {
			engine := newTestEngine(t)
			for _, id := range []NodeID{"test:source", "test:target"} {
				_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Account"}})
				require.NoError(t, err)
			}
			require.NoError(t, engine.CreateEdge(&Edge{
				ID: "test:original", StartNode: "test:source", EndNode: "test:target", Type: "REL",
				Properties: map[string]interface{}{"state": "original"},
			}))
			require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(Constraint{
				Name: "policy", Type: ConstraintPolicy, EntityType: ConstraintEntityRelationship, Label: "REL",
				SourceLabel: "Account", TargetLabel: "Account", PolicyMode: "ALLOWED",
			}))
			require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
				return txn.Set(nodeKey(endpoint), []byte("corrupt-record"))
			}))
			_, decodeErr := engine.decodeNode("test", []byte("corrupt-record"))
			require.Error(t, decodeErr)

			tx, err := engine.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			require.NoError(t, tx.UpdateEdge(&Edge{
				ID: "test:original", StartNode: "test:source", EndNode: "test:target", Type: "REL",
				Properties: map[string]interface{}{"state": "changed"},
			}))
			require.ErrorContains(t, tx.Commit(), decodeErr.Error())
			edge, err := engine.GetEdge("test:original")
			require.NoError(t, err)
			require.Equal(t, "original", edge.Properties["state"], "failed commit must not publish the relationship update")
		})
	}
}

func TestBadgerTransactionDeferredPolicyLabelChanges(t *testing.T) {
	for _, anchor := range []NodeID{"test:source", "test:target"} {
		t.Run(string(anchor), func(t *testing.T) {
			for _, allowed := range []bool{true, false} {
				name := "violating"
				labels := []string{"Forbidden"}
				if allowed {
					name, labels = "allowed", []string{"Account", "Extra"}
				}
				t.Run(name, func(t *testing.T) {
					engine := newTestEngine(t)
					for _, id := range []NodeID{"test:source", "test:target"} {
						_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Account"}})
						require.NoError(t, err)
					}
					require.NoError(t, engine.CreateEdge(&Edge{
						ID: "test:original", StartNode: "test:source", EndNode: "test:target", Type: "REL",
					}))
					require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(Constraint{
						Name: "policy", Type: ConstraintPolicy, EntityType: ConstraintEntityRelationship, Label: "REL",
						SourceLabel: "Account", TargetLabel: "Account", PolicyMode: "ALLOWED",
					}))
					tx, err := engine.BeginTransaction()
					require.NoError(t, err)
					t.Cleanup(func() { _ = tx.Rollback() })
					require.NoError(t, tx.SetDeferredConstraintValidation(true))
					require.NoError(t, tx.UpdateNode(&Node{ID: anchor, Labels: labels}))
					err = tx.Commit()
					if allowed {
						require.NoError(t, err)
					} else {
						require.ErrorContains(t, err, "ALLOWED")
						labels = []string{"Account"}
					}
					node, err := engine.GetNode(anchor)
					require.NoError(t, err)
					require.Equal(t, labels, node.Labels)
				})
			}
		})
	}
}

func TestBadgerTransactionPolicyMissingNodeLabels(t *testing.T) {
	engine := newTestEngine(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("test"))
	labels, err := tx.getNodeLabels("test:missing")
	require.NoError(t, err, "missing endpoints retain their existing validation semantics")
	require.Nil(t, labels)
}

func TestBadgerTransactionDeferredPolicyLabelChangeUsesFinalEdges(t *testing.T) {
	for _, outgoing := range []bool{true, false} {
		direction := "incoming"
		if outgoing {
			direction = "outgoing"
		}
		t.Run(direction, func(t *testing.T) {
			for _, moveEndpoint := range []bool{false, true} {
				name := "type_change"
				if moveEndpoint {
					name = "endpoint_change"
				}
				t.Run(name, func(t *testing.T) {
					engine := newTestEngine(t)
					for _, id := range []NodeID{"test:source", "test:target", "test:spare"} {
						_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Account"}})
						require.NoError(t, err)
					}
					edge := &Edge{
						ID: "test:original", StartNode: "test:source", EndNode: "test:target", Type: "REL",
					}
					require.NoError(t, engine.CreateEdge(edge))
					require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(Constraint{
						Name: "policy", Type: ConstraintPolicy, EntityType: ConstraintEntityRelationship, Label: "REL",
						SourceLabel: "Account", TargetLabel: "Account", PolicyMode: "ALLOWED",
					}))
					anchor := NodeID("test:target")
					if outgoing {
						anchor = "test:source"
					}
					tx, err := engine.BeginTransaction()
					require.NoError(t, err)
					t.Cleanup(func() { _ = tx.Rollback() })
					require.NoError(t, tx.SetDeferredConstraintValidation(true))
					require.NoError(t, tx.UpdateNode(&Node{ID: anchor, Labels: []string{"Forbidden"}}))
					if moveEndpoint {
						if outgoing {
							edge.StartNode = "test:spare"
						} else {
							edge.EndNode = "test:spare"
						}
					} else {
						edge.Type = "UNCONSTRAINED"
					}
					require.NoError(t, tx.UpdateEdge(edge))
					require.NoError(t, tx.Commit(), "policy must use final edge type and endpoints, not the replaced committed edge")
					node, err := engine.GetNode(anchor)
					require.NoError(t, err)
					require.Equal(t, []string{"Forbidden"}, node.Labels)
					committedEdge, err := engine.GetEdge(edge.ID)
					require.NoError(t, err)
					require.Equal(t, edge.Type, committedEdge.Type)
					require.Equal(t, edge.StartNode, committedEdge.StartNode)
					require.Equal(t, edge.EndNode, committedEdge.EndNode)
				})
			}
		})
	}
}
