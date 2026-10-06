package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerConstraintScansRecordErrors(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	scans := []struct {
		name   string
		isNode bool
		scan   func(*BadgerEngine, *badger.Txn) error
	}{
		{"unique", true, func(b *BadgerEngine, txn *badger.Txn) error {
			return b.scanForUniqueViolationInTxn(txn, "test", "Account", "key", "new", "")
		}},
		{"node_key", true, func(b *BadgerEngine, txn *badger.Txn) error {
			return b.scanForNodeKeyViolationInTxn(txn, "test", "Account", []string{"key"}, []interface{}{"new"}, "")
		}},
		{"legacy_temporal", true, func(b *BadgerEngine, txn *badger.Txn) error {
			return b.legacyScanForTemporalOverlapInTxn(txn, "test", "Account", "key", "from", "to", "new", start, start.Add(time.Hour), true, "")
		}},
		{"edge_unique", false, func(b *BadgerEngine, txn *badger.Txn) error {
			return b.checkEdgeUniquenessInTxn(txn, &Edge{Type: "REL", Properties: map[string]interface{}{"key": "new"}},
				Constraint{Type: ConstraintUnique, Properties: []string{"key"}}, "test", "")
		}},
		{"edge_temporal", false, func(b *BadgerEngine, txn *badger.Txn) error {
			return b.checkEdgeTemporalInTxn(txn, &Edge{Type: "REL", Properties: map[string]interface{}{"key": "new", "from": start, "to": start.Add(time.Hour)}},
				Constraint{Type: ConstraintTemporal, Properties: []string{"key", "from", "to"}}, "test", "")
		}},
		{"edge_cardinality_outgoing", false, func(b *BadgerEngine, txn *badger.Txn) error {
			return b.checkEdgeCardinalityInTxn(txn, &Edge{StartNode: "test:source", Type: "REL"},
				Constraint{Type: ConstraintCardinality, Label: "REL", Direction: "OUTGOING", MaxCount: 2}, "test", "")
		}},
		{"edge_cardinality_incoming", false, func(b *BadgerEngine, txn *badger.Txn) error {
			return b.checkEdgeCardinalityInTxn(txn, &Edge{EndNode: "test:target", Type: "REL"},
				Constraint{Type: ConstraintCardinality, Label: "REL", Direction: "INCOMING", MaxCount: 2}, "test", "")
		}},
		{"adjacent_edge_policy_outgoing", false, func(b *BadgerEngine, txn *badger.Txn) error {
			return b.validatePolicyForEdgesWithPrefixInTxn(txn, b.outgoingIndexPrefixString("test:source"),
				&Node{ID: "test:source", Labels: []string{"Account"}}, true, b.GetSchemaForNamespace("test"), "test")
		}},
		{"adjacent_edge_policy_incoming", false, func(b *BadgerEngine, txn *badger.Txn) error {
			return b.validatePolicyForEdgesWithPrefixInTxn(txn, b.incomingIndexPrefixString("test:target"),
				&Node{ID: "test:target", Labels: []string{"Account"}}, false, b.GetSchemaForNamespace("test"), "test")
		}},
	}
	records := []struct {
		name    string
		value   []byte
		missing bool
	}{
		{name: "corrupt", value: []byte("corrupt-record")},
		{name: "empty", value: []byte{}},
		{name: "missing", missing: true},
	}
	for _, scan := range scans {
		t.Run(scan.name, func(t *testing.T) {
			for _, record := range records {
				t.Run(record.name, func(t *testing.T) {
					engine := newTestEngine(t)
					for _, id := range []NodeID{"test:source", "test:target"} {
						_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Account"}, Properties: map[string]interface{}{"key": "old"}})
						require.NoError(t, err)
					}
					require.NoError(t, engine.CreateEdge(&Edge{ID: "test:edge", StartNode: "test:source", EndNode: "test:target", Type: "REL"}))
					key := edgeKey("test:edge")
					var decodeErr error
					if scan.isNode {
						key = nodeKey("test:source")
						_, decodeErr = engine.decodeNode("test", record.value)
					} else {
						_, decodeErr = engine.decodeEdgeBodyByID(record.value, "test:edge")
					}
					if !record.missing {
						require.Error(t, decodeErr)
					}
					require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
						if record.missing {
							return txn.Delete(key)
						}
						return txn.Set(key, record.value)
					}))
					err := engine.withView(func(txn *badger.Txn) error {
						return scan.scan(engine, txn)
					})
					if record.missing {
						require.NoError(t, err, "stale index entries may refer to deleted records")
					} else {
						require.ErrorContains(t, err, decodeErr.Error(), "scan must propagate the decode error")
					}
				})
			}
		})
	}
}

func TestBadgerConstraintPolicyOtherNodeRecordErrors(t *testing.T) {
	for _, outgoing := range []bool{true, false} {
		direction := "incoming"
		if outgoing {
			direction = "outgoing"
		}
		t.Run(direction, func(t *testing.T) {
			for _, missing := range []bool{false, true} {
				name := "corrupt"
				if missing {
					name = "missing"
				}
				t.Run(name, func(t *testing.T) {
					engine := newTestEngine(t)
					for _, id := range []NodeID{"test:source", "test:target"} {
						_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Account"}})
						require.NoError(t, err)
					}
					require.NoError(t, engine.CreateEdge(&Edge{ID: "test:edge", StartNode: "test:source", EndNode: "test:target", Type: "REL"}))
					anchor, other := NodeID("test:target"), NodeID("test:source")
					prefix := engine.incomingIndexPrefixString(anchor)
					if outgoing {
						anchor, other = "test:source", "test:target"
						prefix = engine.outgoingIndexPrefixString(anchor)
					}
					require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
						if missing {
							return txn.Delete(nodeKey(other))
						}
						return txn.Set(nodeKey(other), []byte("corrupt-record"))
					}))
					err := engine.withView(func(txn *badger.Txn) error {
						return engine.validatePolicyForEdgesWithPrefixInTxn(txn, prefix, &Node{ID: anchor},
							outgoing, engine.GetSchemaForNamespace("test"), "test")
					})
					if missing {
						require.NoError(t, err)
					} else {
						_, decodeErr := engine.decodeNode("test", []byte("corrupt-record"))
						require.ErrorContains(t, err, decodeErr.Error())
					}
				})
			}
		})
	}
}

func TestBadgerConstraintEdgePolicyEndpointRecordErrors(t *testing.T) {
	for _, mode := range []string{"ALLOWED", "DISALLOWED"} {
		t.Run(mode, func(t *testing.T) {
			for _, endpoint := range []NodeID{"test:source", "test:target"} {
				t.Run(string(endpoint), func(t *testing.T) {
					for _, record := range []struct {
						name    string
						value   []byte
						missing bool
					}{
						{name: "corrupt", value: []byte("corrupt-record")},
						{name: "empty", value: []byte{}},
						{name: "missing", missing: true},
					} {
						t.Run(record.name, func(t *testing.T) {
							engine := newTestEngine(t)
							for _, id := range []NodeID{"test:source", "test:target"} {
								_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Account"}})
								require.NoError(t, err)
							}
							schema := engine.GetSchemaForNamespace("test")
							require.NoError(t, schema.AddConstraint(Constraint{
								Name: "rel_policy", Type: ConstraintPolicy, EntityType: ConstraintEntityRelationship,
								Label: "REL", SourceLabel: "Account", TargetLabel: "Account", PolicyMode: mode,
							}))
							require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
								if record.missing {
									return txn.Delete(nodeKey(endpoint))
								}
								return txn.Set(nodeKey(endpoint), record.value)
							}))
							err := engine.withView(func(txn *badger.Txn) error {
								return engine.checkEdgePolicyInTxn(txn,
									&Edge{Type: "REL", StartNode: "test:source", EndNode: "test:target"}, schema, "test")
							})
							if record.missing {
								require.NoError(t, err, "missing endpoints are handled by other validation")
							} else {
								_, decodeErr := engine.decodeNode("test", record.value)
								require.Error(t, decodeErr)
								require.ErrorContains(t, err, decodeErr.Error(), "endpoint corruption must not bypass policy validation")
							}
						})
					}
				})
			}
		})
	}
}

func TestBadgerCreateNodeNodeKeyCorruptionDoesNotCommit(t *testing.T) {
	engine := newTestEngine(t)
	require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(Constraint{
		Name: "account_key", Type: ConstraintNodeKey, Label: "Account", Properties: []string{"tenant", "key"},
	}))
	properties := map[string]interface{}{"tenant": "tenant", "key": "duplicate"}
	_, err := engine.CreateNode(&Node{ID: "test:original", Labels: []string{"Account"}, Properties: properties})
	require.NoError(t, err)
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(nodeKey("test:original"), []byte("corrupt-record"))
	}))

	_, err = engine.CreateNode(&Node{ID: "test:duplicate", Labels: []string{"Account"}, Properties: properties})
	require.Error(t, err, "unreadable existing NODE KEY records must prevent creation")
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(nodeKey("test:duplicate"))
		require.ErrorIs(t, err, badger.ErrKeyNotFound, "failed validation must not commit the duplicate")
		return nil
	}))
}
