package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/dgraph-io/badger/v4"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

func TestNumericWireSchemaDomainValuesSurviveRestart(t *testing.T) {
	want := []interface{}{
		int64(9007199254740993), int64(-9007199254740993),
		int64(math.MaxInt64), int64(math.MinInt64), uint64(math.MaxUint64),
		float64(1 << 60), float64(2), math.SmallestNonzeroFloat64, math.Copysign(0, -1),
		"9007199254740993", true,
	}
	for _, format := range []struct {
		name    string
		version int
	}{
		{"legacy_unversioned", 0}, {"legacy_v1", 1}, {"exact_v2", 2},
	} {
		t.Run(format.name, func(t *testing.T) {
			expected := append([]interface{}(nil), want...)
			rejected := []interface{}{int64(1152921504606847000), int64(3), "2"}
			if format.version < 2 {
				// Preserve the former reader, including the rounding of legacy
				// INTEGERs. The original whole FLOAT and -0 remain FLOATs.
				expected[0], expected[1] = float64(9007199254740992), float64(-9007199254740992)
				expected[2], expected[3], expected[4] = float64(math.MaxInt64), float64(math.MinInt64), float64(math.MaxUint64)
				rejected = append(rejected, want[0], want[1], want[2], want[4])
			} else {
				rejected = append(rejected, int64(9007199254740992), int64(-9007199254740992), float64(math.MaxInt64), float64(math.MaxUint64), uint64(math.MaxUint64-1))
			}
			dir := t.TempDir()
			open := func() *BadgerEngine {
				engine, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
				require.NoError(t, err)
				return engine
			}
			engine := open()
			for _, id := range []NodeID{"test:a", "test:b"} {
				_, err := engine.CreateNode(&Node{ID: id})
				require.NoError(t, err)
			}
			constraints := []Constraint{
				{Name: "node_domain", Type: ConstraintDomain, EntityType: ConstraintEntityNode, Label: "Value", Properties: []string{"v"}, AllowedValues: want},
				{Name: "edge_domain", Type: ConstraintDomain, EntityType: ConstraintEntityRelationship, Label: "Value", Properties: []string{"v"}, AllowedValues: want},
			}
			if format.version < 2 {
				// Reproduce the former writer, rather than writing legacy bytes
				// with the new FLOAT-preserving encoder.
				raw, err := json.Marshal(&SchemaDefinition{Version: format.version, Constraints: constraints})
				require.NoError(t, err)
				require.Contains(t, string(raw), `18446744073709551615,1152921504606847000,2,5e-324,-0,`)
				if format.version == 0 {
					raw = bytes.Replace(raw, []byte(`"version":0,`), nil, 1)
				}
				require.NoError(t, engine.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
					return txn.Set(schemaKey("test"), raw)
				}))
			} else {
				for _, constraint := range constraints {
					require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(constraint))
				}
				// The persistence owner upgrades a supplied old version too;
				// exact number bytes must never be written under version 1.
				require.NoError(t, engine.persistSchemaDefinition("test", &SchemaDefinition{Version: 1, Constraints: constraints}))
			}
			storedVersion := func() int {
				var envelope struct {
					Version int `json:"version"`
				}
				require.NoError(t, engine.withView(func(txn *badger.Txn) error {
					item, err := txn.Get(schemaKey("test"))
					if err != nil {
						return err
					}
					return item.Value(func(raw []byte) error { return json.Unmarshal(raw, &envelope) })
				}))
				return envelope.Version
			}
			require.Equal(t, format.version, storedVersion())
			require.NoError(t, engine.Close())

			for _, stage := range []string{"restart", "repersisted"} {
				engine = open()
				domains := 0
				for _, constraint := range engine.GetSchemaForNamespace("test").GetAllConstraints() {
					if constraint.Label == "Value" {
						domains++
						require.Equal(t, expected, constraint.AllowedValues)
						require.True(t, math.Signbit(constraint.AllowedValues[8].(float64)))
					}
				}
				require.Equal(t, 2, domains)
				accepted := append(append([]interface{}(nil), expected...), int64(1<<60), int64(2), int64(0), float64(0))
				for _, entity := range []string{"node", "relationship"} {
					for _, explicit := range []bool{false, true} {
						for i, value := range accepted {
							id := fmt.Sprintf("test:%s-%s-%t-accept-%d", stage, entity, explicit, i)
							require.NoError(t, writeNumericWireSchemaValue(engine, entity, explicit, id, value), "%s: %T(%v)", id, value, value)
						}
						for i, value := range rejected {
							id := fmt.Sprintf("test:%s-%s-%t-reject-%d", stage, entity, explicit, i)
							err := writeNumericWireSchemaValue(engine, entity, explicit, id, value)
							var violation *ConstraintViolationError
							require.ErrorAs(t, err, &violation, "%s: %T(%v)", id, value, value)
							require.Equal(t, ConstraintDomain, violation.Type)
						}
					}
				}
				if stage == "restart" {
					// A normal schema mutation rewrites legacy state as version 2
					// using the decoded FLOAT values, never the old digit tokens.
					require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(Constraint{
						Name: "other_domain", Type: ConstraintDomain, Label: "Other", Properties: []string{"v"}, AllowedValues: []interface{}{"ok"},
					}))
					require.Equal(t, 2, storedVersion())
				}
				require.NoError(t, engine.Close())
			}
		})
	}
}

func writeNumericWireSchemaValue(engine *BadgerEngine, entity string, explicit bool, id string, value interface{}) error {
	var writer interface {
		CreateNode(*Node) (NodeID, error)
		CreateEdge(*Edge) error
	} = engine
	var tx *BadgerTransaction
	if explicit {
		var err error
		tx, err = engine.BeginTransaction()
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := tx.SetNamespace("test"); err != nil {
			return err
		}
		writer = tx
	}
	properties := map[string]interface{}{"v": value}
	var err error
	if entity == "node" {
		_, err = writer.CreateNode(&Node{ID: NodeID(id), Labels: []string{"Value"}, Properties: properties})
	} else {
		err = writer.CreateEdge(&Edge{ID: EdgeID(id), Type: "Value", StartNode: "test:a", EndNode: "test:b", Properties: properties})
	}
	if err == nil && tx != nil {
		err = tx.Commit()
	}
	return err
}
