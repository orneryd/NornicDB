package storage_test

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDecodedNameTemporalConstraintRegistry(t *testing.T) {
	engine, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	schema := engine.GetSchemaForNamespace("nornic")
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	node := func(id string, from, to int) *storage.Node {
		return &storage.Node{ID: storage.NodeID("nornic:" + id), Labels: []string{"A\x00B"}, Properties: map[string]interface{}{
			"C": "same", "valid_from": start.Add(time.Duration(from) * time.Hour), "valid_to": start.Add(time.Duration(to) * time.Hour),
		}}
	}
	_, err = engine.CreateNode(node("existing", 0, 10))
	require.NoError(t, err)
	left := storage.Constraint{Name: "left", Type: storage.ConstraintTemporal, EntityType: storage.ConstraintEntityNode, Label: "A\x00B", Properties: []string{"C", "valid_from", "valid_to"}}
	right := storage.Constraint{Name: "right", Type: storage.ConstraintTemporal, EntityType: storage.ConstraintEntityNode, Label: "A", Properties: []string{"B\x00C", "valid_from", "valid_to"}}
	// The public schema API may register a definition before its history index
	// is built. Completing another label's index must not certify this one.
	require.NoError(t, schema.AddConstraint(left))
	require.NoError(t, schema.AddConstraint(right))
	require.NoError(t, storage.MarkNewTemporalConstraintsForEngine(engine, schema, []storage.Constraint{right}))
	_, err = engine.CreateNode(node("overlap", 5, 15))
	require.Error(t, err, "the distinct left definition must still validate against the existing node")
	_, err = engine.CreateNode(node("adjacent", 10, 20))
	require.NoError(t, err, "an adjacent interval remains admissible")
}
