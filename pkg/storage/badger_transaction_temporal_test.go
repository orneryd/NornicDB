package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTransactionTemporalAcceptanceReadView(t *testing.T) {
	for _, authoritative := range []bool{false, true} {
		for _, mode := range []string{"snapshot", "pending", "delete"} {
			t.Run(mode+map[bool]string{false: "/compatibility", true: "/history"}[authoritative], func(t *testing.T) {
				engine := createTestBadgerEngine(t)
				constraint := Constraint{Name: "view_temporal", Type: ConstraintTemporal, Label: "ViewFact", Properties: []string{"key", "vf", "vt"}}
				require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(constraint))
				start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
				boundary := start.AddDate(3, 0, 0)
				asOf := start.AddDate(6, 0, 0)
				old := &Node{ID: "test:old", Labels: []string{"ViewFact"}, Properties: map[string]interface{}{"key": "one", "vf": start, "vt": boundary}}
				current := &Node{ID: "test:current", Labels: []string{"ViewFact"}, Properties: map[string]interface{}{"key": "one", "vf": boundary}}
				seed, err := engine.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, seed.SetNamespace("test"))
				_, err = seed.CreateNode(old)
				require.NoError(t, err)
				_, err = seed.CreateNode(current)
				require.NoError(t, err)
				require.NoError(t, seed.Commit())
				if authoritative {
					require.NoError(t, engine.RebuildTemporalIndexes(context.Background()))
				}
				reader, err := engine.BeginTransaction()
				require.NoError(t, err)
				t.Cleanup(func() { _ = reader.Rollback() })
				require.NoError(t, reader.SetNamespace("test"))
				visible, err := reader.IsCurrentTemporalNodeInNamespace("test", current, asOf)
				require.NoError(t, err)
				require.True(t, visible)
				visible, err = reader.IsCurrentTemporalNodeInNamespace("test", old, asOf)
				require.NoError(t, err)
				require.False(t, visible)
				if mode == "delete" {
					require.NoError(t, reader.DeleteNode(current.ID))
					visible, err = reader.IsCurrentTemporalNodeInNamespace("test", current, asOf)
					require.NoError(t, err)
					require.False(t, visible)
					return
				}
				writer := reader
				if mode == "snapshot" {
					writer, err = engine.BeginTransaction()
					require.NoError(t, err)
					require.NoError(t, writer.SetNamespace("test"))
				}
				closed := CopyNode(current)
				closed.Properties["vt"] = start.AddDate(5, 0, 0)
				require.NoError(t, writer.UpdateNode(closed))
				next := &Node{ID: "test:next", Labels: []string{"ViewFact"}, Properties: map[string]interface{}{"key": "one", "vf": start.AddDate(5, 0, 0)}}
				_, err = writer.CreateNode(next)
				require.NoError(t, err)
				if mode == "snapshot" {
					require.NoError(t, writer.Commit())
				}
				visible, err = reader.IsCurrentTemporalNodeInNamespace("test", current, asOf)
				require.NoError(t, err)
				require.Equal(t, mode == "snapshot", visible)
				if mode == "pending" {
					visible, err = reader.IsCurrentTemporalNodeInNamespace("test", next, asOf)
					require.NoError(t, err)
					require.True(t, visible)
				}
			})
		}
	}
}
