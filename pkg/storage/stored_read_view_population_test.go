package storage

import (
	"context"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

// StoredViewModeFixtures returns actual retained views for the external typed
// census gate. The bridge exists only in the test binary.
func StoredViewModeFixtures(t *testing.T) map[string]func() any {
	t.Helper()
	base := NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	storedFactNode(t, base, "d7:n", "Old")
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("d7"))
	t.Cleanup(func() { _ = tx.Rollback() })
	snapshot, err := base.OpenReadSnapshot(context.Background())
	require.NoError(t, err)
	t.Cleanup(snapshot.Close)
	scoped, err := NewNamespacedEngine(base, "d7").OpenReadSnapshot(context.Background())
	require.NoError(t, err)
	t.Cleanup(scoped.Close)
	prefix := "github.com/orneryd/nornicdb/pkg/storage."
	return map[string]func() any{
		prefix + "BadgerTransaction":      func() any { return tx },
		prefix + "Transaction":            func() any { return (*Transaction)(tx) }, // Public compatibility alias; same native cut/lifecycle.
		prefix + "badgerReadSnapshot":     func() any { return snapshot },
		prefix + "namespacedReadSnapshot": func() any { return scoped },
		prefix + "storedReadOperation":    func() any { return storedReadOperation{base} },
		prefix + "engineConstraintView":   func() any { return engineConstraintView{engine: base, ctx: context.Background()} },
		prefix + "txConstraintView":       func() any { return txConstraintView{tx} },
	}
}
func TestStoredReadRetainedViewModes(t *testing.T) {
	for name, issue := range StoredViewModeFixtures(t) {
		t.Run(name, func(t *testing.T) {
			owner := issue()
			typ := reflect.TypeOf(owner)
			if typ.Kind() == reflect.Pointer {
				typ = typ.Elem()
			}
			expectedType := name
			if name == "github.com/orneryd/nornicdb/pkg/storage.Transaction" {
				expectedType = "github.com/orneryd/nornicdb/pkg/storage.BadgerTransaction"
			}
			require.Equal(t, expectedType, typ.PkgPath()+"."+typ.Name())
			_, ok := owner.(StoredReadProvider)
			require.True(t, ok)
			id := NodeID("d7:n")
			if _, scoped := owner.(*namespacedReadSnapshot); scoped {
				id = "n"
			}
			for _, mode := range []StoredReadMode{StoredMetadataOnly, StoredSelectedProperties, StoredFullMaterialization} {
				result, err := ReadStored(context.Background(), owner, StoredReadRequest{Mode: mode, Nodes: []NodeID{id}, NodeProperties: []string{"key"}, Physical: true})
				if _, locked := owner.(txConstraintView); locked {
					require.ErrorIs(t, err, ErrNotImplemented)
					require.Nil(t, result)
					continue
				}
				require.NoError(t, err)
				facts := result.StoredFacts()
				require.Len(t, facts.Nodes, 1)
				require.True(t, facts.Nodes[0].Exists)
				require.Equal(t, []string{"Old"}, facts.Nodes[0].Labels)
				require.Equal(t, id, facts.Nodes[0].ID)
				switch mode {
				case StoredMetadataOnly:
					_, ok := result.(StoredMetadata)
					require.True(t, ok)
				case StoredSelectedProperties:
					value := result.(StoredProjection)
					require.Equal(t, map[string]any{"key": "chosen"}, value.Nodes[0].Properties)
				case StoredFullMaterialization:
					require.Contains(t, result.(StoredFullRead).Nodes[0].Properties, "excluded")
				}
			}
		})
	}
}
