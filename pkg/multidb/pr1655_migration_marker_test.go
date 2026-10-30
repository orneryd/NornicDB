package multidb

import (
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type migrationMarkerRejector struct {
	storage.Engine
	reject error
	create bool
	hits   int
}

func (e *migrationMarkerRejector) CreateNode(node *storage.Node) (storage.NodeID, error) {
	if e.create && node.ID == storage.NodeID("system:"+migrationStatusKey) {
		e.hits++
		return "", e.reject
	}
	return e.Engine.CreateNode(node)
}
func (e *migrationMarkerRejector) UpdateNode(request storage.NodeUpdateRequest) error {
	if request.After.ID == storage.NodeID("system:"+migrationStatusKey) {
		// Native blind writes upsert. Explicitly exercise the existing engine
		// ErrNotFound contract too, so its subsequent Create failure cannot
		// be swallowed by either migration-completion caller.
		if e.create {
			return storage.ErrNotFound
		}
		e.hits++
		return e.reject
	}
	return e.Engine.UpdateNode(request)
}

func TestPR1655MigrationCompletionRejectsFailedMarker(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, create := range []bool{false, true} {
			name := "empty"
			if legacy {
				name = "legacy"
			}
			if create {
				name += "/create"
			} else {
				name += "/update"
			}
			t.Run(name, func(t *testing.T) {
				inner := newLegacyTestEngine(t)
				if legacy {
					_, err := inner.CreateNode(&storage.Node{ID: "old-node", Labels: []string{"Legacy"}, Properties: map[string]any{"value": "preserved"}})
					require.NoError(t, err)
				}
				if !create {
					_, err := inner.CreateNode(&storage.Node{ID: storage.NodeID("system:" + migrationStatusKey), Properties: map[string]any{"status": "pending"}})
					require.NoError(t, err)
				}
				rejected := errors.New("migration marker rejected")
				fault := &migrationMarkerRejector{Engine: inner, reject: rejected, create: create}
				manager := &DatabaseManager{inner: fault, config: DefaultConfig()}
				require.ErrorIs(t, manager.migrateLegacyData(), rejected)
				require.Equal(t, 1, fault.hits)
				require.False(t, manager.isMigrationComplete())
				if legacy {
					node, err := inner.GetNode("nornic:old-node")
					require.NoError(t, err)
					require.Equal(t, "preserved", node.Properties["value"])
				}
			})
		}
	}
}
