package multidb

import (
	"errors"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type accountingPresenceProbe struct {
	storage.Engine
	storage.StoredEntityReader
	presence, full int
}

func (p *accountingPresenceProbe) GetInnerEngine() storage.Engine { return p.Engine }
func (p *accountingPresenceProbe) GetStoredNode(storage.NodeID) (*storage.Node, error) {
	p.full++
	return nil, errors.New("count accounting loaded a full node")
}
func (p *accountingPresenceProbe) GetStoredEdge(storage.EdgeID) (*storage.Edge, error) {
	p.full++
	return nil, errors.New("count accounting loaded a full relationship")
}
func (p *accountingPresenceProbe) StoredNodeExists(id storage.NodeID) (bool, error) {
	p.presence++
	_, err := storage.GetStoredNode(p.Engine, id)
	return err == nil, presenceProbeError(err)
}
func (p *accountingPresenceProbe) StoredEdgeExists(id storage.EdgeID) (bool, error) {
	p.presence++
	_, err := storage.GetStoredEdge(p.Engine, id)
	return err == nil, presenceProbeError(err)
}
func presenceProbeError(err error) error {
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	return err
}

func TestCountAdmissionUsesStoredPresence(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, hidden := range []bool{false, true} {
			for _, entity := range []string{"node", "edge"} {
				t.Run(fmt.Sprintf("buffered=%v/hidden=%v/%s", buffered, hidden, entity), func(t *testing.T) {
					base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{InMemory: true, WriteBehind: buffered})
					require.NoError(t, err)
					manager, err := NewDatabaseManager(base, nil)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, manager.Close()) })
					const db = "presence"
					require.NoError(t, manager.CreateDatabase(db))
					engine, err := manager.GetStorage(db)
					require.NoError(t, err)
					for _, id := range []storage.NodeID{"a", "b", "n"} {
						_, err := engine.CreateNode(&storage.Node{ID: id})
						require.NoError(t, err)
					}
					require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK"}))
					if hidden {
						require.NoError(t, base.SetNodeManuallySuppressed(db+":n", true))
						require.NoError(t, base.SetEdgeManuallySuppressed(db+":e", true))
					}
					require.NoError(t, manager.SetDatabaseLimits(db, &Limits{Storage: StorageLimits{MaxNodes: 3, MaxEdges: 1}}))
					tracker := engine.(*sizeTrackingEngine)
					reader, ok := storage.StoredReader(tracker.Engine)
					require.True(t, ok)
					probe := &accountingPresenceProbe{Engine: tracker.Engine, StoredEntityReader: reader}
					tracker.Engine = probe
					if entity == "node" {
						err = engine.UpdateNode(&storage.Node{ID: "n", Properties: map[string]any{"payload": "new"}})
					} else {
						err = engine.UpdateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"payload": "new"}})
					}
					require.NoError(t, err)
					require.Equal(t, 1, probe.presence)
					require.Zero(t, probe.full)
				})
			}
		}
	}
}
