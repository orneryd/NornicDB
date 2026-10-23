package multidb

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type leaderAwareEngine struct {
	storage.Engine
	leader      bool
	createCalls atomic.Int32
	updateCalls atomic.Int32
}

func (e *leaderAwareEngine) IsLeader() bool { return e.leader }

func (e *leaderAwareEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
	e.createCalls.Add(1)
	return e.Engine.CreateNode(node)
}

func (e *leaderAwareEngine) UpdateNode(node *storage.Node) error {
	e.updateCalls.Add(1)
	return e.Engine.UpdateNode(node)
}

func TestNewDatabaseManager_ReadOnlyEngine_SkipsMetadataWrites(t *testing.T) {
	base := &leaderAwareEngine{Engine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), leader: false}

	m, err := NewDatabaseManager(base, &Config{DefaultDatabase: "nornic", SystemDatabase: "system"})
	require.NoError(t, err)

	// Should still be able to resolve storage for system/default so server can start.
	_, err = m.GetStorage("system")
	require.NoError(t, err)
	_, err = m.GetStorage("nornic")
	require.NoError(t, err)

	// No metadata persistence/migrations should have happened.
	require.Equal(t, int32(0), base.createCalls.Load())
	require.Equal(t, int32(0), base.updateCalls.Load())
}
