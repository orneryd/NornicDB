package multidb

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type leaderAwareEngine struct {
	*opaqueEntityEngine
	leader      bool
	updateCalls atomic.Int32
}

func (e *leaderAwareEngine) IsLeader() bool { return e.leader }

func (e *leaderAwareEngine) UpdateNode(node *storage.Node) error {
	e.updateCalls.Add(1)
	return e.Engine.UpdateNode(node)
}

func TestNewDatabaseManager_ReadOnlyEngine_SkipsMetadataWrites(t *testing.T) {
	base := &leaderAwareEngine{opaqueEntityEngine: &opaqueEntityEngine{Engine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))}, leader: false}

	m, err := NewDatabaseManager(t.Context(), base, &Config{DefaultDatabase: "nornic", SystemDatabase: "system"})
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

// This fixture exposes the required catalog receipt contract without exposing
// opaque graph observation capabilities. Its existing leadership gate remains.
func (e *leaderAwareEngine) BeginTransaction() (*storage.BadgerTransaction, error) {
	return e.Engine.(interface {
		BeginTransaction() (*storage.BadgerTransaction, error)
	}).BeginTransaction()
}
func (e *leaderAwareEngine) CheckEntityCommitAdmission() error {
	if !e.leader {
		return fmt.Errorf("standby does not admit catalog mutations: %w", storage.ErrInvalidData)
	}
	return nil
}
func (e *leaderAwareEngine) SubmitEntityCommit(ctx context.Context, prepared *storage.PreparedEntityCommit, settle func(storage.CommitOutcome)) (*storage.CommitReceipt, error) {
	if err := e.CheckEntityCommitAdmission(); err != nil {
		return nil, err
	}
	for _, op := range prepared.Operations() {
		if op.Type == storage.OpUpdateNode {
			e.updateCalls.Add(1)
		}
	}
	route, found := storage.EntityCommitSubmitterFor(e.Engine)
	if !found {
		return nil, storage.ErrInvalidData
	}
	return route.Submitter.SubmitEntityCommit(ctx, prepared, settle)
}
