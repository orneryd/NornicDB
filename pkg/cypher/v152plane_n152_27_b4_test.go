package cypher

import (
	"context"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type compositeCleanupWALEngine struct {
	*ownershipBeginCapture
	wal *storage.WAL
}

func (e *compositeCleanupWALEngine) GetWAL() *storage.WAL { return e.wal }

// The transaction lifetime is bound after BEGIN and before registration. Its
// first Done observation gives this fixture a synchronous fault-injection seam
// without racing a journal close or changing production code.
type compositeCleanupLifetime struct {
	context.Context
	once   sync.Once
	inject func()
}

func (c *compositeCleanupLifetime) Done() <-chan struct{} {
	c.once.Do(c.inject)
	return c.Context.Done()
}

func TestCompositeRegistrationRetainsWALCleanupFailure(t *testing.T) {
	for _, failCleanup := range []bool{false, true} {
		name := "healthy_cleanup"
		if failCleanup {
			name = "wal_cleanup_failure"
		}
		t.Run(name, func(t *testing.T) {
			t.Cleanup(config.WithWALEnabled())
			base := newTestMemoryEngine(t)
			wal, err := storage.NewWAL(t.TempDir(), nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, wal.Close()) })
			engine := &compositeCleanupWALEngine{ownershipBeginCapture: &ownershipBeginCapture{Engine: base, begin: base.BeginTransaction}, wal: wal}
			coordinator := fabric.NewFabricTransaction("nor312-cleanup")
			// This nonexistent shard deterministically refuses registration after
			// the real participant BEGIN has opened, matching the existing owner test.
			sub := &fabric.SubTransaction{ShardName: "local:missing"}
			dispatcher := &cypherFabricExecutor{base: NewStorageExecutor(engine), autoCommit: true, transaction: coordinator}
			t.Cleanup(func() { require.NoError(t, coordinator.Rollback(context.Background(), nil)) })
			injected := false
			lifetime := &compositeCleanupLifetime{Context: context.Background(), inject: func() {
				require.NotNil(t, engine.started, "injection follows real BEGIN")
				require.True(t, engine.started.IsActive())
				require.Greater(t, wal.Sequence(), uint64(0), "BEGIN was journaled before cleanup fault")
				injected = true
				if failCleanup {
					require.NoError(t, wal.Close())
				}
			}}
			ctx := fabric.WithTransactionLifetime(context.Background(), lifetime)
			participant, err := dispatcher.ensureLocalShardTxExecutor(ctx, sub, "one", engine)
			require.True(t, injected)
			require.Nil(t, participant)
			require.ErrorContains(t, err, "not found", "original registration failure retained")
			if failCleanup {
				require.ErrorIs(t, err, storage.ErrWALClosed, "distinct rollback bookkeeping failure retained")
			} else {
				require.NotErrorIs(t, err, storage.ErrWALClosed)
			}
			require.False(t, engine.started.IsActive(), "native snapshot retires even if journal abort fails")
			require.Empty(t, dispatcher.localShardTxExecutors())
		})
	}
}
