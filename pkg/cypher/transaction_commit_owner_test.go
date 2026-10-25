package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type recordingManagedCommitEngine struct {
	*storage.NamespacedEngine
	commits int
}

func (e *recordingManagedCommitEngine) CommitTransaction(ctx context.Context, tx *storage.BadgerTransaction) error {
	e.commits++
	return tx.CommitContext(ctx)
}

// Opening the raw transaction requires unwrapping storage. Its commit must
// still visit the managed owner for both statement and session transactions.
func TestTransactionCommitUsesManagedOwner(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		name := "implicit"
		if explicit {
			name = "explicit"
		}
		t.Run(name, func(t *testing.T) {
			base := runtimeFixtureOwner.Own(t, runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			t.Cleanup(func() { _ = base.Close() })
			engine := &recordingManagedCommitEngine{NamespacedEngine: storage.NewNamespacedEngine(base, "managed")}
			executor := NewStorageExecutor(engine)
			ctx := context.Background()
			if explicit {
				_, err := executor.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			_, err := executor.Execute(ctx, "CREATE (:Accounted {id: 1})", nil)
			require.NoError(t, err)
			if explicit {
				require.Zero(t, engine.commits, "staging a statement does not publish accounting")
				_, err = executor.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
			require.Equal(t, 1, engine.commits, "one managed publication per transaction")
			count, err := engine.NodeCount()
			require.NoError(t, err)
			require.EqualValues(t, 1, count)
		})
	}
}
