package txsession

// Author: Codex. Public idle-expiry class probe, compatible with release API.
import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestOwnerClassSessionReleaseIdleExpiry(t *testing.T) {
	// The idle transaction ends at its configured transaction timeout (its
	// end owner, NOR-284); the session expiry then retires the session.
	engine, err := fixtureowner.TrackResult[*storage.MemoryEngine](t, storage.ErrStorageClosed)(storage.NewMemoryEngineWithOptions(storage.BadgerOptions{
		EngineOptions: storage.EngineOptions{LockWait: storage.LockWaitOptions{Timeout: 20 * time.Millisecond}},
	}))
	require.NoError(t, err)
	fixtureowner.Own(t, engine)
	defer engine.Close()
	var executor *cypher.StorageExecutor
	m := NewManager(20*time.Millisecond, func(string) (*cypher.StorageExecutor, error) {
		executor = cypher.NewStorageExecutor(engine)
		return executor, nil
	}, t.Context())
	defer func() {
		if closer, ok := any(m).(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		if join, ok := any(m).(interface{ WaitClosed(context.Context) error }); ok {
			_ = join.WaitClosed(context.Background())
		}
	}()
	session, err := m.Open(context.Background(), "nornic")
	require.NoError(t, err)
	deadline := time.Now().Add(time.Second)
	retired := false
	for time.Now().Before(deadline) {
		if _, ok := m.Get(session.ID); !ok {
			retired = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !retired {
		if closer, ok := any(m).(interface{ Close() error }); ok {
			require.NoError(t, closer.Close())
		} else {
			_, cleanupErr := executor.Execute(cypher.WithTransactionControl(context.Background()), "ROLLBACK", nil)
			require.NoError(t, cleanupErr)
		}
		t.Fatal("idle transaction retained its snapshot without another request")
	}
	if closer, ok := any(m).(interface{ Close() error }); ok {
		require.NoError(t, closer.Close())
		if join, ok := any(m).(interface{ WaitClosed(context.Context) error }); ok {
			require.NoError(t, join.WaitClosed(context.Background()))
		}
		require.False(t, executor.HasActiveTransaction(), "completion join retained the expired snapshot")
	}
}
