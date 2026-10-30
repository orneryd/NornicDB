package txsession

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Sessions opened at the same time on one database each get their own ID and
// stay their own: committing one doesn't end another (#915).
func TestManagerIssuesDistinctIDsToConcurrentSessions(t *testing.T) {
	mgr := NewManager(time.Minute, newExecutorFactory(t))
	const workers, perWorker = 16, 32
	sessions := make(chan *Session, workers*perWorker)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				session, err := mgr.OpenForOwner(context.Background(), "neo4j", "owner")
				if err != nil {
					t.Error(err)
					return
				}
				sessions <- session
			}
		}()
	}
	wg.Wait()
	close(sessions)

	seen := map[string]bool{}
	for session := range sessions {
		require.False(t, seen[session.ID], "duplicate transaction ID %s", session.ID)
		seen[session.ID] = true
		got, ok := mgr.GetForOwner(session.ID, "owner")
		require.True(t, ok)
		require.Same(t, session, got)
	}
	require.Len(t, seen, workers*perWorker)
	for id := range seen {
		session, _ := mgr.GetForOwner(id, "owner")
		_, err := mgr.CommitAndDelete(context.Background(), session)
		require.NoError(t, err)
	}
}

// Transaction IDs don't depend on the clock: sessions opened while the
// manager's clock stands still still get different IDs.
func TestManagerIDsDontDependOnTheClock(t *testing.T) {
	mgr := NewManager(time.Minute, newExecutorFactory(t))
	frozen := time.Unix(1700000000, 0)
	mgr.nowFunc = func() time.Time { return frozen }
	first, err := mgr.Open(context.Background(), "neo4j")
	require.NoError(t, err)
	second, err := mgr.Open(context.Background(), "neo4j")
	require.NoError(t, err)
	require.NotEqual(t, first.ID, second.ID)
}
