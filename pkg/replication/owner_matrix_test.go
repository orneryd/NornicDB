package replication

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// blockingApplyStorage holds ApplyCommand until released.
type blockingApplyStorage struct {
	*MockStorage
	entered, release chan struct{}
}

func (s *blockingApplyStorage) ApplyCommand(cmd *Command) error {
	close(s.entered)
	<-s.release
	return s.MockStorage.ApplyCommand(cmd)
}

// Shutdown matrix S1, StandaloneReplicator: Shutdown waits for an accepted
// Apply that is still inside storage, and refuses Apply once it began.
func TestOwnerMatrixStandaloneShutdownJoinsHeldApply(t *testing.T) {
	storage := &blockingApplyStorage{MockStorage: NewMockStorage(), entered: make(chan struct{}), release: make(chan struct{})}
	r := NewStandaloneReplicator(DefaultConfig(), storage)
	require.NoError(t, r.Start(context.Background()))

	applied := make(chan error, 1)
	go func() { applied <- r.Apply(&Command{}, time.Second) }()
	select {
	case <-storage.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("Apply did not reach storage")
	}
	stopped := make(chan error, 1)
	go func() { stopped <- r.Shutdown() }()
	select {
	case err := <-stopped:
		close(storage.release)
		t.Fatalf("Shutdown returned while an accepted Apply was inside storage: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.Eventually(t, r.work.Closing, time.Second, time.Millisecond)
	require.True(t, errors.Is(r.Apply(&Command{}, time.Second), ErrClosed), "Apply after Shutdown began must be refused")
	close(storage.release)
	require.NoError(t, <-applied)
	require.NoError(t, <-stopped)
}

// Shutdown matrix S2, StandaloneReplicator: concurrent and repeated Shutdown
// callers join the one stop and receive its outcome.
func TestOwnerMatrixStandaloneConcurrentShutdown(t *testing.T) {
	r := NewStandaloneReplicator(DefaultConfig(), NewMockStorage())
	require.NoError(t, r.Start(context.Background()))
	const callers = 4
	results := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() { results <- r.Shutdown() }()
	}
	var first error
	for i := 0; i < callers; i++ {
		select {
		case err := <-results:
			if i == 0 {
				first = err
			} else {
				require.Equal(t, fmt.Sprint(first), fmt.Sprint(err))
			}
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent Shutdown did not complete")
		}
	}
	require.Equal(t, fmt.Sprint(first), fmt.Sprint(r.Shutdown()))
	select {
	case <-r.work.Stopped():
	default:
		t.Fatal("Shutdown returned before the replicator's stop completed")
	}
}
