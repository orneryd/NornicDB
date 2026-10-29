package fabric

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFabricFailedCleanupRemainsUnconfirmed(t *testing.T) {
	tx := NewFabricTransaction("cleanup-failure")
	_, err := tx.GetOrOpen("writer", true)
	require.NoError(t, err)
	commitErr := errors.New("write rejected")
	cleanupErr := errors.New("rollback failed")
	require.NoError(t, tx.BindParticipantCallbacks("writer", ParticipantCallbacks{Commit: func(*SubTransaction) error { return commitErr }, Rollback: func(*SubTransaction) error { return cleanupErr }}))
	err = tx.Commit(nil, nil)
	require.ErrorIs(t, err, commitErr)
	require.ErrorIs(t, err, cleanupErr)
	require.Equal(t, "cleanup_failed", tx.SubTransactions()["writer"].State, "a failed rollback must not be reported as confirmed")
}

func TestFabricReaderCommitFailurePrecedesWriter(t *testing.T) {
	// Every run must end readers before the sole irreversible writer. Repetition
	// observes the old map-order defect; the required order itself is fixed.
	for i := 0; i < 32; i++ {
		tx := NewFabricTransaction("reader-failure")
		_, err := tx.GetOrOpen("writer", true)
		require.NoError(t, err)
		_, err = tx.GetOrOpen("reader", false)
		require.NoError(t, err)
		writerCommits, writerRollbacks := 0, 0
		require.NoError(t, tx.BindParticipantCallbacks("writer", ParticipantCallbacks{Commit: func(*SubTransaction) error { writerCommits++; return nil }, Rollback: func(*SubTransaction) error { writerRollbacks++; return nil }}))
		require.NoError(t, tx.BindParticipantCallbacks("reader", ParticipantCallbacks{Commit: func(*SubTransaction) error { return errors.New("reader close rejected") }, Rollback: func(*SubTransaction) error { return nil }}))
		require.Error(t, tx.Commit(nil, nil))
		require.Zero(t, writerCommits, "a reader failure must prevent any write commit")
		require.Equal(t, 1, writerRollbacks)
	}
}

func TestFabricParticipantBindingRejectsEndedOwner(t *testing.T) {
	for _, end := range []string{"commit", "rollback"} {
		t.Run(end, func(t *testing.T) {
			tx := NewFabricTransaction("late-binding")
			sub, err := tx.GetOrOpen("late", false)
			require.NoError(t, err)
			if end == "commit" {
				require.NoError(t, tx.Commit(nil, nil))
			} else {
				require.NoError(t, tx.Rollback(nil))
			}
			err = tx.BindParticipantCallbacks("late", ParticipantCallbacks{Commit: func(*SubTransaction) error { return nil }})
			require.Error(t, err, "closed coordinator cannot acquire a newly opened native resource")
			require.Nil(t, sub.commitFn, "failed binding must not replace terminal callbacks")
		})
	}
}
