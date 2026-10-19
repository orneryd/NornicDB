package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNamespaceDropJournalAdmissionIsCommitPoint(t *testing.T) {
	for _, admitted := range []bool{false, true} {
		t.Run(map[bool]string{false: "preparation-error", true: "post-admission-error"}[admitted], func(t *testing.T) {
			b := NewMemoryEngine()
			defer b.Close()
			_, err := b.CreateNode(&Node{ID: "outcome_a:seed"})
			require.NoError(t, err)
			old, err := b.BeginTransaction()
			require.NoError(t, err)
			defer old.Rollback()
			require.NoError(t, old.SetNamespace("outcome_a"))
			completed := false
			op := NewNamespaceDrop(func() error { completed = true; return nil })
			fault := errors.New("journal I/O failure")
			op.SetJournal(func() (bool, error) { return admitted, fault })
			_, _, err = b.DeleteByPrefixContext(WithNamespaceDrop(context.Background(), op), "outcome_a:")
			require.ErrorIs(t, err, fault)
			require.Equal(t, admitted, op.Committed())
			require.Equal(t, admitted, completed)
			_, readErr := b.GetNode("outcome_a:seed")
			if admitted {
				var outcome *NamespaceDropError
				require.ErrorAs(t, err, &outcome)
				require.True(t, outcome.Committed)
				require.ErrorIs(t, readErr, ErrNotFound)
				require.ErrorIs(t, old.Commit(), ErrConflict)
			} else {
				require.NoError(t, readErr)
				require.NoError(t, old.Commit(), "failed preparation does not retire the old lifetime")
			}
			_, err = b.CreateNode(&Node{ID: "outcome_a:fresh"})
			require.NoError(t, err)
		})
	}
}
