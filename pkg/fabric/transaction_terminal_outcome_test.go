package fabric

import (
	"context"
	"errors"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFabricConfirmedCommitRetainsStructuredTerminalFailure(t *testing.T) {
	for _, kind := range []string{"generic", "structured-committed", "structured-rollback-claim"} {
		t.Run(kind, func(t *testing.T) {
			tx := NewFabricTransaction("terminal")
			_, err := tx.GetOrOpen("writer", true)
			require.NoError(t, err)
			cause := errors.New("terminal bookkeeping failed")
			var finish error = cause
			if kind != "generic" {
				finish = nornicerrors.NewTerminalFailure(nornicerrors.TerminalCommit, kind == "structured-committed", cause, cause, localization.Message{})
				if kind == "structured-rollback-claim" {
					finish = nornicerrors.MarkCommitRolledBack(finish)
				}
			}
			commits, rollbacks := 0, 0
			require.NoError(t, tx.BindParticipantCallbacks("writer", ParticipantCallbacks{Commit: func(context.Context, *SubTransaction) error { commits++; return finish }, Rollback: func(context.Context, *SubTransaction) error { rollbacks++; return nil }, Outcome: func() string { return "committed" }}))
			err = tx.Commit(TerminalContexts{Commit: t.Context(), Cleanup: context.WithoutCancel(t.Context())}, nil, nil)
			require.Equal(t, "committed", tx.State())
			require.Equal(t, 1, commits)
			require.Zero(t, rollbacks)
			if kind == "generic" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, cause)
				require.False(t, nornicerrors.IsCommitRolledBack(err))
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.DatabaseError.Transaction.TransactionCommitFailed", code)
			}
		})
	}
}
