// Author: Codex, NOR-180 participant state and completion are independent, 2026-11-01.
package fabric

import (
	"errors"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFabricTransactionKnownStateKeepsTerminalFailure(t *testing.T) {
	for _, writer := range []bool{false, true} {
		for _, state := range []string{"committed", "rolledback"} {
			for _, terminal := range []bool{false, true} {
				name := state + map[bool]string{false: "/reader", true: "/writer"}[writer] + map[bool]string{false: "/ordinary", true: "/terminal"}[terminal]
				t.Run(name, func(t *testing.T) {
					tx := NewFabricTransaction(name)
					_, err := tx.GetOrOpen("one", writer)
					require.NoError(t, err)
					primary := storage.ErrStorageRestoring
					completion := error(primary)
					if terminal {
						completion = nornicerrors.NewTerminalFailure(nornicerrors.TerminalCommit, state == "committed", primary, storage.ErrWALClosed, localization.Message{})
					}
					require.NoError(t, tx.BindParticipantCallbacks("one", ParticipantCallbacks{
						Commit: func(*SubTransaction) error { return completion }, Rollback: func(*SubTransaction) error { return nil }, Outcome: func() string { return state },
					}))
					err = tx.Commit(nil, nil)
					require.ErrorIs(t, err, completion)
					require.ErrorIs(t, err, primary)
					if terminal {
						require.ErrorIs(t, err, storage.ErrWALClosed)
					}
					if state == "committed" {
						code, _ := nornicerrors.Neo4jStatus(err)
						require.False(t, (&db.Neo4jError{Code: code}).IsRetriable())
						require.Equal(t, "committed", tx.SubTransactions()["one"].State)
						if writer {
							require.Equal(t, "committed", tx.State())
							require.NotErrorIs(t, err, ErrCommitKeptNothing)
						}
					} else {
						require.Equal(t, "rolledback", tx.State())
						require.ErrorIs(t, err, ErrCommitKeptNothing)
					}
				})
			}
		}
	}
}

func TestFabricTransactionRollbackKeepsFinalFailure(t *testing.T) {
	primary, cleanup := errors.New("rollback failed"), storage.ErrWALClosed
	tx := NewFabricTransaction("rollback")
	_, err := tx.GetOrOpen("one", true)
	require.NoError(t, err)
	completion := nornicerrors.NewTerminalFailure(nornicerrors.TerminalRollback, false, primary, cleanup, localization.Message{})
	require.NoError(t, tx.BindParticipantCallbacks("one", ParticipantCallbacks{Rollback: func(*SubTransaction) error { return completion }, Outcome: func() string { return "rolledback" }}))
	err = tx.Rollback(nil)
	require.ErrorIs(t, err, completion)
	require.ErrorIs(t, err, primary)
	require.ErrorIs(t, err, cleanup)
	require.Equal(t, "rolledback", tx.State())
	require.Equal(t, "rolledback", tx.SubTransactions()["one"].CommitOutcome)
}
