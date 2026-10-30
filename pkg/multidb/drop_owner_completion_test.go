package multidb

import (
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Author: NOR-288 lane (Claude), 2026-10-30. The DROP owner finishes a drop
// whose intent applied but whose acknowledgement failed, in the same call. The
// accepted outcome is non-retryable, so no client retry is invited.
func TestDropOwnerFinishesAppliedUnacknowledgedDrop(t *testing.T) {
	base, e, m := openDropOwnerFixture(t)
	e.acceptedError = true
	err := m.DropDatabase(t.Context(), "victim")
	var accepted *storage.AcceptedCommitError
	require.ErrorAs(t, err, &accepted)
	require.Equal(t, storage.CommitApplied, accepted.State)
	require.NotErrorIs(t, err, storage.ErrConflict)
	require.True(t, catalogAcceptedDiagnosticContains(accepted, storage.ErrConflict))
	retryable := nornicerrors.IsTransientTransactionError(err)
	require.False(t, retryable, "no automatic retry is invited")
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, nornicerrors.DatabaseTransactionCommitFailed, code)
	require.False(t, m.Exists("victim"), "the owner finished the drop it accepted")
	require.False(t, m.pendingDrops["victim"])
	_, err = base.GetStoredNode("victim:keep")
	require.ErrorIs(t, err, storage.ErrNotFound)
	e.acceptedError = false
	require.NoError(t, m.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "victim"}))
}
