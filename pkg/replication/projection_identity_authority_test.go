package replication

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestProjectionIdentityUsesAcceptedEnvelope(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		logicalIDs, requestIDs [2]string
		count                  int
		sharedPosition         bool
	}{
		{"one accepted command different request IDs", [2]string{"accepted", "accepted"}, [2]string{"first", "retry"}, 1, true},
		{"distinct accepted commands same request ID", [2]string{"first", "second"}, [2]string{"transport", "transport"}, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, fixture := newDatabaseScopedFixture(t)
			var publications [2]CommandPublication
			for i := range 2 {
				cmd := databaseScopedCommand(t, "alpha", tc.logicalIDs[i], tc.logicalIDs[i])
				cmd.RequestID = tc.requestIDs[i]
				publication, err := fixture.adapter.ApplyCommand(t.Context(), cmd, WALPublished)
				require.NoError(t, err)
				publications[i] = publication
			}
			assertNOR291ProjectionCount(t, fixture.adapter, tc.count)
			if tc.sharedPosition {
				require.Equal(t, publications[0].Position, publications[1].Position, "a rewritten request ID must not report a second accepted identity")
			} else {
				require.NotEqual(t, publications[0].Position, publications[1].Position, "a shared request ID must not collapse two accepted commands")
			}
		})
	}
}

// acceptedEntityFixture is one locally accepted transaction for a database. The
// storage owner encodes the snapshot and retains the identity the publication
// journal must key on, while the transport stamps its own request ID on the
// command, so only the retained accepted identity keeps retries idempotent.
type acceptedEntityFixture struct {
	command   *Command
	accepted  *storage.AcceptedEntityCommitHandle
	logicalID string
}

func newAcceptedEntityCommand(t *testing.T, engine *storage.BadgerEngine, database, nodeID, requestID string) acceptedEntityFixture {
	t.Helper()
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace(database))
	_, err = tx.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(database + ":" + nodeID), Labels: []string{"Scoped"}})
	require.NoError(t, err)
	prepared, err := tx.PrepareEntityCommit(context.Background(), storage.EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	t.Cleanup(prepared.Abort)
	accepted, payload, err := storage.AcceptEntityCommit(context.Background(), engine, prepared)
	require.NoError(t, err)
	return acceptedEntityFixture{
		command:   &Command{Type: CmdCommitTransaction, Data: payload, Timestamp: time.Now(), RequestID: requestID, accepted: accepted},
		accepted:  accepted,
		logicalID: prepared.LogicalID(),
	}
}

// retryCommand is the same accepted command as a transport re-delivers it under
// another request ID.
func (f acceptedEntityFixture) retryCommand(requestID string) *Command {
	return &Command{Type: f.command.Type, Data: f.command.Data, Timestamp: f.command.Timestamp, RequestID: requestID, accepted: f.accepted}
}

// The accepted owner's identity is retained at acceptance, so it outlives the
// prepared frame's retirement and a retry stamped with a different request ID
// reports the original publication position instead of a second record.
func TestAcceptedIdentitySurvivesPreparedRetirement(t *testing.T) {
	base, fixture := newDatabaseScopedFixture(t)
	a := fixture.adapter
	accepted := newAcceptedEntityCommand(t, base, "alpha", "retained", "transport-attempt")
	require.NotEqual(t, accepted.command.RequestID, accepted.logicalID, "the transport request ID is not the accepted logical ID")
	namespace, logicalID := accepted.accepted.Identity()
	require.Equal(t, "alpha", namespace)
	require.Equal(t, accepted.logicalID, logicalID)
	require.True(t, accepted.accepted.NativeFrameRetained())
	require.True(t, accepted.accepted.AppliesTo(a.engine), "the accepted owner applies its own command locally")

	first, err := a.ApplyCommand(t.Context(), accepted.command, WALPublished)
	require.NoError(t, err)
	require.NotZero(t, first.Position)
	assertNOR291ProjectionCount(t, a, 1)

	// Native settlement retires the prepared frame; the accepted identity must
	// survive it, because a publication retry still keys on that identity.
	accepted.accepted.Terminal(nil)(storage.CommitOutcome{State: storage.CommitApplied})
	require.False(t, accepted.accepted.NativeFrameRetained())
	namespace, logicalID = accepted.accepted.Identity()
	require.Equal(t, "alpha", namespace)
	require.Equal(t, accepted.logicalID, logicalID)
	require.True(t, accepted.accepted.AppliesTo(a.engine))

	retry, err := a.ApplyCommand(t.Context(), accepted.retryCommand("transport-retry"), WALPublished)
	require.NoError(t, err)
	require.Equal(t, first.Position, retry.Position, "a mutated request ID must not create a second accepted identity")
	assertNOR291ProjectionCount(t, a, 1)
	requireProjectionNodes(t, base, "alpha:retained")
}

// An envelope carries the accepted identity, so a replay whose request ID was
// rewritten still resolves to the retained record after a restart: it reports the
// original position and writes no second projection.
func TestMismatchedRequestIDKeepsAcceptedIdentityAcrossReopen(t *testing.T) {
	base, fixture := newDatabaseScopedFixture(t)
	first := databaseScopedCommand(t, "alpha", "accepted", "node")
	first.RequestID = "first-transport"
	publication, err := fixture.adapter.ApplyCommand(t.Context(), first, WALPublished)
	require.NoError(t, err)
	require.NotZero(t, publication.Position)
	assertNOR291ProjectionCount(t, fixture.adapter, 1)
	require.NoError(t, fixture.adapter.Close())

	reopened, err := NewStorageAdapterWithWAL(base, fixture.walDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	replay := databaseScopedCommand(t, "alpha", "accepted", "node")
	replay.RequestID = "second-transport"
	retried, err := reopened.ApplyCommand(t.Context(), replay, WALPublished)
	require.NoError(t, err)
	require.Equal(t, publication.Position, retried.Position, "the reopened journal keeps the envelope's accepted identity")
	assertNOR291ProjectionCount(t, reopened, 1)
	requireProjectionNodes(t, base, "alpha:node")
}

// The retained accepted identity is the key the reopened journal rebuilt from the
// journal itself: after the frame retired and the adapter restarted, a retry
// under another request ID still reports the original publication position.
func TestRetiredAcceptedIdentityReplaysOriginalPositionAfterReopen(t *testing.T) {
	base, fixture := newDatabaseScopedFixture(t)
	a := fixture.adapter
	accepted := newAcceptedEntityCommand(t, base, "alpha", "retained", "transport-attempt")
	require.NotEqual(t, accepted.command.RequestID, accepted.logicalID, "the transport request ID is not the accepted logical ID")
	require.True(t, accepted.accepted.AppliesTo(a.engine), "the accepted owner applies its own command locally")
	first, err := a.ApplyCommand(t.Context(), accepted.command, WALPublished)
	require.NoError(t, err)
	require.NotZero(t, first.Position)
	assertNOR291ProjectionCount(t, a, 1)

	accepted.accepted.Terminal(nil)(storage.CommitOutcome{State: storage.CommitApplied})
	require.False(t, accepted.accepted.NativeFrameRetained())
	require.NoError(t, a.Close())

	reopened, err := NewStorageAdapterWithWAL(base, fixture.walDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	namespace, logicalID := accepted.accepted.Identity()
	require.Equal(t, "alpha", namespace)
	require.Equal(t, accepted.logicalID, logicalID)
	require.True(t, accepted.accepted.AppliesTo(reopened.engine), "the retained owner applies on its own engine")
	retry, err := reopened.ApplyCommand(t.Context(), accepted.retryCommand("transport-after-restart"), WALPublished)
	require.NoError(t, err)
	require.Equal(t, first.Position, retry.Position, "startup identity and the retained accepted identity are one key")
	assertNOR291ProjectionCount(t, reopened, 1)
	requireProjectionNodes(t, base, "alpha:retained")
}
