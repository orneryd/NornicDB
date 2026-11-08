package replication

// The accepted projection journal owns one record per accepted command. A
// caller's logical ID is only unique inside the database that accepted it, so
// two databases applying the same logical ID must both stay journaled, streamed,
// replicated and retried idempotently.

import (
	"context"
	"sync"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newDatabaseScopedFixture backs one adapter with a native engine that can hold
// several databases, so distinct database namespaces apply natively.
func newDatabaseScopedFixture(t *testing.T) (*storage.BadgerEngine, *adapterFixture) {
	t.Helper()
	base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir:             t.TempDir(),
		WriteBehind:         false,
		WriteBehindInterval: time.Hour,
	}))
	require.NoError(t, err)
	return base, setupTestAdapter(t, adapterFixtureOptions{backing: base})
}

// databaseScopedCommand is one accepted commit for database under the caller's
// logical ID. The encoded envelope carries both identities, as a normalized
// leader command does after AcceptEntityCommit.
func databaseScopedCommand(t *testing.T, database, logicalID, nodeID string) *Command {
	t.Helper()
	data, err := storage.EncodeEntityTransaction(storage.EntityTransactionRequest{
		Namespace: database,
		LogicalID: logicalID,
		Mode:      storage.CommitModeDirect,
		Operations: []storage.Operation{{
			Type: storage.OpCreateNode,
			Node: &storage.Node{ID: storage.NodeID(database + ":" + nodeID), Labels: []string{"Scoped"}},
		}},
	})
	require.NoError(t, err)
	return &Command{Type: CmdCommitTransaction, Data: data, Timestamp: time.Now(), RequestID: logicalID}
}

// databaseScopedFailingCommand is one accepted commit for database whose native
// application fails: its edge endpoints exist in no database.
func databaseScopedFailingCommand(t *testing.T, database, logicalID string) *Command {
	t.Helper()
	data, err := storage.EncodeEntityTransaction(storage.EntityTransactionRequest{
		Namespace: database,
		LogicalID: logicalID,
		Mode:      storage.CommitModeDirect,
		Operations: []storage.Operation{{
			Type: storage.OpCreateEdge,
			Edge: &storage.Edge{ID: storage.EdgeID(database + ":edge"), StartNode: storage.NodeID(database + ":missing-a"), EndNode: storage.NodeID(database + ":missing-b"), Type: "LINK"},
		}},
	})
	require.NoError(t, err)
	return &Command{Type: CmdCommitTransaction, Data: data, Timestamp: time.Now(), RequestID: logicalID}
}

// requireProjectionNodes asserts both databases' writes reached the engine.
func requireProjectionNodes(t *testing.T, engine storage.Engine, ids ...storage.NodeID) {
	t.Helper()
	for _, id := range ids {
		_, err := storage.GetStoredNode(context.Background(), engine, id)
		require.NoError(t, err, "database %q must apply its own accepted command", id)
	}
}

// A same logical ID accepted by two databases is two accepted commands: both
// journal their own projection, both apply natively, and each database's retry
// keeps its own original publication in either publication mode.
func TestProjectionIdentityIsDatabaseScoped(t *testing.T) {
	for _, mode := range []WALPublicationMode{WALPublished, WALQueued} {
		name := "published"
		if mode == WALQueued {
			name = "queued"
		}
		t.Run(name, func(t *testing.T) {
			base, fixture := newDatabaseScopedFixture(t)
			a := fixture.adapter
			alpha := databaseScopedCommand(t, "alpha", "shared-logical-id", "n")
			beta := databaseScopedCommand(t, "beta", "shared-logical-id", "n")

			first, err := a.ApplyCommand(t.Context(), alpha, mode)
			require.NoError(t, err)
			second, err := a.ApplyCommand(t.Context(), beta, mode)
			require.NoError(t, err)
			require.NoError(t, a.FlushWAL())
			entries, err := a.GetWALEntries(0, 10)
			require.NoError(t, err)
			require.Len(t, entries, 2, "both databases' accepted commands must own a projection")
			require.NotEqual(t, entries[0].Position, entries[1].Position)
			if mode == WALPublished {
				require.Equal(t, entries[0].Position, first.Position)
				require.Equal(t, entries[1].Position, second.Position)
			}
			requireProjectionNodes(t, base, "alpha:n", "beta:n")

			retry := func(cmd *Command, want uint64) {
				t.Helper()
				publication, err := a.ApplyCommand(t.Context(), cmd, mode)
				require.NoError(t, err)
				if mode == WALPublished {
					require.Equal(t, want, publication.Position, "a same-database retry keeps its original publication")
				}
			}
			retry(alpha, entries[0].Position)
			retry(beta, entries[1].Position)
			assertNOR291ProjectionCount(t, a, 2)
		})
	}
}

// Pruning the streamed window and reopening the adapter rebuild the identity
// map from the retained journal: retries stay idempotent per database and a
// third database reusing the ID is a new accepted command.
func TestProjectionDatabaseIdentitySurvivesPruneAndReopen(t *testing.T) {
	base, fixture := newDatabaseScopedFixture(t)
	a := fixture.adapter
	alpha := databaseScopedCommand(t, "alpha", "shared", "n")
	beta := databaseScopedCommand(t, "beta", "shared", "n")
	first, err := a.ApplyCommand(t.Context(), alpha, WALPublished)
	require.NoError(t, err)
	second, err := a.ApplyCommand(t.Context(), beta, WALPublished)
	require.NoError(t, err)
	require.NotEqual(t, first.Position, second.Position)

	a.PruneWALEntries(first.Position)
	retried, err := a.ApplyCommand(t.Context(), alpha, WALPublished)
	require.NoError(t, err)
	require.Equal(t, first.Position, retried.Position, "pruned identity still dedups its own database")
	assertNOR291PublishedProjectionCount(t, a, 2)
	require.NoError(t, a.Close())

	reopened, err := NewStorageAdapterWithWAL(base, fixture.walDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	for _, test := range []struct {
		command  *Command
		position uint64
	}{{alpha, first.Position}, {beta, second.Position}} {
		publication, err := reopened.ApplyCommand(t.Context(), test.command, WALPublished)
		require.NoError(t, err)
		require.Equal(t, test.position, publication.Position, "the reopened journal owns each database's projection")
	}
	assertNOR291PublishedProjectionCount(t, reopened, 2)

	gamma := databaseScopedCommand(t, "gamma", "shared", "n")
	publication, err := reopened.ApplyCommand(t.Context(), gamma, WALPublished)
	require.NoError(t, err)
	require.NotEqual(t, first.Position, publication.Position)
	require.NotEqual(t, second.Position, publication.Position)
	assertNOR291ProjectionCount(t, reopened, 3)
	requireProjectionNodes(t, base, "alpha:n", "beta:n", "gamma:n")
}

// A standby streams each database's accepted command, journals its own
// projection for both, and treats a duplicate delivery as a retry.
func TestProjectionDatabaseIdentityReplicatesToStandby(t *testing.T) {
	leaderBase, leaderFixture := newDatabaseScopedFixture(t)
	standbyBase, standbyFixture := newDatabaseScopedFixture(t)
	alpha := databaseScopedCommand(t, "alpha", "shared", "n")
	beta := databaseScopedCommand(t, "beta", "shared", "n")
	for _, cmd := range []*Command{alpha, beta} {
		_, err := leaderFixture.adapter.ApplyCommand(t.Context(), cmd, WALPublished)
		require.NoError(t, err)
	}
	require.NoError(t, leaderFixture.adapter.FlushWAL())
	entries, err := leaderFixture.adapter.GetWALEntries(0, 10)
	require.NoError(t, err)
	require.Len(t, entries, 2, "the leader must stream both databases' accepted commands")

	last, err := NewWALApplier(standbyFixture.adapter).ApplyBatch(t.Context(), entries)
	require.NoError(t, err)
	require.Equal(t, entries[1].Position, last)
	assertNOR291ProjectionCount(t, standbyFixture.adapter, 2)
	requireProjectionNodes(t, standbyBase, "alpha:n", "beta:n")
	requireProjectionNodes(t, leaderBase, "alpha:n", "beta:n")

	// A fresh applier replays the same stream against the retained adapter: both
	// deliveries are retries of the same accepted commands.
	_, err = NewWALApplier(standbyFixture.adapter).ApplyBatch(t.Context(), entries)
	require.NoError(t, err)
	assertNOR291ProjectionCount(t, standbyFixture.adapter, 2)
}

// Native application failure stays visible and independent of the projection,
// and concurrent retries of two databases sharing a logical ID keep exactly one
// record each across prune and reopen.
func TestProjectionDatabaseIdentityNativeFailureAndConcurrency(t *testing.T) {
	base, fixture := newDatabaseScopedFixture(t)
	a := fixture.adapter
	alpha := databaseScopedFailingCommand(t, "alpha", "shared-failure")
	beta := databaseScopedFailingCommand(t, "beta", "shared-failure")
	for range 3 {
		require.Error(t, applyNOR291(a, alpha, WALPublished), "a projection must not hide native application failure")
		require.Error(t, applyNOR291(a, beta, WALPublished), "a projection must not hide native application failure")
	}
	assertNOR291ProjectionCount(t, a, 2)

	var wg sync.WaitGroup
	results := make(chan error, 40)
	for range 20 {
		wg.Go(func() {
			results <- applyNOR291(a, alpha, WALPublished)
			results <- applyNOR291(a, beta, WALPublished)
		})
	}
	wg.Wait()
	close(results)
	for err := range results {
		require.Error(t, err)
	}
	assertNOR291ProjectionCount(t, a, 2)

	entries, err := a.GetWALEntries(0, 10)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	a.PruneWALEntries(entries[1].Position)
	require.Error(t, applyNOR291(a, beta, WALPublished))
	assertNOR291ProjectionCount(t, a, 2)
	require.NoError(t, a.Close())
	reopened, err := NewStorageAdapterWithWAL(base, fixture.walDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	for _, cmd := range []*Command{alpha, beta} {
		require.Error(t, applyNOR291(reopened, cmd, WALPublished), "the reopened journal keeps the native failure visible")
	}
	assertNOR291ProjectionCount(t, reopened, 2)
}

// Collision control. Logical IDs are caller text and may contain any delimiter,
// so identity must never be a concatenation of database and ID. A database name
// cannot contain the entity-ID delimiter: every changed entity ID must parse
// back to the transaction's own namespace, which is asserted here.
func TestProjectionIdentityDelimiterControl(t *testing.T) {
	_, fixture := newDatabaseScopedFixture(t)
	a := fixture.adapter
	// "alpha"+"beta:shared" and "alpha:beta"+"shared" would collide if the
	// identity were database+":"+logicalID.
	first := databaseScopedCommand(t, "alpha", "beta:shared", "one")
	second := databaseScopedCommand(t, "beta", "shared", "two")
	third := databaseScopedCommand(t, "alpha", "beta|shared|one", "three")
	for _, cmd := range []*Command{first, second, third} {
		_, err := a.ApplyCommand(t.Context(), cmd, WALPublished)
		require.NoError(t, err)
	}
	assertNOR291ProjectionCount(t, a, 3)
	entries, err := a.GetWALEntries(0, 10)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	// Every delimited identity retries to its own projection.
	for i, cmd := range []*Command{first, second, third} {
		publication, err := a.ApplyCommand(t.Context(), cmd, WALPublished)
		require.NoError(t, err)
		require.Equal(t, entries[i].Position, publication.Position)
	}
	assertNOR291ProjectionCount(t, a, 3)

	// A delimiter-containing database cannot own a decodable accepted command.
	ambiguous, err := storage.EncodeEntityTransaction(storage.EntityTransactionRequest{
		Namespace: "alpha:beta",
		LogicalID: "shared",
		Mode:      storage.CommitModeDirect,
		Operations: []storage.Operation{{
			Type: storage.OpCreateNode,
			Node: &storage.Node{ID: "alpha:beta:n", Labels: []string{"Scoped"}},
		}},
	})
	if err == nil {
		_, _, err = storage.DecodeEntityTransaction(ambiguous)
	}
	require.Error(t, err, "the transaction wire owns database names without the entity-ID delimiter")
}
