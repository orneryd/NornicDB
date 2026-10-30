package cypher

import (
	"sync/atomic"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// dropSyncFaultNative fails one durable Sync of the native storage owner. The
// DROP owner reaches it through the probe's inner engine, as in production.
type dropSyncFaultNative struct {
	*storage.BadgerEngine
	syncCalls atomic.Int32
	failSync  atomic.Int32
	// When set, the DROP owner's data purge reports entry and waits for a permit.
	purgeEntered chan struct{}
	purgePermit  chan struct{}
}

func (e *dropSyncFaultNative) DeleteByPrefix(prefix string) (int64, int64, error) {
	if e.purgeEntered != nil {
		e.purgeEntered <- struct{}{}
		<-e.purgePermit
	}
	return e.BadgerEngine.DeleteByPrefix(prefix)
}

func (e *dropSyncFaultNative) Sync() error {
	if e.syncCalls.Add(1) == e.failSync.Load() {
		return storage.ErrConflict
	}
	return e.BadgerEngine.Sync()
}

type dropOwnerCypherFixture struct {
	native   *storage.BadgerEngine
	faults   *dropSyncFaultNative
	backing  *createOptionsSubmissionProbe
	manager  *multidb.DatabaseManager
	executor *StorageExecutor
}

// Author: NOR-288 lane (Claude), 2026-10-30. Public Cypher paths over the real
// DROP owner: one authoritative submitter, a native Badger store and its Sync.
func openDropOwnerCypherFixture(t *testing.T) *dropOwnerCypherFixture {
	t.Helper()
	native, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true}))
	require.NoError(t, err)
	faults := &dropSyncFaultNative{BadgerEngine: native}
	backing := &createOptionsSubmissionProbe{Engine: faults, native: native}
	manager, err := multidb.NewDatabaseManager(t.Context(), backing, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	executor := NewStorageExecutor(storage.NewNamespacedEngine(backing, "nornic"))
	executor.SetDatabaseManager(&catalogRequestContextAdapter{optionsTestManager: optionsTestManager{&testDatabaseManagerAdapter{manager: manager}}})
	f := &dropOwnerCypherFixture{native: native, faults: faults, backing: backing, manager: manager, executor: executor}
	f.createWithNode(t, "old")
	return f
}

func (f *dropOwnerCypherFixture) exec(t *testing.T, query string) error {
	t.Helper()
	_, err := f.executor.Execute(t.Context(), query, nil)
	return err
}

func (f *dropOwnerCypherFixture) createWithNode(t *testing.T, id string) {
	t.Helper()
	require.NoError(t, f.exec(t, "CREATE DATABASE victim"))
	view, err := f.manager.GetStorage("victim")
	require.NoError(t, err)
	_, err = view.CreateNode(&storage.Node{ID: storage.NodeID(id)})
	require.NoError(t, err)
}

func (f *dropOwnerCypherFixture) stored(id string) bool {
	_, err := f.native.GetNode(storage.NodeID("victim:" + id))
	return err == nil
}

func requireAcceptedNonRetryable(t *testing.T, err error) {
	t.Helper()
	var accepted *storage.AcceptedCommitError
	require.ErrorAs(t, err, &accepted)
	require.Equal(t, storage.CommitApplied, accepted.State)
	retryable := nornicerrors.IsTransientTransactionError(err)
	require.False(t, retryable, "an accepted DROP outcome invites no automatic retry")
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, nornicerrors.DatabaseTransactionCommitFailed, code)
}

// A driver re-executes the same name-only statement only when it classifies
// the error as transient. The planted control re-adds a transient marking and
// shows the same delayed retry then deletes the replacement database.
func TestCypherDropDelayedRetryCannotReachReplacement(t *testing.T) {
	for _, tc := range []struct {
		name      string
		transient func(error) bool
	}{
		{"accepted-contract", func(err error) bool { return nornicerrors.IsTransientTransactionError(err) }},
		{"planted-transient-marking", func(error) bool { return true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := openDropOwnerCypherFixture(t)
			f.backing.mode = "applied-ack-error"
			errA := f.exec(t, "DROP DATABASE victim") // client A
			f.backing.mode = ""
			requireAcceptedNonRetryable(t, errA)
			require.False(t, f.manager.Exists("victim"), "the owner finished the drop it accepted")
			require.False(t, f.stored("old"))
			// Client B replaces the name.
			require.NoError(t, f.exec(t, "DROP DATABASE victim IF EXISTS"))
			f.createWithNode(t, "fresh")
			// Client A's driver: a delayed automatic retry, only if transient.
			if tc.transient(errA) {
				_ = f.exec(t, "DROP DATABASE victim")
			}
			if tc.name == "planted-transient-marking" {
				require.False(t, f.stored("fresh"), "control: a transient marking lets the stale retry delete the replacement")
				return
			}
			require.True(t, f.stored("fresh"), "a delayed name-only retry must not delete the replacement")
			require.True(t, f.manager.Exists("victim"))
		})
	}
}

// The final metadata Sync of a DROP fails after its catalog removal applied:
// the record is absent and the unavailable-name fence is held. Every public
// DROP form and CREATE OR REPLACE must reach the owner, which finishes it.
func TestCypherDropFinishesPendingDropAfterFinalSyncFailure(t *testing.T) {
	for _, follow := range []string{"DROP DATABASE victim", "DROP DATABASE victim IF EXISTS", "CREATE OR REPLACE DATABASE victim"} {
		t.Run(follow, func(t *testing.T) {
			f := openDropOwnerCypherFixture(t)
			// DROP Syncs once after its intent and once after the final removal.
			f.faults.failSync.Store(f.faults.syncCalls.Load() + 2)
			err := f.exec(t, "DROP DATABASE victim")
			requireAcceptedNonRetryable(t, err)
			require.False(t, f.manager.Exists("victim"))
			require.False(t, f.stored("old"))
			require.ErrorIs(t, f.manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "victim"}), multidb.ErrDatabaseExists, "the pending name stays fenced")
			require.Error(t, f.exec(t, "CREATE DATABASE victim"), "plain CREATE is refused while the drop is pending")
			require.NoError(t, f.exec(t, follow), "the owner finishes the pending drop")
			if follow == "CREATE OR REPLACE DATABASE victim" {
				require.True(t, f.manager.Exists("victim"))
				require.NoError(t, f.exec(t, "DROP DATABASE victim"))
			}
			require.NoError(t, f.exec(t, "CREATE DATABASE victim"), "the fence cleared")
			require.NoError(t, f.exec(t, "DROP DATABASE victim"))
			require.Error(t, f.exec(t, "DROP DATABASE victim"), "an absent name without a fence does not exist")
			require.NoError(t, f.exec(t, "DROP DATABASE victim IF EXISTS"))
		})
	}
}
