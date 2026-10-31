package multidb

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Author: NOR-288 lane (Claude), 2026-10-30. Release-buildable: uses only the
// release fault fixture, so the same file runs as a release red overlay.
func openDropOwnerFixture(t *testing.T) (*storage.MemoryEngine, *managedCatalogFailureEngine, *DatabaseManager) {
	t.Helper()
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	e := &managedCatalogFailureEngine{Engine: base}
	m, err := NewDatabaseManager(t.Context(), e, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })
	require.NoError(t, m.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "victim"}))
	view, err := m.GetStorage("victim")
	require.NoError(t, err)
	_, err = view.CreateNode(&storage.Node{ID: "keep", Properties: map[string]interface{}{"value": "old"}})
	require.NoError(t, err)
	e.updates = 0
	return base, e, m
}

// A driver re-executes the same name-only statement only when it classifies
// the error as transient. A delayed retry of a completed drop must never reach
// a replacement database. The planted control re-adds a transient marking and
// shows that the same sequence then deletes the replacement.
func TestDropDelayedRetryCannotReachReplacement(t *testing.T) {
	for _, tc := range []struct {
		name      string
		transient func(error) bool
	}{
		{"accepted-contract", func(err error) bool { return nornicerrors.IsTransientTransactionError(err) }},
		{"planted-transient-marking", func(error) bool { return true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, e, m := openDropOwnerFixture(t)
			e.acceptedError = true
			errA := m.DropDatabase(t.Context(), "victim") // client A
			require.Error(t, errA)
			e.acceptedError = false
			// Client B drops the name (finishing any pending drop) and replaces it.
			if err := m.DropDatabase(t.Context(), "victim"); err != nil {
				require.ErrorIs(t, err, ErrDatabaseNotFound)
			}
			require.NoError(t, m.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "victim"}))
			fresh, err := m.GetStorage("victim")
			require.NoError(t, err)
			_, err = fresh.CreateNode(&storage.Node{ID: "fresh", Properties: map[string]interface{}{"value": "new"}})
			require.NoError(t, err)
			// Client A's driver: a delayed automatic retry, only if transient.
			if tc.transient(errA) {
				_ = m.DropDatabase(t.Context(), "victim")
			}
			_, err = base.GetStoredNode("victim:fresh")
			if tc.name == "planted-transient-marking" {
				require.ErrorIs(t, err, storage.ErrNotFound, "control: a transient marking lets the stale retry delete the replacement")
				return
			}
			require.NoError(t, err, "a delayed name-only retry must not delete the replacement")
		})
	}
}
