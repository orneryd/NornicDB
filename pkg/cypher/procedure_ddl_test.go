package cypher

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestCreateCallDropProcedureDDL(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)

	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:User {id: 'u-10', age: 21, last_seen: null})", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
CREATE OR REPLACE PROCEDURE nornic.touchUser($id, $ts)
MODE WRITE
AS
MATCH (u:User {id: $id})
SET u.last_seen = $ts
RETURN u
`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "CALL nornic.touchUser('u-10', datetime()) YIELD u RETURN u.id, u.last_seen", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"u.id", "u.last_seen"}, result.Columns)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "u-10", result.Rows[0][0])
	require.NotNil(t, result.Rows[0][1])

	_, err = exec.Execute(ctx, "DROP PROCEDURE nornic.touchUser", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "CALL nornic.touchUser('u-10', datetime())", nil)
	require.Error(t, err)
}

func TestPersistedProcedurePrecompiledOnStartup(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)

	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec1 := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec1.Execute(ctx, "CREATE (:User {id: 'u-1', active: false})", nil)
	require.NoError(t, err)

	_, err = exec1.Execute(ctx, `
CREATE OR REPLACE PROCEDURE nornic.activateUser($id)
MODE WRITE
AS
MATCH (u:User {id: $id})
SET u.active = true
RETURN u
`, nil)
	require.NoError(t, err)

	// Simulate process restart/runtime reset.
	ClearUserProcedures()

	exec2 := NewStorageExecutor(store)
	result, err := exec2.Execute(ctx, "CALL nornic.activateUser('u-1') YIELD u RETURN u.active", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, true, result.Rows[0][0])
}

func TestProcedureCreateRejectedInsideActiveTransaction(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)

	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "BEGIN TRANSACTION", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "CREATE PROCEDURE nornic.bad() MODE READ AS RETURN 1 AS value", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not allowed inside an active transaction")

	_, _ = exec.Execute(ctx, "ROLLBACK", nil)
}

func TestLoadPersistedProcedures_BranchCoverage(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)

	t.Run("loads valid persisted procedures", func(t *testing.T) {
		base := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(base, "test")
		exec := NewStorageExecutor(store)
		ctx := context.Background()

		valid := persistedProcedureRecord{
			Name:      "nornic.loadedProc",
			Mode:      "READ",
			Body:      "RETURN 42 AS value",
			Signature: buildProcedureSignature("nornic.loadedProc", nil),
		}
		validBlob, err := msgpack.Marshal(valid)
		require.NoError(t, err)
		_, err = store.CreateNode(&storage.Node{
			ID:     procedureCatalogNodeID("nornic.loadedProc"),
			Labels: []string{procedureCatalogLabel},
			Properties: map[string]interface{}{
				"record": base64.StdEncoding.EncodeToString(validBlob),
			},
		})
		require.NoError(t, err)

		ClearUserProcedures()
		require.NoError(t, exec.loadPersistedProcedures(t.Context()))

		res, err := exec.Execute(ctx, "CALL nornic.loadedProc()", nil)
		require.NoError(t, err)
		require.Len(t, res.Rows, 1)
		require.Equal(t, int64(42), res.Rows[0][0])
	})

	t.Run("returns decode error for invalid record payload", func(t *testing.T) {
		base := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(base, "test")
		exec := NewStorageExecutor(store)

		_, err := store.CreateNode(&storage.Node{
			ID:     procedureCatalogNodeID("nornic.badb64"),
			Labels: []string{procedureCatalogLabel},
			Properties: map[string]interface{}{
				"record": "not-valid-base64",
			},
		})
		require.NoError(t, err)

		err = exec.loadPersistedProcedures(t.Context())
		require.Error(t, err)
		require.True(t, errors.Is(err, nerrors.ErrProcedureCatalogRecordDecodeFailed))
	})

	t.Run("returns invalid record error when compile fails", func(t *testing.T) {
		base := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(base, "test")
		exec := NewStorageExecutor(store)

		invalidMode := persistedProcedureRecord{
			Name:      "nornic.invalidMode",
			Mode:      "NOPE",
			Body:      "RETURN 1 AS value",
			Signature: buildProcedureSignature("nornic.invalidMode", nil),
		}
		invalidBlob, err := msgpack.Marshal(invalidMode)
		require.NoError(t, err)
		_, err = store.CreateNode(&storage.Node{
			ID:     procedureCatalogNodeID("nornic.invalidMode"),
			Labels: []string{procedureCatalogLabel},
			Properties: map[string]interface{}{
				"record": base64.StdEncoding.EncodeToString(invalidBlob),
			},
		})
		require.NoError(t, err)

		err = exec.loadPersistedProcedures(t.Context())
		require.Error(t, err)
		require.True(t, errors.Is(err, nerrors.ErrProcedureCatalogRecordInvalid))
	})
}

func TestDropProcedure_ErrorBranches(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)

	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.executeDropProcedure(ctx, "DROP PROCEDURE")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid DROP PROCEDURE syntax")

	_, err = exec.executeDropProcedure(ctx, "DROP PROCEDURE nornic.missing")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to drop procedure")

	_, err = exec.Execute(ctx, "BEGIN TRANSACTION", nil)
	require.NoError(t, err)
	_, err = exec.executeDropProcedure(ctx, "DROP PROCEDURE nornic.any")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not allowed inside an active transaction")
	_, _ = exec.Execute(ctx, "ROLLBACK", nil)

	t.Run("reload failure is surfaced with typed error", func(t *testing.T) {
		_, err := exec.Execute(ctx, "CREATE OR REPLACE PROCEDURE nornic.to_drop() MODE READ AS RETURN 1 AS value", nil)
		require.NoError(t, err)

		_, err = store.CreateNode(&storage.Node{
			ID:     procedureCatalogNodeID("nornic.bad_reload"),
			Labels: []string{procedureCatalogLabel},
			Properties: map[string]interface{}{
				"record": "not-valid-base64",
			},
		})
		require.NoError(t, err)

		_, err = exec.executeDropProcedure(ctx, "DROP PROCEDURE nornic.to_drop")
		require.Error(t, err)
		require.True(t, errors.Is(err, nerrors.ErrProcedureRegistryReloadFailed))
	})
}
