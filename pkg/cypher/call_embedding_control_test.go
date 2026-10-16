package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type fakeEmbeddingController struct {
	status EmbeddingStatus
	err    error
	calls  []bool
}

func (f *fakeEmbeddingController) SetEmbeddingPaused(_ context.Context, paused bool) (EmbeddingStatus, error) {
	f.calls = append(f.calls, paused)
	if f.err != nil {
		return EmbeddingStatus{}, f.err
	}
	f.status.Paused = paused
	return f.status, nil
}

func (f *fakeEmbeddingController) EmbeddingStatus() EmbeddingStatus { return f.status }

// db.embedding.pause / resume / status (#1021) return the state in effect,
// one (paused, pending) row, are idempotent, need administrator rights and
// reach executors that share the base executor's embedder.
func TestEmbeddingControlProcedures(t *testing.T) {
	ctx := context.Background()
	controller := &fakeEmbeddingController{status: EmbeddingStatus{Pending: 7}}
	base := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
	base.SetEmbeddingController(controller)
	scoped := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "other"))
	scoped.ShareEmbedderFrom(base)

	row := func(executor *StorageExecutor, query string) []interface{} {
		t.Helper()
		result, err := executor.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"paused", "pending"}, result.Columns)
		require.Len(t, result.Rows, 1)
		return result.Rows[0]
	}
	require.Equal(t, []interface{}{false, int64(7)}, row(scoped, "CALL db.embedding.status()"))
	require.Equal(t, []interface{}{true, int64(7)}, row(scoped, "CALL db.embedding.pause()"))
	require.Equal(t, []interface{}{true, int64(7)}, row(base, "CALL db.embedding.pause() YIELD paused, pending RETURN paused, pending"))
	require.Equal(t, []interface{}{true, int64(7)}, row(base, "CALL db.embedding.status() YIELD paused, pending RETURN paused, pending"))
	require.Equal(t, []interface{}{false, int64(7)}, row(base, "CALL db.embedding.resume()"))
	require.Equal(t, []bool{true, true, false}, controller.calls)

	for _, name := range []string{"db.embedding.pause", "db.embedding.resume", "db.embedding.status"} {
		procedure, ok := globalProcedureRegistry.Get(name)
		require.True(t, ok, name)
		require.True(t, procedure.Spec.Admin, name)
		require.Equal(t, ProcedureModeDBMS, procedure.Spec.Mode, name)
		require.True(t, procedure.Spec.WorksOnSystem, name)
		require.NotEmpty(t, procedure.Spec.DescriptionMessage.ID, name)
		require.Equal(t, procedure.Spec.Description, procedure.Spec.DescriptionMessage.Fallback, name)
		require.NotEmpty(t, procedure.Spec.Description, name)
		require.True(t, QueryPermissionRequirements("CALL "+name+"()").Admin, name)
	}

	viewer := WithPermissionChecker(ctx, func(permission string) bool { return permission == "read" || permission == "write" })
	for _, query := range []string{"CALL db.embedding.pause()", "CALL db.embedding.resume()", "CALL db.embedding.status()"} {
		_, err := base.Execute(viewer, query, nil)
		var denied *PermissionDeniedError
		require.True(t, errors.As(err, &denied), "%s: %v", query, err)
		require.Equal(t, "admin", denied.Permission)
	}
	require.Equal(t, []bool{true, true, false}, controller.calls, "a refused call changes nothing")

	controller.err = errors.New("system database unavailable")
	_, err := base.Execute(ctx, "CALL db.embedding.pause()", nil)
	require.ErrorContains(t, err, "system database unavailable")

	_, err = NewStorageExecutor(newTestMemoryEngine(t)).Execute(ctx, "CALL db.embedding.status()", nil)
	require.ErrorContains(t, err, "auto-embed not enabled", "an executor without a database has no embedding to control")
}
