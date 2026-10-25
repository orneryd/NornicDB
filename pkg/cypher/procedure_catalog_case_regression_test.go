package cypher

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestProcedureCatalogCaseIdentityLifecycle(t *testing.T) {
	t.Cleanup(ClearUserProcedures)
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	run := func(query string) *ExecuteResult {
		t.Helper()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result
	}
	run("CALL apoc.custom.installProcedure('Report() :: (n :: INTEGER)', 'RETURN 1 AS n')")
	run("CALL apoc.custom.installProcedure('report() :: (n :: INTEGER)', 'RETURN 2 AS n')")
	require.Equal(t, [][]interface{}{{int64(1)}}, run("CALL custom.Report() YIELD n RETURN n").Rows)
	require.Equal(t, [][]interface{}{{int64(2)}}, run("CALL custom.report() YIELD n RETURN n").Rows)
	run("CALL apoc.custom.installProcedure('marker() :: (n :: INTEGER)', 'RETURN 3 AS n')")
	run("CALL apoc.custom.dropProcedure('marker')")
	require.Equal(t, [][]interface{}{{int64(1)}}, run("CALL custom.Report() YIELD n RETURN n").Rows)
	require.Equal(t, [][]interface{}{{int64(2)}}, run("CALL custom.report() YIELD n RETURN n").Rows)
	run("CALL apoc.custom.dropProcedure('report')")
	require.Equal(t, [][]interface{}{{int64(1)}}, run("CALL custom.Report() YIELD n RETURN n").Rows)
	_, err := exec.Execute(ctx, "CALL custom.report()", nil)
	require.Error(t, err)
	ClearUserProcedures()
	exec = NewStorageExecutor(store)
	require.Equal(t, [][]interface{}{{int64(1)}}, run("CALL custom.Report() YIELD n RETURN n").Rows)
}

func TestProcedureNameComponentsPreserveExactCase(t *testing.T) {
	for _, query := range []string{"CALL db . awaitIndexes ()", "CALL `db`.`awaitIndexes`()", "cAlL `db` . awaitIndexes ()"} {
		t.Run(query, func(t *testing.T) {
			exec, ctx := newUnitExecutor(t)
			_, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
		})
	}
}

func TestProcedureCatalogLegacyCaseMigration(t *testing.T) {
	t.Cleanup(ClearUserProcedures)
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	record := persistedProcedureRecord{Name: "custom.Legacy", Mode: "READ", Body: "RETURN 7 AS n", Signature: "custom.Legacy() :: (n :: INTEGER)"}
	blob, err := msgpack.Marshal(record)
	require.NoError(t, err)
	legacy := storage.NodeID(procedureCatalogPrefix + "custom.legacy")
	_, err = store.CreateNode(&storage.Node{ID: legacy, Labels: []string{procedureCatalogLabel}, Properties: map[string]interface{}{"name": record.Name, "record": base64.StdEncoding.EncodeToString(blob)}})
	require.NoError(t, err)
	exec := NewStorageExecutor(store)
	result, err := exec.Execute(context.Background(), "CALL custom.Legacy() YIELD n RETURN n", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
	_, err = store.GetNode(legacy)
	require.ErrorIs(t, err, storage.ErrNotFound)
	_, err = store.GetNode(procedureCatalogNodeID(record.Name))
	require.NoError(t, err)
	// Dropping an absent spelling must never delete the differently named procedure.
	_, _ = exec.Execute(context.Background(), "CALL apoc.custom.dropProcedure('legacy')", nil)
	_, err = exec.Execute(context.Background(), "CALL custom.Legacy()", nil)
	require.NoError(t, err)
}

type procedureIdentityFaultEngine struct {
	storage.Engine
	readErr, createErr, deleteErr error
}

func (e *procedureIdentityFaultEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	if e.readErr != nil {
		return nil, e.readErr
	}
	return e.Engine.GetNode(id)
}
func (e *procedureIdentityFaultEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
	if e.createErr != nil {
		return "", e.createErr
	}
	return e.Engine.CreateNode(node)
}
func (e *procedureIdentityFaultEngine) DeleteNode(id storage.NodeID) error {
	if e.deleteErr != nil {
		return e.deleteErr
	}
	return e.Engine.DeleteNode(id)
}

func TestProcedureCatalogMigrationFailuresPreserveRecord(t *testing.T) {
	for _, stage := range []string{"read", "create", "delete"} {
		t.Run(stage, func(t *testing.T) {
			base := newTestMemoryEngine(t)
			record := persistedProcedureRecord{Name: "custom.Report", Mode: "READ", Body: "RETURN 1 AS v", Signature: "custom.Report() :: (v :: INTEGER)"}
			blob, err := msgpack.Marshal(record)
			require.NoError(t, err)
			legacy := &storage.Node{ID: storage.NodeID(procedureCatalogPrefix + "custom.report"), Labels: []string{procedureCatalogLabel}, Properties: map[string]interface{}{"name": record.Name, "record": base64.StdEncoding.EncodeToString(blob)}}
			_, err = base.CreateNode(legacy)
			require.NoError(t, err)
			fault := errors.New("catalog " + stage + " unavailable")
			engine := &procedureIdentityFaultEngine{Engine: base}
			switch stage {
			case "read":
				engine.readErr = fault
			case "create":
				engine.createErr = fault
			case "delete":
				engine.deleteErr = fault
			}
			exec := NewStorageExecutor(engine)
			_, _, err = exec.resolveProcedureCatalogIdentity(t.Context(), engine, record.Name, legacy)
			require.Error(t, err)
			_, err = base.GetNode(legacy.ID)
			require.NoError(t, err, "failed migration must preserve original record")
			engine.readErr = nil
			engine.createErr = nil
			engine.deleteErr = nil
			_, _, err = exec.resolveProcedureCatalogIdentity(t.Context(), engine, record.Name, legacy)
			require.NoError(t, err)
			_, err = base.GetNode(procedureCatalogNodeID(record.Name))
			require.NoError(t, err)
			_, err = base.GetNode(legacy.ID)
			require.ErrorIs(t, err, storage.ErrNotFound)
		})
	}
}

func TestProcedureNameReaderCanonicalComponents(t *testing.T) {
	for _, tc := range []struct {
		input, want string
		found       bool
	}{
		{"db.labels()", "db.labels", true},
		{"DB.Cdc.Query()", "DB.Cdc.Query", true},
		{" db.cdc.query  ()", "db.cdc.query", true},
		{"db.cdc . query()", "db.cdc.query", true},
		{"db.`cdc`.query()", "db.cdc.query", true},
		{"`db` . cdc . `query`()", "db.cdc.query", true},
		{" `db.cdc.query`()", "db.cdc.query", true},
		{"", "", false},
		{" ", "", false},
		{"{ RETURN 1 }", "", false},
		{"(i) { RETURN 1 }", "", false},
		{"db.`unterminated", "", false},
	} {
		t.Run(tc.input, func(t *testing.T) {
			name, found := procedureNameAt(tc.input, 0)
			require.Equal(t, tc.found, found)
			require.Equal(t, tc.want, name)
		})
	}
}
