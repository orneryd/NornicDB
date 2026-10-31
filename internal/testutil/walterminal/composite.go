// Author: Codex, NOR-180 public composite terminal regression, 2026-11-01.
package walterminal

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// CompositeTerminal uses the real catalog, coordinator and native transaction.
// Only the constituent's independent logical WAL and post-commit action fail.
type CompositeTerminal struct {
	*storage.MemoryEngine
	WAL           *storage.WAL
	Executor      *cypher.StorageExecutor
	CloseOnCommit bool
	ActionError   error
}

func NewCompositeTerminal(t testing.TB) *CompositeTerminal {
	t.Helper()
	wal, err := storage.NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	fixtureowner.OwnChecked(t, wal)
	f := &CompositeTerminal{MemoryEngine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), WAL: wal}
	mgr, err := multidb.NewDatabaseManager(context.Background(), fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mgr.Close()) })
	require.NoError(t, mgr.CreateDatabase(context.Background(), multidb.DatabaseCreateRequest{Name: "one"}))
	require.NoError(t, mgr.CreateCompositeDatabase(context.Background(), "cmp", []multidb.ConstituentRef{{Alias: "local", DatabaseName: "one", Type: "local", AccessMode: "read_write"}}))
	store, err := mgr.GetStorage("cmp")
	require.NoError(t, err)
	f.Executor = cypher.NewStorageExecutor(store)
	f.Executor.SetDatabaseManager(compositeTerminalManager{manager: mgr, physical: storage.NewNamespacedEngine(f, "one")})
	return f
}
func (f *CompositeTerminal) GetWAL() *storage.WAL { return f.WAL }
func (f *CompositeTerminal) CommitTransaction(ctx context.Context, tx *storage.BadgerTransaction) error {
	err := tx.CommitContext(ctx)
	if err == nil && f.CloseOnCommit {
		return f.WAL.Close()
	}
	return err
}
func (f *CompositeTerminal) SetManualSuppression(kind storage.StoredEntityKind, id string, suppressed bool) error {
	if f.ActionError != nil {
		return f.ActionError
	}
	return f.MemoryEngine.SetManualSuppression(kind, id, suppressed)
}
func (f *CompositeTerminal) Count(t testing.TB) int64 {
	t.Helper()
	r, err := cypher.NewStorageExecutor(storage.NewNamespacedEngine(f.MemoryEngine, "one")).Execute(context.Background(), "MATCH (n:WalWire) RETURN count(n)", nil)
	require.NoError(t, err)
	return r.Rows[0][0].(int64)
}

type compositeTerminalManager struct {
	cypher.DatabaseManagerInterface
	manager  *multidb.DatabaseManager
	physical storage.Engine
}

func (m compositeTerminalManager) Exists(n string) bool { return m.manager.Exists(n) }
func (m compositeTerminalManager) IsCompositeDatabase(n string) bool {
	return m.manager.IsCompositeDatabase(n)
}
func (m compositeTerminalManager) ResolveDatabase(n string) (string, error) {
	return m.manager.ResolveDatabase(n)
}
func (m compositeTerminalManager) ListAliases(n string) map[string]string {
	return m.manager.ListAliases(n)
}
func (m compositeTerminalManager) GetDatabaseLimits(n string) (interface{}, error) {
	return m.manager.GetDatabaseLimits(n)
}
func (m compositeTerminalManager) GetStorageForUse(n, a string) (interface{}, error) {
	if n == "one" {
		return m.physical, nil
	}
	return m.manager.GetStorageWithAuth(n, a)
}
func (m compositeTerminalManager) GetCompositeConstituents(n string) ([]interface{}, error) {
	cs, err := m.manager.GetCompositeConstituents(n)
	out := make([]interface{}, len(cs))
	for i, c := range cs {
		out[i] = c
	}
	return out, err
}
func (m compositeTerminalManager) ListDatabases() []cypher.DatabaseInfoInterface {
	ds := m.manager.ListDatabases()
	out := make([]cypher.DatabaseInfoInterface, len(ds))
	for i, d := range ds {
		out[i] = compositeTerminalInfo{d}
	}
	return out
}

type compositeTerminalInfo struct{ info *multidb.DatabaseInfo }

func (d compositeTerminalInfo) Name() string         { return d.info.Name }
func (d compositeTerminalInfo) Type() string         { return d.info.Type }
func (d compositeTerminalInfo) Status() string       { return d.info.Status }
func (d compositeTerminalInfo) IsDefault() bool      { return d.info.IsDefault }
func (d compositeTerminalInfo) CreatedAt() time.Time { return d.info.CreatedAt }
