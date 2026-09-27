package cypher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wrappedDatabaseEngine is a database's engine behind another layer, as the
// server's executors see theirs: not a *storage.NamespacedEngine itself.
type wrappedDatabaseEngine struct {
	storage.Engine
	namespace string
}

func (w wrappedDatabaseEngine) Namespace() string { return w.namespace }

type standardDBInfo struct{ name string }

func (i standardDBInfo) Name() string       { return i.name }
func (standardDBInfo) Type() string         { return "standard" }
func (standardDBInfo) Status() string       { return "online" }
func (standardDBInfo) IsDefault() bool      { return false }
func (standardDBInfo) CreatedAt() time.Time { return time.Time{} }

// standardDBManager serves standard databases by name, with aliases.
type standardDBManager struct {
	useAuthDBManager
	engines map[string]storage.Engine
	aliases map[string]string
}

func (m *standardDBManager) ResolveDatabase(nameOrAlias string) (string, error) {
	if target, ok := m.aliases[nameOrAlias]; ok {
		return target, nil
	}
	if _, ok := m.engines[nameOrAlias]; ok {
		return nameOrAlias, nil
	}
	return "", errors.New("database not found")
}
func (m *standardDBManager) IsCompositeDatabase(string) bool { return false }
func (m *standardDBManager) GetStorageForUse(name string, _ string) (interface{}, error) {
	engine, ok := m.engines[name]
	if !ok {
		return nil, errors.New("database not found")
	}
	return engine, nil
}

// A statement's target database (USE clause or :USE) is where it runs, and a
// transaction cannot span databases (#738).
func TestStatementTargetDatabase(t *testing.T) {
	base := newTestMemoryEngine(t)
	home := wrappedDatabaseEngine{Engine: storage.NewNamespacedEngine(base, "nornic"), namespace: "nornic"}
	other := wrappedDatabaseEngine{Engine: storage.NewNamespacedEngine(base, "other"), namespace: "other"}
	exec := NewStorageExecutor(home)
	exec.SetDatabaseManager(&standardDBManager{
		engines: map[string]storage.Engine{"nornic": home, "other": other},
		aliases: map[string]string{"home_alias": "nornic"},
	})
	ctx := context.Background()
	count := func(engine storage.Engine, label string) int {
		nodes, err := engine.GetNodesByLabel(label)
		require.NoError(t, err)
		return len(nodes)
	}

	// The executor's own database, by name or alias.
	_, err := exec.Execute(ctx, "CREATE (:Home)", nil)
	require.NoError(t, err)
	for _, query := range []string{"USE nornic MATCH (n:Home) RETURN count(n) AS c", "USE home_alias MATCH (n:Home) RETURN count(n) AS c", ":USE nornic\nMATCH (n:Home) RETURN count(n) AS c"} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, int64(1), result.Rows[0][0], query)
	}

	// Another database: the statement runs there.
	_, err = exec.Execute(ctx, "USE other CREATE (:ByUse)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, ":USE other\nCREATE (:ByShellUse)", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, count(other, "ByUse"))
	assert.Equal(t, 1, count(other, "ByShellUse"))
	assert.Equal(t, 0, count(home, "ByUse"))
	assert.Equal(t, 0, count(home, "ByShellUse"))

	_, err = exec.Execute(ctx, "USE nosuch RETURN 1", nil)
	require.Error(t, err)
	requireStatusCode(t, err, "Neo.ClientError.Database.DatabaseNotFound")
	assert.Equal(t, "Graph not found: nosuch", err.Error())

	// In an explicit transaction, only the transaction's database. (The
	// transaction runs on a plain namespaced engine, which supports BEGIN.)
	exec = NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"))
	exec.SetDatabaseManager(&standardDBManager{engines: map[string]storage.Engine{"nornic": home, "other": other}})
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:InTx)", nil)
	require.NoError(t, err)
	for query, message := range map[string]string{
		"USE other CREATE (:InTxOther)":            "Writing to more than one database per transaction is not allowed. Attempted write to other, currently writing to nornic",
		":USE other\nCREATE (:InTxOther)":          "Writing to more than one database per transaction is not allowed. Attempted write to other, currently writing to nornic",
		"USE other MATCH (n) RETURN count(n) AS c": "Accessing more than one database per transaction is not allowed. Attempted access to other, currently using nornic",
	} {
		_, err = exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		requireStatusCode(t, err, "Neo.ClientError.Statement.AccessMode")
		assert.Contains(t, err.Error(), message, query)
	}
	_, err = exec.Execute(ctx, "USE nornic CREATE (:InTx)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, count(home, "InTx"))
	assert.Equal(t, 0, count(other, "InTxOther"))
}
