package cypher

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type useAuthDBInfo struct{}

func (useAuthDBInfo) Name() string         { return "cmp" }
func (useAuthDBInfo) Type() string         { return "composite" }
func (useAuthDBInfo) Status() string       { return "online" }
func (useAuthDBInfo) IsDefault() bool      { return false }
func (useAuthDBInfo) CreatedAt() time.Time { return time.Now() }

type useAuthDBManager struct {
	lastAuthToken string
	engine        storage.Engine
}

func (m *useAuthDBManager) CreateDatabase(name string) error { return nil }
func (m *useAuthDBManager) DropDatabase(name string) error   { return nil }
func (m *useAuthDBManager) ListDatabases() []DatabaseInfoInterface {
	return []DatabaseInfoInterface{useAuthDBInfo{}}
}
func (m *useAuthDBManager) Exists(name string) bool                      { return true }
func (m *useAuthDBManager) CreateAlias(alias, databaseName string) error { return nil }
func (m *useAuthDBManager) DropAlias(alias string) error                 { return nil }
func (m *useAuthDBManager) ListAliases(databaseName string) map[string]string {
	return map[string]string{}
}
func (m *useAuthDBManager) ResolveDatabase(nameOrAlias string) (string, error) {
	return nameOrAlias, nil
}
func (m *useAuthDBManager) SetDatabaseLimits(databaseName string, limits interface{}) error {
	return nil
}
func (m *useAuthDBManager) GetDatabaseLimits(databaseName string) (interface{}, error) {
	return nil, nil
}
func (m *useAuthDBManager) CreateCompositeDatabase(name string, constituents []interface{}) error {
	return nil
}
func (m *useAuthDBManager) DropCompositeDatabase(name string) error { return nil }
func (m *useAuthDBManager) AddConstituent(compositeName string, constituent interface{}) error {
	return nil
}
func (m *useAuthDBManager) RemoveConstituent(compositeName string, alias string) error { return nil }
func (m *useAuthDBManager) GetCompositeConstituents(compositeName string) ([]interface{}, error) {
	return []interface{}{}, nil
}
func (m *useAuthDBManager) ListCompositeDatabases() []DatabaseInfoInterface {
	return []DatabaseInfoInterface{useAuthDBInfo{}}
}
func (m *useAuthDBManager) IsCompositeDatabase(name string) bool { return name == "cmp" }
func (m *useAuthDBManager) GetStorageForUse(name string, authToken string) (interface{}, error) {
	m.lastAuthToken = authToken
	return m.engine, nil
}

func TestUseClause_ForwardsAuthTokenToDatabaseManager(t *testing.T) {
	base := storage.NewMemoryEngine()
	ns := storage.NewNamespacedEngine(base, "nornic")
	exec := NewStorageExecutor(ns)

	mgr := &useAuthDBManager{engine: storage.NewNamespacedEngine(base, "cmp")}
	exec.SetDatabaseManager(mgr)

	ctx := WithAuthToken(context.Background(), "Bearer forwarded-token")
	_, err := exec.Execute(ctx, "USE cmp RETURN 1 AS ok", nil)
	require.NoError(t, err)
	require.Equal(t, "Bearer forwarded-token", mgr.lastAuthToken)
}

func TestUseClause_AuthorizesAgainstTargetDatabase(t *testing.T) {
	base := newTestMemoryEngine(t)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"))
	target := storage.NewNamespacedEngine(base, "cmp")
	exec.SetDatabaseManager(&useAuthDBManager{engine: target})

	ctx := WithDatabasePermissionResolver(context.Background(), "nornic", func(database, permission string) bool {
		return permission == "read" || permission == "write" && database == "cmp"
	})
	_, err := exec.Execute(ctx, "USE cmp CREATE (:AllowedOnTarget)", nil)
	require.NoError(t, err)

	nodes, err := target.GetNodesByLabel("AllowedOnTarget")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
}

func TestUseClause_RejectsUnauthorizedTargetBeforeRouting(t *testing.T) {
	base := newTestMemoryEngine(t)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"))
	manager := &useAuthDBManager{engine: storage.NewNamespacedEngine(base, "cmp")}
	exec.SetDatabaseManager(manager)
	ctx := WithDatabasePermissionResolver(context.Background(), "nornic", func(database, permission string) bool {
		return database == "nornic" && permission == "read"
	})

	manager.lastAuthToken = ""
	_, err := exec.Execute(WithAuthToken(ctx, "Bearer should-not-route"), "USE cmp RETURN 1 AS value", nil)
	var denied *PermissionDeniedError
	require.True(t, errors.As(err, &denied))
	require.Equal(t, "read", denied.Permission)
	require.Empty(t, manager.lastAuthToken)

	// USE alone is a SyntaxError, reported before the target is looked up.
	_, err = exec.Execute(WithAuthToken(ctx, "Bearer should-not-route"), "USE cmp", nil)
	require.ErrorContains(t, err, "Query cannot conclude with USE GRAPH")
	require.Empty(t, manager.lastAuthToken)
}

func TestParseUseClauseDynamicGraphRefs(t *testing.T) {
	use, rem, hasUse, err := parseUseClause("USE graph.byName('translations.tr') RETURN 1 AS one", false)
	require.NoError(t, err)
	require.True(t, hasUse)
	require.True(t, use.IsDynamic())
	require.Equal(t, "graph.byName", use.Function)
	require.Equal(t, []string{"'translations.tr'"}, use.Args)
	require.Equal(t, `graph.byName("translations.tr")`, use.Text())
	require.Equal(t, "RETURN 1 AS one", rem)

	use, rem, hasUse, err = parseUseClause("USE graph . byElementId($id) MATCH (n) RETURN n", false)
	require.NoError(t, err)
	require.True(t, hasUse)
	require.Equal(t, "graph.byElementId($id)", use.Text())
	require.Equal(t, "MATCH (n) RETURN n", rem)

	_, _, hasUse, err = parseUseClause("USE graph.byName('unterminated RETURN 1", false)
	require.True(t, hasUse)
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
}

func TestResolveCompositeStorageAndConstituent(t *testing.T) {
	base := storage.NewMemoryEngine()
	ns := storage.NewNamespacedEngine(base, "nornic")
	exec := NewStorageExecutor(ns)

	innerComp := storage.NewCompositeEngine(
		map[string]storage.Engine{
			"tr":  storage.NewNamespacedEngine(base, "tr_db"),
			"txr": storage.NewNamespacedEngine(base, "txr_db"),
		},
		map[string]string{
			"tr":  "tr_db",
			"txr": "txr_db",
		},
		map[string]string{
			"tr":  "read_write",
			"txr": "read_write",
		},
	)

	mgr := &useAuthDBManager{engine: innerComp}
	exec.SetDatabaseManager(mgr)

	scoped, db, err := exec.resolveCompositeStorage("cmp", "Bearer tok")
	require.NoError(t, err)
	require.Equal(t, "cmp", db)
	require.NotNil(t, scoped)
	require.Equal(t, "Bearer tok", mgr.lastAuthToken)

	scoped, db, err = exec.resolveCompositeConstituent("cmp.tr", "cmp", "tr", "Bearer tok2")
	require.NoError(t, err)
	require.Equal(t, "cmp.tr", db)
	require.NotNil(t, scoped)
	require.Equal(t, "Bearer tok2", mgr.lastAuthToken)

	_, _, err = exec.resolveCompositeConstituent("cmp.missing", "cmp", "missing", "")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "not found")
}

func TestScopedExecutorForUse_Branches(t *testing.T) {
	base := storage.NewMemoryEngine()
	ns := storage.NewNamespacedEngine(base, "nornic")
	exec := NewStorageExecutor(ns)

	// Switching to same namespace should return the same executor instance.
	scoped, db, err := exec.scopedExecutorForUse("nornic", "")
	require.NoError(t, err)
	require.Equal(t, "nornic", db)
	require.Same(t, exec, scoped)

	// Switching to a different namespace clones executor with a new namespaced store.
	scoped, db, err = exec.scopedExecutorForUse("tenant_a", "")
	require.NoError(t, err)
	require.Equal(t, "tenant_a", db)
	require.NotSame(t, exec, scoped)

	// Non-namespaced store cannot USE standard db without db manager resolution path.
	plain := NewStorageExecutor(base)
	_, _, err = plain.scopedExecutorForUse("tenant_a", "")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "not supported")
}
