package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testDatabaseManagerAdapter wraps multidb.DatabaseManager to implement DatabaseManagerInterface
type testDatabaseManagerAdapter struct {
	manager *multidb.DatabaseManager
}

func (a *testDatabaseManagerAdapter) CreateDatabase(ctx context.Context, request multidb.DatabaseCreateRequest) error {
	return a.manager.CreateDatabase(ctx, request)
}

func (a *testDatabaseManagerAdapter) DropDatabase(ctx context.Context, name string) error {
	return a.manager.DropDatabase(ctx, name)
}

func (a *testDatabaseManagerAdapter) ListDatabases() []DatabaseInfoInterface {
	dbs := a.manager.ListDatabases()
	result := make([]DatabaseInfoInterface, len(dbs))
	for i, db := range dbs {
		result[i] = &testDatabaseInfoAdapter{info: db}
	}
	return result
}

func (a *testDatabaseManagerAdapter) Exists(name string) bool {
	return a.manager.Exists(name)
}

func (a *testDatabaseManagerAdapter) CreateAlias(ctx context.Context, alias, databaseName string) error {
	return a.manager.CreateAlias(ctx, alias, databaseName)
}

func (a *testDatabaseManagerAdapter) DropAlias(ctx context.Context, alias string) error {
	return a.manager.DropAlias(ctx, alias)
}

func (a *testDatabaseManagerAdapter) ListAliases(databaseName string) map[string]string {
	return a.manager.ListAliases(databaseName)
}

func (a *testDatabaseManagerAdapter) ResolveDatabase(nameOrAlias string) (string, error) {
	return a.manager.ResolveDatabase(nameOrAlias)
}

func (a *testDatabaseManagerAdapter) SetDatabaseLimits(ctx context.Context, databaseName string, limits interface{}) error {
	if limitsPtr, ok := limits.(*multidb.Limits); ok {
		return a.manager.SetDatabaseLimits(ctx, databaseName, limitsPtr)
	}
	return fmt.Errorf("invalid limits type")
}

func (a *testDatabaseManagerAdapter) GetDatabaseLimits(databaseName string) (interface{}, error) {
	return a.manager.GetDatabaseLimits(databaseName)
}

func (a *testDatabaseManagerAdapter) CreateCompositeDatabase(ctx context.Context, name string, constituents []interface{}) error {
	refs := make([]multidb.ConstituentRef, len(constituents))
	for i, c := range constituents {
		if m, ok := c.(map[string]interface{}); ok {
			refs[i] = multidb.ConstituentRef{
				Alias:        getStringFromMap(m, "alias"),
				DatabaseName: getStringFromMap(m, "database_name"),
				Type:         getStringFromMap(m, "type"),
				AccessMode:   getStringFromMap(m, "access_mode"),
				URI:          getStringFromMap(m, "uri"),
				SecretRef:    getStringFromMap(m, "secret_ref"),
				AuthMode:     getStringFromMap(m, "auth_mode"),
				User:         getStringFromMap(m, "user"),
				Password:     getStringFromMap(m, "password"),
			}
		} else {
			return fmt.Errorf("invalid constituent type at index %d", i)
		}
	}
	return a.manager.CreateCompositeDatabase(ctx, name, refs)
}

func (a *testDatabaseManagerAdapter) DropCompositeDatabase(ctx context.Context, name string) error {
	return a.manager.DropCompositeDatabase(ctx, name)
}

func (a *testDatabaseManagerAdapter) AddConstituent(ctx context.Context, compositeName string, constituent interface{}) error {
	var ref multidb.ConstituentRef
	if m, ok := constituent.(map[string]interface{}); ok {
		ref = multidb.ConstituentRef{
			Alias:        getStringFromMap(m, "alias"),
			DatabaseName: getStringFromMap(m, "database_name"),
			Type:         getStringFromMap(m, "type"),
			AccessMode:   getStringFromMap(m, "access_mode"),
			URI:          getStringFromMap(m, "uri"),
			SecretRef:    getStringFromMap(m, "secret_ref"),
			AuthMode:     getStringFromMap(m, "auth_mode"),
			User:         getStringFromMap(m, "user"),
			Password:     getStringFromMap(m, "password"),
		}
	} else {
		return fmt.Errorf("invalid constituent type")
	}
	return a.manager.AddConstituent(ctx, compositeName, ref)
}

func (a *testDatabaseManagerAdapter) RemoveConstituent(ctx context.Context, compositeName string, alias string) error {
	return a.manager.RemoveConstituent(ctx, compositeName, alias)
}

func (a *testDatabaseManagerAdapter) RemoteConstituentForDisplay(ref multidb.ConstituentRef) (multidb.ConstituentRef, error) {
	return a.manager.RemoteConstituentForDisplay(ref)
}

func (a *testDatabaseManagerAdapter) GetCompositeConstituents(compositeName string) ([]interface{}, error) {
	constituents, err := a.manager.GetCompositeConstituents(compositeName)
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, len(constituents))
	for i, c := range constituents {
		result[i] = map[string]interface{}{
			"alias":         c.Alias,
			"connection_v1": c.ConnectionV1,
			"database_name": c.DatabaseName,
			"type":          c.Type,
			"access_mode":   c.AccessMode,
			"uri":           c.URI,
			"secret_ref":    c.SecretRef,
			"auth_mode":     c.AuthMode,
			"user":          c.User,
		}
	}
	return result, nil
}

func (a *testDatabaseManagerAdapter) ListCompositeDatabases() []DatabaseInfoInterface {
	dbs := a.manager.ListCompositeDatabases()
	result := make([]DatabaseInfoInterface, len(dbs))
	for i, db := range dbs {
		result[i] = &testDatabaseInfoAdapter{info: db}
	}
	return result
}

func (a *testDatabaseManagerAdapter) IsCompositeDatabase(name string) bool {
	return a.manager.IsCompositeDatabase(name)
}

func (a *testDatabaseManagerAdapter) GetStorageForUse(name string, authToken string) (interface{}, error) {
	return a.manager.GetStorageWithAuth(name, authToken)
}

type testDatabaseInfoAdapter struct {
	info *multidb.DatabaseInfo
}

func (a *testDatabaseInfoAdapter) Name() string {
	return a.info.Name
}

func (a *testDatabaseInfoAdapter) Type() string {
	return a.info.Type
}

func (a *testDatabaseInfoAdapter) Status() string {
	return a.info.Status
}

func (a *testDatabaseInfoAdapter) IsDefault() bool {
	return a.info.IsDefault
}

func (a *testDatabaseInfoAdapter) CreatedAt() time.Time {
	return a.info.CreatedAt
}

func getStringFromMap(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

type alterAliasTestOperation uint8

const (
	alterAliasTestAdd alterAliasTestOperation = iota
	alterAliasTestDrop
)

func TestExecuteAlterCompositeDatabase_AliasOperations(t *testing.T) {
	cases := []struct {
		name      string
		operation alterAliasTestOperation
		databases []string
		query     string
		wantCount int
	}{
		{"add", alterAliasTestAdd, []string{"db1", "db2", "db3"}, `ALTER COMPOSITE DATABASE composite1
		ADD ALIAS db3 FOR DATABASE db3`, 3},
		{"drop", alterAliasTestDrop, []string{"db1", "db2"}, `ALTER COMPOSITE DATABASE composite1
		DROP ALIAS db2`, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseStore := newTestMemoryEngine(t)

			store := storage.NewNamespacedEngine(baseStore, "test")
			inner := newTestMemoryEngine(t)
			manager, err := multidb.NewDatabaseManager(t.Context(), inner, nil)
			require.NoError(t, err)
			fixtureowner.Own(t, manager)
			adapter := &testDatabaseManagerAdapter{manager: manager}
			exec := NewStorageExecutor(store)
			exec.SetDatabaseManager(adapter)

			ctx := context.Background()

			for _, name := range tc.databases {
				var err error
				if tc.operation == alterAliasTestAdd {
					err = adapter.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: name})
				} else {
					err = manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: name})
				}
				require.NoError(t, err)
			}
			// Create composite database with 2 constituents
			constituents := []interface{}{
				map[string]interface{}{
					"alias":         "db1",
					"database_name": "db1",
					"type":          "local",
					"access_mode":   "read_write",
				},
				map[string]interface{}{
					"alias":         "db2",
					"database_name": "db2",
					"type":          "local",
					"access_mode":   "read_write",
				},
			}
			err = adapter.CreateCompositeDatabase(t.Context(), "composite1", constituents)
			require.NoError(t, err)

			result, err := exec.Execute(ctx, tc.query, nil)
			require.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, 1, len(result.Rows))

			// Verify the operation-specific constituent result
			constituentsList, err := adapter.GetCompositeConstituents("composite1")
			require.NoError(t, err)
			assert.Equal(t, tc.wantCount, len(constituentsList))
			if tc.operation == alterAliasTestDrop {
				assert.Equal(t, "db1", constituentsList[0].(map[string]interface{})["alias"])
			}

		})
	}
}

func TestExecuteAlterCompositeDatabase_InvalidSyntax(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	inner := newTestMemoryEngine(t)
	manager, _ := multidb.NewDatabaseManager(t.Context(), inner, nil)
	adapter := &testDatabaseManagerAdapter{manager: manager}
	exec := NewStorageExecutor(store)
	exec.SetDatabaseManager(adapter)

	ctx := context.Background()

	// Test invalid syntax
	query := `ALTER COMPOSITE DATABASE`
	_, err := exec.Execute(ctx, query, nil)
	assert.Error(t, err)
}

type weirdConstituentAdapter struct {
	*testDatabaseManagerAdapter
}

func (w *weirdConstituentAdapter) GetCompositeConstituents(compositeName string) ([]interface{}, error) {
	return []interface{}{42}, nil
}

func TestExecuteCreateDropAndShowCompositeDatabase_DirectHandlers(t *testing.T) {
	ctx := context.Background()

	t.Run("create composite direct branches", func(t *testing.T) {
		exec := &StorageExecutor{}
		_, err := exec.executeCompositeCommand(ctx, "CREATE COMPOSITE DATABASE c", compositeCreateCommand)
		require.Error(t, err)
		require.Contains(t, err.Error(), "database manager not available")

		baseStore := newTestMemoryEngine(t)
		inner := newTestMemoryEngine(t)
		proof := newConstituentCredentialProof(t)
		cfg := multidb.DefaultConfig()
		cfg.RemoteEngineFactory = proof.factory
		cfg.RemoteCredentialEncryptionKey = "test-key-for-composite-commands"
		manager, err := multidb.NewDatabaseManager(t.Context(), inner, cfg)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, manager.Close()) })
		adapter := &testDatabaseManagerAdapter{manager: manager}
		exec = NewStorageExecutor(storage.NewNamespacedEngine(baseStore, "test"))
		exec.SetDatabaseManager(adapter)

		require.NoError(t, adapter.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "db1"}))
		require.NoError(t, adapter.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "db2"}))

		_, err = exec.executeCompositeCommand(ctx, "CREATE COMPOSITE DATABASE", compositeCreateCommand)
		require.Error(t, err)

		_, err = exec.executeCompositeCommand(ctx, "CREATE COMPOSITE DATABASE c1 ALIAS a1 db1", compositeCreateCommand)
		require.Error(t, err)
		require.Contains(t, err.Error(), "FOR DATABASE expected")

		_, err = exec.executeCompositeCommand(ctx, "CREATE COMPOSITE DATABASE c1", compositeCreateCommand)
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one constituent")

		res, err := exec.executeCompositeCommand(ctx, "CREATE COMPOSITE DATABASE c1 ALIAS a1 FOR DATABASE db1 ALIAS a2 FOR DATABASE db2", compositeCreateCommand)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, [][]interface{}{{"c1"}}, res.Rows)

		res, err = exec.executeCompositeCommand(ctx, "CREATE COMPOSITE DATABASE c_remote ALIAS tr FOR DATABASE nornic_tr AT 'https://shard-a.example/nornic-db' SECRET REF 'spn-a' TYPE remote ACCESS read ALIAS txt FOR DATABASE nornic_txt AT 'https://shard-b.example/nornic-db' SECRET REF 'spn-b' TYPE remote ACCESS read_write", compositeCreateCommand)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"c_remote"}}, res.Rows)
		createdRefs, err := adapter.GetCompositeConstituents("c_remote")
		require.NoError(t, err)
		require.Len(t, createdRefs, 2)
		first := createdRefs[0].(map[string]interface{})
		require.Equal(t, "remote", first["type"])
		require.Equal(t, "https://shard-a.example/nornic-db", first["uri"])
		require.Equal(t, "spn-a", first["secret_ref"])
		require.Equal(t, "read", first["access_mode"])
		require.Equal(t, "oidc_forwarding", first["auth_mode"])

		res, err = exec.executeCompositeCommand(ctx, "CREATE COMPOSITE DATABASE c_remote_basic ALIAS tr FOR DATABASE nornic_tr AT 'https://shard-a.example/nornic-db' USER 'svc-user' PASSWORD 'svc-pass' TYPE remote ACCESS read", compositeCreateCommand)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"c_remote_basic"}}, res.Rows)
		createdRefs, err = adapter.GetCompositeConstituents("c_remote_basic")
		require.NoError(t, err)
		require.Len(t, createdRefs, 1)
		first = createdRefs[0].(map[string]interface{})
		proof.assertConnection(manager, "c_remote_basic", multidb.ConstituentRef{Alias: "tr", DatabaseName: "nornic_tr", Type: "remote", AccessMode: "read", URI: "https://shard-a.example/nornic-db", AuthMode: "user_password", User: "svc-user", Password: "svc-pass"})

		// Flexible whitespace parsing branch.
		res, err = exec.executeCompositeCommand(ctx, "CREATE\tCOMPOSITE\tDATABASE\tc_ws ALIAS a1 FOR DATABASE db1", compositeCreateCommand)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"c_ws"}}, res.Rows)

		// Invalid alias/database extraction branches.
		_, err = exec.executeCompositeCommand(ctx, "CREATE COMPOSITE DATABASE c_bad ALIAS   FOR DATABASE db1", compositeCreateCommand)
		require.Error(t, err)
		require.Contains(t, err.Error(), "FOR DATABASE expected")

		_, err = exec.executeCompositeCommand(ctx, "CREATE COMPOSITE DATABASE c_bad2 ALIAS a1 FOR DATABASE   ", compositeCreateCommand)
		require.Error(t, err)
		require.Contains(t, err.Error(), "database name cannot be empty")

		_, err = exec.executeCompositeCommand(ctx, "CREATE COMPOSITE DATABASE c_bad3 ALIAS a1 FOR DATABASE db1 AT 'https://remote.example' USER 'svc-only'", compositeCreateCommand)
		require.Error(t, err)
		require.Contains(t, err.Error(), "USER and PASSWORD must both be provided")

		_, err = exec.executeCompositeCommand(ctx, "CREATE COMPOSITE DATABASE c_bad4 ALIAS a1 FOR DATABASE db1 AT 'https://remote.example' OIDC CREDENTIAL FORWARDING USER 'svc' PASSWORD 'pass'", compositeCreateCommand)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot combine OIDC CREDENTIAL FORWARDING with USER/PASSWORD")

		_, err = exec.executeCompositeCommand(ctx, "CREATE COMPOSITE DATABASE c_bad5 ALIAS a1 FOR DATABASE db1 USER 'svc' PASSWORD 'pass'", compositeCreateCommand)
		require.Error(t, err)
		require.Contains(t, err.Error(), "require a remote constituent")

		// IF NOT EXISTS: existing composite — should succeed silently.
		res, err = exec.executeCompositeCommand(ctx, "CREATE COMPOSITE DATABASE c1 IF NOT EXISTS ALIAS a1 FOR DATABASE db1", compositeCreateCommand)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"c1"}}, res.Rows)

		// IF NOT EXISTS: existing standard database — should succeed silently.
		res, err = exec.executeCompositeCommand(ctx, "CREATE COMPOSITE DATABASE db1 IF NOT EXISTS ALIAS a1 FOR DATABASE db2", compositeCreateCommand)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"db1"}}, res.Rows)

		// IF NOT EXISTS: new database — should create normally.
		res, err = exec.executeCompositeCommand(ctx, "CREATE COMPOSITE DATABASE c_ifne IF NOT EXISTS ALIAS a1 FOR DATABASE db1", compositeCreateCommand)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"c_ifne"}}, res.Rows)
		require.True(t, adapter.IsCompositeDatabase("c_ifne"))

		// Without IF NOT EXISTS: duplicate — should fail.
		_, err = exec.executeCompositeCommand(ctx, "CREATE COMPOSITE DATABASE c_ifne ALIAS a1 FOR DATABASE db1", compositeCreateCommand)
		require.Error(t, err)
	})

	t.Run("drop composite direct branches", func(t *testing.T) {
		exec := &StorageExecutor{}
		_, err := exec.executeDatabaseDrop(ctx, "DROP COMPOSITE DATABASE c", dropCompositeDatabase)
		require.Error(t, err)
		require.Contains(t, err.Error(), "database manager not available")

		baseStore := newTestMemoryEngine(t)
		inner := newTestMemoryEngine(t)
		manager, _ := multidb.NewDatabaseManager(t.Context(), inner, nil)
		adapter := &testDatabaseManagerAdapter{manager: manager}
		exec = NewStorageExecutor(storage.NewNamespacedEngine(baseStore, "test"))
		exec.SetDatabaseManager(adapter)

		require.NoError(t, adapter.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "db1"}))
		constituents := []interface{}{
			map[string]interface{}{"alias": "a1", "database_name": "db1", "type": "local", "access_mode": "read_write"},
		}
		require.NoError(t, adapter.CreateCompositeDatabase(t.Context(), "c_drop", constituents))

		_, err = exec.executeDatabaseDrop(ctx, "DROP COMPOSITE DATABASE", dropCompositeDatabase)
		require.Error(t, err)
		require.Contains(t, err.Error(), "database name expected")

		_, err = exec.executeDatabaseDrop(ctx, "DROP DATABASE c_drop", dropCompositeDatabase)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid DROP COMPOSITE DATABASE syntax")

		res, err := exec.executeDatabaseDrop(ctx, "DROP COMPOSITE DATABASE c_drop", dropCompositeDatabase)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"c_drop"}}, res.Rows)

		require.NoError(t, adapter.CreateCompositeDatabase(t.Context(), "c_flex", constituents))
		res, err = exec.executeDatabaseDrop(ctx, "DROP\tCOMPOSITE\tDATABASE\tc_flex", dropCompositeDatabase)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"c_flex"}}, res.Rows)
	})

	t.Run("show composite and constituents direct branches", func(t *testing.T) {
		exec := &StorageExecutor{}
		_, err := exec.executeShowCompositeDatabases(ctx, "SHOW COMPOSITE DATABASES")
		require.Error(t, err)
		_, err = exec.executeShowConstituents(ctx, "SHOW CONSTITUENTS FOR COMPOSITE DATABASE x")
		require.Error(t, err)

		baseStore := newTestMemoryEngine(t)
		inner := newTestMemoryEngine(t)
		manager, _ := multidb.NewDatabaseManager(t.Context(), inner, nil)
		adapter := &testDatabaseManagerAdapter{manager: manager}
		exec = NewStorageExecutor(storage.NewNamespacedEngine(baseStore, "test"))
		exec.SetDatabaseManager(adapter)

		require.NoError(t, adapter.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "db1"}))
		require.NoError(t, adapter.CreateCompositeDatabase(t.Context(), "c_show", []interface{}{
			map[string]interface{}{"alias": "a1", "database_name": "db1", "type": "local", "access_mode": "read_write"},
		}))

		res, err := exec.executeShowCompositeDatabases(ctx, "SHOW COMPOSITE DATABASES")
		require.NoError(t, err)
		require.NotEmpty(t, res.Rows)

		_, err = exec.executeShowConstituents(ctx, "SHOW CONSTITUENTS")
		require.Error(t, err)
		require.Contains(t, err.Error(), "FOR COMPOSITE DATABASE name expected")

		res, err = exec.executeShowConstituents(ctx, "SHOW CONSTITUENTS FOR COMPOSITE DATABASE c_show")
		require.NoError(t, err)
		require.NotEmpty(t, res.Rows)
		require.Equal(t, []string{"alias", "database", "type", "access_mode", "uri", "secret_ref", "auth_mode", "user"}, res.Columns)

		// Flexible whitespace branch: hit COMPOSITE + DATABASE parsing path.
		res, err = exec.executeShowConstituents(ctx, "SHOW CONSTITUENTS FOR COMPOSITE\tDATABASE\tc_show")
		require.NoError(t, err)
		require.NotEmpty(t, res.Rows)
		res, err = exec.executeShowConstituents(ctx, "SHOW CONSTITUENTS FOR COMPOSITE\nDATABASE\nc_show")
		require.NoError(t, err)
		require.NotEmpty(t, res.Rows)

		_, err = exec.executeShowConstituents(ctx, "SHOW CONSTITUENTS FOR COMPOSITE DATABASE   ")
		require.Error(t, err)
		require.Contains(t, err.Error(), "name expected")

		_, err = exec.executeShowConstituents(ctx, "SHOW PARTS FOR COMPOSITE DATABASE c_show")
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid SHOW CONSTITUENTS syntax")

		_, err = exec.executeShowConstituents(ctx, "SHOW CONSTITUENTS FOR COMPOSITE DATABASE does_not_exist")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to get constituents")

		weird := &weirdConstituentAdapter{testDatabaseManagerAdapter: adapter}
		exec.SetDatabaseManager(weird)
		res, err = exec.executeShowConstituents(ctx, "SHOW CONSTITUENTS FOR COMPOSITE DATABASE c_show")
		require.NoError(t, err)
		require.Equal(t, []interface{}{"", "", "", "", "", "", "", ""}, res.Rows[0])
	})
}

func TestExecuteAlterCompositeDatabase_DirectErrorBranches(t *testing.T) {
	ctx := context.Background()

	exec := &StorageExecutor{}
	_, err := exec.executeCompositeCommand(ctx, "ALTER COMPOSITE DATABASE c ADD ALIAS a FOR DATABASE d", compositeAlterCommand)
	require.Error(t, err)
	require.Contains(t, err.Error(), "database manager not available")

	baseStore := newTestMemoryEngine(t)
	inner := newTestMemoryEngine(t)
	proof := newConstituentCredentialProof(t)
	alterCfg := multidb.DefaultConfig()
	alterCfg.RemoteEngineFactory = proof.factory
	alterCfg.RemoteCredentialEncryptionKey = "test-key-for-alter-composite"
	manager, err := multidb.NewDatabaseManager(t.Context(), inner, alterCfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	adapter := &testDatabaseManagerAdapter{manager: manager}
	exec = NewStorageExecutor(storage.NewNamespacedEngine(baseStore, "test"))
	exec.SetDatabaseManager(adapter)

	require.NoError(t, adapter.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "d1"}))
	require.NoError(t, adapter.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "d2"}))
	require.NoError(t, adapter.CreateCompositeDatabase(t.Context(), "comp_err", []interface{}{
		map[string]interface{}{"alias": "d1", "database_name": "d1", "type": "local", "access_mode": "read_write"},
	}))

	_, err = exec.executeCompositeCommand(ctx, "ALTER COMPOSITE comp_err ADD ALIAS x FOR DATABASE d2", compositeAlterCommand)
	require.Error(t, err)
	require.Contains(t, err.Error(), "DATABASE expected after COMPOSITE")

	_, err = exec.executeCompositeCommand(ctx, "ALTER COMPOSITE DATABASE", compositeAlterCommand)
	require.Error(t, err)
	require.Contains(t, err.Error(), "database name expected")

	_, err = exec.executeCompositeCommand(ctx, "ALTER COMPOSITE DATABASE comp_err", compositeAlterCommand)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ADD ALIAS or DROP ALIAS expected")

	_, err = exec.executeCompositeCommand(ctx, "ALTER COMPOSITE DATABASE comp_err ADD ALIAS x", compositeAlterCommand)
	require.Error(t, err)
	require.Contains(t, err.Error(), "FOR DATABASE expected")

	_, err = exec.executeCompositeCommand(ctx, "ALTER COMPOSITE DATABASE comp_err ADD ALIAS   FOR DATABASE d2", compositeAlterCommand)
	require.Error(t, err)
	require.Contains(t, err.Error(), "FOR DATABASE expected")

	_, err = exec.executeCompositeCommand(ctx, "ALTER COMPOSITE DATABASE comp_err ADD ALIAS x FOR DATABASE", compositeAlterCommand)
	require.Error(t, err)
	require.Contains(t, err.Error(), "database name cannot be empty")

	_, err = exec.executeCompositeCommand(ctx, "ALTER COMPOSITE DATABASE comp_err ADD ALIAS r FOR DATABASE d2 AT 'https://remote.example/nornic-db' SECRET REF 'spn-nornic' TYPE remote ACCESS read", compositeAlterCommand)
	require.NoError(t, err)

	list, err := adapter.GetCompositeConstituents("comp_err")
	require.NoError(t, err)
	require.Len(t, list, 2)
	last := list[1].(map[string]interface{})
	require.Equal(t, "r", last["alias"])
	require.Equal(t, "remote", last["type"])
	require.Equal(t, "https://remote.example/nornic-db", last["uri"])
	require.Equal(t, "spn-nornic", last["secret_ref"])
	require.Equal(t, "read", last["access_mode"])
	require.Equal(t, "oidc_forwarding", last["auth_mode"])

	_, err = exec.executeCompositeCommand(ctx, "ALTER COMPOSITE DATABASE comp_err ADD ALIAS r2 FOR DATABASE d2 AT 'https://remote.example/nornic-db' USER 'svc-user' PASSWORD 'svc-pass' TYPE remote ACCESS read", compositeAlterCommand)
	require.NoError(t, err)
	list, err = adapter.GetCompositeConstituents("comp_err")
	require.NoError(t, err)
	require.Len(t, list, 3)
	last = list[2].(map[string]interface{})
	proof.assertConnection(manager, "comp_err", multidb.ConstituentRef{Alias: "r2", DatabaseName: "d2", Type: "remote", AccessMode: "read", URI: "https://remote.example/nornic-db", AuthMode: "user_password", User: "svc-user", Password: "svc-pass"})

	_, err = exec.executeCompositeCommand(ctx, "ALTER COMPOSITE DATABASE comp_err ADD ALIAS bad FOR DATABASE d2 AT 'https://remote.example/nornic-db' OIDC CREDENTIAL FORWARDING USER 'svc-user' PASSWORD 'svc-pass'", compositeAlterCommand)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot combine OIDC CREDENTIAL FORWARDING with USER/PASSWORD")

	_, err = exec.executeCompositeCommand(ctx, "ALTER COMPOSITE DATABASE comp_err DROP ALIAS", compositeAlterCommand)
	require.Error(t, err)
	require.Contains(t, err.Error(), "alias name cannot be empty")

	_, err = exec.executeCompositeCommand(ctx, "ALTER COMPOSITE DATABASE comp_err DROP ALIAS missing_alias", compositeAlterCommand)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to remove constituent")
}

func TestParseCypherValueToken(t *testing.T) {
	v, err := parseCypherValueToken("")
	require.NoError(t, err)
	require.Equal(t, "", v)

	v, err = parseCypherValueToken("'abc'")
	require.NoError(t, err)
	require.Equal(t, "abc", v)

	v, err = parseCypherValueToken("\"abc\"")
	require.NoError(t, err)
	require.Equal(t, "abc", v)

	v, err = parseCypherValueToken("`abc`")
	require.NoError(t, err)
	require.Equal(t, "abc", v)

	v, err = parseCypherValueToken("`bad")
	require.NoError(t, err)
	require.Equal(t, "`bad", v)

	v, err = parseCypherValueToken("plain")
	require.NoError(t, err)
	require.Equal(t, "plain", v)
}

func TestParseConstituentFromTokens_Branches(t *testing.T) {
	assertErrContains := func(tokens []string, contains string) {
		t.Helper()
		i := 0
		_, err := parseConstituentFromTokens(tokens, &i)
		require.Error(t, err)
		require.Contains(t, err.Error(), contains)
	}

	assertErrContains([]string{}, "ALIAS expected")
	assertErrContains([]string{"ALIAS"}, "alias name cannot be empty")
	assertErrContains([]string{"ALIAS", "''", "FOR", "DATABASE", "d"}, "alias name cannot be empty")
	assertErrContains([]string{"ALIAS", "a"}, "FOR DATABASE expected")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE"}, "database name cannot be empty")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "''"}, "database name cannot be empty")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "AT"}, "remote URI expected")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "AT", "''"}, "remote URI cannot be empty")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "SECRET"}, "SECRET REF expected")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "SECRET", "REF"}, "secret ref cannot be empty")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "SECRET", "REF", "''"}, "secret ref cannot be empty")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "TYPE"}, "type cannot be empty")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "TYPE", "bad"}, "type must be local or remote")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "AT", "https://x", "TYPE", "local"}, "TYPE local contradicts AT")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "ACCESS"}, "access mode cannot be empty")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "ACCESS", "bad"}, "access mode must be read, write, or read_write")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "USER"}, "user cannot be empty")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "AT", "https://x", "USER", "''", "PASSWORD", "p"}, "user cannot be empty")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "PASSWORD"}, "password cannot be empty")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "AT", "https://x", "USER", "u", "PASSWORD", "''"}, "password cannot be empty")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "OIDC"}, "OIDC CREDENTIAL FORWARDING expected")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "OIDC", "CREDENTIAL"}, "OIDC CREDENTIAL FORWARDING expected")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "USER", "u", "PASSWORD", "p"}, "require a remote constituent")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "AT", "https://x", "USER", "u"}, "USER and PASSWORD must both be provided")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "AT", "https://x", "OIDC", "CREDENTIAL", "FORWARDING", "USER", "u", "PASSWORD", "p"}, "cannot combine OIDC CREDENTIAL FORWARDING with USER/PASSWORD")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "NOPE"}, "unexpected token")
	assertErrContains([]string{"ALIAS", "`a`b`", "FOR", "DATABASE", "d"}, "alias name")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "`d`b`"}, "database name")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "AT", "`u`b`"}, "remote URI")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "SECRET", "REF", "`s`b`"}, "secret ref")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "TYPE", "`t`b`"}, "type")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "ACCESS", "`m`b`"}, "access mode")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "AT", "https://x", "USER", "`u`b`", "PASSWORD", "p"}, "user")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "AT", "https://x", "USER", "u", "PASSWORD", "`p`b`"}, "password")
	assertErrContains([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "OIDC", "CREDENTIAL", "FORWARDING"}, "require a remote constituent")

	i := 0
	ref, err := parseConstituentFromTokens([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "AT", "https://x"}, &i)
	require.NoError(t, err)
	require.Equal(t, "remote", ref["type"])
	require.Equal(t, "oidc_forwarding", ref["auth_mode"])

	i = 0
	ref, err = parseConstituentFromTokens([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "AT", "https://x", "USER", "u", "PASSWORD", "p"}, &i)
	require.NoError(t, err)
	require.Equal(t, "user_password", ref["auth_mode"])
	require.Equal(t, "u", ref["user"])
	require.Equal(t, "p", ref["password"])

	i = 0
	ref, err = parseConstituentFromTokens([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "AT", "https://x", "OIDC", "CREDENTIAL", "FORWARDING"}, &i)
	require.NoError(t, err)
	require.Equal(t, "oidc_forwarding", ref["auth_mode"])

	i = 0
	ref, err = parseConstituentFromTokens([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "AT", "https://x", "SECRET", "REF", "spn-a"}, &i)
	require.NoError(t, err)
	require.Equal(t, "spn-a", ref["secret_ref"])

	i = 0
	ref, err = parseConstituentFromTokens([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "TYPE", "local", "ACCESS", "write"}, &i)
	require.NoError(t, err)
	require.Equal(t, "local", ref["type"])
	require.Equal(t, "write", ref["access_mode"])

	i = 0
	tokens := []string{"ALIAS", "a", "FOR", "DATABASE", "d", "ALIAS", "b", "FOR", "DATABASE", "d2"}
	ref, err = parseConstituentFromTokens(tokens, &i)
	require.NoError(t, err)
	require.Equal(t, "a", ref["alias"])
	require.Equal(t, 5, i)
	require.Equal(t, "ALIAS", tokens[i])

	i = 0
	ref, err = parseConstituentFromTokens([]string{"ALIAS", "a", "FOR", "DATABASE", "d", "TYPE", "remote", "USER", "u", "PASSWORD", "p"}, &i)
	require.NoError(t, err)
	require.Equal(t, "remote", ref["type"])
	require.Equal(t, "user_password", ref["auth_mode"])
}

func (a *testDatabaseManagerAdapter) StatementCacheScope(name string) (*multidb.StatementCacheScope, error) {
	return a.manager.StatementCacheScope(name)
}
