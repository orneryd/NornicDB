package cypher

import (
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// securityDirectoryContractFixture is one native security directory wired to
// the FGAC runtime and an installation-administrator request, as the server
// wires them: the role store, the user store and the executor share the
// system engine.
type securityDirectoryContractFixture struct {
	executor *StorageExecutor
	runtime  *auth.FineGrainedRuntime
	roles    *auth.RoleStore
	users    *auth.Authenticator
	admin    *auth.FineGrainedRequest
}

func securityDirectoryDatabaseID(name string) string {
	switch name {
	case "homedb":
		return "home-id"
	case "otherdb":
		return "other-id"
	}
	return ""
}

func newSecurityDirectoryContractFixture(t *testing.T) *securityDirectoryContractFixture {
	t.Helper()
	ctx := t.Context()
	system := storage.NewNamespacedEngine(newTestMemoryEngine(t), "system")
	roles := auth.NewRoleStore(system)
	require.NoError(t, roles.Load(ctx))
	config := auth.DefaultAuthConfig()
	config.JWTSecret = []byte("security-directory-contract-secret")
	users, err := auth.NewAuthenticator(config, system)
	require.NoError(t, err)
	actions := auth.FineGrainedCoreActions()
	catalog := auth.DefaultFineGrainedCatalog(actions)
	require.NoError(t, roles.ReplaceFineGrainedCatalog(ctx, "", actions, catalog))
	policy, err := auth.CompileFineGrainedPolicy(actions, catalog)
	require.NoError(t, err)
	runtime := auth.NewFineGrainedRuntime(roles, actions, securityDirectoryDatabaseID, func() string { return "homedb" })
	runtime.SetSecurityDirectory(users, auth.RoleMutationStores{Users: users})
	return &securityDirectoryContractFixture{
		executor: NewStorageExecutor(system),
		runtime:  runtime,
		roles:    roles,
		users:    users,
		admin: &auth.FineGrainedRequest{
			Access:     policy.ForRoles([]string{string(auth.RoleAdmin)}),
			Runtime:    runtime,
			DatabaseID: securityDirectoryDatabaseID,
		},
	}
}

func (fixture *securityDirectoryContractFixture) run(t *testing.T, statement string, params map[string]interface{}) (*ExecuteResult, error) {
	t.Helper()
	return fixture.executor.Execute(auth.WithFineGrainedRequest(t.Context(), fixture.admin), statement, params)
}

func (fixture *securityDirectoryContractFixture) mustRun(t *testing.T, statement string, params map[string]interface{}) *ExecuteResult {
	t.Helper()
	result, err := fixture.run(t, statement, params)
	require.NoError(t, err, statement)
	return result
}

func (fixture *securityDirectoryContractFixture) user(t *testing.T, name string) *auth.User {
	t.Helper()
	for _, user := range fixture.users.ListUsers() {
		if user.Username == name {
			return user
		}
	}
	t.Fatalf("user %q is not in the directory", name)
	return nil
}

func (fixture *securityDirectoryContractFixture) roleNames(t *testing.T) []string {
	t.Helper()
	result := fixture.mustRun(t, "SHOW ROLES", nil)
	names := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		name, ok := row[0].(string)
		require.True(t, ok, "SHOW ROLES row is not a role name: %v", row)
		names = append(names, name)
	}
	return names
}

func requireSecurityDirectoryErrorCode(t *testing.T, err error, code string) {
	t.Helper()
	require.Error(t, err)
	var classified interface{ BoltErrorCode() string }
	require.True(t, errors.As(err, &classified), "unclassified error: %v", err)
	require.Equal(t, code, classified.BoltErrorCode(), "%v", err)
}

// TestFineGrainedDirectoryCreateRoleCopyContract proves CREATE ROLE ... AS COPY
// OF creates a role that owns a copy of the source role's persisted catalog
// records, that the copy is visible through introspection, and that a missing
// source role is still refused when IF NOT EXISTS cannot make the statement a
// no-op.
func TestFineGrainedDirectoryCreateRoleCopyContract(t *testing.T) {
	ctx := t.Context()
	fixture := newSecurityDirectoryContractFixture(t)
	fixture.mustRun(t, "CREATE ROLE source", nil)
	fixture.mustRun(t, "GRANT CREATE ROLE ON DBMS TO source", nil)

	catalog, revision, err := fixture.runtime.ReadCatalog(ctx)
	require.NoError(t, err)
	catalog.Definitions = append(catalog.Definitions, auth.FineGrainedEntitlement{
		ID: "source-show", Action: "dbms.role.show", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDBMS},
	})
	catalog.Assignments["source"] = []string{"source-show"}
	require.NoError(t, fixture.roles.ReplaceFineGrainedCatalog(ctx, revision, auth.FineGrainedCoreActions(), catalog))

	fixture.mustRun(t, "CREATE ROLE copied AS COPY OF source", nil)
	require.Contains(t, fixture.roleNames(t), "copied")

	catalog, _, err = fixture.runtime.ReadCatalog(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"source-show"}, catalog.Assignments["copied"])
	privileges := map[string]int{}
	for _, privilege := range catalog.Privileges {
		privileges[privilege.Role]++
	}
	require.Equal(t, 1, privileges["source"])
	require.Equal(t, 1, privileges["copied"])

	introspected, err := fixture.run(t, "SHOW ROLE copied PRIVILEGES", nil)
	require.NoError(t, err)
	require.Len(t, introspected.Rows, 1)
	require.Equal(t, "create_role", introspected.Rows[0][1])
	require.Equal(t, "copied", introspected.Rows[0][5])

	// The target's existence decides IF NOT EXISTS, and PUBLIC is a copyable
	// role whose persisted catalog grants the copy receives.
	fixture.mustRun(t, "CREATE ROLE copied IF NOT EXISTS AS COPY OF source", nil)
	fixture.mustRun(t, "CREATE ROLE public_copy AS COPY OF public", nil)
	publicSource, err := fixture.run(t, "SHOW ROLE public PRIVILEGES", nil)
	require.NoError(t, err)
	require.NotEmpty(t, publicSource.Rows)
	publicCopy, err := fixture.run(t, "SHOW ROLE public_copy PRIVILEGES", nil)
	require.NoError(t, err)
	require.Len(t, publicCopy.Rows, len(publicSource.Rows))
	for i, row := range publicCopy.Rows {
		require.Equal(t, publicSource.Rows[i][1], row[1])
		require.Equal(t, "public_copy", row[5])
	}

	_, err = fixture.run(t, "CREATE ROLE source AS COPY OF copied", nil)
	require.ErrorIs(t, err, auth.ErrRoleExists)
	_, err = fixture.run(t, "CREATE ROLE admin AS COPY OF source", nil)
	require.ErrorIs(t, err, auth.ErrRoleExists)
	_, err = fixture.run(t, "CREATE ROLE absent_copy AS COPY OF missing_role", nil)
	require.ErrorIs(t, err, auth.ErrRoleNotFound)
	_, err = fixture.run(t, "CREATE ROLE absent_copy IF NOT EXISTS AS COPY OF missing_role", nil)
	require.ErrorIs(t, err, auth.ErrRoleNotFound)
	require.NotContains(t, fixture.roleNames(t), "absent_copy")
}

// TestFineGrainedDirectoryParameterContract proves every role name, user name,
// home database and credential position of the directory statements accepts a
// typed parameter binding, and that a wrongly typed or missing binding is
// refused instead of becoming a name.
func TestFineGrainedDirectoryParameterContract(t *testing.T) {
	ctx := t.Context()
	fixture := newSecurityDirectoryContractFixture(t)
	params := map[string]interface{}{
		"role":         "analyst",
		"user":         "wanda",
		"password":     "first-password",
		"nextPassword": "second-password",
		"home":         "homedb",
		"otherHome":    "otherdb",
	}
	fixture.mustRun(t, "CREATE ROLE $role", params)
	fixture.mustRun(t, "CREATE USER $user SET PASSWORD $password CHANGE NOT REQUIRED SET STATUS SUSPENDED SET HOME DATABASE $home", params)
	user := fixture.user(t, "wanda")
	require.False(t, user.PasswordChangeRequired)
	require.True(t, user.Disabled)
	require.Equal(t, "homedb", user.HomeDatabase)

	fixture.mustRun(t, "ALTER USER $user SET STATUS ACTIVE SET PASSWORD $nextPassword CHANGE REQUIRED SET HOME DATABASE $otherHome", params)
	user = fixture.user(t, "wanda")
	require.False(t, user.Disabled)
	require.True(t, user.PasswordChangeRequired)
	require.Equal(t, "otherdb", user.HomeDatabase)

	fixture.mustRun(t, "GRANT ROLE $role TO $user", params)
	require.Contains(t, fixture.user(t, "wanda").Roles, auth.Role("analyst"))
	// Privilege introspection resolves the stored user through the directory
	// owner and reports the privileges of the roles it holds.
	userPrivileges, err := fixture.run(t, "SHOW USER wanda PRIVILEGES", nil)
	require.NoError(t, err)
	require.NotEmpty(t, userPrivileges.Rows)
	for _, row := range userPrivileges.Rows {
		require.Equal(t, "wanda", row[7])
	}

	// The parameter-bound credential is the stored credential.
	token, _, err := fixture.users.Authenticate(ctx, "wanda", "second-password", "", "")
	require.NoError(t, err)
	require.NotNil(t, token)

	fixture.mustRun(t, "REVOKE ROLE $role FROM $user", params)
	fixture.mustRun(t, "DROP USER $user", params)
	fixture.mustRun(t, "DROP ROLE $role", params)

	_, err = fixture.run(t, "CREATE ROLE $role", map[string]interface{}{"role": int64(7)})
	requireSecurityDirectoryErrorCode(t, err, "Neo.ClientError.Statement.TypeError")
	_, err = fixture.run(t, "CREATE USER $user SET PASSWORD $password", map[string]interface{}{"user": "nina", "password": int64(7)})
	requireSecurityDirectoryErrorCode(t, err, "Neo.ClientError.Statement.TypeError")
	_, err = fixture.run(t, "CREATE ROLE $missingRole", nil)
	requireSecurityDirectoryErrorCode(t, err, "Neo.ClientError.Statement.ParameterMissing")
	require.NotContains(t, fixture.roleNames(t), "analyst")
}

// TestFineGrainedDirectoryUserClauseContract proves the documented CREATE USER
// and ALTER USER clause sets are accepted in any order, apply together, and
// keep an omitted CHANGE [NOT] REQUIRED clause unchanged.
func TestFineGrainedDirectoryUserClauseContract(t *testing.T) {
	fixture := newSecurityDirectoryContractFixture(t)
	fixture.mustRun(t, "CREATE USER clara SET STATUS SUSPENDED SET PLAINTEXT PASSWORD 'first-password' CHANGE NOT REQUIRED SET HOME DATABASE homedb", nil)
	user := fixture.user(t, "clara")
	require.True(t, user.Disabled)
	require.False(t, user.PasswordChangeRequired)
	require.Equal(t, "homedb", user.HomeDatabase)

	fixture.mustRun(t, "ALTER USER clara REMOVE HOME DATABASE SET STATUS ACTIVE SET PASSWORD 'second-password' CHANGE REQUIRED", nil)
	user = fixture.user(t, "clara")
	require.False(t, user.Disabled)
	require.Empty(t, user.HomeDatabase)
	require.True(t, user.PasswordChangeRequired)

	// An ALTER USER statement that writes no CHANGE clause keeps the stored
	// requirement; CHANGE [NOT] REQUIRED alone writes it.
	fixture.mustRun(t, "ALTER USER clara SET PASSWORD 'third-password'", nil)
	require.True(t, fixture.user(t, "clara").PasswordChangeRequired)
	fixture.mustRun(t, "ALTER USER clara SET PASSWORD CHANGE NOT REQUIRED", nil)
	require.False(t, fixture.user(t, "clara").PasswordChangeRequired)

	_, err := fixture.run(t, "CREATE USER incomplete SET STATUS ACTIVE", nil)
	requireSecurityDirectoryErrorCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	_, err = fixture.run(t, "ALTER USER clara", nil)
	requireSecurityDirectoryErrorCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	_, err = fixture.run(t, "ALTER USER clara SET STATUS DORMANT", nil)
	requireSecurityDirectoryErrorCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	// A home database the catalog does not know is refused before the account
	// write, so no partially declared account is stored.
	_, err = fixture.run(t, "CREATE USER homeless SET PASSWORD 'first-password' SET HOME DATABASE unknowndb", nil)
	requireSecurityDirectoryErrorCode(t, err, "Neo.ClientError.Database.DatabaseNotFound")
	for _, existing := range fixture.users.ListUsers() {
		require.NotEqual(t, "homeless", existing.Username)
	}
	// SET AUTH is not part of the accepted contract: the native account has
	// no external provider owner.
	_, err = fixture.run(t, "CREATE USER provider_user SET PASSWORD 'first-password' SET AUTH 'native' {SET ID 'x'}", nil)
	requireSecurityDirectoryErrorCode(t, err, "Neo.ClientError.Statement.SyntaxError")
}

// TestFineGrainedDirectoryAlterUserAuthorizationContract proves a combined
// ALTER USER statement needs the privilege of every clause it writes and is
// refused before any clause is applied.
func TestFineGrainedDirectoryAlterUserAuthorizationContract(t *testing.T) {
	ctx := t.Context()
	fixture := newSecurityDirectoryContractFixture(t)
	fixture.mustRun(t, "CREATE USER dana SET PASSWORD 'first-password' CHANGE NOT REQUIRED", nil)

	actions := auth.FineGrainedCoreActions()
	catalog := auth.FineGrainedCatalog{Assignments: map[string][]string{}}
	for _, action := range actions {
		if action.ID != "dbms.user.password.set" {
			continue
		}
		catalog.Definitions = append(catalog.Definitions, auth.FineGrainedEntitlement{
			ID: "limited-password", Action: action.ID, Scope: auth.FineGrainedScope{Kind: action.ResourceKind},
		})
		catalog.Assignments["limited"] = []string{"limited-password"}
	}
	policy, err := auth.CompileFineGrainedPolicy(actions, catalog)
	require.NoError(t, err)
	limited := auth.WithFineGrainedRequest(ctx, &auth.FineGrainedRequest{
		Access:     policy.ForRoles([]string{"limited"}),
		Runtime:    fixture.runtime,
		DatabaseID: securityDirectoryDatabaseID,
	})

	_, err = fixture.executor.Execute(limited, "ALTER USER dana SET PASSWORD 'second-password'", nil)
	require.NoError(t, err)
	_, err = fixture.executor.Execute(limited, "ALTER USER dana SET PASSWORD 'third-password' SET STATUS SUSPENDED", nil)
	require.ErrorIs(t, err, auth.ErrFineGrainedForbidden)
	// A combined clause cannot be used to reach a role copy either: the
	// unprivileged principal keeps no role-create grant.
	_, err = fixture.executor.Execute(limited, "CREATE ROLE copied_admin AS COPY OF admin", nil)
	require.ErrorIs(t, err, auth.ErrFineGrainedForbidden)
	require.NotContains(t, fixture.roleNames(t), "copied_admin")

	user := fixture.user(t, "dana")
	require.False(t, user.Disabled)
	token, _, err := fixture.users.Authenticate(ctx, "dana", "second-password", "", "")
	require.NoError(t, err)
	require.NotNil(t, token)
}
