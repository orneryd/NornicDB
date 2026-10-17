package cypher

import (
	"context"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// memoryRoles is an in-memory RoleAdministration: built-in admin and
// editor, the custom roles, and each user's roles.
type memoryRoles struct {
	custom  map[string]bool
	users   map[string][]string
	changes []int // users per ChangeUserRoles call
}

func newMemoryRoles(users ...string) *memoryRoles {
	roles := &memoryRoles{custom: map[string]bool{}, users: map[string][]string{}}
	for _, user := range users {
		roles.users[user] = nil
	}
	return roles
}

func (m *memoryRoles) builtin(name string) bool { return name == "admin" || name == "editor" }

func (m *memoryRoles) Roles() []RoleListing {
	names := []string{"admin", "editor"}
	for name := range m.custom {
		names = append(names, name)
	}
	listings := make([]RoleListing, 0, len(names))
	for _, name := range names {
		listing := RoleListing{Name: name}
		for user, held := range m.users {
			for _, role := range held {
				if role == name {
					listing.Members = append(listing.Members, user)
				}
			}
		}
		listings = append(listings, listing)
	}
	return listings
}

func (m *memoryRoles) CreateRole(_ context.Context, name string) error {
	name = auth.NormalizeRoleName(name)
	if m.builtin(name) || m.custom[name] {
		return auth.ErrRoleExists
	}
	m.custom[name] = true
	return nil
}

func (m *memoryRoles) DropRole(_ context.Context, name string) error {
	name = auth.NormalizeRoleName(name)
	if m.builtin(name) {
		return auth.ErrCannotDeleteBuiltinRole
	}
	if !m.custom[name] {
		return auth.ErrRoleNotFound
	}
	delete(m.custom, name)
	for user, held := range m.users {
		next, _ := assignedRoles(held, []string{name}, false)
		m.users[user] = next
	}
	return nil
}

func (m *memoryRoles) RenameRole(_ context.Context, from, to string) error {
	from, to = auth.NormalizeRoleName(from), auth.NormalizeRoleName(to)
	if m.builtin(from) {
		return auth.ErrCannotDeleteBuiltinRole
	}
	delete(m.custom, from)
	m.custom[to] = true
	for user, held := range m.users {
		for i, role := range held {
			if role == from {
				held[i] = to
			}
		}
		m.users[user] = held
	}
	return nil
}

func (m *memoryRoles) RoleExists(name string) bool {
	name = auth.NormalizeRoleName(name)
	return m.builtin(name) || m.custom[name]
}

func (m *memoryRoles) UserRoles(user string) ([]string, error) {
	held, ok := m.users[user]
	if !ok {
		return nil, auth.ErrUserNotFound
	}
	return append([]string(nil), held...), nil
}

func (m *memoryRoles) ChangeUserRoles(_ context.Context, users []string, change func([]string) ([]string, bool), skipMissing bool) error {
	m.changes = append(m.changes, len(users))
	next := map[string][]string{}
	for _, user := range users {
		held, ok := m.users[user]
		if !ok {
			if skipMissing {
				continue
			}
			return auth.ErrUserNotFound
		}
		if updated, changed := change(append([]string(nil), held...)); changed {
			next[user] = updated
		}
	}
	for user, roles := range next {
		m.users[user] = roles
	}
	return nil
}

func roleCommandExecutor(t *testing.T, roles *memoryRoles) (*StorageExecutor, context.Context) {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	return exec, WithRequestIdentity(context.Background(), &RequestIdentity{Roles: roles})
}

func sortedHeld(roles *memoryRoles, user string) []string {
	held := append([]string(nil), roles.users[user]...)
	sort.Strings(held)
	return held
}

// The role commands follow Neo4j's role management semantics: CREATE fails
// on an existing role unless IF NOT EXISTS, OR REPLACE replaces it; DROP and
// RENAME fail on a missing role unless IF EXISTS; RENAME onto an existing
// role fails; built-in roles can't be dropped. Each returns no rows.
func TestRoleCommandsCreateDropRename(t *testing.T) {
	roles := newMemoryRoles("alice")
	exec, ctx := roleCommandExecutor(t, roles)
	run := func(query string) error {
		result, err := exec.Execute(ctx, query, nil)
		if err == nil {
			require.Empty(t, result.Rows, query)
		}
		return err
	}

	require.NoError(t, run("CREATE ROLE ops"))
	require.True(t, roles.custom["ops"])
	err := run("CREATE ROLE ops")
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Statement.ArgumentError")
	require.NoError(t, run("CREATE ROLE ops IF NOT EXISTS"))
	require.NoError(t, run("CREATE ROLE `quoted role`"))
	require.True(t, roles.custom["quoted role"])

	roles.users["alice"] = []string{"ops"}
	require.NoError(t, run("CREATE OR REPLACE ROLE ops"))
	require.True(t, roles.custom["ops"])
	require.Empty(t, roles.users["alice"], "OR REPLACE drops the role first, its members with it")

	require.NoError(t, run("RENAME ROLE ops TO operations"))
	require.False(t, roles.custom["ops"])
	require.True(t, roles.custom["operations"])
	err = run("RENAME ROLE ops TO other")
	require.Contains(t, statusText(err), "Neo.ClientError.Statement.ArgumentError")
	require.NoError(t, run("RENAME ROLE ops IF EXISTS TO other"))
	require.False(t, roles.custom["other"])
	err = run("RENAME ROLE operations TO `quoted role`")
	require.Contains(t, statusText(err), "Neo.ClientError.Statement.ArgumentError")

	require.NoError(t, run("DROP ROLE operations"))
	require.False(t, roles.custom["operations"])
	err = run("DROP ROLE operations")
	require.Contains(t, statusText(err), "Neo.ClientError.Statement.ArgumentError")
	require.NoError(t, run("DROP ROLE operations IF EXISTS"))
	err = run("DROP ROLE admin")
	require.Contains(t, statusText(err), "Neo.ClientError.Statement.ArgumentError")

	for _, invalid := range []string{
		"CREATE OR REPLACE ROLE x IF NOT EXISTS",
		"CREATE ROLE",
		"DROP ROLE x y",
		"RENAME ROLE x",
	} {
		err := run(invalid)
		require.Error(t, err, invalid)
		require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError", invalid)
	}
	for _, unsupported := range []string{"CREATE IMMUTABLE ROLE x", "CREATE ROLE x AS COPY OF admin"} {
		err := run(unsupported)
		require.Contains(t, statusText(err), "Neo.ClientError.Statement.UnsupportedAdministrationCommand", unsupported)
	}
	require.False(t, roles.custom["x"])
}

// GRANT ROLE needs every role and user to exist and changes no one
// otherwise; granting a held role or revoking one not held is no change, and
// REVOKE of a missing role or user is no change, as Neo4j reports those with
// notifications. Names may be parameters.
func TestRoleCommandsGrantAndRevoke(t *testing.T) {
	roles := newMemoryRoles("alice", "bob")
	roles.custom["ops"], roles.custom["audit"] = true, true
	exec, ctx := roleCommandExecutor(t, roles)
	run := func(query string, params map[string]interface{}) error {
		_, err := exec.Execute(ctx, query, params)
		return err
	}

	require.NoError(t, run("GRANT ROLES ops, audit TO alice, bob", nil))
	require.Equal(t, []string{"audit", "ops"}, sortedHeld(roles, "alice"))
	require.Equal(t, []string{"audit", "ops"}, sortedHeld(roles, "bob"))
	// Both users change in one ChangeUserRoles, which applies them together.
	require.Equal(t, []int{2}, roles.changes)
	require.NoError(t, run("GRANT ROLE ops TO alice", nil))
	require.Equal(t, []string{"audit", "ops"}, sortedHeld(roles, "alice"))

	err := run("GRANT ROLE missing TO alice", nil)
	require.Contains(t, statusText(err), "Neo.ClientError.Statement.ArgumentError")
	err = run("GRANT ROLE ops TO alice, nobody", nil)
	require.Contains(t, statusText(err), "Neo.ClientError.Statement.ArgumentError")
	require.Equal(t, []string{"audit", "ops"}, sortedHeld(roles, "alice"))

	require.NoError(t, run("REVOKE ROLE $role FROM $user", map[string]interface{}{"role": "audit", "user": "alice"}))
	require.Equal(t, []string{"ops"}, sortedHeld(roles, "alice"))
	require.Equal(t, []string{"audit", "ops"}, sortedHeld(roles, "bob"))
	require.NoError(t, run("REVOKE ROLE audit FROM alice", nil))
	require.NoError(t, run("REVOKE ROLE missing FROM alice", nil))
	require.NoError(t, run("REVOKE ROLE ops FROM nobody", nil))
	require.Equal(t, []string{"ops"}, sortedHeld(roles, "alice"))

	err = run("GRANT ROLE $role TO alice", map[string]interface{}{"role": 7})
	require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError")
}

// SHOW ROLES lists every role (built-in ones too) with Neo4j's columns;
// POPULATED keeps roles with a member; WITH USERS lists a row per role and
// member, and a role without members once with a null member. YIELD and
// WHERE work as for every SHOW command.
func TestShowRoles(t *testing.T) {
	roles := newMemoryRoles("alice", "bob")
	roles.custom["ops"] = true
	roles.users["alice"] = []string{"ops", "admin"}
	roles.users["bob"] = []string{"ops"}
	exec, ctx := roleCommandExecutor(t, roles)

	result, err := exec.Execute(ctx, "SHOW ROLES", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"role"}, result.Columns)
	require.Equal(t, [][]interface{}{{"admin"}, {"editor"}, {"ops"}}, result.Rows)

	result, err = exec.Execute(ctx, "SHOW POPULATED ROLES", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"admin"}, {"ops"}}, result.Rows)

	result, err = exec.Execute(ctx, "SHOW ROLES WITH USERS", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"role", "member"}, result.Columns)
	require.Equal(t, [][]interface{}{{"admin", "alice"}, {"editor", nil}, {"ops", "alice"}, {"ops", "bob"}}, result.Rows)

	result, err = exec.Execute(ctx, "SHOW POPULATED ROLES WITH USERS YIELD role, member WHERE member = 'bob'", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"ops", "bob"}}, result.Rows)

	result, err = exec.Execute(ctx, "SHOW ROLES YIELD *", nil)
	require.NoError(t, err)
	require.Contains(t, result.Columns, "immutable")
}

// Role commands change the server's role stores directly, so they are
// refused inside an explicit transaction, and need administrator rights.
func TestRoleCommandsOutsideTransactionAndAdminOnly(t *testing.T) {
	roles := newMemoryRoles("alice")
	exec, ctx := roleCommandExecutor(t, roles)
	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE ROLE ops", nil)
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Transaction.ForbiddenDueToTransactionType")
	_, err = exec.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)
	require.False(t, roles.custom["ops"])

	for _, query := range []string{
		"CREATE ROLE ops", "CREATE OR REPLACE ROLE ops", "DROP ROLE ops", "RENAME ROLE ops TO x",
		"GRANT ROLE ops TO alice", "REVOKE ROLE ops FROM alice", "SHOW ROLES", "SHOW POPULATED ROLES WITH USERS",
	} {
		require.True(t, QueryPermissionRequirements(query).Admin, query)
	}
	require.False(t, roles.custom["ops"])
}
