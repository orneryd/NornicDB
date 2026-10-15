package auth

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newSystemTestEngine is a system database as the server gives the auth
// stores one: a namespaced engine, in which a role and its records share
// one namespace, so one transaction can change them together.
func newSystemTestEngine(t testing.TB) storage.Engine {
	t.Helper()
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	return storage.NewNamespacedEngine(base, "system")
}

// roleRecordGate pauses a role-record write inside its transaction, at its
// read of the record target, i.e. after the write took its role lock.
type roleRecordGate struct {
	storage.Engine
	target           storage.NodeID
	once             *sync.Once
	entered, release chan struct{}
}

func (g *roleRecordGate) WithStoreTransaction(ctx context.Context, mutate func(storage.Engine) error) error {
	return WithStoreTransaction(ctx, g.Engine, func(view storage.Engine) error {
		return mutate(&roleRecordGate{Engine: view, target: g.target, once: g.once, entered: g.entered, release: g.release})
	})
}

func (g *roleRecordGate) GetNode(id storage.NodeID) (*storage.Node, error) {
	if id == g.target {
		g.once.Do(func() {
			close(g.entered)
			<-g.release
		})
	}
	return g.Engine.GetNode(id)
}

// roleRecordSetters are the single writes of each record a role owns, with
// the ID of the record each writes.
var roleRecordSetters = map[string]struct {
	id    storage.NodeID
	write func(ctx context.Context, engine storage.Engine, role string) error
}{
	"entitlements": {"role_entitlement:ops", func(ctx context.Context, engine storage.Engine, role string) error {
		return NewRoleEntitlementsStore(engine).Set(ctx, role, []string{"admin"})
	}},
	"allowlist": {"role_db_access:ops", func(ctx context.Context, engine storage.Engine, role string) error {
		return NewAllowlistStore(engine).SaveRoleDatabases(ctx, role, []string{"system"})
	}},
	"privilege": {"db_priv:ops:nornic", func(ctx context.Context, engine storage.Engine, role string) error {
		return NewPrivilegesStore(engine).SavePrivilege(ctx, role, "nornic", true, true)
	}},
}

// A write of a role's record racing the role's delete or rename can't
// commit alongside it: the write takes the role's lock in its transaction,
// so one of them fails, and a role created later with the old name starts
// without the record.
func TestRoleRecordWriteRacingRoleChangeLeavesNoRecord(t *testing.T) {
	for _, rename := range []bool{false, true} {
		for kind, setter := range roleRecordSetters {
			t.Run(map[bool]string{false: "delete", true: "rename"}[rename]+"/"+kind, func(t *testing.T) {
				ctx := context.Background()
				base := storage.NewMemoryEngine()
				t.Cleanup(func() { _ = base.Close() })
				system := storage.NewNamespacedEngine(base, "system")
				roles := NewRoleStore(system)
				require.NoError(t, roles.CreateRole(ctx, "ops"))
				gate := &roleRecordGate{Engine: system, target: setter.id, once: &sync.Once{}, entered: make(chan struct{}), release: make(chan struct{})}
				done := make(chan error, 1)
				go func() { done <- setter.write(ctx, gate, "ops") }()
				select {
				case <-gate.entered:
				case err := <-done:
					t.Fatalf("write finished before reading its record: %v", err)
				case <-time.After(10 * time.Second):
					t.Fatal("write did not reach its record")
				}
				var changeErr error
				if rename {
					changeErr = roles.RenameRole(ctx, "ops", "renamed")
				} else {
					changeErr = roles.DeleteRole(ctx, "ops")
				}
				close(gate.release)
				writeErr := <-done
				require.False(t, changeErr == nil && writeErr == nil, "the write and the role change both committed")

				fresh := NewRoleStore(system)
				require.NoError(t, fresh.Load(ctx))
				if !fresh.Exists("ops") {
					require.NoError(t, fresh.CreateRole(ctx, "ops"))
					_, err := system.GetNode(setter.id)
					require.ErrorIs(t, err, storage.ErrNotFound, "a role created again with the old name inherits no record")
				}
			})
		}
	}
}

// Every role-record write requires its role: a write to a role that doesn't
// exist fails with ErrRoleNotFound and writes nothing, for the single
// setters and the batches alike.
func TestRoleRecordWriteToMissingRoleIsRefused(t *testing.T) {
	ctx := context.Background()
	writes := map[string]func(storage.Engine) error{
		"entitlements batch": func(engine storage.Engine) error {
			return NewRoleEntitlementsStore(engine).SetMappings(ctx, map[string][]string{"viewer": {"read"}, "ops": {"admin"}})
		},
		"allowlist batch": func(engine storage.Engine) error {
			return NewAllowlistStore(engine).SaveMappings(ctx, map[string][]string{"viewer": {"nornic"}, "ops": {"system"}})
		},
		"privilege matrix": func(engine storage.Engine) error {
			return NewPrivilegesStore(engine).PutMatrix(ctx, []struct {
				Role     string `json:"role"`
				Database string `json:"database"`
				Read     bool   `json:"read"`
				Write    bool   `json:"write"`
			}{{Role: "viewer", Database: "nornic", Read: true}, {Role: "ops", Database: "nornic", Read: true, Write: true}})
		},
	}
	for kind, setter := range roleRecordSetters {
		write := setter.write
		writes[kind] = func(engine storage.Engine) error { return write(ctx, engine, "ops") }
	}
	for kind, write := range writes {
		t.Run(kind, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			t.Cleanup(func() { _ = base.Close() })
			system := storage.NewNamespacedEngine(base, "system")
			before, err := system.AllNodes()
			require.NoError(t, err)
			err = write(system)
			require.ErrorIs(t, err, ErrRoleNotFound)
			require.ErrorContains(t, err, "ops")
			after, err := system.AllNodes()
			require.NoError(t, err)
			require.Equal(t, len(before), len(after), "nothing is written")
		})
	}
}

// The role-record owner refuses an empty role name, stops at a failed read
// of the role's lock or of the record, and an empty write for a record that
// was never stored changes nothing.
func TestRoleRecordOwnerEdges(t *testing.T) {
	ctx := context.Background()
	system := newSystemTestEngine(t)
	require.NoError(t, NewRoleStore(system).CreateRole(ctx, "ops"))
	require.ErrorIs(t, NewAllowlistStore(system).SaveRoleDatabases(ctx, " ", nil), ErrInvalidRoleName)
	require.NoError(t, NewRoleEntitlementsStore(system).Set(ctx, "ops", nil), "no override stored, none to remove")

	for _, id := range []storage.NodeID{"role:ops", "role_entitlement:ops"} {
		fault := &mutationFault{Engine: system, failOp: "get", failID: id}
		require.ErrorIs(t, NewRoleEntitlementsStore(fault).Set(ctx, "ops", []string{"read"}), errRoleMutation, id)
		_, err := system.GetNode("role_entitlement:ops")
		require.ErrorIs(t, err, storage.ErrNotFound, "nothing is written")
	}
}

// A role's name has one form everywhere (NormalizeRoleName): a write for
// " ROLE_Ops " locks and stores the records of the role ops, and a missing
// role is reported by the name the caller gave.
func TestRoleRecordWritesUseTheOneRoleName(t *testing.T) {
	ctx := context.Background()
	system := newSystemTestEngine(t)
	require.NoError(t, NewRoleStore(system).CreateRole(ctx, "ops"))
	require.Equal(t, "ops", NormalizeRoleName(" ROLE_Ops "))

	allowlist := NewAllowlistStore(system)
	require.NoError(t, allowlist.SaveRoleDatabases(ctx, " ROLE_Ops ", []string{"nornic"}))
	require.Equal(t, map[string][]string{"ops": {"nornic"}}, allowlist.Allowlist())
	_, err := system.GetNode("role_db_access:ops")
	require.NoError(t, err)
	entitlements := NewRoleEntitlementsStore(system)
	require.NoError(t, entitlements.Set(ctx, "ROLE_OPS", []string{"read"}))
	require.Equal(t, []string{"read"}, entitlements.Get("ops"))
	privileges := NewPrivilegesStore(system)
	require.NoError(t, privileges.SavePrivilege(ctx, "Role_Ops", "nornic", true, false))
	require.True(t, privileges.Resolve([]string{"ops"}, "nornic").Read)

	err = NewAllowlistStore(system).SaveRoleDatabases(ctx, "Missing", nil)
	var missing *RoleNotFoundError
	require.ErrorAs(t, err, &missing)
	require.Equal(t, "Missing", missing.Role)
	require.ErrorIs(t, err, ErrRoleNotFound)
}
