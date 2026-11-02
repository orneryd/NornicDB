package auth

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// noteWrite records a mutation the decorated engine was asked for after its
// caller's context was canceled: the fixtures' witness that no record change
// starts once the caller is gone.
func noteWrite(writes *[]string, canceled bool, operation string, id storage.NodeID) {
	if canceled {
		*writes = append(*writes, operation+" "+string(id))
	}
}

// recordScanProbe is shared with the decorator's transaction view, so the
// test observes the real storage calls the role change makes after the
// caller's context is canceled.
type recordScanProbe struct {
	cancel   context.CancelFunc
	armed    bool
	canceled bool
	writes   []string
}

// recordScanCancelEngine decorates the real system engine and cancels the
// caller's context at the first role-record label read, before that read
// reaches the decorated engine. It injects no error: the canceled context is
// the only signal, and the real storage admission is where the operation must
// observe it.
type recordScanCancelEngine struct {
	storage.Engine
	probe *recordScanProbe
}

func (e *recordScanCancelEngine) GetNodesByLabel(ctx context.Context, label string) ([]*storage.Node, error) {
	if e.probe.armed {
		e.probe.armed = false
		e.probe.cancel()
		e.probe.canceled = true
	}
	return e.Engine.GetNodesByLabel(ctx, label)
}

func (e *recordScanCancelEngine) CreateNode(ctx context.Context, node *storage.Node) (storage.NodeID, error) {
	noteWrite(&e.probe.writes, e.probe.canceled, "create", node.ID)
	return e.Engine.CreateNode(ctx, node)
}

func (e *recordScanCancelEngine) DeleteNode(ctx context.Context, id storage.NodeID) error {
	noteWrite(&e.probe.writes, e.probe.canceled, "delete", id)
	return e.Engine.DeleteNode(ctx, id)
}

func (e *recordScanCancelEngine) UpdateNode(ctx context.Context, request storage.NodeUpdateRequest) error {
	id := storage.NodeID("")
	if request.After != nil {
		id = request.After.ID
	}
	noteWrite(&e.probe.writes, e.probe.canceled, "update", id)
	return e.Engine.UpdateNode(ctx, request)
}

// WithStoreTransaction keeps the decorated engine's real transaction and hands
// the staged view to the same decorator.
func (e *recordScanCancelEngine) WithStoreTransaction(ctx context.Context, mutate func(storage.Engine) error) error {
	return WithStoreTransaction(ctx, e.Engine, func(view storage.Engine) error {
		staged := *e
		staged.Engine = view
		return mutate(&staged)
	})
}

// postReadCancelProbe cancels the caller's context immediately after one real
// label read returned, and records what that native read returned and which
// mutations the decorated engine is asked for once the caller's context is
// canceled.
type postReadCancelProbe struct {
	cancel     context.CancelFunc
	trigger    string
	armed      bool
	canceled   bool
	nativeRows int
	writes     []string
}

// postReadCancelEngine decorates the real engine and returns a label read's
// own rows and error unchanged; only after that read succeeded does it cancel
// the caller's context. The cancellation is therefore visible to nobody but
// the caller's own context: whatever must stop here has to poll it.
type postReadCancelEngine struct {
	storage.Engine
	probe *postReadCancelProbe
}

func (e *postReadCancelEngine) GetNodesByLabel(ctx context.Context, label string) ([]*storage.Node, error) {
	nodes, err := e.Engine.GetNodesByLabel(ctx, label)
	if e.probe.armed && label == e.probe.trigger && err == nil {
		e.probe.armed = false
		e.probe.canceled = true
		e.probe.nativeRows = len(nodes)
		e.probe.cancel()
	}
	return nodes, err
}

func (e *postReadCancelEngine) CreateNode(ctx context.Context, node *storage.Node) (storage.NodeID, error) {
	noteWrite(&e.probe.writes, e.probe.canceled, "create", node.ID)
	return e.Engine.CreateNode(ctx, node)
}

func (e *postReadCancelEngine) DeleteNode(ctx context.Context, id storage.NodeID) error {
	noteWrite(&e.probe.writes, e.probe.canceled, "delete", id)
	return e.Engine.DeleteNode(ctx, id)
}

func (e *postReadCancelEngine) UpdateNode(ctx context.Context, request storage.NodeUpdateRequest) error {
	id := storage.NodeID("")
	if request.After != nil {
		id = request.After.ID
	}
	noteWrite(&e.probe.writes, e.probe.canceled, "update", id)
	return e.Engine.UpdateNode(ctx, request)
}

// WithStoreTransaction keeps the decorated engine's real transaction and hands
// the staged view to the same decorator.
func (e *postReadCancelEngine) WithStoreTransaction(ctx context.Context, mutate func(storage.Engine) error) error {
	return WithStoreTransaction(ctx, e.Engine, func(view storage.Engine) error {
		staged := *e
		staged.Engine = view
		return mutate(&staged)
	})
}

// seedContextRole stores one role with one boosted grant for the
// cancellation-before-read fixture.
func seedContextRole(t *testing.T) storage.Engine {
	t.Helper()
	ctx := context.Background()
	system := newSystemEngine(t)
	require.NoError(t, NewRoleStore(system).CreateRole(ctx, "ops"))
	require.NoError(t, NewProcedurePrivilegeStore(system).Grant(ctx, "ops", "db.cdc.*", true, false))
	return system
}

// roleSeedRecords maps each record seedRoleRename stores for role ops to the
// record a rename moves it to.
var roleSeedRecords = map[storage.NodeID]storage.NodeID{
	"role:ops":                       "role:renamed",
	"proc_priv:boosted:ops:db.cdc.*": "proc_priv:boosted:renamed:db.cdc.*",
	"proc_priv:execute:ops:db.a":     "proc_priv:execute:renamed:db.a",
	"db_priv:ops:nornic":             "db_priv:renamed:nornic",
	"role_db_access:ops":             "role_db_access:renamed",
	"role_entitlement:ops":           "role_entitlement:renamed",
}

// A list runs on the caller's context: whatever the caller's cancellation
// timing, a canceled list returns its cancellation and no partial grants. A
// read that fails keeps its own error, and a healthy list is complete and in
// role, procedure, plain-before-boosted order.
func TestProcedurePrivilegeStoreListUsesCallerContext(t *testing.T) {
	ctx := context.Background()
	system := newSystemEngine(t)
	store := NewProcedurePrivilegeStore(system)
	require.NoError(t, store.Grant(ctx, "ops", "db.cdc.*", true, false))
	require.NoError(t, store.Grant(ctx, "alpha", "db.b", true, false))
	require.NoError(t, store.Grant(ctx, "alpha", "db.b", false, false))
	sorted := []ProcedurePrivilege{
		{Role: "alpha", Procedure: "db.b", Boosted: false},
		{Role: "alpha", Procedure: "db.b", Boosted: true},
		{Role: "ops", Procedure: "db.cdc.*", Boosted: true},
	}

	grants, err := store.List(ctx)
	require.NoError(t, err)
	require.Equal(t, sorted, grants)

	// A live cancellable caller context orders the same grants the same way:
	// watching the caller's lifetime changes no healthy list.
	liveCtx, liveCancel := context.WithCancel(ctx)
	defer liveCancel()
	grants, err = store.List(liveCtx)
	require.NoError(t, err)
	require.Equal(t, sorted, grants)

	// Canceled before the read: the real storage read reports it.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	grants, err = store.List(canceled)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, grants, "a canceled list returns no partial grants")

	// The decorator is transparent: an unarmed one lists exactly the same
	// grants as the undecorated store.
	transparent := NewProcedurePrivilegeStore(&postReadCancelEngine{Engine: system, probe: &postReadCancelProbe{}})
	grants, err = transparent.List(ctx)
	require.NoError(t, err)
	require.Equal(t, sorted, grants)

	// Canceled immediately after the real label read returned every grant
	// node: that read succeeded, so only the caller's own context reports the
	// cancellation, and the store must stop before it converts and sorts the
	// whole population. It reports the cancellation, never the rows it had.
	postCtx, postCancel := context.WithCancel(ctx)
	defer postCancel()
	probe := &postReadCancelProbe{cancel: postCancel, trigger: procedurePrivilegeLabel, armed: true}
	grants, err = NewProcedurePrivilegeStore(&postReadCancelEngine{Engine: system, probe: probe}).List(postCtx)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, probe.canceled, "the grant label read reached the decorated real storage")
	require.Equal(t, 3, probe.nativeRows, "the decorated read returned every stored grant node")
	require.Nil(t, grants, "a list canceled after the read returns no partial grants")

	// A read that fails is its own error, not the fixture's cancellation.
	unreadable := storagefault.NewMutationFailureEngine(system, storagefault.MutationScan, errPrivilegeStorage, 0, WithStoreTransaction)
	grants, err = NewProcedurePrivilegeStore(unreadable).List(ctx)
	require.ErrorIs(t, err, errPrivilegeStorage)
	require.Nil(t, grants, "an unreadable store returns no partial grants")

	faultCtx, faultCancel := context.WithCancel(ctx)
	defer faultCancel()
	faultProbe := &postReadCancelProbe{cancel: faultCancel, trigger: procedurePrivilegeLabel, armed: true}
	grants, err = NewProcedurePrivilegeStore(&postReadCancelEngine{Engine: unreadable, probe: faultProbe}).List(faultCtx)
	require.ErrorIs(t, err, errPrivilegeStorage)
	require.Nil(t, grants)
	require.False(t, faultProbe.canceled, "a failed native read keeps its own error")
}

// Rename and deletion share one record enumerator: it reads and converts every
// role record on the caller's context. A healthy change moves (rename) or
// removes (deletion) exactly the records it enumerated, and a caller canceled
// after the real record reads returns applies no record change.
func TestRoleMutationUsesCallerContextAtRecordScan(t *testing.T) {
	ctx := context.Background()

	t.Run("healthy rename", func(t *testing.T) {
		system := newSystemEngine(t)
		seedRoleRename(t, system)
		roles := NewRoleStore(&postReadCancelEngine{Engine: system, probe: &postReadCancelProbe{}})
		require.NoError(t, roles.RenameRoleWithStores(ctx, "ops", "renamed", RoleMutationStores{}))
		require.True(t, roles.Exists("renamed"))
		require.False(t, roles.Exists("ops"))
		for from, to := range roleSeedRecords {
			_, err := system.GetNode(context.Background(), to)
			require.NoError(t, err, "rename moves %s", from)
			_, err = system.GetNode(context.Background(), from)
			require.ErrorIs(t, err, storage.ErrNotFound, "rename leaves no %s", from)
		}
		user, err := system.GetNode(context.Background(), "user:worker")
		require.NoError(t, err)
		require.Equal(t, `["renamed","viewer"]`, user.Properties["roles"])
	})

	t.Run("healthy delete", func(t *testing.T) {
		system := newSystemEngine(t)
		seedRoleRename(t, system)
		roles := NewRoleStore(&postReadCancelEngine{Engine: system, probe: &postReadCancelProbe{}})
		require.NoError(t, roles.DeleteRoleWithStores(ctx, "ops", RoleMutationStores{}))
		require.False(t, roles.Exists("ops"))
		for from := range roleSeedRecords {
			_, err := system.GetNode(context.Background(), from)
			require.ErrorIs(t, err, storage.ErrNotFound, "deletion removes %s", from)
		}
		user, err := system.GetNode(context.Background(), "user:worker")
		require.NoError(t, err)
		require.Equal(t, `["viewer"]`, user.Properties["roles"], "deletion detaches the role from a shared user record")
	})

	t.Run("canceled before the record read", func(t *testing.T) {
		system := seedContextRole(t)
		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		probe := &recordScanProbe{cancel: cancel}
		roles := NewRoleStore(&recordScanCancelEngine{Engine: system, probe: probe})
		probe.armed = true
		err := roles.RenameRoleWithStores(runCtx, "ops", "renamed", RoleMutationStores{})
		require.ErrorIs(t, err, context.Canceled)
		require.True(t, probe.canceled, "the role-record scan reached the decorated real storage")
		require.Empty(t, probe.writes, "a canceled role change stops at the storage reader, before any record mutation")

		reloaded := NewRoleStore(system)
		require.NoError(t, reloaded.Load(ctx))
		require.True(t, reloaded.Exists("ops"))
		require.False(t, reloaded.Exists("renamed"))
		grant, found, err := NewProcedurePrivilegeStore(system).Lookup(ctx, "ops", "db.cdc.*", true)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "ops", grant.Role)
		_, found, err = NewProcedurePrivilegeStore(system).Lookup(ctx, "renamed", "db.cdc.*", true)
		require.NoError(t, err)
		require.False(t, found, "a canceled rename moves no grant")
	})

	// Canceled immediately after the last real record read returned: every
	// label read succeeded, so only the caller's own context reports the
	// cancellation, and the enumerator must stop before applying any record.
	for _, remove := range []bool{false, true} {
		t.Run(map[bool]string{false: "canceled after the record reads", true: "canceled after the record reads, delete"}[remove], func(t *testing.T) {
			system := newSystemEngine(t)
			seedRoleRename(t, system)
			before, err := system.AllNodes(ctx)
			require.NoError(t, err)

			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			probe := &postReadCancelProbe{cancel: cancel, trigger: roleLabel, armed: true}
			roles := NewRoleStore(&postReadCancelEngine{Engine: system, probe: probe})
			if remove {
				err = roles.DeleteRoleWithStores(runCtx, "ops", RoleMutationStores{})
			} else {
				err = roles.RenameRoleWithStores(runCtx, "ops", "renamed", RoleMutationStores{})
			}
			require.ErrorIs(t, err, context.Canceled)
			require.True(t, probe.canceled, "the last role-record label read reached the decorated real storage")
			require.Equal(t, 1, probe.nativeRows, "that real read returned the role node")
			require.Empty(t, probe.writes, "a canceled role change applies no record change")

			after, err := system.AllNodes(ctx)
			require.NoError(t, err)
			require.ElementsMatch(t, before, after, "a canceled role change leaves every record in place")
		})
	}
}
