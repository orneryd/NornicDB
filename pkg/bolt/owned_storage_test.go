package bolt

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type boltOwnedTestEngine struct {
	storage.Engine
	reads, closed int
}

func (e *boltOwnedTestEngine) GetInnerEngine() storage.Engine { return e.Engine }

func (e *boltOwnedTestEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	e.reads++
	return e.Engine.GetNodesByLabel(label)
}

func (e *boltOwnedTestEngine) Close() error {
	e.closed++
	return e.Engine.Close()
}

func TestBoltRemoteResourcesFollowProtocolLifetime(t *testing.T) {
	f := newImpersonationFixture(t)
	var created []*boltOwnedTestEngine
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	manager, err := multidb.NewDatabaseManager(base, &multidb.Config{
		DefaultDatabase: "nornic", SystemDatabase: "system",
		RemoteEngineFactory: func(multidb.ConstituentRef, string) (storage.Engine, error) {
			engine := &boltOwnedTestEngine{Engine: storage.NewMemoryEngine()}
			created = append(created, engine)
			return engine, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, manager.CreateCompositeDatabase("owned_comp", []multidb.ConstituentRef{{
		Alias: "remote", DatabaseName: "peer", Type: "remote", AccessMode: "read_write", URI: "http://unused.invalid",
	}}))
	f.server.dbManager = manager
	provider := &admissionConfiguredExecutor{base: f.executor}
	f.server.executor = provider
	selection := map[string]any{"db": "owned_comp.remote"}
	for i := 0; i < 2; i++ {
		_, code := f.run("RETURN 1", selection)
		require.Empty(t, code)
		require.Len(t, created, i+1)
		require.Equal(t, 1, created[i].closed, "auto-commit releases its remote resource")
		require.Zero(t, created[i].reads, "construction does not read the remote catalog")
	}
	for _, terminal := range []string{"commit", "rollback", "reset", "timeout"} {
		require.Empty(t, f.begin(selection))
		owned := created[len(created)-1]
		require.Zero(t, owned.closed, "BEGIN retains its resource")
		_, code := f.run("RETURN 1", nil)
		require.Empty(t, code)
		require.Zero(t, owned.closed, "transaction RUN retains its resource")
		switch terminal {
		case "commit":
			require.NoError(t, f.session.handleCommit(nil))
		case "rollback":
			require.NoError(t, f.session.handleRollback(nil))
		case "reset":
			require.NoError(t, f.session.handleReset(nil))
		case "timeout":
			// Trigger the existing timer owner deterministically. No protocol
			// message should be needed after timeout cleanup to release it.
			f.session.txLifecycle.expire()
		}
		code, _ = firstFailure(f.responses())
		require.Empty(t, code, terminal)
		require.Equal(t, 1, owned.closed, terminal)
		f.reset()
		require.Equal(t, 1, owned.closed, "RESET cannot release it again")
	}
	require.Zero(t, provider.configured, "remote requests never configure a local service")
	require.Empty(t, f.server.executors, "request-owned resources are not cached")
	f.grant("svc", auth.ImpersonateAllUsers, false)
	_, code := f.run("RETURN 1", map[string]any{"db": "owned_comp.remote", "imp_user": "alice936"})
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code)
	require.Len(t, created, 6, "impersonation denial precedes the remote factory")
	f.session.authResult, err = f.adapter.Authenticate("basic", "alice936", "password123")
	require.NoError(t, err)
	_, code = f.run("CREATE (:Refused)", selection)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code)
	require.Len(t, created, 6, "statement denial precedes the remote factory")
}

func TestBoltCompositeEntityMetadataKeepsAdmittedScope(t *testing.T) {
	f := newImpersonationFixture(t)
	remoteCalls := 0
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	manager, err := multidb.NewDatabaseManager(base, &multidb.Config{
		DefaultDatabase: "nornic", SystemDatabase: "system",
		RemoteEngineFactory: func(multidb.ConstituentRef, string) (storage.Engine, error) {
			remoteCalls++
			return storage.NewMemoryEngine(), nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, manager.CreateDatabase("admitted_local"))
	require.NoError(t, manager.CreateCompositeDatabase("admitted_comp", []multidb.ConstituentRef{
		{Alias: "remote", DatabaseName: "peer", Type: "remote", AccessMode: "read_write", URI: "http://unused.invalid"},
		{Alias: "zlocal", DatabaseName: "admitted_local", Type: "local", AccessMode: "read_write"},
	}))
	local, err := manager.GetStorage("admitted_local")
	require.NoError(t, err)
	_, err = local.CreateNode(&storage.Node{ID: "admitted-node", Labels: []string{"Admitted"}})
	require.NoError(t, err)
	f.server.dbManager = manager
	f.server.executor = &admissionConfiguredExecutor{base: f.executor}
	f.server.SetDatabaseAccessModeResolver(func(roles []string) auth.DatabaseAccessMode {
		return auth.NewAllowlistDatabaseAccessMode(map[string][]string{"svc": {"admitted_comp", "admitted_local"}}, roles)
	})
	f.session.version = BoltV5_0
	query := "CALL { USE admitted_comp.zlocal MATCH (n:Admitted) RETURN n } RETURN n"
	require.NoError(t, f.session.handleRun(buildRunMessageData(query, nil, map[string]any{"db": "admitted_comp"})))
	code, _ := firstFailure(f.responses())
	require.Empty(t, code)
	require.NoError(t, f.session.handlePull(nil))
	require.Contains(t, string(f.conn.writeData), "4:admitted_local:admitted-node")
	code, _ = firstFailure(f.responses())
	require.Empty(t, code)
	require.Zero(t, remoteCalls, "entity metadata cannot open an unadmitted constituent")
}
