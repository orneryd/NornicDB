package cypher

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// remoteProcedureOwnershipFixture creates a composite database whose "remote"
// constituent is a remote alias whose endpoint counts every request. The graph
// a CALL selects owns the procedure it names, so the coordinator's preflight
// must not report the local registry's ProcedureNotFound for the remote one.
// It returns an executor on the default database and one on the composite root,
// which is where a dynamic selection is legal.
func remoteProcedureOwnershipFixture(t *testing.T) (session, composite *StorageExecutor, requests *atomic.Int64) {
	t.Helper()
	requests = &atomic.Int64{}
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNotImplemented)
	}))
	t.Cleanup(endpoint.Close)
	config := multidb.DefaultConfig()
	config.RemoteEngineFactory = func(ref multidb.ConstituentRef, authToken string) (storage.Engine, error) {
		return storage.NewRemoteEngine(storage.RemoteEngineConfig{
			URI: ref.URI, Database: ref.DatabaseName, AuthToken: authToken,
			User: ref.User, Password: ref.Password,
		})
	}
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	mgr, err := multidb.NewDatabaseManager(t.Context(), base, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mgr.Close()) })
	require.NoError(t, mgr.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "shard_one"}))
	require.NoError(t, mgr.CreateCompositeDatabase(t.Context(), "comp", []multidb.ConstituentRef{
		{Alias: "one", DatabaseName: "shard_one", Type: "local", AccessMode: "read_write"},
		{Alias: "remote", DatabaseName: "remote_one", Type: "remote", AccessMode: "read", URI: endpoint.URL},
	}))
	adapter := &testDatabaseManagerAdapter{manager: mgr}
	return newCompositeTestExecutor(t, mgr, mgr.DefaultDatabaseName(), adapter),
		newCompositeTestExecutor(t, mgr, "comp", adapter),
		requests
}

// TestFabricPreflightParticipantProcedureOwnership checks which owner decides a
// procedure's existence and implicit signature. A CALL's USE clause names the
// graph that runs it: an implicit signature the coordinator cannot read from
// its own registry is deferred to that participant, while the local selection
// keeps the local registry's ProcedureNotFound and every scope diagnostic the
// local owner can establish stays a statement error.
func TestFabricPreflightParticipantProcedureOwnership(t *testing.T) {
	session, composite, requests := remoteProcedureOwnershipFixture(t)
	contacted := requests.Load()
	for _, tc := range []struct {
		name, statement, message string
		dynamic                  bool
	}{
		{"remote/implicit-signature", "USE comp.remote CALL security_test.unknownWrite()", "", false},
		{"remote/declared-yields", "USE comp.remote CALL security_test.unknownWrite() YIELD kept RETURN kept", "", false},
		{"remote/dynamic-selection", "USE graph.byName($selectedGraph) CALL security_test.unknownWrite()", "", true},
		{"remote/known-procedure", "USE comp.remote CALL db.labels() YIELD label RETURN label", "", false},
		{"remote/undefined-variable", "USE comp.remote CALL security_test.unknownWrite() YIELD kept RETURN missing", "Variable `missing` not defined", false},
		{"remote/invalid-import", "USE comp.remote CALL (missing) { RETURN 1 AS kept } RETURN kept", "Variable `missing` not defined", false},
		{"local/implicit-signature", "USE comp.one CALL security_test.unknownWrite()", "Neo.ClientError.Procedure.ProcedureNotFound", false},
		{"local/known-procedure", "USE comp.one CALL db.labels() YIELD label RETURN label", "", false},
		{"unselected/implicit-signature", "CALL security_test.unknownWrite()", "Neo.ClientError.Procedure.ProcedureNotFound", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := session
			if tc.dynamic {
				exec = composite
			}
			params := map[string]interface{}{"selectedGraph": "comp.remote"}
			err := exec.validateFabricSemanticScopes(t.Context(), tc.statement, params)
			if tc.message == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.message)
			}
		})
	}
	require.Equal(t, contacted, requests.Load(), "the preflight admits or rejects a participant-owned procedure without contacting it")
}

// TestReadOnlyRemoteAliasRejectsOpaqueProcedure checks the executed outcome of
// the deferral: the selected read-only constituent's authorization owner refuses
// the statement before the participant is contacted or changes anything.
func TestReadOnlyRemoteAliasRejectsOpaqueProcedure(t *testing.T) {
	session, composite, requests := remoteProcedureOwnershipFixture(t)
	contacted := requests.Load()
	for _, tc := range []struct {
		statement string
		dynamic   bool
	}{
		{"USE comp.remote CALL security_test.unknownWrite()", false},
		{"USE comp.remote CALL security_test.unknownWrite() YIELD kept RETURN kept", false},
		{"USE graph.byName($selectedGraph) CALL security_test.unknownWrite()", true},
	} {
		exec := session
		if tc.dynamic {
			exec = composite
		}
		_, err := exec.Execute(t.Context(), tc.statement, map[string]interface{}{"selectedGraph": "comp.remote"})
		require.Error(t, err, tc.statement)
		require.Contains(t, err.Error(), "read-only", tc.statement)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Security.Forbidden", code, tc.statement)
	}
	require.Equal(t, contacted, requests.Load(), "authorization precedes participant contact")
}

// TestLocalParticipantOwnsDeclaredYieldProcedure checks that a CALL with
// declared yields stays locally bindable whatever its selection, so the graph
// that runs it reports its own registry's answer instead of the coordinator's.
func TestLocalParticipantOwnsDeclaredYieldProcedure(t *testing.T) {
	session, _, requests := remoteProcedureOwnershipFixture(t)
	contacted := requests.Load()
	for _, statement := range []string{
		"USE comp.one CALL security_test.unknownWrite() YIELD kept RETURN kept",
		"USE comp.one CALL security_test.unknownWrite()",
	} {
		_, err := session.Execute(t.Context(), statement, nil)
		require.Error(t, err, statement)
		require.Contains(t, err.Error(), "Neo.ClientError.Procedure.ProcedureNotFound", statement)
	}
	require.Equal(t, contacted, requests.Load(), "a local constituent answers without a remote participant")
}
