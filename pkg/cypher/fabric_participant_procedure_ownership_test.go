package cypher

import (
	"context"
	"encoding/json"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
//
// The optional handler answers the endpoint's requests for a fixture that
// exercises an executed participant statement. Without one the endpoint reports
// 501 Not Implemented, as a participant that supports no statement does.
func remoteProcedureOwnershipFixture(t *testing.T, remoteHandler ...http.HandlerFunc) (session, composite *StorageExecutor, requests *atomic.Int64) {
	t.Helper()
	requests = &atomic.Int64{}
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if len(remoteHandler) > 0 && remoteHandler[0] != nil {
			remoteHandler[0](w, r)
			return
		}
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

// remoteProcedureOracle is the fixture's remote participant. It records every
// statement the coordinator asks it to run, acknowledges the explicit
// transaction the HTTP transport opens and commits around that statement, and
// answers only the procedure this participant owns. A statement it does not
// own, or one whose arity it does not support, gets the participant's own error
// instead of a row set, so a coordinator that sends anything else cannot pass as
// a participant that ran the call.
type remoteProcedureOracle struct {
	t          *testing.T
	mu         sync.Mutex
	statements []string
}

// remoteEnrichArguments reports the argument texts of the custom.enrich call a
// participant statement names and whether it names that procedure at all. The
// fixture's participant declares only the calls this scanner can see, so the
// count is the participant's own signature evidence.
func remoteEnrichArguments(ctx context.Context, statement string) ([]string, bool) {
	syntax, err := cypherantlr.ParseSyntax(ctx, statement, cypherantlr.StatementEntry, false, cypherantlr.AcceptedSource)
	if err != nil {
		return nil, false
	}
	var arguments []string
	found := false
	syntax.Walk(func(node cypherantlr.SyntaxNode) func() {
		switch node.Rule() {
		case cypherantlr.CypherParserRULE_queryCallSt, cypherantlr.CypherParserRULE_standaloneCall:
			if node.ChildRule(cypherantlr.CypherParserRULE_invocationName).QualifiedName() == "custom.enrich" {
				arguments = procedureArgumentTextsFromSyntax(node)
				found = true
			}
		}
		return nil
	})
	return arguments, found
}

// remoteProcedureCatalogRead reports the coordinator's request for the procedure
// records a graph persists. This participant publishes none.
func remoteProcedureCatalogRead(statement string) bool {
	return strings.Contains(statement, "MATCH (n:`_ProcedureCatalog`) RETURN n")
}

func (oracle *remoteProcedureOracle) record(statement string) {
	oracle.mu.Lock()
	defer oracle.mu.Unlock()
	oracle.statements = append(oracle.statements, statement)
}

// received returns the statements the participant was asked to run, oldest
// first.
func (oracle *remoteProcedureOracle) received() []string {
	oracle.mu.Lock()
	defer oracle.mu.Unlock()
	return append([]string(nil), oracle.statements...)
}

// aritiesSince reports the argument counts of the custom.enrich calls the
// participant was asked to run after the first skipped statements.
func (oracle *remoteProcedureOracle) aritiesSince(first int) []int {
	received := oracle.received()
	if first > len(received) {
		return nil
	}
	var arities []int
	for _, statement := range received[first:] {
		if arguments, owned := remoteEnrichArguments(oracle.t.Context(), statement); owned {
			arities = append(arities, len(arguments))
		}
	}
	return arities
}

func (oracle *remoteProcedureOracle) respondError(w http.ResponseWriter, status int, code, message string) {
	encoded, err := json.Marshal(message)
	if err != nil {
		oracle.t.Errorf("remote participant could not encode its error: %v", err)
		return
	}
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"results":[],"errors":[{"code":"` + code + `","message":` + string(encoded) + `}]}`))
}

func (oracle *remoteProcedureOracle) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		oracle.respondError(w, http.StatusMethodNotAllowed, "Neo.ClientError.Request.Invalid", "the participant endpoint accepts POST requests only")
		return
	}
	var request struct {
		Statements []struct {
			Statement string `json:"statement"`
		} `json:"statements"`
	}
	body, err := io.ReadAll(r.Body)
	if err == nil {
		err = json.Unmarshal(body, &request)
	}
	if err != nil {
		oracle.t.Errorf("remote participant could not read its request: %v", err)
		oracle.respondError(w, http.StatusBadRequest, "Neo.ClientError.Request.Invalid", err.Error())
		return
	}
	for _, statement := range request.Statements {
		oracle.record(statement.Statement)
	}
	if len(request.Statements) == 0 {
		if strings.HasSuffix(r.URL.Path, "/commit") {
			_, _ = w.Write([]byte(`{"results":[],"errors":[]}`))
			return
		}
		// The coordinator's explicit transaction handshake names the commit
		// resource its statement and commit requests follow.
		_, _ = w.Write([]byte(`{"commit":"http://` + r.Host + strings.TrimSuffix(r.URL.Path, "/") + `/commit","errors":[]}`))
		return
	}
	results := make([]string, 0, len(request.Statements))
	for _, statement := range request.Statements {
		if remoteProcedureCatalogRead(statement.Statement) {
			// The coordinator's composite view asks each graph for the procedure
			// records it persists. This participant publishes none, so the
			// coordinator cannot read its signatures before the call runs.
			results = append(results, `{"columns":["n"],"data":[]}`)
			continue
		}
		arguments, owned := remoteEnrichArguments(oracle.t.Context(), statement.Statement)
		if !owned || len(arguments) < 1 || len(arguments) > 2 {
			oracle.t.Errorf("remote participant received a statement it does not own: %q", statement.Statement)
			oracle.respondError(w, http.StatusBadRequest, "Neo.ClientError.Procedure.ProcedureNotFound", "the participant has no procedure for "+statement.Statement)
			return
		}
		results = append(results, `{"columns":["value"],"data":[{"row":[42],"meta":[null]}]}`)
	}
	_, _ = w.Write([]byte(`{"results":[` + strings.Join(results, ",") + `],"errors":[]}`))
}

// TestFabricParticipantOwnsKnownProcedureArity checks that a procedure the
// coordinator registered locally still has its arity admitted by the graph its
// CALL selects. The selected remote participant supports a two-argument call
// that the local registration's maximum arity would reject, so the coordinator
// must leave the call to that participant instead of reporting the local
// maximum. The one-argument remote call is the control that the participant
// runs and answers the statement at all, and the local selection keeps the
// local registration's arity without contacting any remote.
func TestFabricParticipantOwnsKnownProcedureArity(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		var localCalls atomic.Int64
		registerProcedureForTest(t, ProcedureSpec{
			Name:      "custom.enrich",
			Signature: "custom.enrich(name :: STRING) :: (value :: INTEGER)",
			Mode:      ProcedureModeRead,
			MinArgs:   1,
			MaxArgs:   1,
			Params:    []ProcedureParam{{Name: "name", Type: "STRING"}},
			Returns:   []ProcedureColumn{{Name: "value", Type: "INTEGER"}},
		}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
			localCalls.Add(1)
			return &ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{{int64(7)}}}, nil
		})
		oracle := &remoteProcedureOracle{t: t}
		session, _, requests := remoteProcedureOwnershipFixture(t, oracle.ServeHTTP)

		t.Run("remote one-argument control", func(t *testing.T) {
			contacted := requests.Load()
			seen := len(oracle.received())
			result, err := session.Execute(t.Context(),
				"USE comp.remote CALL custom.enrich('alice') YIELD value RETURN value", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(42)}}, result.Rows)
			require.Greater(t, requests.Load(), contacted, "the selected remote participant runs the call")
			require.Equal(t, []int{1}, oracle.aritiesSince(seen), "the participant ran the one-argument call")
		})

		t.Run("remote two-argument call is the participant's", func(t *testing.T) {
			contacted := requests.Load()
			seen := len(oracle.received())
			result, err := session.Execute(t.Context(),
				"USE comp.remote CALL custom.enrich('alice', {}) YIELD value RETURN value", nil)
			require.NoError(t, err,
				"the coordinator's local registration must not decide the selected participant's arity")
			require.Equal(t, [][]interface{}{{int64(42)}}, result.Rows)
			require.Greater(t, requests.Load(), contacted, "the selected remote participant runs the call")
			require.Equal(t, []int{2}, oracle.aritiesSince(seen),
				"the participant received the two-argument call the coordinator's registry rejects")
		})

		t.Run("participant-owned call keeps its scope errors", func(t *testing.T) {
			contacted := requests.Load()
			seen := len(oracle.received())
			_, err := session.Execute(t.Context(),
				"USE comp.remote CALL custom.enrich('alice', {}) YIELD value RETURN missing", nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), "Variable `missing` not defined")
			require.Equal(t, contacted, requests.Load(), "the scope error precedes participant dispatch")
			require.Empty(t, oracle.aritiesSince(seen), "the participant runs nothing the scope check rejects")
		})

		t.Run("local valid call uses the local registration", func(t *testing.T) {
			contacted := requests.Load()
			result, err := session.Execute(t.Context(), "USE comp.one CALL custom.enrich('alice') YIELD value RETURN value", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
			require.Equal(t, contacted, requests.Load())
		})

		t.Run("local two-argument call keeps the local registration", func(t *testing.T) {
			contacted := requests.Load()
			_, err := session.Execute(t.Context(),
				"USE comp.one CALL custom.enrich('alice', {}) YIELD value RETURN value", nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), "accepts at most 1 arguments, got 2")
			requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
			require.Equal(t, contacted, requests.Load(), "a local participant answers without contacting a remote")
		})

		require.EqualValues(t, 1, localCalls.Load(), "only the explicitly local valid call runs the local registration")
	})
}
