package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

const matrixSecondWrite = "Writing to more than one database per transaction is not allowed"

// matrixWritePaths are the ways a fragment on one constituent writes. The
// write is visible in the text only for the first two; the others write
// through a procedure, dynamic Cypher or batches, and are known only when
// they write: user_read_runner is a READ user procedure whose body runs the
// statement it is given, defined the same way on this server and on the
// remote. LOAD CSV is not among them: NornicDB refuses it.
var matrixWritePaths = []struct{ name, body string }{
	{"cypher", "CREATE (:AcAtom {run: $run}) RETURN 1 AS w"},
	{"union_branch", "CREATE (:AcAtom {run: $run}) RETURN 1 AS w UNION ALL USE mxcmp.two MATCH (m:AcMissing) RETURN 0 AS w"},
	{"write_procedure", "CALL acatom.write($run) YIELD w RETURN w"},
	{"apoc_doit", "CALL apoc.cypher.doIt($body, {run: $run}) YIELD value RETURN value.w AS w"},
	{"apoc_run", "CALL apoc.cypher.run($body, {run: $run}) YIELD value RETURN value.w AS w"},
	{"subquery", "CALL { CREATE (:AcAtom {run: $run}) RETURN 1 AS c } RETURN c AS w"},
	{"periodic", "CALL apoc.periodic.iterate($iterate, 'CREATE (:AcAtom {run: $r})', {batchSize: 1}) YIELD total RETURN total AS w"},
	{"user_read_runner", "CALL acatom.runner($body, $run) YIELD w RETURN w"},
	{"in_transactions", "UNWIND [1] AS i CALL { WITH i CALL apoc.cypher.run($body, {run: $run}) YIELD value RETURN value.w AS w } IN TRANSACTIONS OF 1 ROW RETURN w"},
}

// A composite statement writes to one database, however the write is made
// (matrixWritePaths), on a local constituent or a remote one (a NornicDB
// server reached over Bolt or HTTP), and it commits all or nothing:
//
//   - success: the write persists;
//   - failure: a later runtime error leaves nothing;
//   - second_write_after / second_write_before: a write to another database
//     is refused with Neo.ClientError.Statement.AccessMode and leaves nothing;
//   - writer_commit_failure: the writer's failed commit is the statement's
//     error and leaves nothing;
//   - reader_commit_failure: a read-only participant that fails to commit
//     fails the statement before the writer commits, which leaves nothing.
//
// CALL { ... } IN TRANSACTIONS commits its batches on their own, as on a
// single database: a batch committed before the statement fails stays. Every
// case runs through the Cypher executor, a Bolt client and the HTTP API.
func TestCompositeWriteParticipantMatrix(t *testing.T) {
	ctx := context.Background()
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")

	// The remote: another NornicDB server, over Bolt and over HTTP (through a
	// proxy that can fail its transactions' COMMIT).
	remote, remoteAuth := setupTestServer(t)
	remoteToken := "Bearer " + getAuthToken(t, remoteAuth, "admin")
	// failCommit fails the end of the remote's transactions: a write
	// session's COMMIT, and a read session's ROLLBACK, which is how it ends.
	var failCommit atomic.Bool
	explicitCommit := regexp.MustCompile(`/tx/[^/]+/commit$`)
	explicitTx := regexp.MustCompile(`/tx/[^/]+$`)
	remoteRouter := remote.buildRouter()
	remoteHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failCommit.Load() && (r.Method == http.MethodPost && explicitCommit.MatchString(r.URL.Path) || r.Method == http.MethodDelete && explicitTx.MatchString(r.URL.Path)) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"results":[],"errors":[{"code":"Neo.TransientError.Transaction.Terminated","message":"injected commit failure"}]}`))
			return
		}
		// The proxy signs every request in, whichever client route sent it.
		r.Header.Set("Authorization", remoteToken)
		remoteRouter.ServeHTTP(w, r)
	}))
	t.Cleanup(remoteHTTP.Close)
	remoteBolt := matrixBoltServer(t, remote)

	require.NoError(t, server.dbManager.CreateDatabase("mxone"))
	require.NoError(t, server.dbManager.CreateDatabase("mxtwo"))
	require.NoError(t, server.dbManager.CreateCompositeDatabase("mxcmp", []multidb.ConstituentRef{
		{Alias: "one", DatabaseName: "mxone", Type: "local", AccessMode: "read_write"},
		{Alias: "two", DatabaseName: "mxtwo", Type: "local", AccessMode: "read_write"},
		{Alias: "nbolt", DatabaseName: "nornic", Type: "remote", AccessMode: "read_write", URI: remoteBolt},
		{Alias: "nhttp", DatabaseName: "nornic", Type: "remote", AccessMode: "read_write", URI: remoteHTTP.URL},
	}))

	post := func(target *Server, auth, path, statement string, params map[string]any) TransactionResponse {
		rec := makeRequest(t, target, http.MethodPost, path, map[string]any{"statements": []map[string]any{{"statement": statement, "parameters": params}}}, auth)
		var resp TransactionResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp), rec.Body.String())
		return resp
	}
	require.Empty(t, post(server, token, "/db/mxone/tx/commit", "CREATE OR REPLACE PROCEDURE acatom.write($run) MODE WRITE AS CREATE (:AcAtom {run: $run}) RETURN 1 AS w", nil).Errors)
	t.Cleanup(func() { post(server, token, "/db/mxone/tx/commit", "DROP PROCEDURE acatom.write", nil) })
	for _, definer := range []struct {
		srv        *Server
		auth, path string
	}{{server, token, "/db/mxone/tx/commit"}, {remote, remoteToken, "/db/nornic/tx/commit"}} {
		require.Empty(t, post(definer.srv, definer.auth, definer.path, "CREATE OR REPLACE PROCEDURE acatom.runner($stmt, $run) MODE READ AS CALL apoc.cypher.run($stmt, {run: $run}) YIELD value RETURN value.w AS w", nil).Errors)
	}
	t.Cleanup(func() { post(server, token, "/db/mxone/tx/commit", "DROP PROCEDURE acatom.runner", nil) })

	count := func(participant, run string) float64 {
		var resp TransactionResponse
		params := map[string]any{"run": run}
		if participant == "nbolt" || participant == "nhttp" {
			resp = post(remote, remoteToken, "/db/nornic/tx/commit", "MATCH (n:AcAtom {run: $run}) RETURN count(n) AS c", params)
		} else {
			resp = post(server, token, "/db/mx"+participant+"/tx/commit", "MATCH (n:AcAtom {run: $run}) RETURN count(n) AS c", params)
		}
		require.Empty(t, resp.Errors)
		return resp.Results[0].Data[0].Row[0].(float64)
	}

	clientBolt := matrixBoltServer(t, server)
	driver, err := neo4j.NewDriverWithContext(clientBolt, neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(ctx) })
	cmpStore, err := server.dbManager.GetStorage("mxcmp")
	require.NoError(t, err)
	executor := cypher.NewStorageExecutor(cmpStore)
	executor.SetDatabaseManager(&databaseManagerAdapter{manager: server.dbManager})

	routes := map[string]func(statement string, params map[string]any) error{
		"executor": func(statement string, params map[string]any) error {
			_, err := executor.Execute(ctx, statement, params)
			return err
		},
		"bolt": func(statement string, params map[string]any) error {
			session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "mxcmp"})
			defer func() { _ = session.Close(ctx) }()
			result, err := session.Run(ctx, statement, params)
			if err == nil {
				_, err = result.Consume(ctx)
			}
			return err
		},
		"http": func(statement string, params map[string]any) error {
			resp := post(server, token, "/db/mxcmp/tx/commit", statement, params)
			if len(resp.Errors) > 0 {
				return fmt.Errorf("%s: %s", resp.Errors[0].Code, resp.Errors[0].Message)
			}
			return nil
		},
	}

	type outcome struct {
		statement  string
		failCommit bool
		err        string // "" for success, matrixSecondWrite, or "any"
		left       int64  // nodes left on the participant
	}
	outcomes := func(participant, body string, batches bool) map[string]outcome {
		fragment := "CALL { USE mxcmp." + participant + " " + body + " }"
		other := "CALL { WITH w USE mxcmp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS z }"
		committedBatch := int64(0)
		if batches {
			committedBatch = 1 // committed before the statement failed
		}
		cases := map[string]outcome{
			"success":             {statement: "USE mxcmp " + fragment + " RETURN w", left: 1},
			"failure":             {statement: "USE mxcmp " + fragment + " CALL { USE mxcmp.two RETURN 1 / $d AS z } RETURN w, z", err: "any", left: committedBatch},
			"second_write_after":  {statement: "USE mxcmp " + fragment + " " + other + " RETURN z", err: matrixSecondWrite, left: committedBatch},
			"second_write_before": {statement: "USE mxcmp CALL { USE mxcmp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS z } " + fragment + " RETURN z, w", err: matrixSecondWrite},
		}
		if participant == "nhttp" {
			cases["writer_commit_failure"] = outcome{statement: "USE mxcmp " + fragment + " RETURN w", failCommit: true, err: "any"}
		}
		return cases
	}

	for _, path := range matrixWritePaths {
		for _, participant := range []string{"one", "nbolt", "nhttp"} {
			if path.name == "in_transactions" && participant != "one" {
				continue // a remote runs a fragment in its own transaction
			}
			for name, want := range outcomes(participant, path.body, path.name == "in_transactions") {
				for _, route := range []string{"executor", "bolt", "http"} {
					t.Run(fmt.Sprintf("%s/%s/%s/%s", path.name, participant, name, route), func(t *testing.T) {
						run := fmt.Sprintf("mx-%s-%s-%s-%s-%d", path.name, participant, name, route, time.Now().UnixNano())
						params := map[string]any{"run": run, "d": int64(0), "body": "CREATE (:AcAtom {run: $run}) RETURN 1 AS w", "iterate": "UNWIND ['" + run + "'] AS r RETURN r"}
						failCommit.Store(want.failCommit)
						err := routes[route](want.statement, params)
						failCommit.Store(false)
						switch want.err {
						case "":
							require.NoError(t, err)
						case "any":
							require.Error(t, err)
						default:
							require.ErrorContains(t, err, want.err)
						}
						require.Equal(t, float64(want.left), count(participant, run), "AcAtom nodes left on %s", participant)
						require.Equal(t, float64(0), count("two", run), "AcAtom nodes left on two")
					})
				}
			}
		}
	}

	// A read-only remote participant whose end fails (its read session's
	// rollback), in a statement whose local participant wrote: the statement
	// fails and the write is not kept,
	// whatever order the participants joined in (repeated: the order in which
	// a coordinator walks its participants must not decide it).
	readerStatements := map[string]string{
		"writer_first": "USE mxcmp CALL { USE mxcmp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { WITH w USE mxcmp.nhttp MATCH (n:AcMissing) RETURN count(n) AS r } RETURN w, r",
		"reader_first": "USE mxcmp CALL { USE mxcmp.nhttp MATCH (n:AcMissing) RETURN count(n) AS r } CALL { WITH r USE mxcmp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS w } RETURN w, r",
	}
	for order, statement := range readerStatements {
		for _, route := range []string{"executor", "bolt", "http"} {
			for attempt := 0; attempt < 8; attempt++ {
				t.Run(fmt.Sprintf("reader_commit_failure/%s/%s/%d", order, route, attempt), func(t *testing.T) {
					run := fmt.Sprintf("mx-reader-%s-%s-%d-%d", order, route, attempt, time.Now().UnixNano())
					failCommit.Store(true)
					err := routes[route](statement, map[string]any{"run": run})
					failCommit.Store(false)
					require.Error(t, err)
					require.Equal(t, float64(0), count("one", run), "a failed statement keeps its local write")
				})
			}
		}
	}
	// Procedures whose change is not a node or relationship write: a hidden
	// node (kp.suppress) and new indexes. Each is a write by its registered
	// mode, so with a write to another database the statement is refused
	// before either runs, in both orders.
	for name, call := range map[string]string{
		"kp_suppress":    "MATCH (n:AcSup {run: $run}) CALL kp.suppress(n) RETURN count(*) AS w",
		"fulltext_index": "CALL db.index.fulltext.createNodeIndex($index, ['AcAtom'], ['run']) YIELD name RETURN 1 AS w",
		"vector_index":   "CALL db.index.vector.createNodeIndex($index, 'AcAtom', 'embedding', 4, 'cosine') RETURN 1 AS w",
	} {
		for order, statement := range map[string]string{
			"procedure_first":  "USE mxcmp CALL { USE mxcmp.one " + call + " } CALL { WITH w USE mxcmp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS z } RETURN w, z",
			"procedure_second": "USE mxcmp CALL { USE mxcmp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS z } CALL { WITH z USE mxcmp.one " + call + " } RETURN w, z",
		} {
			for _, route := range []string{"executor", "bolt", "http"} {
				t.Run(fmt.Sprintf("schema_and_visibility/%s/%s/%s", name, order, route), func(t *testing.T) {
					run := fmt.Sprintf("mx-sup-%s-%s-%s-%d", name, order, route, time.Now().UnixNano())
					index := strings.ReplaceAll(run, "-", "_")
					require.Empty(t, post(server, token, "/db/mxone/tx/commit", "CREATE (:AcSup {run: $run})", map[string]any{"run": run}).Errors)
					err := routes[route](statement, map[string]any{"run": run, "index": index})
					require.ErrorContains(t, err, matrixSecondWrite)
					visible := post(server, token, "/db/mxone/tx/commit", "MATCH (n:AcSup {run: $run}) RETURN count(n) AS c", map[string]any{"run": run})
					require.Empty(t, visible.Errors)
					require.Equal(t, float64(1), visible.Results[0].Data[0].Row[0], "the node on one is not hidden")
					indexes := post(server, token, "/db/mxone/tx/commit", "SHOW INDEXES YIELD name WHERE name = $index RETURN count(*) AS c", map[string]any{"index": index})
					require.Empty(t, indexes.Errors)
					require.Equal(t, float64(0), indexes.Results[0].Data[0].Row[0], "no index on one")
					require.Equal(t, float64(0), count("two", run), "AcAtom nodes left on two")
				})
			}
		}
	}

	// kp.suppress makes one the statement's writer; the read-only remote
	// participant's commit fails, before the writer commits: the node on one
	// stays visible.
	for _, route := range []string{"executor", "bolt", "http"} {
		t.Run("writer_kept_nothing_when_reader_commit_fails/"+route, func(t *testing.T) {
			run := fmt.Sprintf("mx-sup-commit-%s-%d", route, time.Now().UnixNano())
			require.Empty(t, post(server, token, "/db/mxone/tx/commit", "CREATE (:AcSup {run: $run})", map[string]any{"run": run}).Errors)
			failCommit.Store(true)
			err := routes[route]("USE mxcmp CALL { USE mxcmp.one MATCH (n:AcSup {run: $run}) CALL kp.suppress(n) RETURN count(*) AS w } CALL { WITH w USE mxcmp.nhttp MATCH (n:AcMissing) RETURN count(n) AS r } RETURN w, r", map[string]any{"run": run})
			failCommit.Store(false)
			require.Error(t, err)
			visible := post(server, token, "/db/mxone/tx/commit", "MATCH (n:AcSup {run: $run}) RETURN count(n) AS c", map[string]any{"run": run})
			require.Empty(t, visible.Errors)
			require.Equal(t, float64(1), visible.Results[0].Data[0].Row[0], "a failed statement keeps its suppression")
		})
	}

	// Every registered procedure contract, run through a user procedure alias
	// with its declaration and a handler that only records that it ran, on a
	// local constituent and on a remote one reached over Bolt and HTTP, with
	// a write to another database before or after it. On the local
	// constituent a READ contract never claims the statement's write
	// database: the statement succeeds. A WRITE or SCHEMA contract always
	// claims it; so does any user procedure on a remote, which is the
	// remote's own definition. The claim refuses the statement when the
	// second write opens: a procedure that claims second never runs.
	const alias = "mxcontract.proc"
	var ran atomic.Bool
	contracts := map[string]int{}
	for _, contract := range cypher.ListRegisteredProcedures() {
		if contract.Name == alias {
			continue
		}
		contracts[string(contract.Mode)]++
		if contract.ExecutesStatements {
			contracts["READ, runs a statement"]++
		}
		spec := contract
		spec.Name, spec.Signature = alias, alias+"() :: (w :: INTEGER)"
		spec.Params, spec.Returns = nil, []cypher.ProcedureColumn{{Name: "w", Type: "INTEGER"}}
		spec.MinArgs, spec.MaxArgs = 0, 0
		writes := contract.Mode == cypher.ProcedureModeWrite || contract.Mode == cypher.ProcedureModeSchema
		for _, participant := range []string{"one", "nbolt", "nhttp"} {
			claims := writes || participant != "one"
			for order, statement := range map[string]string{
				"procedure_first":  "USE mxcmp CALL { USE mxcmp." + participant + " CALL mxcontract.proc() YIELD w RETURN w } CALL { WITH w USE mxcmp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS z } RETURN w, z",
				"procedure_second": "USE mxcmp CALL { USE mxcmp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS z } CALL { USE mxcmp." + participant + " CALL mxcontract.proc() YIELD w RETURN w } RETURN w, z",
			} {
				t.Run(fmt.Sprintf("registry/%s/%s/%s", contract.Name, participant, order), func(t *testing.T) {
					require.NoError(t, cypher.RegisterUserProcedure(spec, func(context.Context, *cypher.StorageExecutor, string, []interface{}) (*cypher.ExecuteResult, error) {
						ran.Store(true)
						return &cypher.ExecuteResult{Columns: []string{"w"}, Rows: [][]interface{}{{int64(1)}}}, nil
					}))
					ran.Store(false)
					run := fmt.Sprintf("mx-registry-%s-%s-%d", participant, order, time.Now().UnixNano())
					err := routes["executor"](statement, map[string]any{"run": run})
					if claims {
						require.ErrorContains(t, err, matrixSecondWrite)
						require.Equal(t, order == "procedure_first", ran.Load(), "a procedure that claims second never runs")
						require.Equal(t, float64(0), count("two", run), "AcAtom nodes left on two")
						return
					}
					require.NoError(t, err)
					require.True(t, ran.Load())
					require.Equal(t, float64(1), count("two", run), "AcAtom nodes on two")
				})
			}
		}
	}
	t.Logf("registered procedure contracts by mode: %v", contracts)

	// On a remote, a built-in procedure's mode is the remote's: a READ one
	// claims nothing, a WRITE one, or a READ one that runs a statement it is
	// given, claims the remote.
	for name, call := range map[string]struct {
		call   string
		claims bool
	}{
		"read_builtin":       {"CALL db.labels() YIELD label RETURN count(label) AS w", false},
		"write_builtin":      {"CALL apoc.cypher.doIt('RETURN 1 AS w', {}) YIELD value RETURN value.w AS w", true},
		"statement_runner":   {"CALL apoc.cypher.run('RETURN 1 AS w', {}) YIELD value RETURN value.w AS w", true},
		"unknown_procedure":  {"CALL mxunknown.proc() YIELD w RETURN w", true},
		"read_user_on_local": {"CALL acatom.runner('RETURN 1 AS w', $run) YIELD w RETURN w", false},
	} {
		participants := []string{"nbolt", "nhttp"}
		if name == "read_user_on_local" {
			participants = []string{"one"}
		}
		for _, participant := range participants {
			for _, route := range []string{"executor", "bolt", "http"} {
				t.Run(fmt.Sprintf("remote_builtin/%s/%s/%s", name, participant, route), func(t *testing.T) {
					run := fmt.Sprintf("mx-builtin-%s-%s-%s-%d", name, participant, route, time.Now().UnixNano())
					statement := "USE mxcmp CALL { USE mxcmp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS z } CALL { USE mxcmp." + participant + " " + call.call + " } RETURN w, z"
					err := routes[route](statement, map[string]any{"run": run})
					if call.claims {
						require.ErrorContains(t, err, matrixSecondWrite)
						require.Equal(t, float64(0), count("two", run), "AcAtom nodes left on two")
						return
					}
					require.NoError(t, err)
					require.Equal(t, float64(1), count("two", run), "AcAtom nodes on two")
				})
			}
		}
	}
}

// matrixBoltServer serves the server's databases over Bolt on a loopback
// port and returns its URI.
func matrixBoltServer(t *testing.T, server *Server) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	peer := bolt.NewWithDatabaseManager(&bolt.Config{ReadBufferSize: 8192, WriteBufferSize: 8192}, nil, server.dbManager)
	done := make(chan error, 1)
	go func() { done <- peer.Serve(listener) }()
	t.Cleanup(func() {
		_ = peer.Close()
		<-done
	})
	return "bolt://" + listener.Addr().String()
}
