package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

// NOR-146. A statement whose last clause is a CALL subquery that returns
// columns is Neo4j's statement-admission SyntaxError ("Query cannot conclude
// with CALL"), so it never runs. NornicDB answers with the execution-contract
// diagnostic "Execution result columns do not match the statement schema"
// from the result-schema check that runs after execution. The rejection and
// the controls below hold on both parsers, through the server's own Execute
// entry point, Bolt and HTTP, in auto-commit and in an explicit transaction.
const (
	// nor146Witness is the ticket's exact reproduction.
	nor146Witness = "UNWIND [0] AS m CALL { UNWIND range(1,3) AS x RETURN x }"
	// nor146WitnessFinalReturn is the same query with Neo4j's final RETURN.
	nor146WitnessFinalReturn = nor146Witness + " RETURN x"
	// nor146WitnessRuntimeError returns columns too, so it is the same
	// admission rule, but its body would fail at run time (1 / 0) if the
	// statement were executed: a diagnostic from the body proves execution
	// started, an admission diagnostic proves it did not.
	nor146WitnessRuntimeError = "UNWIND [0] AS m CALL { UNWIND [0] AS x RETURN 1 / x AS y }"
	// nor146WitnessCreateBefore writes before the rejected CALL subquery.
	nor146WitnessCreateBefore = "UNWIND [0] AS m CREATE (n:Nor146Admission {m: m}) WITH m CALL { UNWIND range(1,3) AS x RETURN x }"
	// nor146UnitTrailingCall is Neo4j's valid trailing subquery form: the
	// body returns no columns, so the CALL concludes the statement.
	nor146UnitTrailingCall = "UNWIND [0] AS m CALL { WITH m CREATE (n:Nor146Unit {m: m}) }"
)

// nor146ConclusionMessage is Neo4j's statement-admission diagnostic for a
// query that ends in a returning CALL subquery.
var nor146ConclusionMessage = localization.CypherMatchingQueryCannotConclude("CALL").Fallback

// nor146Routes are the public statement routes of the ticket's acceptance:
// the server's database-scoped Execute, Bolt and HTTP, each as an auto-commit
// statement and as one statement of an explicit transaction.
var nor146Routes = []string{"Execute-auto", "Execute-tx", "Bolt-auto", "Bolt-tx", "HTTP-auto", "HTTP-tx"}

// nor146Outcome is one statement's wire outcome. Rows are canonical JSON so a
// Bolt int64 and an HTTP JSON number compare as the same value.
type nor146Outcome struct {
	Columns []string
	Rows    []string
	Code    string
	Msg     string
}

// nor146CanonicalRows encodes each row as JSON.
func nor146CanonicalRows(t *testing.T, rows [][]any) []string {
	t.Helper()
	encoded := make([]string, 0, len(rows))
	for _, row := range rows {
		text, err := json.Marshal(row)
		require.NoError(t, err)
		encoded = append(encoded, string(text))
	}
	return encoded
}

// nor146DriverError maps a driver failure to the status its client receives.
func nor146DriverError(err error) nor146Outcome {
	var neo4jErr *neo4j.Neo4jError
	if errors.As(err, &neo4jErr) {
		return nor146Outcome{Code: neo4jErr.Code, Msg: neo4jErr.Msg}
	}
	return nor146Outcome{Code: "TRANSPORT: " + err.Error(), Msg: err.Error()}
}

// nor146Wire is one route's statement runner over the real server: the
// database-scoped executor, or a Bolt or HTTP client of the in-process peer.
type nor146Wire struct {
	route  string
	exec   *cypher.StorageExecutor
	driver neo4j.DriverWithContext
	peer   *httptest.Server
	token  string
}

// nor146Start starts the server peer and the client a route needs. The Bolt
// peer listens on a loopback ephemeral port and the HTTP peer serves the
// router in-process, so no port is published beyond the test process. The
// server, databases and data are the package's small temporary ones.
func nor146Start(t *testing.T, route string) nor146Wire {
	t.Helper()
	srv, authenticator := setupTestServer(t)
	wire := nor146Wire{route: route}
	ctx := context.Background()
	switch route {
	case "Execute-auto", "Execute-tx":
		executor, err := srv.newExecutorForDatabase("nornic")
		require.NoError(t, err)
		wire.exec = executor
	case "Bolt-auto", "Bolt-tx":
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		peer := bolt.NewWithDatabaseManager(bolt.DefaultConfig(), nil, srv.GetDatabaseManager())
		done := make(chan error, 1)
		go func() { done <- peer.Serve(listener) }()
		t.Cleanup(func() {
			require.NoError(t, peer.Close())
			<-done
		})
		driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, driver.Close(ctx)) })
		wire.driver = driver
	case "HTTP-auto", "HTTP-tx":
		wire.peer = httptest.NewServer(srv.buildRouter())
		t.Cleanup(wire.peer.Close)
		wire.token = getAuthToken(t, authenticator, "admin")
	default:
		t.Fatalf("unknown route %q", route)
	}
	return wire
}

// post sends one statement request to path and returns its status and decoded
// response. An empty query opens or commits a transaction with an empty
// statements list, as the endpoint requires.
func (w nor146Wire) post(t *testing.T, path, query string) (int, TransactionResponse) {
	t.Helper()
	statements := []map[string]any{}
	if query != "" {
		statements = append(statements, map[string]any{"statement": query})
	}
	body, err := json.Marshal(map[string]any{"statements": statements})
	require.NoError(t, err)
	request, err := http.NewRequest(http.MethodPost, w.peer.URL+path, bytes.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+w.token)
	response, err := w.peer.Client().Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	var decoded TransactionResponse
	require.NoError(t, json.NewDecoder(response.Body).Decode(&decoded))
	return response.StatusCode, decoded
}

// nor146HTTPOutcome converts one HTTP statement response to its wire outcome.
func nor146HTTPOutcome(t *testing.T, response TransactionResponse) nor146Outcome {
	t.Helper()
	if len(response.Errors) > 0 {
		return nor146Outcome{Code: response.Errors[0].Code, Msg: response.Errors[0].Message}
	}
	out := nor146Outcome{}
	if len(response.Results) > 0 {
		out.Columns = response.Results[0].Columns
		rows := make([][]any, 0, len(response.Results[0].Data))
		for _, row := range response.Results[0].Data {
			rows = append(rows, row.Row)
		}
		out.Rows = nor146CanonicalRows(t, rows)
	}
	return out
}

// nor146HTTPStatement runs query over HTTP as an auto-commit request, or as
// the first statement of an explicit transaction it commits when the
// statement succeeds. A failed statement has already ended its transaction on
// the server (#683), so nothing commits it.
func (w nor146Wire) nor146HTTPStatement(t *testing.T, query string) nor146Outcome {
	t.Helper()
	if w.route == "HTTP-auto" {
		status, response := w.post(t, "/db/nornic/tx/commit", query)
		require.Equal(t, http.StatusOK, status)
		return nor146HTTPOutcome(t, response)
	}
	status, opened := w.post(t, "/db/nornic/tx", query)
	require.Equal(t, http.StatusCreated, status)
	out := nor146HTTPOutcome(t, opened)
	if out.Code != "" {
		return out
	}
	commit, err := url.Parse(opened.Commit)
	require.NoError(t, err)
	transactionPath := strings.TrimSuffix(commit.Path, "/commit")
	status, committed := w.post(t, transactionPath+"/commit", "")
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, committed.Errors)
	return out
}

// run executes query over the route as one statement: its own auto-commit
// transaction, or one statement of an explicit transaction that is committed
// when the statement succeeds and rolled back when it fails.
func (w nor146Wire) run(t *testing.T, query string) nor146Outcome {
	t.Helper()
	ctx := context.Background()
	switch w.route {
	case "Execute-auto", "Execute-tx":
		if w.route == "Execute-tx" {
			_, err := w.exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
		}
		result, err := w.exec.Execute(ctx, query, nil)
		if w.route == "Execute-tx" {
			end := "COMMIT"
			if err != nil {
				end = "ROLLBACK"
			}
			_, endErr := w.exec.Execute(ctx, end, nil)
			require.NoError(t, endErr)
		}
		if err != nil {
			code, message := nornicerrors.Neo4jStatus(err)
			return nor146Outcome{Code: code, Msg: message}
		}
		return nor146Outcome{Columns: result.Columns, Rows: nor146CanonicalRows(t, result.Rows)}
	case "Bolt-auto", "Bolt-tx":
		session := w.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
		defer session.Close(ctx)
		var result neo4j.ResultWithContext
		var err error
		var tx neo4j.ExplicitTransaction
		if w.route == "Bolt-tx" {
			tx, err = session.BeginTransaction(ctx)
			require.NoError(t, err)
			defer tx.Close(ctx)
			result, err = tx.Run(ctx, query, nil)
		} else {
			result, err = session.Run(ctx, query, nil)
		}
		if err != nil {
			return nor146DriverError(err)
		}
		columns, err := result.Keys()
		if err != nil {
			return nor146DriverError(err)
		}
		records, err := result.Collect(ctx)
		if err != nil {
			return nor146DriverError(err)
		}
		if tx != nil {
			require.NoError(t, tx.Commit(ctx))
		}
		rows := make([][]any, 0, len(records))
		for _, record := range records {
			rows = append(rows, record.Values)
		}
		return nor146Outcome{Columns: columns, Rows: nor146CanonicalRows(t, rows)}
	case "HTTP-auto", "HTTP-tx":
		return w.nor146HTTPStatement(t, query)
	default:
		t.Fatalf("unknown route %q", w.route)
		return nor146Outcome{}
	}
}

// nor146RequireAdmissionSyntaxError asserts Neo4j's statement-admission
// diagnostic, and that it is not the post-execution result-schema mismatch
// the ticket reproduces.
func nor146RequireAdmissionSyntaxError(t *testing.T, got nor146Outcome) {
	t.Helper()
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", got.Code, got.Msg)
	require.Equal(t, nor146ConclusionMessage, got.Msg)
}

// nor146RequireAccepted asserts the statement produced no error.
func nor146RequireAccepted(t *testing.T, got nor146Outcome) {
	t.Helper()
	require.Empty(t, got.Code, got.Msg)
}

// TestNor146TrailingReturningCallTransportConsumers reproduces NOR-146 on
// every public route of the ticket's acceptance, for both parsers, and keeps
// the ticket's controls on the same matrix: the exact witness and its
// run-time-failing twin are rejected at admission, the final-RETURN form
// still returns 1, 2, 3, a trailing unit CALL subquery still runs, and the
// rejected statement that writes first leaves nothing behind.
func TestNor146TrailingReturningCallTransportConsumers(t *testing.T) {
	previous := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(previous) })
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		config.SetParserType(parser)
		for _, route := range nor146Routes {
			t.Run(parser+"/"+route, func(t *testing.T) {
				wire := nor146Start(t, route)

				t.Run("witness-rejected", func(t *testing.T) {
					nor146RequireAdmissionSyntaxError(t, wire.run(t, nor146Witness))
				})
				t.Run("rejected-before-execution", func(t *testing.T) {
					// The body would divide by zero at run time, so a body
					// diagnostic here would prove the statement had run.
					nor146RequireAdmissionSyntaxError(t, wire.run(t, nor146WitnessRuntimeError))
				})
				t.Run("final-return-rows", func(t *testing.T) {
					rows := wire.run(t, nor146WitnessFinalReturn)
					nor146RequireAccepted(t, rows)
					require.Equal(t, []string{"x"}, rows.Columns)
					require.Equal(t, []string{"[1]", "[2]", "[3]"}, rows.Rows)
				})
				t.Run("unit-call-accepted", func(t *testing.T) {
					nor146RequireAccepted(t, wire.run(t, nor146UnitTrailingCall))
					created := wire.run(t, "MATCH (n:Nor146Unit) RETURN count(n) AS c")
					nor146RequireAccepted(t, created)
					require.Equal(t, []string{"[1]"}, created.Rows)
				})
				t.Run("write-first-has-no-effect", func(t *testing.T) {
					out := wire.run(t, nor146WitnessCreateBefore)
					remaining := wire.run(t, "MATCH (n:Nor146Admission) RETURN count(n) AS c")
					nor146RequireAccepted(t, remaining)
					require.Equal(t, []string{"[0]"}, remaining.Rows, "reported outcome: %+v", out)
					nor146RequireAdmissionSyntaxError(t, out)
				})
			})
		}
	}
}
