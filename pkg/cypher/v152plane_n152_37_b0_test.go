package cypher

// NOR-45 reproduction stage: composite CALL / UNION / OPTIONAL CALL row
// semantics and remote participant completion on the v1.5.1 backend.
//
// The two public reports are replayed with their filed statements:
//
//	#1479 - after a composite CALL, "WITH k ORDER BY k" followed by an
//	        aggregating RETURN aggregates the complete input into one row.
//	#1480 - a CALL body that is a UNION of branches with their own USE splits,
//	        runs each branch on its own constituent and combines the rows
//	        (node-bearing constituents, as reported).
//
// The same release carries the OPTIONAL CALL, unit CALL and UNION contracts
// the reports depend on, and remote participant completion (a participant's
// COMMIT/ROLLBACK completes after the enlisting statement or APPLY batch
// context has been cancelled), so those are reproduced here as well.
//
// Scope: tests only; no production change is made. The fixture owners are the
// existing ones: newFabricCallRowsExecutor and fabricCallRowsCompositeExecutor
// (fabric_call_rows_test.go), restoreParserTypeAfter (parser_comparison_test.go),
// unionNarrowObserve (fabric_union_narrow_test.go), newCompositeTestExecutor
// (composite_schema_test.go), newCompositeNativeManager
// (composite_native_fixture_test.go), newTestMemoryEngine (test_helpers_test.go)
// and testDatabaseManagerAdapter (composite_commands_test.go).
//
// The parser is process-wide (config.SetParserType), so every case installs
// each parser selection itself and restores the previous one afterwards, and
// each parser gets a fresh executor with its statement cache disabled so no
// plan or parse result crosses a parser boundary.
//
// This is not publication certification: only the focused cases below are
// checked, and only on the recorded macOS host. The parsed implementations were
// fixed by #1484 (release/v1.4.4); no fail-first run against an unfixed head is
// claimed here, and the original failing evidence remains the reports.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nor45StatementDeadline bounds one statement so a plan that re-enters its own
// composite fails its case instead of stalling the package.
const nor45StatementDeadline = 20 * time.Second

// nor45EachParser runs one case under each parser selection in the same
// process, restoring whatever selection the process started with.
func nor45EachParser(t *testing.T, run func(t *testing.T)) {
	t.Helper()
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run("parser="+parser, func(t *testing.T) {
			restoreParserTypeAfter(t)
			config.SetParserType(parser)
			run(t)
		})
	}
}

// nor45AssertRows compares rows with an exact JSON expectation.
func nor45AssertRows(t *testing.T, rows [][]interface{}, want string) {
	t.Helper()
	require.JSONEq(t, want, nor45RowsJSON(t, rows))
}

// nor45RowsJSON renders rows for an exact comparison with diagnostics.
func nor45RowsJSON(t *testing.T, rows [][]interface{}) string {
	t.Helper()
	encoded, err := json.Marshal(append(make([][]interface{}, 0, len(rows)), rows...))
	require.NoError(t, err)
	return string(encoded)
}

// nor45RunModes runs query on exec in autocommit and in an explicit composite
// transaction, and asserts its rows in both.
func nor45RunModes(t *testing.T, exec *StorageExecutor, query, want string) {
	t.Helper()
	for _, mode := range []string{"autocommit", "transaction"} {
		t.Run(mode, func(t *testing.T) {
			runExec := exec
			ctx := context.Background()
			if mode == "transaction" {
				runExec = fabricCallRowsCompositeExecutor(t, exec)
				_, err := runExec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() {
					_, err := runExec.Execute(context.Background(), "ROLLBACK", nil)
					require.NoError(t, err)
				})
			}
			statementCtx, cancel := context.WithTimeout(ctx, nor45StatementDeadline)
			defer cancel()
			result, err := runExec.Execute(statementCtx, query, nil)
			require.NoError(t, err, query)
			nor45AssertRows(t, result.Rows, want)
		})
	}
}

// nor45CallRowsExecutor is the shared composite fixture with its statement
// cache disabled, so each parser plans and runs its own text.
func nor45CallRowsExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	exec := newFabricCallRowsExecutor(t)
	exec.cache = nil
	return exec
}

// TestNOR45Report1479OrderedAggregateAfterCompositeCall replays #1479: the filed
// statement and its controls. The defect was one aggregated row per input row;
// the contract is one row over the complete ordered input.
func TestNOR45Report1479OrderedAggregateAfterCompositeCall(t *testing.T) {
	nor45EachParser(t, func(t *testing.T) {
		exec := nor45CallRowsExecutor(t)
		const call = "USE comp CALL { USE comp.one UNWIND [2, 1] AS k RETURN k } "

		// The filed statement: one aggregated row, ordered by k.
		for _, mode := range []string{"autocommit", "transaction"} {
			t.Run("report/"+mode, func(t *testing.T) {
				runExec := exec
				ctx := context.Background()
				if mode == "transaction" {
					runExec = fabricCallRowsCompositeExecutor(t, exec)
					_, err := runExec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					t.Cleanup(func() {
						_, err := runExec.Execute(context.Background(), "ROLLBACK", nil)
						require.NoError(t, err)
					})
				}
				statementCtx, cancel := context.WithTimeout(ctx, nor45StatementDeadline)
				defer cancel()
				result, err := runExec.Execute(statementCtx,
					call+"WITH k ORDER BY k RETURN collect(k) AS ks", nil)
				require.NoError(t, err)
				require.Len(t, result.Rows, 1, "the ordered aggregate is one row over the complete input")
				nor45AssertRows(t, result.Rows, "[[[1,2]]]")
			})
		}

		// The report's controls, kept distinct: the ordered aggregate is one
		// row, the aggregate without ordering is also one row over all rows,
		// the ordered projection keeps both rows and count counts both.
		//
		// The filed control table prints collect(k) as [1, 2] for the unordered
		// aggregate; the ordered input is [2, 1] and collect without ORDER BY
		// keeps input order, so the unordered value asserted here is [2, 1] and
		// the ordered value is the contract the report states.
		nor45RunModes(t, exec, call+"RETURN collect(k) AS ks", "[[[2,1]]]")
		nor45RunModes(t, exec, call+"RETURN count(*) AS c", "[[2]]")
		nor45RunModes(t, exec, call+"RETURN k ORDER BY k", "[[1],[2]]")
	})
}

// TestNOR45Report1480UnionOfConstituentBranches replays #1480 on node-bearing
// constituents: each UNION branch of a CALL body runs on its own USE target and
// the branches' rows are combined.
func TestNOR45Report1480UnionOfConstituentBranches(t *testing.T) {
	nor45EachParser(t, func(t *testing.T) {
		exec := nor45CallRowsExecutor(t)

		// The filed statement, on the two :P nodes the fixture holds.
		nor45RunModes(t, exec,
			"USE comp CALL { USE comp.one MATCH (n:P) RETURN n UNION ALL USE comp.two MATCH (n:P) RETURN n } RETURN n.db AS db ORDER BY db",
			"[[\"one\"],[\"two\"]]")

		// The same node-bearing body aggregating after an ordering horizon is
		// one row over both constituents (the #1479 shape on real rows).
		nor45RunModes(t, exec,
			"USE comp CALL { USE comp.one MATCH (n:P) RETURN n UNION ALL USE comp.two MATCH (n:P) RETURN n } WITH n ORDER BY n.db RETURN collect(n.db) AS dbs",
			"[[[\"one\",\"two\"]]]")

		// UNION compares rows by Cypher equivalence: the identical :P node
		// returned by both branches is one row, UNION ALL keeps both.
		nor45RunModes(t, exec,
			"USE comp CALL { USE comp.one MATCH (n:P) RETURN n UNION USE comp.one MATCH (n:P) RETURN n } RETURN n.db AS db",
			"[[\"one\"]]")
		nor45RunModes(t, exec,
			"USE comp CALL { USE comp.one MATCH (n:P) RETURN n UNION ALL USE comp.one MATCH (n:P) RETURN n } RETURN n.db AS db ORDER BY db",
			"[[\"one\"],[\"one\"]]")

		// Three branches, each planned on its own USE target.
		nor45RunModes(t, exec,
			"USE comp CALL { USE comp.one MATCH (n:P) RETURN n UNION ALL USE comp.two MATCH (n:P) RETURN n UNION ALL USE comp.one MATCH (n:P) RETURN n } RETURN n.db AS db ORDER BY db",
			"[[\"one\"],[\"one\"],[\"two\"]]")
	})
}

// TestNOR45UnionBodySchemaContract keeps the UNION contract the reports' fix
// depends on: columns match by name across branches, other names are the
// single-graph DifferentColumnsInUnion syntax error, and UNION's DISTINCT
// compares values by equivalence.
func TestNOR45UnionBodySchemaContract(t *testing.T) {
	nor45EachParser(t, func(t *testing.T) {
		exec := nor45CallRowsExecutor(t)
		nor45RunModes(t, exec,
			"USE comp CALL { USE comp.one RETURN 1 AS a, 2 AS b UNION ALL USE comp.two RETURN 2 AS b, 1 AS a } RETURN a, b",
			"[[1,2],[1,2]]")
		nor45RunModes(t, exec,
			"USE comp CALL { USE comp.one RETURN 1 AS v UNION USE comp.two RETURN 1.0 AS v } RETURN v",
			"[[1]]")
		nor45RunModes(t, exec,
			"USE comp CALL { USE comp.one RETURN 1 AS v UNION ALL USE comp.two RETURN 1 AS v } RETURN count(*) AS c",
			"[[2]]")

		got := unionNarrowObserve(exec, "", "",
			"USE comp CALL { USE comp.one RETURN 1 AS a UNION USE comp.two RETURN 2 AS b } RETURN a")
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", got.Code, got.Error)
		require.Equal(t, "DifferentColumnsInUnion", got.Detail, got.Error)
	})
}

// TestNOR45OptionalAndUnitCallRows reproduces the OPTIONAL CALL and unit CALL
// row rules the reports' fix carries: OPTIONAL keeps an input row whose
// returning body has no rows with null for the body's columns, a plain CALL
// drops it, and a body without RETURN keeps each input row exactly once while
// every body effect still runs.
func TestNOR45OptionalAndUnitCallRows(t *testing.T) {
	nor45EachParser(t, func(t *testing.T) {
		exec := nor45CallRowsExecutor(t)

		// OPTIONAL CALL, no matching rows: both input rows survive with null.
		nor45RunModes(t, exec,
			"USE comp UNWIND [1,2] AS x OPTIONAL CALL { WITH x USE comp.one MATCH (n:Nor45Missing) RETURN n } RETURN x, n ORDER BY x",
			"[[1,null],[2,null]]")
		// The plain CALL control drops both rows.
		nor45RunModes(t, exec,
			"USE comp UNWIND [1,2] AS x CALL { WITH x USE comp.one MATCH (n:Nor45Missing) RETURN n } RETURN x, n ORDER BY x",
			"[]")
		// OPTIONAL CALL with a body that has rows only for the second input row.
		nor45RunModes(t, exec,
			"USE comp UNWIND [1,2] AS x OPTIONAL CALL { WITH x USE comp.one WITH x WHERE x = 2 RETURN x * 10 AS y } RETURN x, y ORDER BY x",
			"[[1,null],[2,20]]")

		// A unit body producing three rows per input row keeps each input row
		// exactly once.
		unit := "USE comp UNWIND [1,2] AS x CALL { WITH x USE comp.two UNWIND [1,2,3] AS y CREATE (n:Nor45Unit {x: x, y: y}) } RETURN x ORDER BY x"
		nor45RunModes(t, exec, unit, "[[1],[2]]")

		// Every body effect ran: three rows for each of the two input rows.
		mgr := exec.dbManager.(*testDatabaseManagerAdapter).manager
		store, err := mgr.GetStorage("two")
		require.NoError(t, err)
		two := NewStorageExecutor(store)
		result, err := two.Execute(context.Background(), "MATCH (n:Nor45Unit) RETURN count(n) AS c", nil)
		require.NoError(t, err)
		nor45AssertRows(t, result.Rows, "[[6]]")
	})
}

// nor45RemoteParticipant is one HTTP constituent that answers the NornicDB HTTP
// transaction API and records what it was asked to do. Its statements run on a
// real executor, so returned rows are real; this fixture asserts terminal
// participant completion (the remote request that ends the enlisted
// transaction), not remote isolation or durability, which the package-level
// protocol workflow owns.
type nor45RemoteParticipant struct {
	executor *StorageExecutor
	server   *httptest.Server

	mu        sync.Mutex
	nextTx    int
	opens     int
	txs       map[string]*nor45RemoteTx
	terminals []string
	failures  []string
}

type nor45RemoteTx struct {
	statements int
}

// nor45RemoteSnapshot is one consistent view of what the participant observed.
type nor45RemoteSnapshot struct {
	opens      int
	terminals  []string
	failures   []string
	statements int
}

func newNOR45RemoteParticipant(t *testing.T) *nor45RemoteParticipant {
	t.Helper()
	p := &nor45RemoteParticipant{
		executor: NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor45remote")),
		txs:      make(map[string]*nor45RemoteTx),
	}
	p.server = httptest.NewServer(http.HandlerFunc(p.serve))
	t.Cleanup(p.server.Close)
	return p
}

func (p *nor45RemoteParticipant) database() string { return "nor45remote" }

func (p *nor45RemoteParticipant) writeJSON(w http.ResponseWriter, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func (p *nor45RemoteParticipant) serve(w http.ResponseWriter, r *http.Request) {
	base := "/db/" + p.database() + "/tx"
	switch {
	case r.Method == http.MethodPost && r.URL.Path == base:
		p.mu.Lock()
		p.nextTx++
		id := fmt.Sprintf("nor45-tx-%d", p.nextTx)
		p.txs[id] = &nor45RemoteTx{}
		p.opens++
		p.mu.Unlock()
		p.writeJSON(w, map[string]interface{}{
			"results":     []interface{}{},
			"errors":      []interface{}{},
			"commit":      p.server.URL + base + "/" + id + "/commit",
			"transaction": map[string]interface{}{},
		})
	case r.Method == http.MethodPost && r.URL.Path == base+"/commit":
		// An autocommit statement: run it and answer with its rows.
		p.runStatements(w, r, "")
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, base+"/") && strings.HasSuffix(r.URL.Path, "/commit"):
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, base+"/"), "/commit")
		p.mu.Lock()
		p.terminals = append(p.terminals, "commit:"+id)
		p.mu.Unlock()
		p.writeJSON(w, map[string]interface{}{"results": []interface{}{}, "errors": []interface{}{}})
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, base+"/"):
		id := strings.TrimPrefix(r.URL.Path, base+"/")
		p.mu.Lock()
		p.terminals = append(p.terminals, "rollback:"+id)
		p.mu.Unlock()
		p.writeJSON(w, map[string]interface{}{"results": []interface{}{}, "errors": []interface{}{}})
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, base+"/"):
		// A statement inside the enlisted transaction, or a lease renewal
		// (which carries no statement).
		p.runStatements(w, r, strings.TrimPrefix(r.URL.Path, base+"/"))
	default:
		http.NotFound(w, r)
	}
}

func (p *nor45RemoteParticipant) runStatements(w http.ResponseWriter, r *http.Request, txID string) {
	var request struct {
		Statements []struct {
			Statement  string                 `json:"statement"`
			Parameters map[string]interface{} `json:"parameters"`
		} `json:"statements"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil && err.Error() != "EOF" {
		p.mu.Lock()
		p.failures = append(p.failures, "decode: "+err.Error())
		p.mu.Unlock()
	}
	results := make([]interface{}, 0, len(request.Statements))
	for _, statement := range request.Statements {
		if strings.TrimSpace(statement.Statement) == "" {
			continue
		}
		p.mu.Lock()
		if txID != "" {
			if tx := p.txs[txID]; tx != nil {
				tx.statements++
			}
		}
		p.mu.Unlock()
		result, err := p.executor.Execute(context.Background(), statement.Statement, statement.Parameters)
		if err != nil {
			p.mu.Lock()
			p.failures = append(p.failures, statement.Statement+": "+err.Error())
			p.mu.Unlock()
			results = append(results, map[string]interface{}{"columns": []string{}, "data": []interface{}{}})
			continue
		}
		data := make([]interface{}, 0, len(result.Rows))
		for _, row := range result.Rows {
			data = append(data, map[string]interface{}{"row": row})
		}
		results = append(results, map[string]interface{}{"columns": result.Columns, "data": data})
	}
	p.writeJSON(w, map[string]interface{}{"results": results, "errors": []interface{}{}})
}

func (p *nor45RemoteParticipant) snapshot() nor45RemoteSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	snapshot := nor45RemoteSnapshot{
		opens:     p.opens,
		terminals: append([]string(nil), p.terminals...),
		failures:  append([]string(nil), p.failures...),
	}
	for _, tx := range p.txs {
		snapshot.statements += tx.statements
	}
	return snapshot
}

// nor45RemoteFixture is a composite comp whose remote constituent is the
// recording HTTP participant above and whose local constituent one carries the
// input rows.
func nor45RemoteFixture(t *testing.T) (*StorageExecutor, *nor45RemoteParticipant) {
	t.Helper()
	participant := newNOR45RemoteParticipant(t)
	mgr := newCompositeNativeManager(t, &multidb.Config{
		DefaultDatabase: "nornic",
		SystemDatabase:  "system",
		RemoteEngineFactory: func(ref multidb.ConstituentRef, authToken string) (storage.Engine, error) {
			return storage.NewRemoteEngine(storage.RemoteEngineConfig{
				URI:       ref.URI,
				Database:  ref.DatabaseName,
				AuthToken: authToken,
				User:      ref.User,
				Password:  ref.Password,
			})
		},
	})
	t.Cleanup(func() { require.NoError(t, mgr.Close()) })
	require.NoError(t, mgr.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "nor45local"}))
	require.NoError(t, mgr.CreateCompositeDatabase(t.Context(), "comp", []multidb.ConstituentRef{
		{Alias: "one", DatabaseName: "nor45local", Type: "local", AccessMode: "read_write"},
		{Alias: "remote", DatabaseName: participant.database(), Type: "remote", AccessMode: "read_write", URI: participant.server.URL, AuthMode: "oidc_forwarding"},
	}))
	exec := newCompositeTestExecutor(t, mgr, "comp", &testDatabaseManagerAdapter{manager: mgr})
	exec.cache = nil
	return exec, participant
}

// TestNOR45RemoteParticipantCompletion reproduces the participant completion
// contract: a participant enlisted inside a composite statement completes its
// COMMIT or ROLLBACK after the enlisting statement's context has been
// cancelled, so the terminal request still reaches the constituent exactly once.
func TestNOR45RemoteParticipantCompletion(t *testing.T) {
	const write = "USE comp UNWIND [1,2,3] AS k CALL { WITH k USE comp.remote CREATE (n:Nor45Remote {k: k}) RETURN k } RETURN count(*) AS c"

	nor45EachParser(t, func(t *testing.T) {
		for _, mode := range []string{"autocommit", "commit", "rollback"} {
			t.Run("write/"+mode, func(t *testing.T) {
				exec, participant := nor45RemoteFixture(t)
				ctx := context.Background()
				if mode != "autocommit" {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				statementCtx, cancel := context.WithTimeout(ctx, nor45StatementDeadline)
				result, err := exec.Execute(statementCtx, write, nil)
				require.NoError(t, err)
				require.Empty(t, participant.snapshot().failures,
					"the participant failed a statement: %+v", participant.snapshot())
				cancel()
				require.Equal(t, "[[3]]", nor45RowsJSON(t, result.Rows), "participant: %+v", participant.snapshot())

				switch mode {
				case "autocommit":
					// The statement's own transaction ends its participants
					// before it returns.
				case "commit":
					_, err = exec.Execute(context.Background(), "COMMIT", nil)
					require.NoError(t, err, "participant completion")
				case "rollback":
					_, err = exec.Execute(context.Background(), "ROLLBACK", nil)
					require.NoError(t, err, "participant completion")
				}

				snapshot := participant.snapshot()
				require.Empty(t, snapshot.failures, "the participant failed a statement")
				require.Equal(t, 1, snapshot.opens, "the statement enlisted one remote transaction")
				require.Equal(t, 3, snapshot.statements, "one statement per input row reached the participant")
				require.Len(t, snapshot.terminals, 1, "terminal requests: %v", snapshot.terminals)
				wantTerminal := "commit:"
				if mode == "rollback" {
					wantTerminal = "rollback:"
				}
				require.True(t, strings.HasPrefix(snapshot.terminals[0], wantTerminal),
					"terminal request %q, want %q", snapshot.terminals[0], wantTerminal)
			})
		}

		// A batched correlated lookup runs its participant statements inside
		// cancelled APPLY batch contexts; the enlisted read participant still
		// completes (its read session ends with the transaction) exactly once.
		t.Run("batched-apply/completion", func(t *testing.T) {
			exec, participant := nor45RemoteFixture(t)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			query := "USE comp CALL { USE comp.one UNWIND range(1,513) AS k RETURN k } " +
				"CALL { WITH k USE comp.remote MATCH (n:Nor45RemoteMissing) WHERE n.key = k RETURN n.key AS key } RETURN count(*) AS rows"
			statementCtx, cancel := context.WithTimeout(ctx, nor45StatementDeadline)
			result, err := exec.Execute(statementCtx, query, nil)
			require.NoError(t, err)
			nor45AssertRows(t, result.Rows, "[[0]]")
			require.True(t, exec.LastHotPathTrace().FabricBatchedApplyRows,
				"the correlated lookup ran as an APPLY batch")
			cancel()
			_, err = exec.Execute(context.Background(), "COMMIT", nil)
			require.NoError(t, err, "participant completion")

			snapshot := participant.snapshot()
			require.Empty(t, snapshot.failures, "the participant failed a statement")
			require.Equal(t, 1, snapshot.opens)
			require.Equal(t, 2, snapshot.statements, "513 keys are two APPLY batches")
			require.Len(t, snapshot.terminals, 1, "terminal requests: %v", snapshot.terminals)
			require.True(t, strings.HasPrefix(snapshot.terminals[0], "rollback:"),
				"a read participant's session ends with the transaction: %q", snapshot.terminals[0])
		})
	})
}
