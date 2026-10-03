package tck

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

func TestGh531_BoltSchemaTransactionLifetime(t *testing.T) {
	referenceURI := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if referenceURI == "" {
		t.Skip("set NORNICDB_NEO4J_REFERENCE_URI for schema transaction comparison")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	nornic, shutdown := startConformanceServer(t)
	defer shutdown()
	reference, err := neo4j.NewDriverWithContext(referenceURI, neo4j.NoAuth())
	require.NoError(t, err)
	defer reference.Close(context.Background())
	require.NoError(t, waitForReference(ctx, reference))
	for _, backend := range []struct {
		name, database string
		driver         neo4j.DriverWithContext
	}{{"nornicdb", "nornic", nornic}, {"neo4j", "neo4j", reference}} {
		t.Run(backend.name, func(t *testing.T) {
			session := backend.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: backend.database})
			defer session.Close(ctx)
			run := func(t *testing.T, statement string, tx neo4j.ExplicitTransaction) ([][]any, error) {
				t.Helper()
				var result neo4j.ResultWithContext
				var err error
				if tx == nil {
					result, err = session.Run(ctx, statement, nil)
				} else {
					result, err = tx.Run(ctx, statement, nil)
				}
				if err != nil {
					return nil, err
				}
				records, err := result.Collect(ctx)
				rows := make([][]any, 0, len(records))
				for _, record := range records {
					rows = append(rows, record.Values)
				}
				return rows, err
			}
			for _, schema := range []struct{ name, create, drop, show string }{
				{"range", "CREATE RANGE INDEX gh531_tx_index FOR (n:Gh531SchemaTX) ON (n.id)", "DROP INDEX gh531_tx_index", "SHOW INDEXES YIELD name WHERE name = 'gh531_tx_index' RETURN name"},
				{"text", "CREATE TEXT INDEX gh531_tx_index FOR (n:Gh531SchemaTX) ON (n.id)", "DROP INDEX gh531_tx_index", "SHOW INDEXES YIELD name WHERE name = 'gh531_tx_index' RETURN name"},
				{"point", "CREATE POINT INDEX gh531_tx_index FOR (n:Gh531SchemaTX) ON (n.id)", "DROP INDEX gh531_tx_index", "SHOW INDEXES YIELD name WHERE name = 'gh531_tx_index' RETURN name"},
				{"fulltext", "CREATE FULLTEXT INDEX gh531_tx_index FOR (n:Gh531SchemaTX) ON EACH [n.id]", "DROP INDEX gh531_tx_index", "SHOW INDEXES YIELD name WHERE name = 'gh531_tx_index' RETURN name"},
				{"vector", "CREATE VECTOR INDEX gh531_tx_index FOR (n:Gh531SchemaTX) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", "DROP INDEX gh531_tx_index", "SHOW INDEXES YIELD name WHERE name = 'gh531_tx_index' RETURN name"},
				{"vector_procedure", "CALL db.index.vector.createNodeIndex('gh531_tx_index', 'Gh531SchemaTX', 'embedding', 3, 'cosine')", "DROP INDEX gh531_tx_index", "SHOW INDEXES YIELD name WHERE name = 'gh531_tx_index' RETURN name"},
				{"unique", "CREATE CONSTRAINT gh531_tx_unique FOR (n:Gh531SchemaTX) REQUIRE n.id IS UNIQUE", "DROP CONSTRAINT gh531_tx_unique", "SHOW CONSTRAINTS YIELD name WHERE name = 'gh531_tx_unique' RETURN name"},
			} {
				for _, dropping := range []bool{false, true} {
					for _, commit := range []bool{false, true} {
						t.Run(fmt.Sprintf("%s/drop=%v/commit=%v", schema.name, dropping, commit), func(t *testing.T) {
							resetDifferentialBackend(t, ctx, newDifferentialBackend(t, backend.driver, backend.database, AutocommitMode), backend.name)
							_, err := run(t, "CREATE (:Gh531SchemaTX {id: 'seed'})", nil)
							require.NoError(t, err)
							statement := schema.create
							if dropping {
								_, err = run(t, schema.create, nil)
								require.NoError(t, err)
								statement = schema.drop
							}
							transaction, err := session.BeginTransaction(ctx)
							require.NoError(t, err)
							defer transaction.Close(ctx)
							_, err = run(t, statement, transaction)
							require.NoError(t, err)
							if commit {
								err = transaction.Commit(ctx)
							} else {
								err = transaction.Rollback(ctx)
							}
							require.NoError(t, err)
							rows, err := run(t, schema.show, nil)
							require.NoError(t, err)
							wantPresent := dropping != commit
							require.Equal(t, wantPresent, len(rows) == 1)
							_, err = run(t, "CALL db.awaitIndexes(30)", nil)
							require.NoError(t, err)
							data, err := run(t, "MATCH (n:Gh531SchemaTX {id: 'seed'}) RETURN n.id", nil)
							require.NoError(t, err)
							require.Equal(t, [][]any{{"seed"}}, data)
							t.Logf("ISSUE531_SCHEMA_RESULT backend=%s schema=%s drop=%v commit=%v present=%v data=%v", backend.name, schema.name, dropping, commit, wantPresent, data)
						})
					}
				}
			}
			for _, procedure := range []bool{false, true} {
				for _, schemaFirst := range []bool{false, true} {
					t.Run(fmt.Sprintf("mixed/procedure=%v/schema_first=%v", procedure, schemaFirst), func(t *testing.T) {
						resetDifferentialBackend(t, ctx, newDifferentialBackend(t, backend.driver, backend.database, AutocommitMode), backend.name)
						statements := []string{"CREATE (:Gh531SchemaTX {id: 'mixed'})", "CREATE INDEX gh531_tx_index FOR (n:Gh531SchemaTX) ON (n.id)"}
						if procedure {
							statements[1] = "CALL db.index.vector.createNodeIndex('gh531_tx_index', 'Gh531SchemaTX', 'embedding', 3, 'cosine')"
						}
						if schemaFirst {
							statements[0], statements[1] = statements[1], statements[0]
						}
						transaction, err := session.BeginTransaction(ctx)
						require.NoError(t, err)
						defer transaction.Close(ctx)
						_, err = run(t, statements[0], transaction)
						require.NoError(t, err)
						_, err = run(t, statements[1], transaction)
						var diagnostic *neo4j.Neo4jError
						require.ErrorAs(t, err, &diagnostic)
						code := "Neo.ClientError.Transaction.ForbiddenDueToTransactionType"
						if procedure && !schemaFirst {
							code = "Neo.ClientError.Procedure.ProcedureCallFailed"
						}
						require.Equal(t, code, diagnostic.Code)
						require.Error(t, transaction.Commit(ctx))
						rows, err := run(t, "SHOW INDEXES YIELD name WHERE name = 'gh531_tx_index' RETURN name", nil)
						require.NoError(t, err)
						require.Empty(t, rows)
						data, err := run(t, "MATCH (n:Gh531SchemaTX) RETURN count(n)", nil)
						require.NoError(t, err)
						require.Equal(t, [][]any{{int64(0)}}, data)
						t.Logf("ISSUE531_SCHEMA_RESULT backend=%s mixed_schema_first=%v code=%s rows=%v data=%v", backend.name, schemaFirst, diagnostic.Code, rows, data)
					})
				}
			}
		})
	}
}

type differentialCase struct {
	Name            string         `json:"name"`
	Setup           []string       `json:"setup"`
	Query           string         `json:"query"`
	Ordered         bool           `json:"ordered"`
	UnorderedLabels bool           `json:"unordered_labels"`
	Parameters      map[string]any `json:"parameters"`
	ExpectedCode    string         `json:"expected_code"`
	ExpectedMessage string         `json:"expected_message"`
	ExpectedPhase   string         `json:"expected_phase"`
	NoEffects       bool           `json:"no_effects"`
}

func TestReportedProductBoundariesOnLiveServer(t *testing.T) {
	uri := os.Getenv("NORNICDB_CORRECTNESS_SERVER_URI")
	if uri == "" {
		t.Skip("set NORNICDB_CORRECTNESS_SERVER_URI for the memory-capped server correctness replay")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close(context.Background())
	if err := waitForReference(ctx, driver); err != nil {
		t.Fatal(err)
	}
	run := func(t *testing.T, query string, explicit bool) []*neo4j.Record {
		t.Helper()
		session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
		defer session.Close(ctx)
		var result neo4j.ResultWithContext
		if explicit {
			tx, err := session.BeginTransaction(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Close(ctx)
			result, err = tx.Run(ctx, query, nil)
			if err != nil {
				t.Fatal(err)
			}
			records, err := result.Collect(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			return records
		}
		result, err = session.Run(ctx, query, nil)
		if err != nil {
			t.Fatal(err)
		}
		records, err := result.Collect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return records
	}
	run(t, "MATCH (n) DETACH DELETE n", false)
	run(t, "UNWIND range(0,4999) AS i CREATE (:Doc {id:i})", false)
	for _, explicit := range []bool{false, true} {
		for _, prefix := range []string{
			"MATCH (a:Doc), (b:Doc)",
			"MATCH (a:Doc) MATCH (b:Doc)",
			"MATCH (a:Doc), (b:Doc) WITH a,b",
			"MATCH (a:Doc), (b:Doc) WITH a",
			"MATCH (a:Doc), (b:Doc) WITH a.id AS x",
		} {
			t.Run(fmt.Sprintf("explicit=%v/%s", explicit, prefix), func(t *testing.T) {
				records := run(t, prefix+" RETURN count(*) AS c", explicit)
				if len(records) != 1 || len(records[0].Values) != 1 || records[0].Values[0] != int64(25000000) {
					t.Fatalf("expected one count of 25000000, got %#v", records)
				}
			})
		}
	}
}

func TestGh809_BoltTransactionIndexVisibility(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	nornic, shutdown := startConformanceServer(t)
	defer shutdown()
	backends := []struct {
		name, database string
		driver         neo4j.DriverWithContext
	}{{"nornicdb", "nornic", nornic}}
	if uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI"); uri != "" {
		reference, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
		if err != nil {
			t.Fatal(err)
		}
		defer reference.Close(context.Background())
		if err := waitForReference(ctx, reference); err != nil {
			t.Fatal(err)
		}
		backends = append(backends, struct {
			name, database string
			driver         neo4j.DriverWithContext
		}{"neo4j", "neo4j", reference})
	}
	rows := make([]any, 0, 48)
	ids := make([]any, 0, 16)
	for document := 0; document < 16; document++ {
		documentID := fmt.Sprintf("doc-%d", document)
		ids = append(ids, documentID)
		for _, kind := range []string{"document", "version", "origin"} {
			rows = append(rows, map[string]any{"properties": map[string]any{
				"id": documentID + "-" + kind, "document_id": documentID, "kind": kind,
				"revision": int64(1), "body": "body", "created_at": "created", "updated_at": "updated",
			}})
		}
	}
	selector := "MATCH (n:GH809) WHERE n.document_id IN $ids AND n.kind IN $kinds RETURN n.id AS id,n.kind AS kind,n.revision AS revision,n.body AS body,n.created_at AS created_at,n.updated_at AS updated_at ORDER BY id"
	parameters := map[string]any{"ids": ids, "kinds": []any{"version", "origin", "unused"}}
	observed := make(map[string][][]any)
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			session := backend.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: backend.database})
			defer session.Close(ctx)
			run := func(query string, params map[string]any, tx neo4j.ExplicitTransaction) [][]any {
				t.Helper()
				var result neo4j.ResultWithContext
				var err error
				if tx == nil {
					result, err = session.Run(ctx, query, params)
				} else {
					result, err = tx.Run(ctx, query, params)
				}
				if err != nil {
					t.Fatal(err)
				}
				records, err := result.Collect(ctx)
				if err != nil {
					t.Fatal(err)
				}
				values := make([][]any, 0, len(records))
				for _, record := range records {
					values = append(values, record.Values)
				}
				return values
			}
			for _, property := range []string{"", "document_id", "kind"} {
				for _, commit := range []bool{false, true} {
					t.Run(fmt.Sprintf("index=%s/commit=%v", property, commit), func(t *testing.T) {
						run("MATCH (n:GH809) DETACH DELETE n", nil, nil)
						for _, indexed := range []string{"document_id", "kind"} {
							run("DROP INDEX gh809_"+indexed+" IF EXISTS", nil, nil)
						}
						if property != "" {
							run("CREATE INDEX gh809_"+property+" FOR (n:GH809) ON (n."+property+")", nil, nil)
							if backend.name == "neo4j" {
								run("CALL db.awaitIndexes(30)", nil, nil)
							}
						}
						tx, err := session.BeginTransaction(ctx)
						if err != nil {
							t.Fatal(err)
						}
						defer tx.Close(ctx)
						written := run("UNWIND $rows AS row CREATE (n:GH809) SET n = row.properties RETURN n.id AS id", map[string]any{"rows": rows}, tx)
						if len(written) != 48 {
							t.Fatalf("expected 48 writes, got %d", len(written))
						}
						actual := run(selector, parameters, tx)
						if len(actual) != 32 {
							t.Fatalf("expected 32 selected rows, got %#v", actual)
						}
						key := fmt.Sprintf("%s/%v", property, commit)
						if backend.name == "nornicdb" {
							observed[key] = actual
						} else if !reflect.DeepEqual(observed[key], actual) {
							t.Fatalf("row mismatch: NornicDB=%#v Neo4j=%#v", observed[key], actual)
						}
						if commit {
							err = tx.Commit(ctx)
						} else {
							err = tx.Rollback(ctx)
						}
						if err != nil {
							t.Fatal(err)
						}
						stored := run(selector, parameters, nil)
						if commit && !reflect.DeepEqual(actual, stored) || !commit && len(stored) != 0 {
							t.Fatalf("unexpected committed state: commit=%v rows=%#v", commit, stored)
						}
						evidence, err := json.Marshal(map[string]any{"backend": backend.name, "route": "bolt", "index": property, "commit": commit, "rows": actual, "durable_rows": stored})
						if err != nil {
							t.Fatal(err)
						}
						t.Logf("ISSUE809_RESULT %s", evidence)
					})
				}
			}
		})
	}
}

func TestFixedDifferentialCorpusMatchesPinnedNeo4j(t *testing.T) {
	runDifferentialCases(t, loadDifferentialCases(t))
}

func TestGh810_NullPropertyMapsMatchPinnedNeo4j(t *testing.T) {
	var cases []differentialCase
	for _, schema := range []string{"", "CREATE INDEX ix FOR (n:PDRecord) ON (n.id)", "CREATE CONSTRAINT uq FOR (n:PDRecord) REQUIRE n.id IS UNIQUE"} {
		setup := []string{"CREATE (a:PDRecord {id:'r1',kind:'a'})-[:R]->(b:PDRecord {id:'r2',kind:'b'}), (:PDRecord {id:'r3',kind:'a'}), (:PDRecord {kind:'noid'})"}
		if schema != "" {
			setup = append(setup, schema)
		}
		for _, query := range []string{
			"MATCH (n:PDRecord {id:$id}) RETURN n.id",
			"MATCH (n:PDRecord {id:null}) RETURN n.id",
			"MATCH (n:PDRecord {id:$id}) RETURN count(n)",
			"MATCH (n {id:$id}) RETURN n.id",
			"MATCH (n:PDRecord {id:$id,kind:$kind}) RETURN n.id",
			"MATCH (a:PDRecord {id:$id})-[:R]->(b) RETURN b.id",
			"MATCH (a)-[:R]->(b:PDRecord {id:$id}) RETURN a.id",
			"OPTIONAL MATCH (n:PDRecord {id:$id}) RETURN n.id",
			"MATCH (n:PDRecord {id:$id}) SET n.touched=true RETURN count(n)",
			"MATCH (n:PDRecord {id:$id}) DETACH DELETE n",
			"MATCH (n {id:$id}) DETACH DELETE n RETURN count(*)",
			"MERGE (n:PDRecord {id:$id}) RETURN n.id",
		} {
			cases = append(cases, differentialCase{
				Name:  "GH810/schema=" + schema + "/" + query,
				Setup: setup, Query: query, Parameters: map[string]any{"id": nil, "kind": "a"}, NoEffects: true,
			})
		}
	}
	runDifferentialCases(t, cases)
}

// differentialCaseTimeout bounds one corpus case (resets, setup, the
// statement and the snapshots, on both servers). Each case has its own
// deadline: one deadline for the whole corpus made the run fail once the
// corpus grew past it, every later case then failing with a timeout that
// says nothing about NornicDB (#754).
const differentialCaseTimeout = 30 * time.Second

func runDifferentialCases(t *testing.T, cases []differentialCase) {
	t.Helper()
	referenceURI := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if referenceURI == "" {
		t.Skip("set NORNICDB_NEO4J_REFERENCE_URI to run the pinned Neo4j differential corpus")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	referenceDriver, err := neo4j.NewDriverWithContext(referenceURI, neo4j.NoAuth())
	if err != nil {
		t.Fatalf("create Neo4j reference driver: %v", err)
	}
	defer func() {
		if err := referenceDriver.Close(context.Background()); err != nil {
			t.Errorf("close Neo4j reference driver: %v", err)
		}
	}()
	if err := waitForReference(ctx, referenceDriver); err != nil {
		t.Fatalf("connect to pinned Neo4j reference: %v", err)
	}

	for _, mode := range []TransactionMode{AutocommitMode, ExplicitTransactionMode} {
		t.Run(string(mode), func(t *testing.T) {
			nornicDriver, shutdown := startConformanceServer(t)
			defer shutdown()
			nornic := newDifferentialBackend(t, nornicDriver, "nornic", mode)
			reference := newDifferentialBackend(t, referenceDriver, "neo4j", mode)
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), differentialCaseTimeout)
				defer cancel()
				resetDifferentialBackend(t, ctx, reference, "Neo4j")
			}()

			for _, testCase := range cases {
				t.Run(testCase.Name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), differentialCaseTimeout)
					defer cancel()
					resetDifferentialBackend(t, ctx, nornic, "NornicDB")
					resetDifferentialBackend(t, ctx, reference, "Neo4j")
					for _, setup := range testCase.Setup {
						executeDifferentialSetup(t, ctx, nornic, "NornicDB", setup)
						executeDifferentialSetup(t, ctx, reference, "Neo4j", setup)
					}

					nornicBefore := snapshotDifferentialBackend(t, ctx, nornic, "NornicDB")
					referenceBefore := snapshotDifferentialBackend(t, ctx, reference, "Neo4j")
					nornicResult, nornicErr := nornic.Execute(ctx, testCase.Query, testCase.Parameters)
					referenceResult, referenceErr := reference.Execute(ctx, testCase.Query, testCase.Parameters)
					nornicAfter := snapshotDifferentialBackend(t, ctx, nornic, "NornicDB")
					referenceAfter := snapshotDifferentialBackend(t, ctx, reference, "Neo4j")
					nornicEffects, err := ObserveSideEffects(nornicBefore, nornicAfter)
					if err != nil {
						t.Fatalf("observe NornicDB side effects: %v", err)
					}
					referenceEffects, err := ObserveSideEffects(referenceBefore, referenceAfter)
					if err != nil {
						t.Fatalf("observe Neo4j side effects: %v", err)
					}
					compareDifferentialErrors(t, nornicErr, referenceErr)
					assertDifferentialDiagnostic(t, "NornicDB", nornicErr, testCase)
					assertDifferentialDiagnostic(t, "Neo4j", referenceErr, testCase)
					outcome := func(result QueryResult, executionErr error) interface{} {
						var queryErr *QueryError
						if errors.As(executionErr, &queryErr) {
							diagnostic := map[string]string{"error": queryErr.Type, "phase": queryErr.Phase}
							var boltErr *neo4j.Neo4jError
							if errors.As(executionErr, &boltErr) {
								diagnostic["code"] = boltErr.Code
								diagnostic["message"] = boltErr.Msg
							}
							return diagnostic
						}
						rows := make([][]any, len(result.Rows))
						for index, row := range result.Rows {
							rows[index] = differentialEvidenceValue(row).([]any)
						}
						result.Rows = rows
						return result
					}
					evidence, err := json.Marshal(map[string]interface{}{
						"case": testCase.Name, "route": "bolt/" + string(mode),
						"query": testCase.Query, "parameters": differentialEvidenceValue(testCase.Parameters),
						"nornicdb": outcome(nornicResult, nornicErr), "neo4j": outcome(referenceResult, referenceErr),
						"nornicdb_effects": nornicEffects, "neo4j_effects": referenceEffects,
					})
					if err != nil {
						t.Fatalf("encode differential evidence: %v", err)
					}
					t.Logf("DIFFERENTIAL_RESULT %s", evidence)
					if referenceErr == nil && nornicErr == nil {
						if err := CompareResults(nornicResult, referenceResult, testCase.Ordered, testCase.UnorderedLabels); err != nil {
							t.Errorf("result differs from Neo4j: %v", err)
						}
					}

					if !reflect.DeepEqual(nornicEffects, referenceEffects) {
						t.Errorf("side effects differ: NornicDB %#v, Neo4j %#v", nornicEffects, referenceEffects)
					}
					if testCase.NoEffects && (nornicEffects != (SideEffects{}) || referenceEffects != (SideEffects{})) {
						t.Errorf("expected no effects: NornicDB %#v, Neo4j %#v", nornicEffects, referenceEffects)
					}
				})
			}
		})
	}
}

func loadDifferentialCases(t *testing.T) []differentialCase {
	t.Helper()
	path := filepath.Join("testdata", "differential", "cases.json")
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read differential corpus: %v", err)
	}
	var cases []differentialCase
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.UseNumber()
	if err := decoder.Decode(&cases); err != nil {
		t.Fatalf("decode differential corpus: %v", err)
	}
	if len(cases) == 0 {
		t.Fatal("differential corpus is empty")
	}
	for index, testCase := range cases {
		if testCase.Name == "" || testCase.Query == "" {
			t.Fatalf("differential case %d requires name and query", index)
		}
		parameters, err := convertDifferentialParameter(testCase.Parameters)
		if err != nil {
			t.Fatalf("differential case %q parameters: %v", testCase.Name, err)
		}
		cases[index].Parameters = parameters.(map[string]any)
	}
	return cases
}

func convertDifferentialParameter(value any) (any, error) {
	switch typed := value.(type) {
	case json.Number:
		return ParseValue(typed.String())
	case []any:
		result := make([]any, len(typed))
		for index, item := range typed {
			converted, err := convertDifferentialParameter(item)
			if err != nil {
				return nil, err
			}
			result[index] = converted
		}
		return result, nil
	case map[string]any:
		if atom, ok := typed["$tck"].(string); ok && len(typed) == 1 {
			switch atom {
			case "NaN", "Inf", "-Inf", "-0.0":
				return ParseValue(atom)
			default:
				return nil, fmt.Errorf("unsupported typed parameter %q", atom)
			}
		}
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			converted, err := convertDifferentialParameter(item)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", key, err)
			}
			result[key] = converted
		}
		return result, nil
	default:
		return value, nil
	}
}

func assertDifferentialDiagnostic(t *testing.T, backend string, err error, testCase differentialCase) {
	t.Helper()
	if mismatch := differentialDiagnosticError(err, testCase); mismatch != nil {
		t.Errorf("%s diagnostic for %q: %v", backend, testCase.Query, mismatch)
	}
}

func differentialDiagnosticError(err error, testCase differentialCase) error {
	if testCase.ExpectedCode == "" && testCase.ExpectedMessage == "" && testCase.ExpectedPhase == "" {
		return nil
	}
	var queryErr *QueryError
	if !errors.As(err, &queryErr) {
		return fmt.Errorf("expected diagnostic, got %v", err)
	}
	var boltErr *neo4j.Neo4jError
	var mismatches []error
	if testCase.ExpectedCode != "" && (!errors.As(err, &boltErr) || boltErr.Code != testCase.ExpectedCode) {
		mismatches = append(mismatches, fmt.Errorf("expected code %q, got %v", testCase.ExpectedCode, err))
	}
	if testCase.ExpectedMessage != "" && (!errors.As(err, &boltErr) || boltErr.Msg != testCase.ExpectedMessage) {
		mismatches = append(mismatches, fmt.Errorf("expected message %q, got %v", testCase.ExpectedMessage, err))
	}
	if testCase.ExpectedPhase != "" && queryErr.Phase != testCase.ExpectedPhase {
		mismatches = append(mismatches, fmt.Errorf("expected phase %q, got %q", testCase.ExpectedPhase, queryErr.Phase))
	}
	return errors.Join(mismatches...)
}

func differentialEvidenceValue(value any) any {
	switch typed := value.(type) {
	case float64:
		if math.IsNaN(typed) {
			return map[string]string{"$tck": "NaN"}
		}
		if math.IsInf(typed, 1) {
			return map[string]string{"$tck": "Inf"}
		}
		if math.IsInf(typed, -1) {
			return map[string]string{"$tck": "-Inf"}
		}
		if typed == 0 && math.Signbit(typed) {
			return map[string]string{"$tck": "-0.0"}
		}
	case []any:
		result := make([]any, len(typed))
		for index, item := range typed {
			result[index] = differentialEvidenceValue(item)
		}
		return result
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			result[key] = differentialEvidenceValue(item)
		}
		return result
	case NodeValue:
		typed.Properties = differentialEvidenceValue(typed.Properties).(map[string]any)
		return typed
	case RelationshipValue:
		typed.Properties = differentialEvidenceValue(typed.Properties).(map[string]any)
		return typed
	}
	return value
}

func TestDifferentialParameterDecoding(t *testing.T) {
	decoder := json.NewDecoder(strings.NewReader(`{"integer":9007199254740993,"float":1.0,"negative_zero":-0.0,"nested":[null,true,"NaN",{"x":2}],"nan":{"$tck":"NaN"},"positive_infinity":{"$tck":"Inf"},"negative_infinity":{"$tck":"-Inf"},"typed_negative_zero":{"$tck":"-0.0"}}`))
	decoder.UseNumber()
	var input map[string]any
	if err := decoder.Decode(&input); err != nil {
		t.Fatal(err)
	}
	converted, err := convertDifferentialParameter(input)
	if err != nil {
		t.Fatal(err)
	}
	parameters := converted.(map[string]any)
	if parameters["integer"] != int64(9007199254740993) || parameters["float"] != float64(1) {
		t.Fatalf("numeric types or precision lost: %#v", parameters)
	}
	for _, name := range []string{"negative_zero", "typed_negative_zero"} {
		if value := parameters[name].(float64); value != 0 || !math.Signbit(value) {
			t.Errorf("%s lost its sign: %v", name, value)
		}
	}
	if !math.IsNaN(parameters["nan"].(float64)) || !math.IsInf(parameters["positive_infinity"].(float64), 1) || !math.IsInf(parameters["negative_infinity"].(float64), -1) {
		t.Errorf("nonfinite parameters lost: %#v", parameters)
	}
	if !reflect.DeepEqual(parameters["nested"], []any{nil, true, "NaN", map[string]any{"x": int64(2)}}) {
		t.Errorf("nested values changed: %#v", parameters["nested"])
	}
	if _, err := json.Marshal(differentialEvidenceValue(parameters)); err != nil {
		t.Fatalf("nonfinite evidence is not JSON-safe: %v", err)
	}
	for _, invalid := range []any{
		json.Number("9223372036854775808"),
		map[string]any{"$tck": "bogus"},
		map[string]any{"nested": []any{json.Number("invalid")}},
	} {
		if _, err := convertDifferentialParameter(invalid); err == nil {
			t.Errorf("accepted invalid parameter %#v", invalid)
		}
	}
}

func TestDifferentialDiagnosticAssertions(t *testing.T) {
	queryErr := &QueryError{
		Type: "ArithmeticError", Phase: "runtime", Detail: "not the server message",
		Cause: &neo4j.Neo4jError{Code: "Neo.ClientError.Statement.ArithmeticError", Msg: "/ by zero"},
	}
	tests := []struct {
		name     string
		err      error
		contract differentialCase
		wantErr  bool
	}{
		{name: "legacy success", contract: differentialCase{}},
		{name: "legacy error retains optional messages", err: queryErr},
		{name: "wrapped raw diagnostic", err: errors.Join(queryErr, errors.New("cleanup")), contract: differentialCase{ExpectedCode: "Neo.ClientError.Statement.ArithmeticError", ExpectedMessage: "/ by zero", ExpectedPhase: "runtime"}},
		{name: "code only", err: queryErr, contract: differentialCase{ExpectedCode: "Neo.ClientError.Statement.ArithmeticError"}},
		{name: "message only", err: queryErr, contract: differentialCase{ExpectedMessage: "/ by zero"}},
		{name: "phase only", err: queryErr, contract: differentialCase{ExpectedPhase: "runtime"}},
		{name: "missing error", contract: differentialCase{ExpectedPhase: "runtime"}, wantErr: true},
		{name: "unclassified error", err: errors.New("transport"), contract: differentialCase{ExpectedPhase: "runtime"}, wantErr: true},
		{name: "different code", err: queryErr, contract: differentialCase{ExpectedCode: "Neo.ClientError.Statement.SyntaxError"}, wantErr: true},
		{name: "different message", err: queryErr, contract: differentialCase{ExpectedMessage: "divide by zero"}, wantErr: true},
		{name: "exact message whitespace", err: queryErr, contract: differentialCase{ExpectedMessage: "/ by zero "}, wantErr: true},
		{name: "different phase", err: queryErr, contract: differentialCase{ExpectedPhase: "compile time"}, wantErr: true},
		{name: "no raw driver diagnostic", err: &QueryError{Type: "ArithmeticError", Phase: "runtime"}, contract: differentialCase{ExpectedMessage: "/ by zero"}, wantErr: true},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if err := differentialDiagnosticError(testCase.err, testCase.contract); (err != nil) != testCase.wantErr {
				t.Fatalf("diagnostic mismatch = %v, want error %v", err, testCase.wantErr)
			}
		})
	}
}

func TestDifferentialCorpusIntegrity(t *testing.T) {
	cases := loadDifferentialCases(t)
	if len(cases) < 116 || cases[115].Name != "canonical indexed comprehension map projection" {
		t.Fatal("original 116-case prefix was displaced")
	}
	names := make(map[string]bool, len(cases))
	matrixCells := 0
	for _, testCase := range cases {
		if names[testCase.Name] {
			t.Errorf("duplicate differential case %q", testCase.Name)
		}
		names[testCase.Name] = true
		if strings.HasPrefix(testCase.Name, "division matrix ") {
			matrixCells++
			if len(testCase.Parameters) != 2 || !testCase.NoEffects {
				t.Errorf("division cell %q requires two parameters and no-effects check", testCase.Name)
			}
		}
	}
	if matrixCells != 48 {
		t.Fatalf("division matrix has %d cells, want 48", matrixCells)
	}
}

func newDifferentialBackend(t *testing.T, driver neo4j.DriverWithContext, database string, mode TransactionMode) *BoltBackend {
	t.Helper()
	backend, err := NewBoltBackend(BoltBackendConfig{Driver: driver, DatabaseName: database, Mode: mode})
	if err != nil {
		t.Fatalf("create %s backend: %v", database, err)
	}
	return backend
}

func resetDifferentialBackend(t *testing.T, ctx context.Context, backend *BoltBackend, name string) {
	t.Helper()
	if err := backend.Reset(ctx); err != nil {
		t.Fatalf("reset %s: %v", name, err)
	}
	for _, query := range []string{
		"CREATE LOOKUP INDEX differential_node_labels IF NOT EXISTS FOR (n) ON EACH labels(n)",
		"CREATE LOOKUP INDEX differential_relationship_types IF NOT EXISTS FOR ()-[r]-() ON EACH type(r)",
	} {
		if _, err := backend.Execute(ctx, query, nil); err != nil {
			t.Fatalf("restore default %s lookup indexes: %v", name, err)
		}
	}
}

func executeDifferentialSetup(t *testing.T, ctx context.Context, backend *BoltBackend, name, query string) {
	t.Helper()
	if _, err := backend.Execute(ctx, query, nil); err != nil {
		t.Fatalf("execute %s setup: %v", name, err)
	}
}

func snapshotDifferentialBackend(t *testing.T, ctx context.Context, backend *BoltBackend, name string) GraphSnapshot {
	t.Helper()
	snapshot, err := backend.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot %s: %v", name, err)
	}
	return snapshot
}

func compareDifferentialErrors(t *testing.T, nornicErr, referenceErr error) {
	t.Helper()
	if nornicErr == nil && referenceErr == nil {
		return
	}
	if nornicErr == nil || referenceErr == nil {
		t.Errorf("error behavior differs: NornicDB %v, Neo4j %v", nornicErr, referenceErr)
		return
	}
	var nornicQueryErr, referenceQueryErr *QueryError
	if !errors.As(nornicErr, &nornicQueryErr) || !errors.As(referenceErr, &referenceQueryErr) {
		t.Errorf("unclassified differential errors: NornicDB %T %v, Neo4j %T %v", nornicErr, nornicErr, referenceErr, referenceErr)
		return
	}
	if nornicQueryErr.Type != referenceQueryErr.Type || nornicQueryErr.Phase != referenceQueryErr.Phase {
		t.Errorf("error classification differs: NornicDB %s/%s, Neo4j %s/%s", nornicQueryErr.Type, nornicQueryErr.Phase, referenceQueryErr.Type, referenceQueryErr.Phase)
	}
}

func waitForReference(ctx context.Context, driver neo4j.DriverWithContext) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		verifyCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		lastErr = driver.VerifyConnectivity(verifyCtx)
		cancel()
		if lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %v", ctx.Err(), lastErr)
		case <-ticker.C:
		}
	}
}
