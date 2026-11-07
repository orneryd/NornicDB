package cypher

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// LOAD CSV is a remote pass-through clause. Exercise its complete grammar
// without adding a CSV implementation to the local storage executor.
func TestFabricQueryStructureLoadCSVSeparators(t *testing.T) {
	base := newFabricCallRowsExecutor(t)
	driver := fabricCallRowsReference(t)
	if driver == nil {
		t.Skip("remote LOAD CSV oracle requires --ref neo-ref")
	}
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	target, err := url.Parse(uri)
	require.NoError(t, err)
	route, err := net.Dial("udp", target.Host)
	require.NoError(t, err)
	host := route.LocalAddr().(*net.UDPAddr).IP.String()
	require.NoError(t, route.Close())
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	csv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/headers" {
			fmt.Fprint(w, "v\n7\n")
		} else {
			fmt.Fprint(w, "7\n")
		}
	}))
	require.NoError(t, csv.Listener.Close())
	csv.Listener = listener
	csv.Start()
	defer csv.Close()
	source := "http://" + net.JoinHostPort(host, fmt.Sprint(listener.Addr().(*net.TCPAddr).Port))
	manager := base.dbManager.(*testDatabaseManagerAdapter).manager
	require.NoError(t, manager.AddConstituent(t.Context(), "comp", multidb.ConstituentRef{Alias: "csv", DatabaseName: "neo4j", Type: "remote", AccessMode: "read_write", URI: uri}))
	for _, normalization := range []bool{true, false} {
		t.Run(fmt.Sprintf("normalization_%v", normalization), func(t *testing.T) {
			restore := func() {}
			if !normalization {
				restore = config.WithCypherQueryNormalizationDisabled()
			}
			defer restore()
			for _, separator := range loadCSVClauseSeparators {
				for _, headers := range []bool{false, true} {
					t.Run(fmt.Sprintf("headers_%v/%q", headers, separator), func(t *testing.T) {
						path := "/plain"
						if headers {
							path = "/headers"
						}
						clause := loadCSVClause(source, path, "row", separator, headers, "")
						// The remote query itself has no nested CALL: Neo4j's single database is
						// both the source constituent and the direct reference execution target.
						fabricCompareGrammarCase(t, base, driver, "USE comp.csv "+clause, clause)
					})
				}
			}
			for _, word := range fabricGrammarWords() {
				for _, quoted := range []bool{false, true} {
					name := strings.ToLower(word)
					if quoted {
						name = "`" + name + "`"
					}
					t.Run(fmt.Sprintf("binding_%s/quoted_%v", word, quoted), func(t *testing.T) {
						clause := "LOAD CSV FROM '" + source + "/plain' AS " + name + " RETURN " + name + " AS k"
						fabricCompareGrammarCase(t, base, driver, "USE comp.csv "+clause, clause)
					})
				}
			}
		})
	}
}

// loadCSVClauseSeparators are the word separators and comments the shared
// lexer accepts between the clause's own tokens.
var loadCSVClauseSeparators = []string{" ", "  ", "\n", "\t", " /* UNION USE CALL */ ", " // UNION USE CALL\n"}

// loadCSVClause spells one complete LOAD CSV clause: the reading clause, its
// optional HEADERS and FIELDTERMINATOR forms, and the RETURN pipeline of the
// bound record variable, so the whole statement - not a prefix - is exercised.
func loadCSVClause(source, path, variable, separator string, headers bool, terminator string) string {
	projection := "toInteger(" + variable + "[0]) AS k"
	clause := "LOAD" + separator + "CSV FROM '" + source + path + "' AS " + variable
	if headers {
		clause = "LOAD" + separator + "CSV" + separator + "WITH" + separator + "HEADERS FROM '" + source + path + "' AS " + variable
		projection = "toInteger(" + variable + ".v) AS k"
	}
	if terminator != "" {
		clause += " FIELDTERMINATOR '" + terminator + "'"
	}
	return clause + " RETURN " + projection
}

// loadCSVRecordedRemote is the supported participant stand-in: a storage Engine
// whose Cypher capability records every statement Fabric hands it, so the
// forwarded statement and its read/write mode are observed rather than inferred
// from a connection failure. It answers with the columns and rows a remote
// participant returns.
type loadCSVRecordedRemote struct {
	storage.Engine
	columns []string
	rows    [][]interface{}
	mu      sync.Mutex
	sent    []string
	modes   []storage.RemoteAccessMode
}

func (remote *loadCSVRecordedRemote) record(statement string, mode storage.RemoteAccessMode) {
	remote.mu.Lock()
	defer remote.mu.Unlock()
	remote.sent = append(remote.sent, statement)
	remote.modes = append(remote.modes, mode)
}

func (remote *loadCSVRecordedRemote) statementModes() []storage.RemoteAccessMode {
	remote.mu.Lock()
	defer remote.mu.Unlock()
	return append([]storage.RemoteAccessMode(nil), remote.modes...)
}

func (remote *loadCSVRecordedRemote) QueryCypher(_ context.Context, statement string, _ map[string]interface{}, mode storage.RemoteAccessMode) ([]string, [][]interface{}, error) {
	remote.record(statement, mode)
	return remote.columns, remote.rows, nil
}

func (remote *loadCSVRecordedRemote) BeginCypherTx(_ context.Context, mode storage.RemoteAccessMode) (storage.RemoteCypherTx, error) {
	return &loadCSVRecordedRemoteTx{remote: remote, mode: mode}, nil
}

// Close is a no-op: the fixture owns the embedded engine it decorates.
func (remote *loadCSVRecordedRemote) Close() error { return nil }

type loadCSVRecordedRemoteTx struct {
	remote *loadCSVRecordedRemote
	mode   storage.RemoteAccessMode
}

func (tx *loadCSVRecordedRemoteTx) QueryCypher(_ context.Context, statement string, _ map[string]interface{}) ([]string, [][]interface{}, error) {
	tx.remote.record(statement, tx.mode)
	return tx.remote.columns, tx.remote.rows, nil
}

func (tx *loadCSVRecordedRemoteTx) ResolvedWriteMode() (bool, bool) {
	return tx.mode == storage.RemoteAccessWrite, true
}

func (tx *loadCSVRecordedRemoteTx) Commit(context.Context) error   { return nil }
func (tx *loadCSVRecordedRemoteTx) Rollback(context.Context) error { return nil }

// newLoadCSVParticipantExecutor is the composite fixture whose "comp.csv"
// constituent is the recorded participant in the requested access mode.
func newLoadCSVParticipantExecutor(t *testing.T, accessMode string) (*StorageExecutor, *loadCSVRecordedRemote) {
	t.Helper()
	remote := &loadCSVRecordedRemote{
		Engine:  newTestMemoryEngine(t),
		columns: []string{"k"},
		rows:    [][]interface{}{{int64(7)}},
	}
	cfg := multidb.DefaultConfig()
	cfg.RemoteEngineFactory = func(multidb.ConstituentRef, string) (storage.Engine, error) { return remote, nil }
	manager := newCompositeNativeManager(t, cfg)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "comp", []multidb.ConstituentRef{
		{Alias: "csv", DatabaseName: "neo4j", Type: "remote", AccessMode: accessMode, URI: "neo4j://127.0.0.1:1"},
	}))
	composite, err := manager.GetStorage("comp")
	require.NoError(t, err)
	executor := NewStorageExecutor(composite)
	executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	return executor, remote
}

// loadCSVStatementEffects reads the retained grammar effects of one statement.
func loadCSVStatementEffects(t *testing.T, query string) cypherantlr.StatementEffects {
	t.Helper()
	syntax, err := cypherantlr.ParseSyntax(context.Background(), query, cypherantlr.StatementEntry, false, cypherantlr.AcceptedSource)
	require.NoError(t, err, "the shared grammar must read the LOAD CSV statement: %s", query)
	require.True(t, syntax.Valid())
	return syntax.Effects()
}

// requireLoadCSVReadForwarded asserts the whole accepted path of one read-only
// LOAD CSV statement: the grammar's tail is read (a mutating tail would be
// classified as a write), the statement scope binds the record variable and
// projects its columns, the remote statement policy proves the forwarded
// statement read-only, and the exact statement - LOAD CSV clause and its
// pipeline - reaches the participant, which supplies the rows.
func requireLoadCSVReadForwarded(t *testing.T, executor *StorageExecutor, remote *loadCSVRecordedRemote, query, clause string) {
	t.Helper()
	ctx := context.Background()
	require.False(t, loadCSVStatementEffects(t, query).Writes(), "a read LOAD CSV tail is not an update: %s", query)
	require.True(t, loadCSVStatementEffects(t, query).RemoteReadOnly(), "a read LOAD CSV tail is proven read-only: %s", query)
	contract, err := readStatementExecutionContract(ctx, query)
	require.NoError(t, err, "execution contract: %s", query)
	require.False(t, contract.writes, "a read LOAD CSV tail is not classified as mutating: %s", query)
	policy, err := readRemoteStatementPolicy(ctx, query[strings.Index(query, "LOAD"):])
	require.NoError(t, err, "remote statement policy: %s", query)
	require.False(t, policy.Uncertain, "the shared grammar reads the forwarded statement: %s", query)
	require.Equal(t, storage.RemoteAccessRead, policy.AccessMode, "a read LOAD CSV tail forwards a read: %s", query)
	admission, err := executor.admitStatement(ctx, query, fabricAdmission{})
	require.NoError(t, err, "statement scope and result columns: %s", query)
	require.True(t, admission.graph, "a LOAD CSV statement is a graph statement with columns: %s", query)
	require.NoError(t, admission.verifyColumns([]string{"k"}), "the projected column is the statement's: %s", query)
	before := len(fixtureowner.Snapshot(&remote.mu, &remote.sent))
	result, err := executor.Execute(ctx, query, nil)
	require.NoError(t, err, "the recorded participant answers the forwarded statement: %s", query)
	require.Equal(t, []string{"k"}, result.Columns)
	require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
	sent := fixtureowner.Snapshot(&remote.mu, &remote.sent)
	require.Len(t, sent, before+1, "exactly one statement reaches the participant: %s", query)
	require.Equal(t, storage.RemoteAccessRead, remote.statementModes()[before], "the participant is read: %s", query)
	expected := clause
	if config.IsCypherQueryNormalizationEnabled() {
		expected, _, err = normalizeQueryText(ctx, clause)
		require.NoError(t, err)
	}
	require.Equal(t, expected, sent[before], "the complete resource, header mode, record binding and tail survive the configured canonicalization")
	require.False(t, strings.HasPrefix(sent[before], "USE"), "the USE target is consumed by Fabric: %s", sent[before])
	require.True(t, strings.HasPrefix(sent[before], "LOAD"), "the LOAD CSV clause itself is forwarded: %s", sent[before])
	tail := clause[strings.Index(clause, "RETURN"):]
	require.True(t, strings.HasSuffix(sent[before], tail), "the whole LOAD CSV pipeline is forwarded: %s", sent[before])
}

// TestFabricLoadCSVReadIsForwarded is the always-on LOAD CSV coverage. It needs
// no Neo4j reference and claims no Neo4j behavior: for both parsers, with and
// without normalization, for every clause separator, for headered and plain
// resources, with and without FIELDTERMINATOR, and for plain, quoted and
// keyword record names, it proves the shared grammar admits the statement, its
// scope binds the record variable, its effects classify the tail as a read, and
// Fabric forwards the whole clause to the participant USE names.
func TestFabricLoadCSVReadIsForwarded(t *testing.T) {
	const source = "http://127.0.0.1:1"
	forEachCypherParserSetting(t, func(t *testing.T) {
		executor, remote := newLoadCSVParticipantExecutor(t, "read_write")
		for _, separator := range loadCSVClauseSeparators {
			for _, headers := range []bool{false, true} {
				t.Run(fmt.Sprintf("headers_%v/%q", headers, separator), func(t *testing.T) {
					path := "/plain"
					if headers {
						path = "/headers"
					}
					clause := loadCSVClause(source, path, "row", separator, headers, "")
					requireLoadCSVReadForwarded(t, executor, remote, "USE comp.csv "+clause, clause)
				})
			}
			clause := loadCSVClause(source, "/plain", "row", separator, false, ";")
			requireLoadCSVReadForwarded(t, executor, remote, "USE comp.csv "+clause, clause)
		}
		// A quoted name and a clause keyword as the record name are legal
		// bindings; the grammar's symbol owner keeps every clause keyword usable
		// as a name.
		for _, name := range []string{"row", "`row`", "csv", "load", "`load`", "`with`", "`match`", "`fieldterminator`"} {
			clause := loadCSVClause(source, "/plain", name, " ", false, "")
			requireLoadCSVReadForwarded(t, executor, remote, "USE comp.csv "+clause, clause)
		}
	})
}

// TestFabricLoadCSVWriteTailsAreClassifiedAsWrites is the write control: the
// same clause with an updating tail (CREATE, MERGE, and a CALL subquery that
// creates) is a write, and the participant is asked to write.
func TestFabricLoadCSVWriteTailsAreClassifiedAsWrites(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {

		const source = "http://127.0.0.1:1"
		executor, remote := newLoadCSVParticipantExecutor(t, "read_write")
		ctx := context.Background()
		for _, tail := range []struct {
			name  string
			query string
		}{
			{"create", "CREATE (:Imported {k: toInteger(row[0])})"},
			{"merge", "MERGE (:Imported {k: toInteger(row[0])})"},
			{"call_create", "CALL { WITH row CREATE (:Imported) RETURN 1 AS one } RETURN one AS k"},
		} {
			t.Run(tail.name, func(t *testing.T) {
				query := "USE comp.csv " + loadCSVClause(source, "/plain", "row", " ", false, "")
				query = strings.Replace(query, " RETURN toInteger(row[0]) AS k", " "+tail.query, 1)
				effects := loadCSVStatementEffects(t, query)
				require.True(t, effects.Writes(), "the tail's updating clause is a write: %s", query)
				require.False(t, effects.RemoteReadOnly(), "the tail's updating clause is not read-only: %s", query)
				contract, err := readStatementExecutionContract(ctx, query)
				require.NoError(t, err)
				require.True(t, contract.writes, "the execution contract is mutating: %s", query)
				policy, err := readRemoteStatementPolicy(ctx, query[strings.Index(query, "LOAD"):])
				require.NoError(t, err)
				require.Equal(t, storage.RemoteAccessWrite, policy.AccessMode, "the forwarded statement is a write: %s", query)
				if tail.name == "call_create" {
					// The CALL body is a local scope: the participant control below is
					// the read-only rejection, which needs no execution.
					return
				}
				before := len(fixtureowner.Snapshot(&remote.mu, &remote.sent))
				_, err = executor.Execute(ctx, query, nil)
				require.NoError(t, err)
				require.Len(t, fixtureowner.Snapshot(&remote.mu, &remote.sent), before+1)
				require.Equal(t, storage.RemoteAccessWrite, remote.statementModes()[before])
				require.Contains(t, fixtureowner.Snapshot(&remote.mu, &remote.sent)[before], " "+strings.Fields(tail.query)[0]+" ")
			})
		}
	})
}

// TestFabricLoadCSVReadOnlyConstituentControls is the read-only constituent
// control pair: a read LOAD CSV pipeline is accepted and forwarded, while a
// LOAD CSV pipeline that writes is rejected as a read-only violation and never
// forwarded.
func TestFabricLoadCSVReadOnlyConstituentControls(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {

		const source = "http://127.0.0.1:1"
		executor, remote := newLoadCSVParticipantExecutor(t, "read")
		ctx := context.Background()
		read := "USE comp.csv " + loadCSVClause(source, "/plain", "row", " ", false, "")
		requireLoadCSVReadForwarded(t, executor, remote, read, read[strings.Index(read, "LOAD"):])
		for _, tail := range []struct {
			name  string
			query string
		}{
			{"create", "CREATE (:Imported {k: toInteger(row[0])})"},
			{"merge", "MERGE (:Imported {k: toInteger(row[0])})"},
			{"call_create", "CALL { WITH row CREATE (:Imported) RETURN 1 AS one } RETURN one AS k"},
		} {
			t.Run(tail.name, func(t *testing.T) {
				query := strings.Replace(read, " RETURN toInteger(row[0]) AS k", " "+tail.query, 1)
				before := len(fixtureowner.Snapshot(&remote.mu, &remote.sent))
				_, err := executor.Execute(ctx, query, nil)
				require.ErrorContains(t, err, "Selected constituent is read-only", "a writing LOAD CSV pipeline is rejected: %s", query)
				require.Len(t, fixtureowner.Snapshot(&remote.mu, &remote.sent), before, "a rejected statement never reaches the participant: %s", query)
			})
		}
	})
}

// TestLoadCSVMalformedClauseRejected pins the grammar's own boundary on both
// parsers: a LOAD CSV clause missing its resource, its record variable, the
// whole-with-HEADERS form, or a string field terminator is a syntax error, not
// an admitted statement.
func TestLoadCSVMalformedClauseRejected(t *testing.T) {
	const source = "http://127.0.0.1:1"
	malformed := []struct {
		name  string
		query string
	}{
		{"missing_from", "LOAD CSV '" + source + "/plain' AS row RETURN row AS k"},
		{"missing_variable", "LOAD CSV FROM '" + source + "/plain' RETURN 1 AS k"},
		{"missing_as", "LOAD CSV FROM '" + source + "/plain'"},
		{"headers_without_with", "LOAD CSV HEADERS FROM '" + source + "/plain' AS row RETURN row AS k"},
		{"headers_without_from", "LOAD CSV WITH HEADERS '" + source + "/plain' AS row RETURN row AS k"},
		{"terminator_without_literal", "LOAD CSV FROM '" + source + "/plain' AS row FIELDTERMINATOR RETURN row AS k"},
		{"terminator_variable", "LOAD CSV FROM '" + source + "/plain' AS row FIELDTERMINATOR row RETURN row AS k"},
		{"terminator_before_binding", "LOAD CSV FROM '" + source + "/plain' FIELDTERMINATOR ';' AS row RETURN row AS k"},
	}
	forEachCypherParserSetting(t, func(t *testing.T) {
		federated, participant := newLoadCSVParticipantExecutor(t, "read_write")
		for _, route := range []struct {
			name, prefix string
			executor     *StorageExecutor
		}{
			{"local", "", NewStorageExecutor(newTestMemoryEngine(t))},
			{"remote_use", "USE comp.csv ", federated},
		} {
			t.Run(route.name, func(t *testing.T) {
				for _, malformed := range malformed {
					t.Run(malformed.name, func(t *testing.T) {
						query := route.prefix + malformed.query
						before := len(fixtureowner.Snapshot(&participant.mu, &participant.sent))
						_, err := route.executor.Execute(context.Background(), query, nil)
						require.Error(t, err, "a malformed LOAD CSV clause is rejected: %s", query)
						require.Contains(t, strings.ToLower(err.Error()), "syntax", "the rejection is a syntax error: %v", err)
						require.Len(t, fixtureowner.Snapshot(&participant.mu, &participant.sent), before, "malformed input must never reach the remote participant")
					})
				}
			})
		}
	})
}

// TestLoadCSVLocalStatementStaysUnsupported pins the other side of the same
// owner: without a USE clause the statement is NornicDB's embedded residual,
// not a local CSV reader, on both parsers and both normalizations. The grammar
// still reads the tail, so a name the clause does not bind is reported before
// the residual.
func TestLoadCSVLocalStatementStaysUnsupported(t *testing.T) {
	var requests atomic.Int32
	csv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = fmt.Fprint(w, "v\n7\n")
	}))
	defer csv.Close()
	// The pipeline has no reader for a remote resource, so a LOAD CSV statement
	// is never planned as a local clause pipeline.
	_, pipeline := canExecuteAsPipeline(context.Background(), "LOAD CSV FROM 'file:///rows.csv' AS row RETURN row AS k")
	require.False(t, pipeline, "LOAD CSV is not a local pipeline clause")
	forEachCypherParserSetting(t, func(t *testing.T) {
		executor := NewStorageExecutor(newTestMemoryEngine(t))
		for _, query := range []string{
			"LOAD CSV FROM '" + csv.URL + "/plain' AS row RETURN toInteger(row[0]) AS k",
			"LOAD CSV WITH HEADERS FROM '" + csv.URL + "/headers' AS row RETURN toInteger(row.v) AS k",
		} {
			_, err := executor.Execute(context.Background(), query, nil)
			require.ErrorContains(t, err, "LOAD CSV is not supported in NornicDB embedded mode", "statement: %s", query)
		}
		_, err := executor.Execute(context.Background(), "LOAD CSV FROM '"+csv.URL+"/plain' AS row RETURN missing AS k", nil)
		require.ErrorContains(t, err, "missing", "the clause's tail is read in the statement scope")
		require.NotContains(t, err.Error(), "LOAD CSV is not supported", "the scope error precedes the residual")
	})
	require.Zero(t, requests.Load(), "NornicDB has no local CSV reader: the resource is never fetched")
}
