package server

// N152-273 / X07: fine-grained unique-constraint diagnostics.
//
// A restricted writer holds CREATE and SET PROPERTY on Person but no READ on
// its unique property (ssn). Before this fix, the v1.5.2 hotfix redacted the
// single-property diagnostic but still returned different outcomes for held
// and absent hidden values; composite diagnostics also exposed the holder.
// These public HTTP and Bolt tests require uniform denial and no disclosure,
// while keeping unconstrained writes and readable unique writes available.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	neo4j "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/bolt"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	uniqueDiagDatabase   = "fgac_unique_diag"
	uniqueDiagRole       = "fgac_unique_diag_writer"
	uniqueDiagUser       = "fgac_unique_diag_user"
	uniqueDiagPassword   = "password123"
	uniqueDiagHeldSSN    = "ZQX-UNIQUE-1"
	uniqueDiagOtherSSN   = "ZQX-UNIQUE-2"
	uniqueDiagAbsentSSN  = "ZQX-UNIQUE-ABSENT"
	uniqueDiagHeldPair   = "ZQX-PAIR-1"
	uniqueDiagAbsentPair = "ZQX-PAIR-ABSENT"
	uniqueDiagHolder     = "alice"
)

// uniqueDiagIdentity is the holder's public identities: the element id and the
// raw node id the diagnostic used to print.
type uniqueDiagIdentity struct {
	elementID string
	nodeID    string
}

// uniqueDiagEnv is one server with a restricted user, the fixture graph and a
// Bolt listener bound to the same FGAC admission as HTTP.
type uniqueDiagEnv struct {
	t          *testing.T
	router     http.Handler
	adminToken string
	token      string
	bolt       neo4j.DriverWithContext

	holder     uniqueDiagIdentity
	pairHolder uniqueDiagIdentity
}

// uniqueDiagSetup builds the fixture and returns the environment. tokenGrant
// selects the GRANT CREATE NEW PROPERTY NAME mode with which a caller may write
// a property it cannot read; without it every probe is refused by FGAC.
func uniqueDiagSetup(t *testing.T, tokenGrant bool) *uniqueDiagEnv {
	t.Helper()
	server, authenticator := setupTestServerWithDatabaseConfig(t, nil, nil)
	if _, err := authenticator.CreateUser(context.Background(), uniqueDiagUser, uniqueDiagPassword, []auth.Role{auth.RoleViewer}); err != nil {
		t.Fatalf("create scoped user: %v", err)
	}
	env := &uniqueDiagEnv{t: t}
	manager := server.GetDatabaseManager()
	if err := manager.CreateDatabase(context.Background(), multidb.DatabaseCreateRequest{Name: uniqueDiagDatabase}); err != nil {
		t.Fatalf("create database: %v", err)
	}
	env.adminToken = getAuthToken(t, authenticator, "admin")
	env.router = server.buildRouter()
	for _, statement := range []string{
		"CREATE CONSTRAINT " + uniqueDiagDatabase + "_ssn IF NOT EXISTS FOR (p:Person) REQUIRE p.ssn IS UNIQUE",
		"CREATE (:Person {name: '" + uniqueDiagHolder + "', ssn: '" + uniqueDiagHeldSSN + "'})",
		"CREATE (:Person {name: 'bob', ssn: '" + uniqueDiagOtherSSN + "'})",
	} {
		if message := env.adminCypher(statement); message != "" {
			t.Fatalf("fixture %q: %s", statement, message)
		}
	}
	if err := server.initializeFineGrainedRuntime(context.Background()); err != nil {
		t.Fatalf("fine-grained runtime: %v", err)
	}
	env.router = server.buildRouter()
	grants := []string{
		"CREATE ROLE " + uniqueDiagRole + " IF NOT EXISTS",
		"GRANT ROLE " + uniqueDiagRole + " TO " + uniqueDiagUser,
		"GRANT ACCESS ON DATABASE " + uniqueDiagDatabase + " TO " + uniqueDiagRole,
		"GRANT TRAVERSE ON GRAPH " + uniqueDiagDatabase + " NODES Person TO " + uniqueDiagRole,
		"GRANT READ {name} ON GRAPH " + uniqueDiagDatabase + " NODES Person TO " + uniqueDiagRole,
		"GRANT CREATE ON GRAPH " + uniqueDiagDatabase + " NODES Person TO " + uniqueDiagRole,
		"GRANT SET PROPERTY {name, ssn, note} ON GRAPH " + uniqueDiagDatabase + " NODES Person TO " + uniqueDiagRole,
	}
	if tokenGrant {
		grants = append(grants, "GRANT CREATE NEW PROPERTY NAME ON DATABASE "+uniqueDiagDatabase+" TO "+uniqueDiagRole)
	}
	for _, statement := range grants {
		if message := env.adminCypher(statement); message != "" {
			t.Fatalf("grant %q: %s", statement, message)
		}
	}
	env.token = getAuthToken(t, authenticator, uniqueDiagUser)
	env.holder = env.identity("Person", uniqueDiagHolder)
	env.bolt = uniqueDiagDriver(t, uniqueDiagStartBolt(t, server, authenticator))
	return env
}

func uniqueDiagStartBolt(t *testing.T, server *Server, authenticator *auth.Authenticator) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	config := bolt.DefaultConfig()
	adapter := bolt.NewAuthenticatorAdapter(authenticator)
	adapter.SetGetEffectivePermissions(server.GetEffectivePermissions)
	config.Authenticator = adapter
	config.RequireAuth = true
	config.DatabaseExecutorOptions = server.DatabaseScopedExecutorOptions
	peer := bolt.NewWithDatabaseManager(config, nil, server.GetDatabaseManager())
	peer.SetDatabaseAccessModeResolver(server.GetDatabaseAccessModeForRoles)
	peer.SetResolvedAccessResolver(server.GetResolvedAccessForRoles)
	peer.SetProcedureGrantResolver(server.ProcedureGrantChecker)
	peer.SetFineGrainedAdmission(server.FineGrainedAdmission())
	finished := make(chan error, 1)
	go func() { finished <- peer.Serve(listener) }()
	t.Cleanup(func() {
		_ = peer.Close()
		select {
		case <-finished:
		case <-time.After(10 * time.Second):
		}
	})
	return "bolt://" + listener.Addr().String()
}

func uniqueDiagDriver(t *testing.T, uri string) neo4j.DriverWithContext {
	t.Helper()
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(uniqueDiagUser, uniqueDiagPassword, ""))
	if err != nil {
		t.Fatalf("driver: %v", err)
	}
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	return driver
}

// adminCypher runs one statement as admin and returns its first error message.
func (e *uniqueDiagEnv) adminCypher(statement string) string {
	code, body := e.request(http.MethodPost, "/db/"+uniqueDiagDatabase+"/tx/commit", e.adminToken,
		map[string]any{"statements": []any{map[string]any{"statement": statement}}})
	var response TransactionResponse
	if err := json.Unmarshal(body, &response); err == nil && len(response.Errors) > 0 {
		return response.Errors[0].Code + ": " + response.Errors[0].Message
	}
	if code >= 400 {
		return fmt.Sprintf("http %d: %.400s", code, body)
	}
	return ""
}

// identity returns a labelled node's element id and raw node id.
func (e *uniqueDiagEnv) identity(label, name string) uniqueDiagIdentity {
	code, body := e.request(http.MethodPost, "/db/"+uniqueDiagDatabase+"/tx/commit", e.adminToken, map[string]any{
		"statements": []any{map[string]any{"statement": "MATCH (p:" + label + " {name: '" + name + "'}) RETURN elementId(p) AS e, id(p) AS i"}},
	})
	if code != http.StatusOK {
		e.t.Fatalf("identity: http %d: %.400s", code, body)
	}
	var response TransactionResponse
	if err := json.Unmarshal(body, &response); err != nil || len(response.Results) == 0 || len(response.Results[0].Data) == 0 {
		e.t.Fatalf("identity decode: %v", err)
	}
	row := response.Results[0].Data[0].Row
	elementID, _ := row[0].(string)
	return uniqueDiagIdentity{elementID: elementID, nodeID: fmt.Sprint(row[1])}
}

func (e *uniqueDiagEnv) request(method, path, token string, body any) (int, []byte) {
	e.t.Helper()
	var payload []byte
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			e.t.Fatalf("marshal: %v", err)
		}
		payload = encoded
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	recorder := httptest.NewRecorder()
	e.router.ServeHTTP(recorder, req)
	return recorder.Code, recorder.Body.Bytes()
}

// uniqueDiagOutcome is everything one transport returned for one probe.
type uniqueDiagOutcome struct {
	transport string
	code      string
	message   string
	rows      []string
	raw       string
}

func (o uniqueDiagOutcome) failed() bool { return o.code != "" }

func (o uniqueDiagOutcome) rowsText() string { return strings.Join(o.rows, " | ") }

// httpProbe runs one statement in a query-API transaction and rolls it back.
func (e *uniqueDiagEnv) httpProbe(statement string, params map[string]any) uniqueDiagOutcome {
	e.t.Helper()
	code, body := e.request(http.MethodPost, "/db/"+uniqueDiagDatabase+"/query/v2/tx", e.token, map[string]any{})
	if code < 200 || code >= 300 {
		return uniqueDiagOutcome{transport: "http", code: fmt.Sprintf("HTTP-OPEN-%d", code), message: string(body), raw: string(body)}
	}
	var opened struct {
		Transaction *struct {
			ID string `json:"id"`
		} `json:"transaction"`
	}
	if err := json.Unmarshal(body, &opened); err != nil || opened.Transaction == nil || opened.Transaction.ID == "" {
		return uniqueDiagOutcome{transport: "http", code: "HTTP-OPEN-UNDECODED", message: string(body), raw: string(body)}
	}
	txPath := "/db/" + uniqueDiagDatabase + "/query/v2/tx/" + opened.Transaction.ID
	code, body = e.request(http.MethodPost, txPath, e.token, map[string]any{"statement": statement, "parameters": params})
	e.request(http.MethodDelete, txPath, e.token, nil)

	out := uniqueDiagOutcome{transport: "http", raw: string(body)}
	var response queryAPIResponse
	if err := json.Unmarshal(body, &response); err != nil {
		out.code = fmt.Sprintf("HTTP-%d", code)
		out.message = string(body)
		return out
	}
	if len(response.Errors) > 0 {
		out.code = response.Errors[0].Code
		out.message = response.Errors[0].Message
	}
	if response.Data != nil {
		for _, row := range response.Data.Values {
			out.rows = append(out.rows, fmt.Sprint(row))
		}
	}
	if out.code == "" && code >= 400 {
		out.code = fmt.Sprintf("HTTP-%d", code)
		out.message = string(body)
	}
	return out
}

// boltProbe runs one statement in an explicit Bolt transaction and rolls it
// back.
func (e *uniqueDiagEnv) boltProbe(statement string, params map[string]any) uniqueDiagOutcome {
	e.t.Helper()
	out := uniqueDiagOutcome{transport: "bolt"}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	session := e.bolt.NewSession(ctx, neo4j.SessionConfig{DatabaseName: uniqueDiagDatabase})
	defer session.Close(context.Background())
	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		out.code = "BOLT-BEGIN"
		out.message = err.Error()
		out.raw = out.code + " " + out.message
		return out
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	result, runErr := tx.Run(ctx, statement, params)
	if runErr != nil {
		out.code, out.message = uniqueDiagBoltError(runErr)
	} else if result != nil {
		records, collectErr := result.Collect(ctx)
		if collectErr != nil {
			out.code, out.message = uniqueDiagBoltError(collectErr)
		}
		for _, record := range records {
			out.rows = append(out.rows, fmt.Sprint(record.Values))
		}
	}
	out.raw = out.code + " " + out.message + " " + out.rowsText()
	return out
}

// committedProbe observes constraint checks that occur at commit rather than
// during statement execution. An absent control uses a distinct value per
// transport so its successful commit cannot influence the next probe.
func (e *uniqueDiagEnv) committedProbe(transport, statement string, params map[string]any) uniqueDiagOutcome {
	e.t.Helper()
	if transport == "http" {
		code, body := e.request(http.MethodPost, "/db/"+uniqueDiagDatabase+"/tx/commit", e.token,
			map[string]any{"statements": []any{map[string]any{"statement": statement, "parameters": params}}})
		out := uniqueDiagOutcome{transport: transport, raw: string(body)}
		var response TransactionResponse
		if err := json.Unmarshal(body, &response); err != nil || code >= 400 {
			out.code = fmt.Sprintf("HTTP-%d", code)
			out.message = string(body)
		} else if len(response.Errors) > 0 {
			out.code, out.message = response.Errors[0].Code, response.Errors[0].Message
		}
		return out
	}
	out := uniqueDiagOutcome{transport: transport}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	session := e.bolt.NewSession(ctx, neo4j.SessionConfig{DatabaseName: uniqueDiagDatabase})
	defer session.Close(context.Background())
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, statement, params)
		if err != nil {
			return nil, err
		}
		_, err = result.Consume(ctx)
		return nil, err
	})
	if err != nil {
		out.code, out.message = uniqueDiagBoltError(err)
	}
	out.raw = out.code + " " + out.message
	return out
}

func uniqueDiagBoltError(err error) (code, message string) {
	var neoErr *neo4j.Neo4jError
	if errors.As(err, &neoErr) {
		return neoErr.Code, neoErr.Msg
	}
	return "BOLT-ERROR", err.Error()
}

// uniqueDiagNoDisclosure asserts the hotfix contract: a scoped diagnostic and
// its returned rows do not carry the unreadable value, and no answer names the
// holder by node id, element id or readable name.
func uniqueDiagNoDisclosure(t *testing.T, where string, out uniqueDiagOutcome, holder uniqueDiagIdentity, hiddenValues ...string) {
	t.Helper()
	for _, value := range hiddenValues {
		assert.NotContainsf(t, out.message, value, "%s [%s]: diagnostic discloses the unreadable value", where, out.transport)
		assert.NotContainsf(t, out.rowsText(), value, "%s [%s]: returned rows disclose the unreadable value", where, out.transport)
	}
	for _, identity := range []string{holder.nodeID, holder.elementID} {
		if identity == "" {
			continue
		}
		assert.NotContainsf(t, out.message, identity, "%s [%s]: diagnostic discloses the holder node id", where, out.transport)
		assert.NotContainsf(t, out.rowsText(), identity, "%s [%s]: returned rows disclose the holder node id", where, out.transport)
	}
	assert.NotContainsf(t, out.raw, uniqueDiagHolder, "%s [%s]: the answer names the holder", where, out.transport)
}

// uniqueDiagProbe is one public write form. held/absent are the same statement
// with the held value and with a value no node holds.
type uniqueDiagProbe struct {
	form      string
	statement string
	held      map[string]any
	absent    map[string]any
}

func uniqueDiagProbes() []uniqueDiagProbe {
	return []uniqueDiagProbe{
		{
			form:      "create",
			statement: "CREATE (:Person {name: $name, ssn: $value})",
			held:      map[string]any{"name": "probe-create", "value": uniqueDiagHeldSSN},
			absent:    map[string]any{"name": "probe-create-absent", "value": uniqueDiagAbsentSSN},
		},
		{
			form:      "set",
			statement: "MATCH (p:Person {name: 'bob'}) SET p.ssn = $value",
			held:      map[string]any{"value": uniqueDiagHeldSSN},
			absent:    map[string]any{"value": uniqueDiagAbsentSSN},
		},
		{
			form:      "merge-property",
			statement: "MERGE (p:Person {ssn: $value}) RETURN p.name AS name",
			held:      map[string]any{"value": uniqueDiagHeldSSN},
			absent:    map[string]any{"value": uniqueDiagAbsentSSN},
		},
		{
			form:      "merge-named",
			statement: "MERGE (p:Person {name: $name, ssn: $value}) RETURN p.name AS name",
			held:      map[string]any{"name": "probe-merge", "value": uniqueDiagHeldSSN},
			absent:    map[string]any{"name": "probe-merge-absent", "value": uniqueDiagAbsentSSN},
		},
	}
}

// TestFineGrainedUniqueConstraintDiagnostic checks public HTTP and Bolt
// diagnostics with both parsers. The held and absent controls must have the
// same refusal so a write cannot reveal whether an unreadable value is held.
func TestFineGrainedUniqueConstraintDiagnostic(t *testing.T) {
	for _, parser := range []string{"default", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			restore := nornicConfig.WithNornicParser()
			if parser == "antlr" {
				restore = nornicConfig.WithANTLRParser()
			}
			defer restore()

			t.Run("writer-with-property-name-grant", func(t *testing.T) {
				env := uniqueDiagSetup(t, true)
				for _, probe := range uniqueDiagProbes() {
					for _, transport := range []string{"http", "bolt"} {
						t.Run(probe.form+"/"+transport, func(t *testing.T) {
							held := env.probe(transport, probe.statement, probe.held)
							absent := env.probe(transport, probe.statement, probe.absent)
							t.Logf("held=%+v absent=%+v", held, absent)

							uniqueDiagNoDisclosure(t, probe.form+"/held", held, env.holder, uniqueDiagHeldSSN)
							uniqueDiagNoDisclosure(t, probe.form+"/absent", absent, env.holder, uniqueDiagHeldSSN)

							require.Truef(t, held.failed(), "%s: held value must be refused", probe.form)
							require.Truef(t, absent.failed(), "%s: absent value must be refused", probe.form)
							assert.Equal(t, held.code, absent.code, "held and absent values must have the same error code")
							assert.Equal(t, held.message, absent.message, "held and absent values must have the same error message")
						})
					}
				}
				for _, transport := range []string{"http", "bolt"} {
					out := env.probe(transport, "CREATE (:Person {name: $name, note: $value})", map[string]any{"name": "unconstrained-note", "value": "allowed"})
					require.Falsef(t, out.failed(), "unconstrained unreadable property must retain property-name grant: %s: %s", out.code, out.message)
				}
			})

			t.Run("writer-without-property-name-grant", func(t *testing.T) {
				env := uniqueDiagSetup(t, false)
				for _, probe := range uniqueDiagProbes() {
					for _, transport := range []string{"http", "bolt"} {
						t.Run(probe.form+"/"+transport, func(t *testing.T) {
							held := env.probe(transport, probe.statement, probe.held)
							absent := env.probe(transport, probe.statement, probe.absent)
							t.Logf("held=%+v absent=%+v", held, absent)

							uniqueDiagNoDisclosure(t, probe.form+"/held", held, env.holder, uniqueDiagHeldSSN)
							require.Truef(t, held.failed(), "%s: the write must be refused without the property-name grant", probe.form)
							require.Truef(t, absent.failed(), "%s: the absent-value control must be refused without the property-name grant", probe.form)
							assert.Equalf(t, held.code, absent.code, "%s: held and absent refusals differ in code", probe.form)
							assert.Equalf(t, held.message, absent.message, "%s: held and absent refusals differ in message", probe.form)
						})
					}
				}
			})

			t.Run("reader-writer", func(t *testing.T) {
				env := uniqueDiagSetup(t, true)
				if message := env.adminCypher("GRANT READ {ssn} ON GRAPH " + uniqueDiagDatabase + " NODES Person TO " + uniqueDiagRole); message != "" {
					t.Fatal(message)
				}
				for _, transport := range []string{"http", "bolt"} {
					t.Run(transport, func(t *testing.T) {
						out := env.probe(transport, "CREATE (:Person {name: $name, ssn: $value})", map[string]any{"name": "readable-unique", "value": "new-readable-value"})
						require.Falsef(t, out.failed(), "a reader-writer may set an available unique value: %s", out.raw)
					})
				}
			})

			t.Run("reader-without-traverse", func(t *testing.T) {
				env := uniqueDiagSetup(t, true)
				for _, statement := range []string{
					"GRANT READ {ssn} ON GRAPH " + uniqueDiagDatabase + " NODES Person TO " + uniqueDiagRole,
					"DENY TRAVERSE ON GRAPH " + uniqueDiagDatabase + " NODES Person TO " + uniqueDiagRole,
				} {
					if message := env.adminCypher(statement); message != "" {
						t.Fatal(message)
					}
				}
				for _, transport := range []string{"http", "bolt"} {
					t.Run(transport, func(t *testing.T) {
						const create = "CREATE (:Person {name: $name, ssn: $value})"
						held := env.probe(transport, create, map[string]any{"name": "hidden-holder-probe", "value": uniqueDiagHeldSSN})
						absent := env.probe(transport, create, map[string]any{"name": "hidden-holder-probe", "value": uniqueDiagAbsentSSN})
						require.Truef(t, held.failed(), "held value must be refused: %s", held.raw)
						require.Truef(t, absent.failed(), "absent value must be refused: %s", absent.raw)
						assert.Equal(t, held.code, absent.code)
						assert.Equal(t, held.message, absent.message)
					})
				}
			})

			t.Run("hidden-key-removal", func(t *testing.T) {
				env := uniqueDiagSetup(t, true)
				for _, statement := range []string{
					"MATCH (p:Person {name: 'bob'}) SET p.ssn = null",
					"MATCH (p:Person {name: 'bob'}) SET p = {name: 'bob'}",
				} {
					for _, transport := range []string{"http", "bolt"} {
						t.Run(transport, func(t *testing.T) {
							out := env.probe(transport, statement, nil)
							require.Truef(t, out.failed(), "an unreadable constrained key must not be removed: %s", out.raw)
							require.Equal(t, "Neo.ClientError.Security.Forbidden", out.code)
						})
					}
				}
			})

			t.Run("same-transaction-unique", func(t *testing.T) {
				env := uniqueDiagSetup(t, true)
				const hidden = "ZQX-PENDING-UNIQUE"
				const statement = "CREATE (:Person {name: 'pending-one', ssn: '" + hidden + "'}), (:Person {name: 'pending-two', ssn: '" + hidden + "'})"
				for _, transport := range []string{"http", "bolt"} {
					t.Run(transport, func(t *testing.T) {
						out := env.probe(transport, statement, nil)
						require.True(t, out.failed(), "a duplicate pending unique value must be rejected")
						uniqueDiagNoDisclosure(t, "same-transaction-unique", out, uniqueDiagIdentity{}, hidden)
					})
				}
			})

			t.Run("multi-property-unique", func(t *testing.T) {
				env := uniqueDiagSetup(t, true)
				for _, statement := range []string{
					"CREATE CONSTRAINT " + uniqueDiagDatabase + "_pair IF NOT EXISTS FOR (p:Pair) REQUIRE (p.name, p.ssn) IS UNIQUE",
					"CREATE (:Pair {name: '" + uniqueDiagHolder + "', ssn: '" + uniqueDiagHeldPair + "'})",
					"GRANT TRAVERSE ON GRAPH " + uniqueDiagDatabase + " NODES Pair TO " + uniqueDiagRole,
					"GRANT READ {name} ON GRAPH " + uniqueDiagDatabase + " NODES Pair TO " + uniqueDiagRole,
					"GRANT CREATE ON GRAPH " + uniqueDiagDatabase + " NODES Pair TO " + uniqueDiagRole,
					"GRANT SET PROPERTY {name, ssn} ON GRAPH " + uniqueDiagDatabase + " NODES Pair TO " + uniqueDiagRole,
				} {
					if message := env.adminCypher(statement); message != "" {
						t.Fatalf("%q: %s", statement, message)
					}
				}
				env.pairHolder = env.identity("Pair", uniqueDiagHolder)
				const statement = "CREATE (:Pair {name: '" + uniqueDiagHolder + "', ssn: '" + uniqueDiagHeldPair + "'})"
				const absentStatement = "CREATE (:Pair {name: '" + uniqueDiagHolder + "', ssn: '" + uniqueDiagAbsentPair + "'})"
				for _, transport := range []string{"http", "bolt"} {
					t.Run(transport, func(t *testing.T) {
						held := env.probe(transport, statement, nil)
						absent := env.probe(transport, absentStatement, nil)
						t.Logf("held=%+v absent=%+v", held, absent)

						uniqueDiagNoDisclosure(t, "multi-property-unique/held", held, env.pairHolder, uniqueDiagHeldPair)
						require.Truef(t, held.failed(), "multi-property-unique: held key must be refused")
						require.Truef(t, absent.failed(), "multi-property-unique: absent key must be refused")
						assert.Equal(t, held.code, absent.code, "held and absent composite keys must have the same error code")
						assert.Equal(t, held.message, absent.message, "held and absent composite keys must have the same error message")
					})
				}
				const pending = "ZQX-PENDING-PAIR"
				const pendingStatement = "CREATE (:Pair {name: 'pending', ssn: '" + pending + "'}), (:Pair {name: 'pending', ssn: '" + pending + "'})"
				for _, transport := range []string{"http", "bolt"} {
					t.Run("same-transaction/"+transport, func(t *testing.T) {
						out := env.probe(transport, pendingStatement, nil)
						require.True(t, out.failed(), "a duplicate pending composite key must be rejected")
						uniqueDiagNoDisclosure(t, "same-transaction-composite", out, uniqueDiagIdentity{}, pending)
					})
				}
			})

			t.Run("relationship-unique", func(t *testing.T) {
				env := uniqueDiagSetup(t, true)
				for _, statement := range []string{
					"CREATE CONSTRAINT " + uniqueDiagDatabase + "_rel_key IF NOT EXISTS FOR ()-[r:SecretRel]-() REQUIRE r.key IS UNIQUE",
					"MATCH (a:Person {name: 'alice'}) MATCH (b:Person {name: 'bob'}) CREATE (a)-[:SecretRel {key: 'held-rel-key'}]->(b)",
					"GRANT CREATE ON GRAPH " + uniqueDiagDatabase + " RELATIONSHIPS SecretRel TO " + uniqueDiagRole,
					"GRANT SET PROPERTY {key, note} ON GRAPH " + uniqueDiagDatabase + " RELATIONSHIPS SecretRel TO " + uniqueDiagRole,
				} {
					if message := env.adminCypher(statement); message != "" {
						t.Fatalf("%q: %s", statement, message)
					}
				}
				const create = "MATCH (a:Person {name: 'alice'}) MATCH (b:Person {name: 'bob'}) CREATE (a)-[:SecretRel {key: $value}]->(b)"
				for _, transport := range []string{"http", "bolt"} {
					t.Run(transport, func(t *testing.T) {
						held := env.probe(transport, create, map[string]any{"value": "held-rel-key"})
						absent := env.probe(transport, create, map[string]any{"value": "absent-rel-key"})
						require.Truef(t, held.failed(), "held relationship key must be refused: %s", held.raw)
						require.Truef(t, absent.failed(), "absent relationship key must be refused: %s", absent.raw)
						assert.Equal(t, held.code, absent.code)
						assert.Equal(t, held.message, absent.message)
						uniqueDiagNoDisclosure(t, "relationship-unique", held, uniqueDiagIdentity{}, "held-rel-key")
						control := env.probe(transport, "MATCH (a:Person {name: 'alice'}) MATCH (b:Person {name: 'bob'}) CREATE (a)-[:SecretRel {note: 'allowed'}]->(b)", nil)
						require.Falsef(t, control.failed(), "unconstrained relationship property must remain writable: %s", control.raw)
					})
				}
			})

			t.Run("relationship-hidden-endpoint", func(t *testing.T) {
				env := uniqueDiagSetup(t, true)
				for _, statement := range []string{
					"CREATE CONSTRAINT " + uniqueDiagDatabase + "_hidden_rel IF NOT EXISTS FOR ()-[r:HiddenRel]-() REQUIRE r.key IS UNIQUE",
					"CREATE (:PrivateEndpoint {name: 'hidden-endpoint'})",
					"MATCH (a:PrivateEndpoint {name: 'hidden-endpoint'}) MATCH (b:Person {name: 'bob'}) CREATE (a)-[:HiddenRel {key: 'hidden-rel-held'}]->(b)",
					"GRANT CREATE ON GRAPH " + uniqueDiagDatabase + " RELATIONSHIPS HiddenRel TO " + uniqueDiagRole,
					"GRANT TRAVERSE ON GRAPH " + uniqueDiagDatabase + " RELATIONSHIPS HiddenRel TO " + uniqueDiagRole,
					"GRANT READ {key} ON GRAPH " + uniqueDiagDatabase + " RELATIONSHIPS HiddenRel TO " + uniqueDiagRole,
					"GRANT SET PROPERTY {key} ON GRAPH " + uniqueDiagDatabase + " RELATIONSHIPS HiddenRel TO " + uniqueDiagRole,
				} {
					if message := env.adminCypher(statement); message != "" {
						t.Fatalf("%q: %s", statement, message)
					}
				}
				const create = "CREATE (:Person {name: 'visible-start'})-[:HiddenRel {key: $value}]->(:Person {name: 'visible-end'})"
				for _, transport := range []string{"http", "bolt"} {
					t.Run(transport, func(t *testing.T) {
						held := env.committedProbe(transport, create, map[string]any{"value": "hidden-rel-held"})
						absent := env.committedProbe(transport, create, map[string]any{"value": "hidden-rel-absent-" + transport})
						require.Truef(t, held.failed(), "held hidden relationship key must be refused: %s", held.raw)
						require.Truef(t, absent.failed(), "absent hidden relationship key must be refused: %s", absent.raw)
						assert.Equal(t, held.code, absent.code)
						assert.Equal(t, held.message, absent.message)
					})
				}
				if message := env.adminCypher("GRANT TRAVERSE ON GRAPH " + uniqueDiagDatabase + " NODES * TO " + uniqueDiagRole); message != "" {
					t.Fatal(message)
				}
				for _, transport := range []string{"http", "bolt"} {
					t.Run("authorized/"+transport, func(t *testing.T) {
						available := env.committedProbe(transport, create, map[string]any{"value": "visible-rel-available-" + transport})
						require.Falsef(t, available.failed(), "fully visible relationship writer may set an available unique key: %s", available.raw)
					})
				}
			})

			t.Run("label-addition", func(t *testing.T) {
				env := uniqueDiagSetup(t, true)
				for _, statement := range []string{
					"CREATE (:Draft {name: 'draft-held', ssn: '" + uniqueDiagHeldSSN + "'})",
					"CREATE (:Draft {name: 'draft-absent', ssn: '" + uniqueDiagAbsentSSN + "'})",
					"GRANT TRAVERSE ON GRAPH " + uniqueDiagDatabase + " NODES Draft TO " + uniqueDiagRole,
					"GRANT READ {name} ON GRAPH " + uniqueDiagDatabase + " NODES Draft TO " + uniqueDiagRole,
					"GRANT SET LABEL Person ON GRAPH " + uniqueDiagDatabase + " TO " + uniqueDiagRole,
				} {
					if message := env.adminCypher(statement); message != "" {
						t.Fatalf("%q: %s", statement, message)
					}
				}
				for _, transport := range []string{"http", "bolt"} {
					t.Run(transport, func(t *testing.T) {
						held := env.probe(transport, "MATCH (p:Draft {name: 'draft-held'}) SET p:Person", nil)
						absent := env.probe(transport, "MATCH (p:Draft {name: 'draft-absent'}) SET p:Person", nil)
						require.Truef(t, held.failed(), "held key must be refused before label mutation: %s", held.raw)
						require.Truef(t, absent.failed(), "absent key must be refused before label mutation: %s", absent.raw)
						assert.Equal(t, held.code, absent.code)
						assert.Equal(t, held.message, absent.message)
					})
				}
			})
		})
	}
}

func (e *uniqueDiagEnv) probe(transport, statement string, params map[string]any) uniqueDiagOutcome {
	if transport == "http" {
		return e.httpProbe(statement, params)
	}
	return e.boltProbe(statement, params)
}
