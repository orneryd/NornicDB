package server

// FGAC search and index differential (scratch, test-only).
//
// A restricted principal runs every query that can reach an index or a search
// path in three data states of the same database:
//
//	S0 hidden values present,
//	S1 the same hidden entities with different hidden values,
//	S2 every hidden value and hidden entity removed.
//
// Visible data, node IDs and grants are identical in all three states. The
// restricted principal must get exactly the same answer in every state: rows,
// order, scores, counts, errors, plans, profile counters and HTTP metadata.
// Any difference is a value the principal may not read deciding what it gets
// back. Every hidden string contains the marker "zqx"; a restricted response
// carrying it (outside the principal's own statement and parameters) is a
// direct leak.
//
// Two cache orders: variant "restricted-first" (restricted, then wide, then
// restricted again in each state) and variant "wide-first" (wide, then
// restricted).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	neo4j "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// fsDB is the fixture database; the embedded scenario uses the default
// database so that GraphQL (which reads the default database) sees it.
var fsDB = "fgs"

// fsEmbedded switches the scenario to a server with a deterministic embedder.
var fsEmbedded = false

// fsTokenGrant also grants CREATE NEW PROPERTY NAME on the database, the grant
// with which a caller may write a property it cannot read (#1963).
var fsTokenGrant = false

const (
	fsMarker   = "zqx"
	fsOtherDB  = "fgo"
	fsRole     = "fs_reader"
	fsUser     = "fsnarrow"
	fsPassword = "password123"
	fsDeadline = 90 * time.Second
)

// ---------------------------------------------------------------------------
// Outcome.

type fsOut struct {
	Canon  string // canonical JSON of everything returned, volatile keys removed
	Sorted string // Canon with result rows sorted (order-insensitive form)
	Raw    string // complete response text, for the marker check
	Rows   int
	Errors []string
}

func fsVolatileKey(key string) bool {
	lower := strings.ToLower(key)
	for _, word := range []string{"time", "expire", "bookmark", "elapsed", "duration", "took", "latency", "qid", "memory", "pagecache"} {
		if strings.Contains(lower, word) {
			return true
		}
	}
	return lower == "commit" || lower == "transaction" || lower == "receipt" || lower == "optimistic"
}

func fsStrip(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			if fsVolatileKey(key) {
				continue
			}
			out[key] = fsStrip(item)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = fsStrip(item)
		}
		return out
	}
	return value
}

func fsJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("unencodable:%v", err)
	}
	return string(encoded)
}

func fsNormalize(value any) any {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("unencodable:%v", err)
	}
	var out any
	if err := json.Unmarshal(encoded, &out); err != nil {
		return fmt.Sprintf("undecodable:%v", err)
	}
	return out
}

// fsBuild assembles canonical forms from the parts of a response.
func fsBuild(parts map[string]any, rows []any, raw string, errs []string) fsOut {
	if normalized, ok := fsStrip(fsNormalize(parts)).(map[string]any); ok {
		parts = normalized
	} else {
		parts = map[string]any{"unencodable": fmt.Sprint(parts)}
	}
	if normalized, ok := fsStrip(fsNormalize(rows)).([]any); ok {
		rows = normalized
	} else {
		rows = []any{}
	}
	parts["rows"] = rows
	parts["errors"] = errs
	canon := fsJSON(parts)
	sortedRows := make([]string, len(rows))
	for i, row := range rows {
		sortedRows[i] = fsJSON(row)
	}
	sort.Strings(sortedRows)
	parts["rows"] = sortedRows
	return fsOut{Canon: canon, Sorted: fsJSON(parts), Raw: raw, Rows: len(rows), Errors: errs}
}

// ---------------------------------------------------------------------------
// HTTP.

type fsHTTP struct {
	t      *testing.T
	router http.Handler
}

func (h *fsHTTP) do(method, path, token string, body any) (int, http.Header, []byte) {
	h.t.Helper()
	var payload []byte
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			h.t.Fatalf("marshal: %v", err)
		}
		payload = encoded
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	var panicked any
	go func() {
		defer close(done)
		defer func() { panicked = recover() }()
		h.router.ServeHTTP(rec, req)
	}()
	select {
	case <-done:
	case <-time.After(fsDeadline):
		return 0, nil, []byte("HANG: no answer within deadline")
	}
	if panicked != nil {
		return 599, nil, []byte(fmt.Sprintf("PANIC: %v", panicked))
	}
	return rec.Code, rec.Header(), rec.Body.Bytes()
}

// cypherOut decodes a transactional endpoint response.
func fsCypherOut(code int, body []byte) fsOut {
	var generic map[string]any
	var errs []string
	if err := json.Unmarshal(body, &generic); err != nil {
		errs = append(errs, fmt.Sprintf("http %d undecodable: %v: %.300s", code, err, body))
		return fsBuild(map[string]any{"status": code}, nil, string(body), errs)
	}
	var rows []any
	parts := map[string]any{"status": code}
	if list, ok := generic["errors"].([]any); ok {
		for _, item := range list {
			if m, ok := item.(map[string]any); ok {
				errs = append(errs, fmt.Sprintf("%v: %v", m["code"], m["message"]))
			}
		}
	}
	if results, ok := generic["results"].([]any); ok && len(results) > 0 {
		if first, ok := results[0].(map[string]any); ok {
			parts["columns"] = first["columns"]
			if data, ok := first["data"].([]any); ok {
				for _, entry := range data {
					rows = append(rows, entry)
				}
			}
			for key, value := range first {
				if key != "columns" && key != "data" {
					parts["result."+key] = value
				}
			}
		}
	}
	for key, value := range generic {
		if key != "results" && key != "errors" {
			parts[key] = value
		}
	}
	return fsBuild(parts, rows, string(body), errs)
}

func (h *fsHTTP) cypher(database, token, statement string, params map[string]any) fsOut {
	code, _, body := h.do(http.MethodPost, "/db/"+database+"/tx/commit", token, map[string]any{
		"statements": []map[string]any{{"statement": statement, "parameters": params}},
	})
	return fsCypherOut(code, body)
}

func (h *fsHTTP) begin(database, token string) (string, error) {
	code, _, body := h.do(http.MethodPost, "/db/"+database+"/tx", token, map[string]any{"statements": []any{}})
	var resp TransactionResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", fmt.Errorf("begin http %d: %v: %s", code, err, body)
	}
	if len(resp.Errors) > 0 || resp.Commit == "" {
		return "", fmt.Errorf("begin http %d: %s", code, body)
	}
	commit, err := url.Parse(resp.Commit)
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(commit.Path, "/commit"), nil
}

func (h *fsHTTP) inTx(txPath, token, statement string, params map[string]any) fsOut {
	code, _, body := h.do(http.MethodPost, txPath, token, map[string]any{
		"statements": []map[string]any{{"statement": statement, "parameters": params}},
	})
	return fsCypherOut(code, body)
}

func (h *fsHTTP) rollback(txPath, token string) {
	h.do(http.MethodDelete, txPath, token, nil)
}

// endpoint posts a JSON request to a NornicDB endpoint and records status,
// the search fallback header and the body.
func (h *fsHTTP) endpoint(path, token string, request map[string]any) (fsOut, map[string]any) {
	code, header, body := h.do(http.MethodPost, path, token, request)
	parts := map[string]any{"status": code}
	if header != nil {
		parts["fallbackHeader"] = header.Get("X-NornicDB-Search-Fallback-Reason")
	}
	var decoded any
	var errs []string
	if err := json.Unmarshal(body, &decoded); err != nil {
		errs = append(errs, fmt.Sprintf("undecodable: %v: %.300s", err, body))
	}
	var rows []any
	var object map[string]any
	switch typed := decoded.(type) {
	case []any:
		rows = typed
	case map[string]any:
		object = typed
		if results, ok := typed["results"].([]any); ok {
			rows = results
		}
		for key, value := range typed {
			if key != "results" {
				parts["body."+key] = value
			}
		}
	}
	if code >= 400 {
		errs = append(errs, fmt.Sprintf("http %d", code))
	}
	return fsBuild(parts, rows, string(body), errs), object
}

// ---------------------------------------------------------------------------
// Bolt.

type fsBolt struct {
	driver neo4j.DriverWithContext
}

func fsStartBolt(t *testing.T, server *Server, authenticator *auth.Authenticator) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	config := bolt.DefaultConfig()
	adapter := bolt.NewAuthenticatorAdapter(authenticator)
	adapter.SetGetEffectivePermissions(server.GetEffectivePermissions)
	config.Authenticator = adapter
	config.RequireAuth = true
	config.DatabaseExecutorOptions = server.DatabaseScopedExecutorOptions
	boltServer := bolt.NewWithDatabaseManager(config, nil, server.GetDatabaseManager())
	boltServer.SetDatabaseAccessModeResolver(server.GetDatabaseAccessModeForRoles)
	boltServer.SetResolvedAccessResolver(server.GetResolvedAccessForRoles)
	boltServer.SetProcedureGrantResolver(server.ProcedureGrantChecker)
	boltServer.SetFineGrainedAdmission(server.FineGrainedAdmission())
	finished := make(chan error, 1)
	go func() { finished <- boltServer.Serve(listener) }()
	t.Cleanup(func() {
		_ = boltServer.Close()
		select {
		case <-finished:
		case <-time.After(10 * time.Second):
		}
	})
	return "bolt://" + listener.Addr().String()
}

func fsNewBolt(t *testing.T, uri, user string) *fsBolt {
	t.Helper()
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(user, fsPassword, ""))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	return &fsBolt{driver: driver}
}

func fsIsNil(value any) bool {
	if value == nil {
		return true
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Interface, reflect.Func, reflect.Chan:
		return rv.IsNil()
	}
	return false
}

func fsProfileTree(plan neo4j.ProfiledPlan) any {
	if fsIsNil(plan) {
		return nil
	}
	children := []any{}
	for _, child := range plan.Children() {
		children = append(children, fsProfileTree(child))
	}
	return map[string]any{"op": plan.Operator(), "args": plan.Arguments(), "ids": plan.Identifiers(),
		"records": plan.Records(), "dbHits": plan.DbHits(), "children": children}
}

func fsPlanTree(plan neo4j.Plan) any {
	if fsIsNil(plan) {
		return nil
	}
	children := []any{}
	for _, child := range plan.Children() {
		children = append(children, fsPlanTree(child))
	}
	return map[string]any{"op": plan.Operator(), "args": plan.Arguments(), "ids": plan.Identifiers(), "children": children}
}

func fsCollectBolt(ctx context.Context, result neo4j.ResultWithContext, runErr error) fsOut {
	parts := map[string]any{}
	var errs []string
	if runErr != nil {
		errs = append(errs, runErr.Error())
		return fsBuild(parts, nil, runErr.Error(), errs)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		errs = append(errs, err.Error())
		return fsBuild(parts, nil, err.Error(), errs)
	}
	if keys, keysErr := result.Keys(); keysErr == nil {
		parts["columns"] = keys
	}
	var rows []any
	for _, record := range records {
		rows = append(rows, record.Values)
	}
	summary, err := result.Consume(ctx)
	if err == nil && !fsIsNil(summary) {
		parts["profile"] = fsProfileTree(summary.Profile())
		parts["plan"] = fsPlanTree(summary.Plan())
		var notes []string
		for _, notification := range summary.Notifications() {
			notes = append(notes, notification.Title()+" | "+notification.Description())
		}
		parts["notifications"] = notes
		counters := summary.Counters()
		if !fsIsNil(counters) {
			parts["counters"] = map[string]any{"nodesCreated": counters.NodesCreated(), "propertiesSet": counters.PropertiesSet()}
		}
	} else if err != nil {
		errs = append(errs, err.Error())
	}
	out := fsBuild(parts, rows, "", errs)
	out.Raw = out.Canon
	return out
}

func (b *fsBolt) auto(database, statement string, params map[string]any) fsOut {
	ctx, cancel := context.WithTimeout(context.Background(), fsDeadline)
	defer cancel()
	session := b.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
	defer session.Close(context.Background())
	result, err := session.Run(ctx, statement, params)
	return fsCollectBolt(ctx, result, err)
}

// rolledBack runs one statement in an explicit transaction and rolls it back.
func (b *fsBolt) rolledBack(database, statement string, params map[string]any) fsOut {
	ctx, cancel := context.WithTimeout(context.Background(), fsDeadline)
	defer cancel()
	session := b.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
	defer session.Close(context.Background())
	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return fsBuild(map[string]any{}, nil, err.Error(), []string{"begin: " + err.Error()})
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	result, runErr := tx.Run(ctx, statement, params)
	return fsCollectBolt(ctx, result, runErr)
}

// ---------------------------------------------------------------------------
// Fixture.

var fsNames = []string{"alice", "bob", "carol", "dave"}

func fsFixtureSchema() []string {
	return []string{
		"CREATE INDEX fs_name IF NOT EXISTS FOR (p:Person) ON (p.name)",
		"CREATE INDEX fs_rank IF NOT EXISTS FOR (p:Person) ON (p.rank)",
		"CREATE INDEX fs_dept_ssn IF NOT EXISTS FOR (p:Person) ON (p.dept, p.ssn)",
		"CREATE TEXT INDEX fs_txt IF NOT EXISTS FOR (p:Person) ON (p.secretnote)",
		"CREATE CONSTRAINT fs_ssn_unique IF NOT EXISTS FOR (p:Person) REQUIRE p.ssn IS UNIQUE",
		"CREATE INDEX fs_rsecret IF NOT EXISTS FOR ()-[r:KNOWS]-() ON (r.secret)",
		"CREATE FULLTEXT INDEX fs_ft IF NOT EXISTS FOR (p:Person) ON EACH [p.name, p.bio, p.secretnote]",
		"CREATE FULLTEXT INDEX fs_ft_note IF NOT EXISTS FOR (n:Note) ON EACH [n.text]",
		"CREATE FULLTEXT INDEX fs_ftr IF NOT EXISTS FOR ()-[r:KNOWS|HATES]-() ON EACH [r.note, r.secret]",
		"CREATE VECTOR INDEX fs_vec IF NOT EXISTS FOR (p:Person) ON (p.vec) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}",
		"CREATE VECTOR INDEX fs_hvec IF NOT EXISTS FOR (p:Person) ON (p.hvec) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}",
		"CREATE VECTOR INDEX fs_rvec IF NOT EXISTS FOR ()-[r:KNOWS]-() ON (r.rvec) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}",
		"CREATE VECTOR INDEX fs_hrvec IF NOT EXISTS FOR ()-[r:KNOWS]-() ON (r.hrvec) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}",
	}
}

// fsFixtureData builds state S0.
func fsFixtureData() []string {
	return []string{
		"CREATE (:Person {name: 'alice', bio: 'alpha bravo', dept: 'eng', vec: [1.0, 0.0, 0.0, 0.0], ssn: 'ZQX-SSN-0', rank: 4, secretnote: 'zqxkeyword alpha alpha alpha', hvec: [0.0, 0.0, 0.0, 1.0]})",
		"CREATE (:Person {name: 'bob', bio: 'alpha charlie', dept: 'eng', vec: [0.0, 1.0, 0.0, 0.0], ssn: 'ZQX-SSN-1', rank: 1, secretnote: 'delta echo', hvec: [0.0, 0.0, 1.0, 0.0]})",
		"CREATE (:Person {name: 'carol', bio: 'bravo charlie', dept: 'eng', vec: [0.7, 0.7, 0.0, 0.0], ssn: 'ZQX-SSN-2', rank: 3, secretnote: 'zqxkeyword', hvec: [0.0, 1.0, 0.0, 0.0]})",
		"CREATE (:Person {name: 'dave', bio: 'alpha', dept: 'ops', vec: [0.0, 0.0, 1.0, 0.0], ssn: 'ZQX-SSN-3', rank: 2, hvec: [1.0, 0.0, 0.0, 0.0]})",
		"CREATE (:Person:Secret {name: 'aaa-zqx', bio: 'alpha alpha alpha bravo', dept: 'eng', vec: [1.0, 0.0, 0.0, 0.0], ssn: 'ZQX-SSN-H', rank: 0, secretnote: 'zqxkeyword alpha', hvec: [0.0, 0.0, 0.0, 1.0]})",
		"CREATE (:Note {text: 'alpha memo', level: 'low'})",
		"CREATE (:Note {text: 'alpha alpha zqxtop memo', level: 'top'})",
		"MATCH (a:Person {name: 'alice'}), (b:Person {name: 'bob'}) CREATE (a)-[:KNOWS {note: 'alpha friend', rvec: [1.0, 0.0, 0.0, 0.0], secret: 'zqxrelsecret alpha alpha', hrvec: [0.0, 0.0, 0.0, 1.0]}]->(b)",
		"MATCH (a:Person {name: 'bob'}), (b:Person {name: 'carol'}) CREATE (a)-[:KNOWS {note: 'bravo friend', rvec: [0.0, 1.0, 0.0, 0.0]}]->(b)",
		"MATCH (a:Person {name: 'carol'}), (b:Person {name: 'aaa-zqx'}) CREATE (a)-[:KNOWS {note: 'alpha to hidden', rvec: [1.0, 0.0, 0.0, 0.0]}]->(b)",
		"MATCH (a:Person {name: 'alice'}), (b:Person {name: 'dave'}) CREATE (a)-[:HATES {note: 'alpha alpha hate zqxhate'}]->(b)",
	}
}

// fsToS1 changes every hidden value; hidden entities stay, visible data and
// IDs do not change.
func fsToS1() []string {
	return []string{
		"MATCH (p:Person) REMOVE p.ssn",
		"MATCH (p:Person {name: 'alice'}) SET p.ssn = 'ZQX-SSN-3', p.rank = 1, p.hvec = [1.0, 0.0, 0.0, 0.0] REMOVE p.secretnote",
		"MATCH (p:Person {name: 'bob'}) SET p.ssn = 'ZQX-SSN-0', p.rank = 4, p.secretnote = 'zqxkeyword alpha alpha alpha', p.hvec = [0.0, 1.0, 0.0, 0.0]",
		"MATCH (p:Person {name: 'carol'}) SET p.ssn = 'ZQX-SSN-1', p.rank = 2, p.secretnote = 'delta echo', p.hvec = [0.0, 0.0, 1.0, 0.0]",
		"MATCH (p:Person {name: 'dave'}) SET p.ssn = 'ZQX-SSN-2', p.rank = 3, p.secretnote = 'zqxkeyword', p.hvec = [0.0, 0.0, 0.0, 1.0]",
		"MATCH (p:Secret {name: 'aaa-zqx'}) SET p.rank = 9, p.ssn = 'ZQX-SSN-H2', p.secretnote = 'zqxkeyword alpha alpha'",
		"MATCH (n:Note {level: 'top'}) SET n.text = 'alpha zqxtop'",
		"MATCH (:Person {name: 'alice'})-[r:KNOWS]->(:Person {name: 'bob'}) REMOVE r.secret SET r.hrvec = [1.0, 0.0, 0.0, 0.0]",
		"MATCH (:Person {name: 'bob'})-[r:KNOWS]->(:Person {name: 'carol'}) SET r.secret = 'zqxrelsecret alpha alpha', r.hrvec = [0.0, 0.0, 0.0, 1.0]",
		"MATCH ()-[r:HATES]->() SET r.note = 'alpha hate zqxhate'",
		"MATCH (d:Doc:Secret) SET d.title = 'zqx doc changed'",
	}
}

// fsToS2 removes every hidden value and hidden entity.
func fsToS2() []string {
	return []string{
		"MATCH (p:Person) REMOVE p.ssn, p.rank, p.secretnote, p.hvec",
		"MATCH ()-[r:KNOWS]->() REMOVE r.secret, r.hrvec",
		"MATCH ()-[r:HATES]->() DELETE r",
		"MATCH (n:Secret) DETACH DELETE n",
		"MATCH (n:Note {level: 'top'}) DETACH DELETE n",
	}
}

func fsOtherDBData() []string {
	return []string{
		"CREATE FULLTEXT INDEX fo_ft IF NOT EXISTS FOR (p:Person) ON EACH [p.name, p.bio]",
		"CREATE (:Person {name: 'zqx-other', bio: 'alpha alpha zqxother'})",
	}
}

func fsGrantStatements() []string {
	g := " ON GRAPH " + fsDB + " "
	statements := []string{
		"CREATE ROLE " + fsRole + " IF NOT EXISTS",
		"GRANT ROLE " + fsRole + " TO " + fsUser,
		"GRANT ACCESS ON DATABASE " + fsDB + " TO " + fsRole,
		"GRANT TRAVERSE" + g + "NODES Person, Note, Doc TO " + fsRole,
		"GRANT READ {name, bio, dept, vec}" + g + "NODES Person TO " + fsRole,
		"DENY TRAVERSE" + g + "NODES Secret TO " + fsRole,
		"GRANT READ {text, level}" + g + "NODES Note TO " + fsRole,
		"DENY MATCH {*}" + g + "FOR (n:Note) WHERE n.level = 'top' TO " + fsRole,
		"GRANT READ {title}" + g + "NODES Doc TO " + fsRole,
		"GRANT TRAVERSE" + g + "RELATIONSHIPS KNOWS TO " + fsRole,
		"GRANT READ {note, rvec}" + g + "RELATIONSHIPS KNOWS TO " + fsRole,
		"GRANT CREATE" + g + "NODES Person TO " + fsRole,
		"GRANT SET PROPERTY {name, bio, ssn}" + g + "NODES Person TO " + fsRole,
		"GRANT EXECUTE PROCEDURE * ON DBMS TO " + fsRole,
	}
	if fsTokenGrant {
		statements = append(statements, "GRANT CREATE NEW PROPERTY NAME ON DATABASE "+fsDB+" TO "+fsRole)
	}
	return statements
}

// ---------------------------------------------------------------------------
// Queries.

type fsQ struct {
	name   string
	stmt   string
	params map[string]any
	// write: run only inside a rolled-back explicit transaction.
	write bool
	// minRows: sanity floor in S2 (shows that grants admit the read).
	minRows int
}

func fsP(kv ...any) map[string]any {
	params := map[string]any{}
	for i := 0; i+1 < len(kv); i += 2 {
		params[kv[i].(string)] = kv[i+1]
	}
	return params
}

func fsCypherQueries() []fsQ {
	v := "ZQX-SSN-1"
	q := func(name, stmt string, params map[string]any) fsQ { return fsQ{name: name, stmt: stmt, params: params} }
	qs := []fsQ{
		// Fulltext on nodes.
		{name: "ft-alpha-ordered", stmt: "CALL db.index.fulltext.queryNodes('fs_ft', 'alpha') YIELD node, score RETURN node.name AS n, score ORDER BY score DESC, n", minRows: 3},
		q("ft-alpha-procedure-order", "CALL db.index.fulltext.queryNodes('fs_ft', 'alpha') YIELD node, score RETURN node.name AS n, score", nil),
		q("ft-alpha-whole-node", "CALL db.index.fulltext.queryNodes('fs_ft', 'alpha') YIELD node, score RETURN node, score", nil),
		q("ft-standalone", "CALL db.index.fulltext.queryNodes('fs_ft', 'alpha')", nil),
		q("ft-hidden-term", "CALL db.index.fulltext.queryNodes('fs_ft', 'zqxkeyword') YIELD node, score RETURN node.name AS n, score", nil),
		q("ft-hidden-field", "CALL db.index.fulltext.queryNodes('fs_ft', 'secretnote:alpha') YIELD node, score RETURN node.name AS n, score", nil),
		q("ft-limit", "CALL db.index.fulltext.queryNodes('fs_ft', 'alpha', {limit: 2}) YIELD node, score RETURN node.name AS n, score", nil),
		q("ft-skip-limit", "CALL db.index.fulltext.queryNodes('fs_ft', 'alpha', {skip: 1, limit: 2}) YIELD node, score RETURN node.name AS n, score", nil),
		q("ft-wildcard", "CALL db.index.fulltext.queryNodes('fs_ft', '*') YIELD node, score RETURN node.name AS n, score", nil),
		q("ft-presence-hidden", "CALL db.index.fulltext.queryNodes('fs_ft', 'secretnote:*') YIELD node, score RETURN node.name AS n, score", nil),
		q("ft-prefix-hidden", "CALL db.index.fulltext.queryNodes('fs_ft', 'zqx*') YIELD node, score RETURN node.name AS n, score", nil),
		q("ft-fuzzy-hidden", "CALL db.index.fulltext.queryNodes('fs_ft', 'zqxkeywrd~') YIELD node, score RETURN node.name AS n, score", nil),
		q("ft-boolean", "CALL db.index.fulltext.queryNodes('fs_ft', 'alpha AND NOT delta') YIELD node, score RETURN node.name AS n, score", nil),
		q("ft-or-hidden", "CALL db.index.fulltext.queryNodes('fs_ft', 'bravo OR zqxkeyword') YIELD node, score RETURN node.name AS n, score", nil),
		q("ft-count", "CALL db.index.fulltext.queryNodes('fs_ft', 'alpha') YIELD node RETURN count(node) AS c", nil),
		q("ft-note-predicate-deny", "CALL db.index.fulltext.queryNodes('fs_ft_note', 'alpha') YIELD node, score RETURN node.text AS t, score", nil),
		// Fulltext on relationships.
		{name: "ftr-alpha", stmt: "CALL db.index.fulltext.queryRelationships('fs_ftr', 'alpha') YIELD relationship, score RETURN relationship.note AS n, type(relationship) AS t, score ORDER BY score DESC, n", minRows: 1},
		q("ftr-whole", "CALL db.index.fulltext.queryRelationships('fs_ftr', 'alpha') YIELD relationship, score RETURN relationship, score", nil),
		q("ftr-hidden-term", "CALL db.index.fulltext.queryRelationships('fs_ftr', 'zqxrelsecret') YIELD relationship, score RETURN relationship.note AS n, score", nil),
		q("ftr-hidden-field", "CALL db.index.fulltext.queryRelationships('fs_ftr', 'secret:alpha') YIELD relationship, score RETURN relationship.note AS n, score", nil),
		q("ftr-hidden-type", "CALL db.index.fulltext.queryRelationships('fs_ftr', 'hate') YIELD relationship, score RETURN relationship.note AS n, score", nil),
		q("ftr-wildcard", "CALL db.index.fulltext.queryRelationships('fs_ftr', '*') YIELD relationship, score RETURN relationship.note AS n, score", nil),
		q("ftr-limit", "CALL db.index.fulltext.queryRelationships('fs_ftr', 'alpha', {limit: 1}) YIELD relationship, score RETURN relationship.note AS n, score", nil),
		// Vector.
		{name: "vec-visible", stmt: "CALL db.index.vector.queryNodes('fs_vec', 5, [1.0, 0.0, 0.0, 0.0]) YIELD node, score RETURN node.name AS n, score", minRows: 1},
		q("vec-visible-k2", "CALL db.index.vector.queryNodes('fs_vec', 2, [1.0, 0.0, 0.0, 0.0]) YIELD node, score RETURN node.name AS n, score", nil),
		q("vec-hidden-property", "CALL db.index.vector.queryNodes('fs_hvec', 4, [0.0, 0.0, 0.0, 1.0]) YIELD node, score RETURN node.name AS n, score", nil),
		q("vec-rel-visible", "CALL db.index.vector.queryRelationships('fs_rvec', 5, [1.0, 0.0, 0.0, 0.0]) YIELD relationship, score RETURN relationship.note AS n, score", nil),
		q("vec-rel-hidden-property", "CALL db.index.vector.queryRelationships('fs_hrvec', 5, [0.0, 0.0, 0.0, 1.0]) YIELD relationship, score RETURN relationship.note AS n, score", nil),
		q("vec-managed", "CALL db.index.vector.queryNodes('fs_no_such_index', 5, [1.0, 0.0, 0.0, 0.0]) YIELD node, score RETURN node.title AS t, score", nil),
		q("vec-text-alpha", "CALL db.index.vector.queryNodes('fs_no_such_index', 10, 'alpha') YIELD node, score RETURN coalesce(node.name, node.title, node.text) AS n, score", nil),
		q("vec-text-hidden", "CALL db.index.vector.queryNodes('fs_no_such_index', 10, 'zqxkeyword') YIELD node, score RETURN coalesce(node.name, node.title, node.text) AS n, score", nil),
		q("vec-text-person-index", "CALL db.index.vector.queryNodes('fs_vec', 5, 'alpha') YIELD node, score RETURN node.name AS n, score", nil),
		q("vec-cosine-fastpath", "MATCH (p:Person) RETURN p.name AS n, vector.similarity.cosine(p.vec, [1.0, 0.0, 0.0, 0.0]) AS s ORDER BY s DESC, n LIMIT 3", nil),
		q("vec-cosine-hidden", "MATCH (p:Person) RETURN p.name AS n, vector.similarity.cosine(p.hvec, [0.0, 0.0, 0.0, 1.0]) AS s ORDER BY s DESC LIMIT 2", nil),
		// Retrieval procedures.
		q("rag-retrieve", "CALL db.retrieve({query: 'alpha', limit: 10})", nil),
		q("rag-retrieve-hidden", "CALL db.retrieve({query: 'zqxkeyword', limit: 10})", nil),
		q("rag-rretrieve", "CALL db.rretrieve({query: 'alpha', limit: 10})", nil),
		q("rag-retrieve-relationships", "CALL db.retrieveRelationships({query: 'alpha', limit: 10})", nil),
		q("rag-retrieve-relationships-hidden", "CALL db.retrieveRelationships({query: 'zqxrelsecret', limit: 10})", nil),
		// Index-backed MATCH.
		q("seek-eq", "MATCH (p:Person) WHERE p.ssn = $v RETURN p.name AS n", fsP("v", v)),
		q("seek-inline", "MATCH (p:Person {ssn: $v}) RETURN p.name AS n", fsP("v", v)),
		q("seek-in-list", "MATCH (p:Person) WHERE p.ssn IN $l RETURN p.name AS n", fsP("l", []any{"ZQX-SSN-0", "ZQX-SSN-1"})),
		q("seek-or", "MATCH (p:Person) WHERE p.ssn = $v OR p.name = 'dave' RETURN p.name AS n ORDER BY n", fsP("v", v)),
		q("seek-range", "MATCH (p:Person) WHERE p.rank > 2 RETURN p.name AS n ORDER BY n", nil),
		q("seek-prefix", "MATCH (p:Person) WHERE p.ssn STARTS WITH 'ZQX' RETURN count(p) AS c", nil),
		q("seek-composite", "MATCH (p:Person) WHERE p.dept = 'eng' AND p.ssn = $v RETURN p.name AS n", fsP("v", v)),
		q("seek-text-contains", "MATCH (p:Person) WHERE p.secretnote CONTAINS 'zqx' RETURN count(*) AS c", nil),
		q("seek-text-ends", "MATCH (p:Person) WHERE p.secretnote ENDS WITH 'alpha' RETURN p.name AS n", nil),
		q("seek-not-null", "MATCH (p:Person) WHERE p.ssn IS NOT NULL RETURN count(p) AS c", nil),
		q("seek-hidden-label-by-name", "MATCH (p:Person {name: 'aaa-zqx'}) RETURN count(p) AS c", nil),
		{name: "seek-visible-name", stmt: "MATCH (p:Person {name: 'bob'}) RETURN p.name AS n, p.bio AS b, p.ssn AS s", minRows: 1},
		q("order-hidden-limit", "MATCH (p:Person) RETURN p.name AS n ORDER BY p.rank LIMIT 2", nil),
		q("order-hidden-desc-limit", "MATCH (p:Person) RETURN p.name AS n ORDER BY p.rank DESC LIMIT 2", nil),
		q("order-hidden-skip-limit", "MATCH (p:Person) RETURN p.name AS n ORDER BY p.ssn SKIP 1 LIMIT 2", nil),
		q("order-hidden-collect", "MATCH (p:Person) WITH p ORDER BY p.ssn DESC RETURN collect(p.name) AS l", nil),
		q("order-hidden-then-visible", "MATCH (p:Person) RETURN p.name AS n ORDER BY p.rank, p.name", nil),
		q("order-visible-limit", "MATCH (p:Person) RETURN p.name AS n ORDER BY p.name LIMIT 3", nil),
		q("order-visible-skip", "MATCH (p:Person) RETURN p.name AS n ORDER BY p.name SKIP 1", nil),
		q("limit-no-order", "MATCH (p:Person) RETURN p.name AS n LIMIT 2", nil),
		q("rel-seek", "MATCH ()-[r:KNOWS]->() WHERE r.secret = $s RETURN r.note AS n", fsP("s", "zqxrelsecret alpha alpha")),
		q("rel-not-null", "MATCH ()-[r:KNOWS]->() WHERE r.secret IS NOT NULL RETURN count(r) AS c", nil),
		q("rel-order-hidden", "MATCH ()-[r:KNOWS]->() RETURN r.note AS n ORDER BY r.secret LIMIT 1", nil),
		q("min-max-hidden", "MATCH (p:Person) RETURN min(p.rank) AS mn, max(p.ssn) AS mx", nil),
		q("distinct-hidden", "MATCH (p:Person) RETURN DISTINCT p.secretnote AS s", nil),
		// Counts and existence.
		q("count-person", "MATCH (p:Person) RETURN count(p) AS c", nil),
		q("count-all", "MATCH (n) RETURN count(n) AS c", nil),
		q("count-secret", "MATCH (s:Secret) RETURN count(s) AS c", nil),
		q("count-rels", "MATCH ()-[r]->() RETURN count(r) AS c", nil),
		q("count-knows", "MATCH ()-[r:KNOWS]->() RETURN count(r) AS c", nil),
		q("count-knows-from-person", "MATCH (:Person)-[r:KNOWS]->() RETURN count(r) AS c", nil),
		q("count-knows-to-person", "MATCH ()-[r:KNOWS]->(:Person) RETURN count(r) AS c", nil),
		q("count-subquery-degree", "MATCH (p:Person {name: 'carol'}) RETURN COUNT { (p)--() } AS d, size([(p)-->() | 1]) AS o", nil),
		q("count-alice-degree", "MATCH (p:Person {name: 'alice'}) RETURN COUNT { (p)-->() } AS d", nil),
		q("count-note", "MATCH (n:Note) RETURN count(n) AS c", nil),
		q("exists-hidden", "MATCH (p:Person) WHERE EXISTS { MATCH (q:Person) WHERE q.ssn = $v } RETURN count(p) AS c", fsP("v", v)),
		q("exists-hidden-label", "RETURN EXISTS { MATCH (s:Secret) } AS e", nil),
		q("exists-hidden-rel", "MATCH (a:Person {name: 'alice'}) RETURN EXISTS { (a)-[:HATES]->() } AS e", nil),
		// Statistics procedures.
		q("db-labels", "CALL db.labels()", nil),
		q("db-rel-types", "CALL db.relationshipTypes()", nil),
		q("db-property-keys", "CALL db.propertyKeys()", nil),
		q("db-stats", "CALL db.stats.retrieve('GRAPH COUNTS')", nil),
		// EXPLAIN and PROFILE.
		q("explain-seek", "EXPLAIN MATCH (p:Person) WHERE p.ssn = $v RETURN p.name AS n", fsP("v", v)),
		q("explain-label", "EXPLAIN MATCH (p:Person) RETURN p.name AS n", nil),
		q("profile-seek", "PROFILE MATCH (p:Person) WHERE p.ssn = $v RETURN p.name AS n", fsP("v", v)),
		q("profile-label-scan", "PROFILE MATCH (p:Person) RETURN p.name AS n ORDER BY n", nil),
		q("profile-order-hidden", "PROFILE MATCH (p:Person) RETURN p.name AS n ORDER BY p.rank LIMIT 2", nil),
		q("profile-fulltext", "PROFILE CALL db.index.fulltext.queryNodes('fs_ft', 'alpha') YIELD node, score RETURN node.name AS n, score", nil),
		q("profile-count", "PROFILE MATCH (p:Person) RETURN count(p) AS c", nil),
		q("profile-rels", "PROFILE MATCH (a:Person)-[r]->(b) RETURN a.name AS a, b.name AS b ORDER BY a, b", nil),
		// Writes that consult indexes and constraints (rolled back).
		{name: "merge-hidden-value", stmt: "MERGE (p:Person {ssn: $v}) RETURN p.name AS n, p.bio AS b", params: fsP("v", v), write: true},
		{name: "merge-visible-and-hidden", stmt: "MERGE (p:Person {name: 'bob', ssn: $v}) RETURN p.bio AS b", params: fsP("v", v), write: true},
		{name: "merge-hidden-label-name", stmt: "MERGE (p:Person {name: 'aaa-zqx'}) RETURN p.bio AS b", write: true},
		{name: "create-unique-hidden", stmt: "CREATE (p:Person {name: 'newcomer', ssn: $v}) RETURN p.name AS n", params: fsP("v", v), write: true},
		{name: "set-unique-hidden", stmt: "MATCH (p:Person {name: 'dave'}) SET p.ssn = $v RETURN p.name AS n", params: fsP("v", v), write: true},
	}
	return qs
}

type fsEndpointQ struct {
	name    string
	path    string
	request map[string]any
	minRows int
}

func fsEndpointQueries(aliceID string) []fsEndpointQ {
	s := func(name string, request map[string]any) fsEndpointQ {
		request["database"] = fsDB
		return fsEndpointQ{name: name, path: "/nornicdb/search", request: request}
	}
	qs := []fsEndpointQ{
		s("http-search-alpha", map[string]any{"query": "alpha", "limit": 10}),
		s("http-search-limit2", map[string]any{"query": "alpha", "limit": 2}),
		s("http-search-hidden-term", map[string]any{"query": "zqxkeyword", "limit": 10}),
		s("http-search-hidden-prefix", map[string]any{"query": "zqx", "limit": 10}),
		s("http-search-labels", map[string]any{"query": "alpha", "labels": []any{"Person"}, "limit": 10}),
		s("http-search-labels-secret", map[string]any{"query": "alpha", "labels": []any{"Secret"}, "limit": 10}),
		s("http-search-filter-hidden", map[string]any{"query": "alpha", "filters": map[string]any{"ssn": []any{"ZQX-SSN-1"}}, "limit": 10}),
		s("http-search-include-hidden", map[string]any{"query": "alpha", "include_properties": []any{"ssn", "secretnote", "name"}, "limit": 10}),
		s("http-search-rel", map[string]any{"query": "alpha", "relationships": true, "limit": 10}),
		s("http-search-rel-hidden", map[string]any{"query": "zqxrelsecret", "relationships": true, "limit": 10}),
		s("http-search-rel-hidden-type", map[string]any{"query": "hate", "relationships": true, "limit": 10}),
		s("http-search-continuation", map[string]any{"query": "alpha", "n": 2}),
		s("http-search-doc", map[string]any{"query": "doc", "limit": 10}),
		{name: "http-search-other-db", path: "/nornicdb/search", request: map[string]any{"database": fsOtherDB, "query": "alpha", "limit": 10}},
		{name: "http-similar-doc", path: "/nornicdb/similar", request: map[string]any{"database": fsDB, "node_id": "fs-doc-0", "limit": 5}},
	}
	gql := func(name, query string) fsEndpointQ {
		return fsEndpointQ{name: name, path: "/graphql", request: map[string]any{"query": query}}
	}
	qs = append(qs,
		gql("graphql-search-alpha", `{ search(query: "alpha", options: {limit: 10}) { totalCount results { score rrfScore vectorRank bm25Rank foundBy node { internalId labels properties } } } }`),
		gql("graphql-search-hidden", `{ search(query: "zqxkeyword", options: {limit: 10}) { totalCount results { score node { internalId properties } } } }`),
		gql("graphql-search-labels", `{ search(query: "alpha", options: {limit: 10, labels: ["Person"]}) { totalCount results { score node { internalId properties } } } }`),
		gql("graphql-similar-doc", `{ similar(nodeId: "fs-doc-0", limit: 5) { similarity node { internalId properties } } }`),
		s("http-search-vector-hidden-term", map[string]any{"query": "zqxkeyword delta", "limit": 10}),
	)
	if aliceID != "" {
		qs = append(qs, fsEndpointQ{name: "http-similar-alice", path: "/nornicdb/similar", request: map[string]any{"database": fsDB, "node_id": aliceID, "limit": 5}})
		qs = append(qs, gql("graphql-similar-alice", `{ similar(nodeId: "`+aliceID+`", limit: 5) { similarity node { internalId properties } } }`))
	}
	return qs
}

// ---------------------------------------------------------------------------
// Environment.

type fsEnv struct {
	t       *testing.T
	server  *Server
	http    *fsHTTP
	admin   string
	narrow  string
	adminB  *fsBolt
	narrowB *fsBolt
	aliceID string
	leaks   int
}

func (env *fsEnv) adminRun(database, statement string) {
	env.t.Helper()
	var o fsOut
	for attempt := 0; attempt < 5; attempt++ {
		o = env.http.cypher(database, env.admin, statement, nil)
		if len(o.Errors) == 0 {
			return
		}
		if !strings.Contains(strings.Join(o.Errors, " "), "TransientError") {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	env.t.Logf("DIAG admin-http write failed %q: %v; retrying through an executor without a principal", statement, o.Errors)
	store, err := env.server.dbManager.GetStorage(database)
	if err != nil {
		env.t.Fatalf("storage %s: %v", database, err)
	}
	var lastErr error
	for attempt := 0; attempt < 10; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), fsDeadline)
		_, lastErr = cypher.NewStorageExecutor(store).Execute(ctx, statement, nil)
		cancel()
		if lastErr == nil {
			env.t.Logf("DIAG internal executor write succeeded %q (attempt %d)", statement, attempt+1)
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	env.t.Fatalf("admin %q: http %v; internal %v", statement, o.Errors, lastErr)
}

// diagnose logs what the schema holds, through FGAC-scoped admin and through
// an executor without a principal.
func (env *fsEnv) diagnose(label string) {
	for _, statement := range []string{
		"SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties, state",
		"SHOW CONSTRAINTS YIELD name, type, labelsOrTypes, properties",
		"CALL db.index.fulltext.queryNodes('fs_ft', 'alpha') YIELD node, score RETURN node.name AS n, score",
	} {
		out := env.http.cypher(fsDB, env.admin, statement, nil)
		env.t.Logf("DIAG [%s] admin-http %s => %.1500s", label, statement, out.Canon)
	}
	store, err := env.server.dbManager.GetStorage(fsDB)
	if err == nil {
		if schema := store.GetSchema(); schema != nil {
			_, ft := schema.GetFulltextIndex("fs_ft")
			_, probe := schema.GetFulltextIndex("fs_ft_probe")
			env.t.Logf("DIAG [%s] storage schema GetFulltextIndex fs_ft=%v fs_ft_probe=%v", label, ft, probe)
		}
	}
}

func fsSetup(t *testing.T) *fsEnv {
	t.Helper()
	server, authenticator := setupTestServerWithDatabaseConfig(t, func(c *nornicdb.Config) {
		c.Memory.SearchRelationshipsEnabled = true
		if fsEmbedded {
			c.Memory.EmbeddingEnabled = true
		}
	}, nil)
	if fsEmbedded && !server.db.SetEmbedder(&fsHashEmbedder{dims: 1024}) {
		t.Fatalf("embedder not accepted")
	}
	if fsEmbedded {
		// The server applies db.nornic.embedding.enabled from its own global
		// config to the databases that exist at start (pkg/server/server.go:1780);
		// the test helper's server config leaves it off, so the default
		// database's nodes are never admitted to the embed queue. Enable it
		// for the fixture database, as an operator enabling embeddings would.
		server.db.SetDatabaseEmbeddingsEnabled(fsDB, true)
		t.Logf("EMB setup: database embeddings enabled for %s", fsDB)
	}
	if _, err := authenticator.CreateUser(context.Background(), fsUser, fsPassword, []auth.Role{auth.RoleViewer}); err != nil {
		t.Fatalf("create user: %v", err)
	}
	// Schema and data are written before FGAC is switched on (see DIAG lines:
	// on the first run schema written under FGAC was not visible to queries).
	env := &fsEnv{t: t, server: server, http: &fsHTTP{t: t, router: server.buildRouter()}}
	env.admin = getAuthToken(t, authenticator, "admin")
	manager := server.GetDatabaseManager()
	for _, name := range []string{fsDB, fsOtherDB} {
		if name == manager.DefaultDatabaseName() {
			continue
		}
		if err := manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: name}); err != nil {
			t.Fatalf("create database %s: %v", name, err)
		}
	}
	for _, statement := range fsFixtureSchema() {
		if o := env.http.cypher(fsDB, env.admin, statement, nil); len(o.Errors) > 0 {
			t.Errorf("SETUP schema %q not accepted: %v", statement, o.Errors)
		}
	}
	for _, statement := range fsFixtureData() {
		env.adminRun(fsDB, statement)
	}
	for _, statement := range fsOtherDBData() {
		env.adminRun(fsOtherDB, statement)
	}
	store, err := manager.GetStorage(fsDB)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	for _, node := range []*storage.Node{
		{ID: "fs-doc-0", Labels: []string{"Doc"}, Properties: map[string]any{"title": "alpha doc"}, NamedEmbeddings: map[string][]float32{"default": {1, 0, 0, 0}}},
		{ID: "fs-doc-1", Labels: []string{"Doc"}, Properties: map[string]any{"title": "bravo doc"}, NamedEmbeddings: map[string][]float32{"default": {0, 1, 0, 0}}},
		{ID: "fs-doc-h", Labels: []string{"Doc", "Secret"}, Properties: map[string]any{"title": "zqx doc"}, NamedEmbeddings: map[string][]float32{"default": {0.9, 0.1, 0, 0}}},
	} {
		if _, err := store.CreateNode(t.Context(), node); err != nil {
			t.Fatalf("doc %s: %v", node.ID, err)
		}
	}
	_ = env.http.cypher(fsDB, env.admin, "CALL db.awaitIndexes(60)", nil)
	env.diagnose("before-fgac")
	if err := server.initializeFineGrainedRuntime(t.Context()); err != nil {
		t.Fatalf("fgac: %v", err)
	}
	env.http = &fsHTTP{t: t, router: server.buildRouter()}
	env.admin = getAuthToken(t, authenticator, "admin")
	env.narrow = getAuthToken(t, authenticator, fsUser)
	if o := env.http.cypher(fsDB, env.admin, "CREATE FULLTEXT INDEX fs_ft_probe IF NOT EXISTS FOR (p:Person) ON EACH [p.bio]", nil); len(o.Errors) > 0 {
		t.Logf("DIAG probe index under FGAC not accepted: %v", o.Errors)
	}
	_ = env.http.cypher(fsDB, env.admin, "CALL db.awaitIndexes(60)", nil)
	env.diagnose("after-fgac")
	probe := env.http.cypher(fsDB, env.admin, "CALL db.index.fulltext.queryNodes('fs_ft_probe', 'alpha') YIELD node RETURN node.name AS n", nil)
	t.Logf("DIAG probe index created under FGAC, queried by admin => %.800s", probe.Canon)
	if o := env.http.cypher(fsDB, env.admin, "MATCH (p:Person {name: 'alice'}) RETURN elementId(p) AS e, id(p) AS i", nil); len(o.Errors) == 0 {
		var decoded TransactionResponse
		code, _, body := env.http.do(http.MethodPost, "/db/"+fsDB+"/tx/commit", env.admin, map[string]any{
			"statements": []map[string]any{{"statement": "MATCH (p:Person {name: 'alice'}) RETURN elementId(p) AS e"}},
		})
		if code == http.StatusOK && json.Unmarshal(body, &decoded) == nil && len(decoded.Results) > 0 && len(decoded.Results[0].Data) > 0 {
			if id, ok := decoded.Results[0].Data[0].Row[0].(string); ok {
				env.aliceID = id
				if i := strings.LastIndex(id, ":"); i >= 0 {
					env.aliceID = id[i+1:]
				}
			}
		}
	}
	for _, statement := range fsGrantStatements() {
		if o := env.http.cypher("system", env.admin, statement, nil); len(o.Errors) > 0 {
			t.Errorf("SETUP grant %q not accepted: %v", statement, o.Errors)
		}
	}
	env.grantCatalog()
	boltURI := fsStartBolt(t, server, authenticator)
	env.adminB = fsNewBolt(t, boltURI, "admin")
	env.narrowB = fsNewBolt(t, boltURI, fsUser)
	env.waitSearchReady()
	return env
}

// grantCatalog adds the two entitlements Cypher has no statement for: the
// search endpoint and managed-embedding reads.
func (env *fsEnv) grantCatalog() {
	t := env.t
	code, _, body := env.http.do(http.MethodGet, "/auth/fgac/catalog", env.admin, nil)
	if code != http.StatusOK {
		t.Fatalf("catalog get %d: %s", code, body)
	}
	var document fineGrainedCatalogResponse
	if err := json.Unmarshal(body, &document); err != nil {
		t.Fatalf("catalog decode: %v", err)
	}
	identifier, _ := env.server.dbManager.DatabaseIdentity(fsDB)
	if identifier == "" {
		t.Fatalf("no identity for %s", fsDB)
	}
	definitions := []auth.FineGrainedEntitlement{
		{ID: "fs_search", Action: "database.search.execute", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase, DatabaseID: identifier}},
		{ID: "fs_embedding_read", Action: "graph.node.embedding.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, DatabaseID: identifier}},
		{ID: "fs_graphql", Action: "dbms.api.graphql", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDBMS}},
	}
	document.Catalog.Definitions = append(document.Catalog.Definitions, definitions...)
	if document.Catalog.Assignments == nil {
		document.Catalog.Assignments = map[string][]string{}
	}
	document.Catalog.Assignments[fsRole] = append(document.Catalog.Assignments[fsRole], "fs_search", "fs_embedding_read", "fs_graphql")
	code, _, body = env.http.do(http.MethodPut, "/auth/fgac/catalog", env.admin, map[string]any{"catalog": document.Catalog, "revision": document.Revision})
	if code != http.StatusNoContent {
		t.Fatalf("catalog put %d: %s", code, body)
	}
}

func (env *fsEnv) waitSearchReady() {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		code, _, _ := env.http.do(http.MethodPost, "/nornicdb/search", env.admin, map[string]any{"database": fsDB, "query": "alpha", "limit": 1})
		if code == http.StatusOK {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	env.t.Logf("NOTE search endpoint not ready for %s after 60s", fsDB)
}

// ---------------------------------------------------------------------------
// Execution and judgement.

// fsScrub removes the principal's own statement and parameter strings, so that
// an echo of its own input is not counted as a leak.
func fsScrub(raw, statement string, params map[string]any) string {
	lower := strings.ToLower(raw)
	lower = strings.ReplaceAll(lower, strings.ToLower(statement), "")
	var walk func(any)
	walk = func(value any) {
		switch typed := value.(type) {
		case string:
			if typed != "" {
				lower = strings.ReplaceAll(lower, strings.ToLower(typed), "")
				encoded, _ := json.Marshal(typed)
				lower = strings.ReplaceAll(lower, strings.ToLower(string(encoded)), "")
			}
		case []any:
			for _, item := range typed {
				walk(item)
			}
		case map[string]any:
			for _, item := range typed {
				walk(item)
			}
		}
	}
	walk(params)
	return lower
}

type fsKey struct {
	variant, position, transport, query string
}

type fsRecorder struct {
	results map[fsKey]map[string]fsOut // key -> state -> outcome
	order   []fsKey
}

func (r *fsRecorder) put(key fsKey, state string, out fsOut) {
	if r.results[key] == nil {
		r.results[key] = map[string]fsOut{}
		r.order = append(r.order, key)
	}
	r.results[key][state] = out
}

func (env *fsEnv) leakCheck(where, name, statement string, params map[string]any, out fsOut) {
	env.t.Helper()
	scrubbed := fsScrub(out.Raw, statement, params)
	scrubbed += " " + fsScrub(strings.Join(out.Errors, " "), statement, params)
	if strings.Contains(scrubbed, fsMarker) {
		env.leaks++
		at := strings.Index(scrubbed, fsMarker)
		from := max(0, at-200)
		to := min(len(scrubbed), at+200)
		env.t.Errorf("LEAK [%s] %s: restricted response contains a hidden value\n  statement: %s\n  params: %v\n  context: ...%s...", where, name, statement, params, scrubbed[from:to])
	}
}

func (env *fsEnv) restricted(rec *fsRecorder, variant, position, state string) {
	for _, q := range fsCypherQueries() {
		where := fmt.Sprintf("%s/%s/%s", variant, position, state)
		if q.write {
			for _, transport := range []string{"http-tx", "bolt-tx"} {
				var out fsOut
				if transport == "http-tx" {
					txPath, err := env.http.begin(fsDB, env.narrow)
					if err != nil {
						out = fsBuild(map[string]any{}, nil, err.Error(), []string{"begin: " + err.Error()})
					} else {
						out = env.http.inTx(txPath, env.narrow, q.stmt, q.params)
						env.http.rollback(txPath, env.narrow)
					}
				} else {
					out = env.narrowB.rolledBack(fsDB, q.stmt, q.params)
				}
				env.leakCheck(where+"/"+transport, q.name, q.stmt, q.params, out)
				rec.put(fsKey{variant, position, transport, q.name}, state, out)
			}
			continue
		}
		httpOut := env.http.cypher(fsDB, env.narrow, q.stmt, q.params)
		boltOut := env.narrowB.auto(fsDB, q.stmt, q.params)
		for transport, out := range map[string]fsOut{"http": httpOut, "bolt": boltOut} {
			env.leakCheck(where+"/"+transport, q.name, q.stmt, q.params, out)
			rec.put(fsKey{variant, position, transport, q.name}, state, out)
			if state == "S2" && q.minRows > 0 && out.Rows < q.minRows {
				env.t.Errorf("SETUP [%s/%s] %s: expected at least %d rows for a permitted read, got %d (errors %v): %.600s", where, transport, q.name, q.minRows, out.Rows, out.Errors, out.Canon)
			}
		}
	}
	for _, q := range fsEndpointQueries(env.aliceID) {
		where := fmt.Sprintf("%s/%s/%s", variant, position, state)
		out, object := env.http.endpoint(q.path, env.narrow, q.request)
		env.leakCheck(where+"/endpoint", q.name, "", q.request, out)
		rec.put(fsKey{variant, position, "endpoint", q.name}, state, out)
		if qid, ok := object["qid"].(string); ok && qid != "" {
			next, _ := env.http.endpoint(q.path, env.narrow, map[string]any{"database": fsDB, "qid": qid, "n": 2})
			env.leakCheck(where+"/endpoint", q.name+"-page2", "", q.request, next)
			rec.put(fsKey{variant, position, "endpoint", q.name + "-page2"}, state, next)
			_, _ = env.http.endpoint(q.path, env.narrow, map[string]any{"database": fsDB, "qid": qid, "discard": true})
		}
	}
	// Other database: refused whatever it holds.
	for _, statement := range []string{
		"CALL db.index.fulltext.queryNodes('fo_ft', 'alpha') YIELD node, score RETURN node.name AS n, score",
		"MATCH (p:Person) RETURN count(p) AS c",
	} {
		httpOut := env.http.cypher(fsOtherDB, env.narrow, statement, nil)
		boltOut := env.narrowB.auto(fsOtherDB, statement, nil)
		for transport, out := range map[string]fsOut{"http": httpOut, "bolt": boltOut} {
			env.leakCheck(variant+"/"+position+"/"+state+"/other-db/"+transport, statement, statement, nil, out)
			rec.put(fsKey{variant, position, "other-db-" + transport, statement}, state, out)
		}
	}
}

// wide runs the full-rights control and warms every cache.
func (env *fsEnv) wide(state string) {
	visible := 0
	for _, q := range fsCypherQueries() {
		if q.write {
			continue
		}
		httpOut := env.http.cypher(fsDB, env.admin, q.stmt, q.params)
		_ = env.adminB.auto(fsDB, q.stmt, q.params)
		if state == "S0" {
			env.t.Logf("WIDE-OUT http %s S0: %.700s", q.name, httpOut.Canon)
		}
		if strings.Contains(strings.ToLower(httpOut.Raw), fsMarker) {
			visible++
		}
	}
	for _, q := range fsEndpointQueries(env.aliceID) {
		out, _ := env.http.endpoint(q.path, env.admin, q.request)
		if state == "S0" {
			env.t.Logf("WIDE-OUT endpoint %s S0: %.700s", q.name, out.Canon)
		}
		if strings.Contains(strings.ToLower(out.Raw), fsMarker) {
			visible++
		}
	}
	env.t.Logf("CONTROL [%s] wide responses containing a hidden value: %d", state, visible)
	if state != "S2" && visible == 0 {
		env.t.Errorf("CONTROL [%s] the full-rights user saw no hidden value: the detector cannot see a leak", state)
	}
	if state == "S0" {
		control := env.http.cypher(fsDB, env.admin, "CALL db.index.fulltext.queryNodes('fs_ft', 'zqxkeyword') YIELD node RETURN node.name AS n", nil)
		env.t.Logf("CONTROL [S0] admin fulltext on hidden term: rows=%d %v", control.Rows, control.Errors)
	}
}

func (env *fsEnv) toState(state string) {
	var statements []string
	switch state {
	case "S1":
		statements = fsToS1()
	case "S2":
		statements = fsToS2()
	default:
		return
	}
	for _, statement := range statements {
		env.adminRun(fsDB, statement)
	}
	if state == "S2" {
		env.adminRun(fsOtherDB, "MATCH (n) DETACH DELETE n")
	}
	_ = env.http.cypher(fsDB, env.admin, "CALL db.awaitIndexes(60)", nil)
}

// settle waits for the embedding worker (embedded scenario) and logs the
// unique-constraint lookup that decides finding 1.
func (env *fsEnv) settle(state string) {
	if fsEmbedded {
		env.embedAll(state)
	}
	store, err := env.server.dbManager.GetStorage(fsDB)
	if err != nil {
		return
	}
	if schema := store.GetSchema(); schema != nil {
		id, found, constrained, complete, lookupErr := schema.LookupUniqueConstraintValue(context.Background(), "Person", "ssn", "ZQX-SSN-1")
		name := ""
		if found {
			if node, err := store.GetNode(context.Background(), id); err == nil {
				name = fmt.Sprint(node.Properties["name"])
			}
		}
		env.t.Logf("CAUSE1 [%s] LookupUniqueConstraintValue(Person, ssn, ZQX-SSN-1) => id=%s found=%v constrained=%v cacheComplete=%v err=%v holder=%q", state, id, found, constrained, complete, lookupErr, name)
	}
	if fsEmbedded {
		if store != nil {
			listed, _ := store.GetNodesByLabel(context.Background(), "Person")
			for _, item := range listed {
				node, err := store.GetNode(context.Background(), item.ID)
				if err != nil {
					continue
				}
				probe := &fsHashEmbedder{dims: 1024}
				hiddenTerm, _ := probe.Embed(context.Background(), "zqxkeyword")
				visibleTerm, _ := probe.Embed(context.Background(), "alpha")
				dot := func(a, b []float32) float64 {
					var sum float64
					for i := range a {
						if i < len(b) {
							sum += float64(a[i] * b[i])
						}
					}
					return sum
				}
				var withHidden, withVisible float64
				if len(node.ChunkEmbeddings) > 0 {
					withHidden, withVisible = dot(node.ChunkEmbeddings[0], hiddenTerm), dot(node.ChunkEmbeddings[0], visibleTerm)
				}
				env.t.Logf("CAUSE-EMB [%s] embedding of %v (labels %v): chunks=%d named=%d dot(zqxkeyword)=%.4f dot(alpha)=%.4f", state, node.Properties["name"], node.Labels, len(node.ChunkEmbeddings), len(node.NamedEmbeddings), withHidden, withVisible)
			}
		}
	}
}

// embedAll makes the managed embedding of every fixture node current and
// logs the evidence: storage pending markers, queue stats, embedder calls and
// each node's embedding state. Probing starts only when every visible and
// hidden node has a chunk embedding, or after a 90 s deadline (logged).
func (env *fsEnv) embedAll(state string) {
	ctx := context.Background()
	store, err := env.server.dbManager.GetStorage(fsDB)
	if err != nil {
		env.t.Fatalf("storage: %v", err)
	}
	queue := env.server.db.GetEmbedQueue()
	env.t.Logf("EMB [%s] before: queue=%v pendingMarkers=%d pending=%d calls=%d stats=%s", state, queue != nil,
		storage.CountPendingEmbeddingIDs(ctx, store, nil), env.server.db.PendingEmbeddingsCount(), fsEmbedCalls.Load(), fsJSON(env.server.db.EmbedQueueStats()))
	ids := map[storage.NodeID]string{}
	for _, label := range []string{"Person", "Note", "Doc"} {
		nodes, _ := store.GetNodesByLabel(ctx, label)
		for _, node := range nodes {
			ids[node.ID] = fmt.Sprint(node.Labels, node.Properties["name"], node.Properties["title"], node.Properties["text"])
		}
	}
	if queue != nil {
		queue.StartWorkers()
		for id := range ids {
			queue.Enqueue(fsDB + ":" + string(id))
		}
		queue.TriggerImmediate()
	}
	deadline := time.Now().Add(90 * time.Second)
	missing := 0
	for {
		missing = 0
		for id := range ids {
			node, err := store.GetNode(ctx, id)
			if err != nil || len(node.ChunkEmbeddings) == 0 || len(node.ChunkEmbeddings[0]) == 0 {
				if node == nil || len(node.NamedEmbeddings) == 0 {
					missing++
				}
			}
		}
		if missing == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	waitErr := env.server.db.WaitForEmbeddings(waitCtx)
	cancel()
	count := 0
	for end := time.Now().Add(30 * time.Second); time.Now().Before(end); time.Sleep(250 * time.Millisecond) {
		if svc, svcErr := env.server.db.GetOrCreateSearchService(fsDB, store); svcErr == nil {
			count = svc.EmbeddingCount()
		}
		if count >= len(ids)-3 {
			break
		}
	}
	env.t.Logf("EMB [%s] after: missing=%d of %d wait=%v pendingMarkers=%d calls=%d searchEmbeddingCount=%d stats=%s", state, missing, len(ids), waitErr,
		storage.CountPendingEmbeddingIDs(ctx, store, nil), fsEmbedCalls.Load(), count, fsJSON(env.server.db.EmbedQueueStats()))
	probe := &fsHashEmbedder{dims: 1024}
	hiddenTerm, _ := probe.Embed(ctx, "zqxkeyword")
	visibleTerm, _ := probe.Embed(ctx, "alpha")
	dot := func(a, b []float32) float64 {
		var sum float64
		for i := range a {
			if i < len(b) {
				sum += float64(a[i] * b[i])
			}
		}
		return sum
	}
	for id, what := range ids {
		node, err := store.GetNode(ctx, id)
		if err != nil {
			env.t.Logf("EMB [%s] node %s %s: read error %v", state, id, what, err)
			continue
		}
		var h, v float64
		if len(node.ChunkEmbeddings) > 0 {
			h, v = dot(node.ChunkEmbeddings[0], hiddenTerm), dot(node.ChunkEmbeddings[0], visibleTerm)
		}
		env.t.Logf("EMB [%s] node %s %s: chunks=%d named=%d meta=%s skipped=%v dot(zqxkeyword)=%.4f dot(alpha)=%.4f", state, id, what,
			len(node.ChunkEmbeddings), len(node.NamedEmbeddings), fsJSON(node.EmbedMeta), node.Properties["embedding_skipped"], h, v)
	}
	fsEmbedMu.Lock()
	for i, text := range fsEmbedSample {
		env.t.Logf("EMB [%s] embedded text %d: %.200q", state, i, text)
	}
	fsEmbedSample = nil
	fsEmbedMu.Unlock()
}

// fsHashEmbedder embeds text deterministically: each lowercased word adds 1
// to one dimension chosen by its FNV hash; the vector is normalized.
type fsHashEmbedder struct{ dims int }

// fsEmbedCalls counts every text the test embedder receives; fsEmbedSample
// keeps the last texts, so the log shows what the product embeds.
var (
	fsEmbedCalls  atomic.Int64
	fsEmbedMu     sync.Mutex
	fsEmbedSample []string
)

func (e *fsHashEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	fsEmbedCalls.Add(1)
	fsEmbedMu.Lock()
	fsEmbedSample = append(fsEmbedSample, text)
	if len(fsEmbedSample) > 40 {
		fsEmbedSample = fsEmbedSample[len(fsEmbedSample)-40:]
	}
	fsEmbedMu.Unlock()
	vector := make([]float32, e.dims)
	for _, word := range strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-')
	}) {
		hash := uint32(2166136261)
		for i := 0; i < len(word); i++ {
			hash = (hash ^ uint32(word[i])) * 16777619
		}
		vector[int(hash%uint32(e.dims))]++
	}
	var norm float64
	for _, value := range vector {
		norm += float64(value * value)
	}
	if norm == 0 {
		vector[0] = 1
		return vector, nil
	}
	scale := float32(1 / math.Sqrt(norm))
	for i := range vector {
		vector[i] *= scale
	}
	return vector, nil
}

func (e *fsHashEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, text := range texts {
		out[i], _ = e.Embed(ctx, text)
	}
	return out, nil
}

func (e *fsHashEmbedder) Dimensions() int { return e.dims }
func (e *fsHashEmbedder) Model() string   { return "fs-hash-embedder" }
func (e *fsHashEmbedder) Backend() string { return "cpu" }
func (e *fsHashEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return []string{text}, nil
}

// TestFGACSearchEmbedded runs the same differential with a deterministic
// embedder, in the default database (GraphQL reads it).
func TestFGACSearchEmbedded(t *testing.T) {
	fsEmbedded, fsDB = true, "nornic"
	defer func() { fsEmbedded, fsDB = false, "fgs" }()
	fsScenario(t)
}

// TestFGACSearchDifferential is the scenario.
func TestFGACSearchDifferential(t *testing.T) { fsScenario(t) }

// TestFGACSearchDifferentialTokenGrant repeats the scenario with
// CREATE NEW PROPERTY NAME granted to the restricted role.
func TestFGACSearchDifferentialTokenGrant(t *testing.T) {
	fsTokenGrant = true
	defer func() { fsTokenGrant = false }()
	fsScenario(t)
}

func fsScenario(t *testing.T) {
	rec := &fsRecorder{results: map[fsKey]map[string]fsOut{}}
	leaks := 0
	for _, variant := range []string{"restricted-first", "wide-first"} {
		t.Run(variant, func(t *testing.T) {
			env := fsSetup(t)
			for _, state := range []string{"S0", "S1", "S2"} {
				env.toState(state)
				env.settle(state)
				if variant == "restricted-first" {
					env.restricted(rec, variant, "cold", state)
					env.wide(state)
					env.restricted(rec, variant, "after-wide", state)
				} else {
					env.wide(state)
					env.restricted(rec, variant, "after-wide", state)
				}
			}
			leaks += env.leaks
		})
	}
	// Differential.
	differences := 0
	unstable := map[string]bool{}
	// A query is order-unstable when, within one state, two runs that differ
	// only in cache position return the same rows in a different order.
	for _, key := range rec.order {
		if key.position != "cold" {
			continue
		}
		other := fsKey{key.variant, "after-wide", key.transport, key.query}
		for state, out := range rec.results[key] {
			if again, ok := rec.results[other][state]; ok && again.Canon != out.Canon && again.Sorted == out.Sorted {
				unstable[key.transport+"|"+key.query] = true
			}
		}
	}
	for _, key := range rec.order {
		states := rec.results[key]
		base, ok := states["S2"]
		if !ok {
			continue
		}
		for _, state := range []string{"S0", "S1"} {
			out, ok := states[state]
			if !ok {
				continue
			}
			if out.Canon == base.Canon {
				continue
			}
			kind := "DIFF"
			if out.Sorted == base.Sorted {
				kind = "DIFF-ORDER"
				if unstable[key.transport+"|"+key.query] {
					kind = "DIFF-ORDER-UNSTABLE"
				}
			}
			differences++
			t.Errorf("%s [%s/%s/%s] %s: %s differs from S2 (hidden data absent)\n  %s: %.2500s\n  S2: %.2500s", kind, key.variant, key.position, key.transport, key.query, state, state, out.Canon, base.Canon)
		}
	}
	// Same-state consistency between cache orders.
	for _, key := range rec.order {
		if key.variant != "restricted-first" || key.position != "cold" {
			continue
		}
		other := fsKey{"wide-first", "after-wide", key.transport, key.query}
		for state, out := range rec.results[key] {
			if again, ok := rec.results[other][state]; ok && again.Canon != out.Canon && again.Sorted != out.Sorted {
				t.Logf("CACHE-ORDER [%s/%s] %s %s: restricted-first cold and wide-first answers differ\n  cold: %.1200s\n  wide-first: %.1200s", key.transport, key.query, state, key.variant, out.Canon, again.Canon)
			}
		}
	}
	t.Logf("SUMMARY keys=%d differences=%d leaks=%d unstable=%d", len(rec.order), differences, leaks, len(unstable))
	// Full outcome dump for the S2 baseline (restricted-first, cold), one line per query.
	for _, key := range rec.order {
		if key.variant == "restricted-first" && key.position == "cold" {
			for _, state := range []string{"S0", "S2"} {
				if out, ok := rec.results[key][state]; ok {
					t.Logf("OUT %s %s %s: %.900s", key.transport, key.query, state, out.Canon)
				}
			}
		}
	}
}
