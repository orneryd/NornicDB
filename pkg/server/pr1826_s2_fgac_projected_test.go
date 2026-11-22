package server

// PR #1826 review, lane S2 (security scenarios). Test-only file on a scratch
// branch; it is identical on the PR head and on its base so both can be run.
//
// Every denied property value, hidden label's value, hidden node's value and
// other database's value in the fixtures contains the marker "ZQX". A
// restricted principal's complete response (rows, columns, plan, profile,
// errors, notifications) must never contain it. Wide principals must see the
// full values, whichever principal read the nodes first.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strings"
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

const (
	s2Marker   = "zqx"
	s2DB       = "s2a"
	s2OtherDB  = "s2b"
	s2Comp     = "s2comp"
	s2Role     = "s2_reader"
	s2User     = "narrow"
	s2Password = "password123"
	s2TwinID   = "pr1826s2twin"
	s2Deadline = 90 * time.Second
)

var s2VisibleNames = []string{"n0", "n1", "n2", "n3", "n4", "n5"}

// ---------------------------------------------------------------------------
// Outcome of one statement on any transport.

type s2Outcome struct {
	Columns     []string
	Rows        [][]any
	Errors      []string
	Raw         string
	HasProfile  bool
	ProfileRows []int64
	ProfileHits []int64
	Hang        bool
	Crash       bool
}

func (o s2Outcome) rowsJSON() string {
	encoded, _ := json.Marshal(o.Rows)
	return string(encoded)
}

func s2Normalize(values any) any {
	encoded, err := json.Marshal(values)
	if err != nil {
		return fmt.Sprintf("unencodable:%v", err)
	}
	var out any
	if err := json.Unmarshal(encoded, &out); err != nil {
		return fmt.Sprintf("undecodable:%v", err)
	}
	return out
}

// ---------------------------------------------------------------------------
// HTTP transport.

type s2HTTP struct {
	t      *testing.T
	router http.Handler
}

func (h *s2HTTP) do(method, path, token string, body any) (int, []byte, bool, bool) {
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
	case <-time.After(s2Deadline):
		return 0, []byte("HANG: no answer within deadline"), true, false
	}
	if panicked != nil {
		return 599, []byte(fmt.Sprintf("PANIC: %v", panicked)), false, true
	}
	return rec.Code, rec.Body.Bytes(), false, false
}

func s2DecodeHTTP(code int, body []byte, hang, crash bool) s2Outcome {
	out := s2Outcome{Raw: string(body), Hang: hang, Crash: crash}
	if hang || crash {
		out.Errors = append(out.Errors, string(body))
		return out
	}
	var resp TransactionResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		out.Errors = append(out.Errors, fmt.Sprintf("http %d undecodable: %v", code, err))
		return out
	}
	if code >= 500 {
		out.Crash = true
		out.Errors = append(out.Errors, fmt.Sprintf("http status %d", code))
	}
	for _, e := range resp.Errors {
		out.Errors = append(out.Errors, e.Code+": "+e.Message)
	}
	if len(resp.Results) > 0 {
		r := resp.Results[0]
		out.Columns = r.Columns
		for _, row := range r.Data {
			out.Rows = append(out.Rows, row.Row)
		}
		// The HTTP API returns a PROFILE's counters in "plan" (profile mode).
		for _, plan := range []map[string]any{r.Profile, r.Plan} {
			if plan != nil {
				out.HasProfile = true
				s2WalkProfileMap(plan, &out)
			}
		}
	}
	return out
}

// s2WalkProfileMap collects per-operator row and db-hit counters in a
// deterministic order (sorted keys).
func s2WalkProfileMap(value any, out *s2Outcome) {
	switch v := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			lower := strings.ToLower(key)
			if number, ok := v[key].(float64); ok {
				switch lower {
				case "rows":
					out.ProfileRows = append(out.ProfileRows, int64(number))
				case "dbhits":
					out.ProfileHits = append(out.ProfileHits, int64(number))
				}
				continue
			}
			s2WalkProfileMap(v[key], out)
		}
	case []any:
		for _, item := range v {
			s2WalkProfileMap(item, out)
		}
	}
}

func (h *s2HTTP) autocommit(database, token, statement string, params map[string]any) s2Outcome {
	code, body, hang, crash := h.do(http.MethodPost, "/db/"+database+"/tx/commit", token, map[string]any{
		"statements": []map[string]any{{"statement": statement, "parameters": params}},
	})
	return s2DecodeHTTP(code, body, hang, crash)
}

// begin opens an explicit HTTP transaction and returns its path.
func (h *s2HTTP) begin(database, token string) (string, error) {
	code, body, hang, crash := h.do(http.MethodPost, "/db/"+database+"/tx", token, map[string]any{"statements": []any{}})
	if hang || crash {
		return "", fmt.Errorf("begin: %s", body)
	}
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

func (h *s2HTTP) inTx(txPath, token, statement string, params map[string]any) s2Outcome {
	code, body, hang, crash := h.do(http.MethodPost, txPath, token, map[string]any{
		"statements": []map[string]any{{"statement": statement, "parameters": params}},
	})
	return s2DecodeHTTP(code, body, hang, crash)
}

func (h *s2HTTP) rollback(txPath, token string) {
	h.do(http.MethodDelete, txPath, token, nil)
}

// ---------------------------------------------------------------------------
// Bolt transport.

type s2Bolt struct {
	driver neo4j.DriverWithContext
}

func s2StartBolt(t *testing.T, server *Server, authenticator *auth.Authenticator) string {
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

func s2NewBolt(t *testing.T, uri, user string) *s2Bolt {
	t.Helper()
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(user, s2Password, ""))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	return &s2Bolt{driver: driver}
}

func s2IsNil(value any) bool {
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

func s2FlattenProfile(plan neo4j.ProfiledPlan, out *s2Outcome, sb *strings.Builder) {
	if s2IsNil(plan) {
		return
	}
	out.HasProfile = true
	out.ProfileRows = append(out.ProfileRows, plan.Records())
	out.ProfileHits = append(out.ProfileHits, plan.DbHits())
	fmt.Fprintf(sb, "|%s %v %v", plan.Operator(), plan.Arguments(), plan.Identifiers())
	for _, child := range plan.Children() {
		s2FlattenProfile(child, out, sb)
	}
}

func s2FlattenPlan(plan neo4j.Plan, sb *strings.Builder) {
	if s2IsNil(plan) {
		return
	}
	fmt.Fprintf(sb, "|%s %v %v", plan.Operator(), plan.Arguments(), plan.Identifiers())
	for _, child := range plan.Children() {
		s2FlattenPlan(child, sb)
	}
}

func s2CollectBolt(ctx context.Context, result neo4j.ResultWithContext, runErr error) s2Outcome {
	var out s2Outcome
	fail := func(err error) s2Outcome {
		out.Errors = append(out.Errors, err.Error())
		out.Raw += " ERROR " + err.Error()
		if ctx.Err() != nil {
			out.Hang = true
		}
		return out
	}
	if runErr != nil {
		return fail(runErr)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return fail(err)
	}
	if keys, keysErr := result.Keys(); keysErr == nil {
		out.Columns = keys
	}
	for _, record := range records {
		row, _ := s2Normalize(record.Values).([]any)
		out.Rows = append(out.Rows, row)
	}
	var sb strings.Builder
	sb.WriteString(out.rowsJSON())
	summary, err := result.Consume(ctx)
	if err == nil && !s2IsNil(summary) {
		s2FlattenProfile(summary.Profile(), &out, &sb)
		s2FlattenPlan(summary.Plan(), &sb)
		for _, notification := range summary.Notifications() {
			fmt.Fprintf(&sb, "|N %s %s", notification.Title(), notification.Description())
		}
	} else if err != nil {
		out.Errors = append(out.Errors, err.Error())
		sb.WriteString(" ERROR " + err.Error())
	}
	out.Raw += sb.String()
	return out
}

func (b *s2Bolt) autocommit(database, statement string, params map[string]any) s2Outcome {
	ctx, cancel := context.WithTimeout(context.Background(), s2Deadline)
	defer cancel()
	session := b.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
	defer session.Close(context.Background())
	result, err := session.Run(ctx, statement, params)
	return s2CollectBolt(ctx, result, err)
}

// explicit runs statements in one explicit transaction and rolls it back.
func (b *s2Bolt) explicit(database string, statements []s2Query) []s2Outcome {
	ctx, cancel := context.WithTimeout(context.Background(), s2Deadline*time.Duration(len(statements)+1))
	defer cancel()
	session := b.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
	defer session.Close(context.Background())
	tx, err := session.BeginTransaction(ctx)
	outs := make([]s2Outcome, len(statements))
	if err != nil {
		for i := range outs {
			outs[i] = s2Outcome{Errors: []string{"begin: " + err.Error()}, Raw: err.Error()}
		}
		return outs
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	for i, statement := range statements {
		result, runErr := tx.Run(ctx, statement.statement, statement.params)
		outs[i] = s2CollectBolt(ctx, result, runErr)
	}
	return outs
}

// ---------------------------------------------------------------------------
// Internal (no principal) executor: the path that uses projected reads on the
// PR head without fine-grained access control in the way.

func s2Internal(store storage.Engine, statement string, params map[string]any) s2Outcome {
	exec := cypher.NewStorageExecutor(store)
	ctx, cancel := context.WithTimeout(context.Background(), s2Deadline)
	defer cancel()
	var out s2Outcome
	done := make(chan struct{})
	var (
		res      *cypher.ExecuteResult
		err      error
		panicked any
	)
	go func() {
		defer close(done)
		defer func() { panicked = recover() }()
		res, err = exec.Execute(ctx, statement, params)
	}()
	select {
	case <-done:
	case <-time.After(s2Deadline + 5*time.Second):
		out.Hang = true
		out.Errors = []string{"HANG"}
		return out
	}
	if panicked != nil {
		out.Crash = true
		out.Errors = []string{fmt.Sprintf("PANIC: %v", panicked)}
		out.Raw = out.Errors[0]
		return out
	}
	if err != nil {
		out.Errors = []string{err.Error()}
		out.Raw = err.Error()
		return out
	}
	out.Columns = res.Columns
	for _, row := range res.Rows {
		normalized, _ := s2Normalize(row).([]any)
		out.Rows = append(out.Rows, normalized)
	}
	out.Raw = out.rowsJSON()
	return out
}

// ---------------------------------------------------------------------------
// Checks.

type s2Query struct {
	name      string
	statement string
	params    map[string]any
	// allowErr: a refusal is an acceptable (non-leaking) outcome.
	allowErr bool
	check    func(o s2Outcome) string
}

func s2All(checks ...func(o s2Outcome) string) func(o s2Outcome) string {
	return func(o s2Outcome) string {
		var problems []string
		for _, check := range checks {
			if problem := check(o); problem != "" {
				problems = append(problems, problem)
			}
		}
		return strings.Join(problems, "; ")
	}
}

func s2RowCount(n int) func(o s2Outcome) string {
	return func(o s2Outcome) string {
		if len(o.Rows) != n {
			return fmt.Sprintf("expected %d rows, got %d: %s", n, len(o.Rows), o.rowsJSON())
		}
		return ""
	}
}

func s2Cell(o s2Outcome, row, col int) (any, bool) {
	if row >= len(o.Rows) || col >= len(o.Rows[row]) {
		return nil, false
	}
	return o.Rows[row][col], true
}

func s2ColumnNil(col int) func(o s2Outcome) string {
	return func(o s2Outcome) string {
		for i := range o.Rows {
			if value, ok := s2Cell(o, i, col); ok && value != nil {
				return fmt.Sprintf("column %d row %d expected null, got %v", col, i, value)
			}
		}
		return ""
	}
}

func s2ColumnStrings(col int, expected []string) func(o s2Outcome) string {
	return func(o s2Outcome) string {
		got := make([]any, 0, len(o.Rows))
		for i := range o.Rows {
			value, _ := s2Cell(o, i, col)
			got = append(got, value)
		}
		want := make([]any, 0, len(expected))
		for _, value := range expected {
			want = append(want, value)
		}
		if !reflect.DeepEqual(got, want) {
			return fmt.Sprintf("column %d expected %v, got %v", col, want, got)
		}
		return ""
	}
}

func s2Number(o s2Outcome, row, col int) (float64, bool) {
	value, ok := s2Cell(o, row, col)
	if !ok {
		return 0, false
	}
	number, ok := value.(float64)
	return number, ok
}

func s2ScalarIs(col int, expected float64) func(o s2Outcome) string {
	return func(o s2Outcome) string {
		if len(o.Rows) != 1 {
			return fmt.Sprintf("expected 1 row, got %d: %s", len(o.Rows), o.rowsJSON())
		}
		number, ok := s2Number(o, 0, col)
		if !ok || number != expected {
			value, _ := s2Cell(o, 0, col)
			return fmt.Sprintf("column %d expected %v, got %v", col, expected, value)
		}
		return ""
	}
}

func s2ScalarZeroOrNull(col int) func(o s2Outcome) string {
	return func(o s2Outcome) string {
		value, ok := s2Cell(o, 0, col)
		if !ok {
			return "missing row"
		}
		if value == nil {
			return ""
		}
		if number, isNumber := value.(float64); isNumber && number == 0 {
			return ""
		}
		return fmt.Sprintf("column %d expected 0 or null, got %v", col, value)
	}
}

func s2EmptyList(col int) func(o s2Outcome) string {
	return func(o s2Outcome) string {
		value, ok := s2Cell(o, 0, col)
		if !ok {
			return "missing row"
		}
		list, isList := value.([]any)
		if !isList || len(list) != 0 {
			return fmt.Sprintf("column %d expected [], got %v", col, value)
		}
		return ""
	}
}

func s2RowsLack(words ...string) func(o s2Outcome) string {
	return func(o s2Outcome) string {
		rows := o.rowsJSON()
		for _, word := range words {
			if strings.Contains(rows, word) {
				return fmt.Sprintf("rows contain %s: %s", word, rows)
			}
		}
		return ""
	}
}

// ---------------------------------------------------------------------------
// Fixtures.

func s2SSN(i int) string { return fmt.Sprintf("ZQXSSN%04d", 5-i) }

func s2AllSSN() []string {
	values := []string{"ZQXSSNHIDDEN"}
	for i := 0; i < 6; i++ {
		values = append(values, s2SSN(i))
	}
	sort.Strings(values)
	return values
}

func s2FixtureStatements() []string {
	statements := []string{
		"CREATE INDEX s2_ssn IF NOT EXISTS FOR (p:Person) ON (p.ssn)",
		"CREATE INDEX s2_name IF NOT EXISTS FOR (p:Person) ON (p.name)",
		"CREATE INDEX s2_dept_ssn IF NOT EXISTS FOR (p:Person) ON (p.dept, p.ssn)",
		"CREATE INDEX s2_memo_text IF NOT EXISTS FOR (m:Memo) ON (m.text)",
		"CREATE CONSTRAINT s2_doc_title IF NOT EXISTS FOR (d:Doc) REQUIRE d.title IS UNIQUE",
		"CREATE FULLTEXT INDEX s2_ft IF NOT EXISTS FOR (p:Person) ON EACH [p.ssn, p.name]",
	}
	for i := 0; i < 6; i++ {
		statements = append(statements, fmt.Sprintf(
			"CREATE (:Person {name: 'n%d', ssn: '%s', salary: %d, dept: 'eng'})", i, s2SSN(i), 913700+i))
	}
	statements = append(statements,
		"CREATE (:Person:Secret {name: 'ZQXHIDDENNAME', ssn: 'ZQXSSNHIDDEN', salary: 913799, dept: 'eng'})",
		"MATCH (a:Person {name: 'n0'}), (b:Person {name: 'ZQXHIDDENNAME'}) CREATE (a)-[:KNOWS {since: 'ZQXSINCE'}]->(b)",
		"MATCH (a:Person {name: 'n0'}), (b:Person {name: 'n1'}) CREATE (a)-[:KNOWS]->(b)",
		"CREATE (:Doc {title: 'pub-0', classified: false})",
		"CREATE (:Doc {title: 'pub-1', classified: false})",
		"CREATE (:Doc {title: 'ZQXCLASSIFIED0', classified: true})",
		"CREATE (:Doc {title: 'ZQXCLASSIFIED1', classified: true})",
		"CREATE (:Memo {text: 'memo-low-0', level: 'low'})",
		"CREATE (:Memo {text: 'memo-low-1', level: 'low'})",
		"CREATE (:Memo {text: 'ZQXTOPMEMO', level: 'top'})",
	)
	return statements
}

func s2GrantStatements() []string {
	return []string{
		"CREATE ROLE " + s2Role + " IF NOT EXISTS",
		"GRANT ROLE " + s2Role + " TO " + s2User,
		"GRANT ACCESS ON DATABASE " + s2DB + " TO " + s2Role,
		"GRANT ACCESS ON DATABASE " + s2Comp + " TO " + s2Role,
		"GRANT TRAVERSE ON GRAPH " + s2DB + " NODES Person, Doc, Memo TO " + s2Role,
		"GRANT TRAVERSE ON GRAPH " + s2DB + " RELATIONSHIPS KNOWS TO " + s2Role,
		"GRANT READ {name, dept} ON GRAPH " + s2DB + " NODES Person TO " + s2Role,
		"DENY TRAVERSE ON GRAPH " + s2DB + " NODES Secret TO " + s2Role,
		"GRANT READ {title, classified} ON GRAPH " + s2DB + " NODES Doc TO " + s2Role,
		"DENY READ {title} ON GRAPH " + s2DB + " FOR (d:Doc) WHERE d.classified = true TO " + s2Role,
		"GRANT READ {text, level} ON GRAPH " + s2DB + " NODES Memo TO " + s2Role,
		"DENY MATCH {*} ON GRAPH " + s2DB + " FOR (m:Memo) WHERE m.level = 'top' TO " + s2Role,
		"GRANT SET PROPERTY {dept} ON GRAPH " + s2DB + " NODES Person TO " + s2Role,
	}
}

// s2RestrictedQueries are property-only reads (plus whole-node controls) run
// by the restricted principal. useWrap prefixes a composite USE clause.
func s2RestrictedQueries(prefix string) []s2Query {
	v := s2SSN(2) // n2's ssn, exists
	p := func(kv ...any) map[string]any {
		params := map[string]any{}
		for i := 0; i+1 < len(kv); i += 2 {
			params[kv[i].(string)] = kv[i+1]
		}
		return params
	}
	q := func(name, statement string, params map[string]any, check func(o s2Outcome) string) s2Query {
		return s2Query{name: name, statement: prefix + statement, params: params, check: check}
	}
	queries := []s2Query{
		q("denied-columns", "MATCH (p:Person) RETURN p.name AS name, p.ssn AS s, p.salary AS sal ORDER BY name", nil,
			s2All(s2RowCount(6), s2ColumnStrings(0, s2VisibleNames), s2ColumnNil(1), s2ColumnNil(2))),
		q("where-denied-eq", "MATCH (p:Person) WHERE p.ssn = $v RETURN p.name AS n", p("v", v), s2RowCount(0)),
		q("inline-denied-indexed", "MATCH (p:Person {ssn: $v}) RETURN p.name AS n", p("v", v), s2RowCount(0)),
		q("where-denied-prefix", "MATCH (p:Person) WHERE p.ssn STARTS WITH $pre RETURN count(p) AS c", p("pre", "ZQX"), s2ScalarIs(0, 0)),
		q("where-denied-range", "MATCH (p:Person) WHERE p.ssn < $hi RETURN count(p) AS c", p("hi", "ZZZZ"), s2ScalarIs(0, 0)),
		q("where-denied-not-null", "MATCH (p:Person) WHERE p.ssn IS NOT NULL RETURN count(p) AS c", nil, s2ScalarIs(0, 0)),
		q("aggregate-denied", "MATCH (p:Person) RETURN count(p.ssn) AS c, collect(p.ssn) AS l, max(p.salary) AS mx, sum(p.salary) AS s", nil,
			s2All(s2ScalarIs(0, 0), s2EmptyList(1), s2ColumnNil(2), s2ScalarZeroOrNull(3))),
		q("group-by-denied", "MATCH (p:Person) RETURN p.ssn AS s, count(*) AS c", nil,
			s2All(s2RowCount(1), s2ColumnNil(0), s2ScalarIs(1, 6))),
		q("whole-node-shapes", "MATCH (p:Person) RETURN keys(p) AS k, properties(p) AS pr, p{.*} AS m, p AS n", nil,
			s2All(s2RowCount(6), s2RowsLack(`"ssn"`, `"salary"`, "Secret"))),
		q("map-projection-named", "MATCH (p:Person) RETURN p{.name, .ssn, .salary} AS m", nil,
			s2All(s2RowCount(6), s2RowsLack("ZQX"))),
		q("with-denied", "MATCH (p:Person) WITH p, p.ssn AS s WHERE s IS NULL RETURN count(*) AS c", nil, s2ScalarIs(0, 6)),
		q("optional-denied", "OPTIONAL MATCH (p:Person {name: $n}) RETURN p.ssn AS s", p("n", "n2"), s2All(s2RowCount(1), s2ColumnNil(0))),
		q("union-denied", "MATCH (p:Person) RETURN p.ssn AS v UNION MATCH (d:Doc) RETURN d.title AS v", nil, s2RowsLack("ZQX")),
		q("subquery-denied", "CALL { MATCH (p:Person) RETURN p.ssn AS s } RETURN collect(s) AS l", nil, s2EmptyList(0)),
		q("exists-denied", "MATCH (p:Person) WHERE EXISTS { MATCH (q:Person) WHERE q.ssn = $v } RETURN count(p) AS c", p("v", v), s2ScalarIs(0, 0)),
		q("order-by-denied", "MATCH (p:Person) RETURN p.name AS n ORDER BY p.ssn, p.name", nil, s2ColumnStrings(0, s2VisibleNames)),
		q("composite-index-denied", "MATCH (p:Person) WHERE p.dept = $d AND p.ssn = $v RETURN p.name AS n", p("d", "eng", "v", v), s2RowCount(0)),
		q("error-message-denied", "MATCH (p:Person) RETURN date(p.ssn) AS d", nil, s2All(s2RowCount(6), s2ColumnNil(0))),
		q("comparison-denied", "MATCH (p:Person) RETURN p.name AS n, p.ssn = $v AS eq ORDER BY n", p("v", v), s2All(s2RowCount(6), s2ColumnNil(1))),
		q("name-seek-denied", "MATCH (p:Person) WHERE p.name = $n RETURN p.name AS n, p.ssn AS s LIMIT 1", p("n", "n2"),
			s2All(s2ColumnStrings(0, []string{"n2"}), s2ColumnNil(1))),
		q("collect-salary", "MATCH (p:Person) WITH p ORDER BY p.name RETURN collect(p.salary) AS l", nil, s2EmptyList(0)),
		q("skip-limit-denied", "MATCH (p:Person) RETURN p.name AS n, p.ssn AS s ORDER BY n SKIP 1 LIMIT 3", nil,
			s2All(s2ColumnStrings(0, []string{"n1", "n2", "n3"}), s2ColumnNil(1))),
		q("unwind-node-match", "UNWIND $names AS nm MATCH (p:Person {name: nm}) RETURN p.name AS n, p.ssn AS s ORDER BY n", p("names", []any{"n1", "n4", "ZQXHIDDENNAME"}),
			s2All(s2ColumnStrings(0, []string{"n1", "n4"}), s2ColumnNil(1))),
		q("two-patterns", "MATCH (a:Person {name: $n}), (b:Person {name: $m}) RETURN a.ssn AS x, b.ssn AS y", p("n", "n1", "m", "n3"),
			s2All(s2RowCount(1), s2ColumnNil(0), s2ColumnNil(1))),
		q("repeated-variable", "MATCH (p:Person {name: $n}) MATCH (p {dept: $d}) RETURN p.ssn AS s", p("n", "n2", "d", "eng"),
			s2All(s2RowCount(1), s2ColumnNil(0))),
		q("hidden-label-scan", "MATCH (n:Secret) RETURN n.name AS n", nil, s2RowCount(0)),
		q("hidden-by-name", "MATCH (p:Person) WHERE p.name = $h RETURN count(p) AS c", p("h", "ZQXHIDDENNAME"), s2ScalarIs(0, 0)),
		q("hidden-count", "MATCH (p:Person) RETURN count(p) AS c", nil, s2ScalarIs(0, 6)),
		q("hidden-count-all", "MATCH (n) RETURN count(n) AS c", nil, s2RowsLack("ZQX")),
		q("hidden-labels", "MATCH (p:Person) RETURN labels(p) AS l", nil, s2All(s2RowCount(6), s2RowsLack("Secret"))),
		q("hidden-via-relationship", "MATCH (p:Person)-[r:KNOWS]->(q) RETURN q.name AS n, q.ssn AS s, r.since AS since", nil,
			s2All(s2ColumnStrings(0, []string{"n1"}), s2ColumnNil(1))),
		q("predicate-deny-titles", "MATCH (d:Doc) RETURN d.title AS t ORDER BY t", nil, s2All(s2RowCount(4), s2RowsLack("ZQX"))),
		q("predicate-deny-where", "MATCH (d:Doc) WHERE d.title = $t RETURN count(d) AS c", p("t", "ZQXCLASSIFIED0"), s2ScalarIs(0, 0)),
		q("predicate-deny-unique-seek", "MATCH (d:Doc {title: $t}) RETURN d.classified AS c", p("t", "ZQXCLASSIFIED1"), s2RowCount(0)),
		q("predicate-deny-match", "MATCH (m:Memo) RETURN m.text AS t, m.level AS l ORDER BY t", nil, s2RowsLack("ZQX")),
		q("predicate-deny-match-seek", "MATCH (m:Memo) WHERE m.text = $t RETURN count(m) AS c", p("t", "ZQXTOPMEMO"), s2ScalarIs(0, 0)),
		q("no-traverse-label", "MATCH (t:Twin) RETURN t.v AS v", nil, s2RowCount(0)),
		q("explain-denied", "EXPLAIN MATCH (p:Person) WHERE p.ssn = $v RETURN p.name AS n", p("v", v), func(o s2Outcome) string { return "" }),
	}
	if prefix == "" {
		queries = append(queries, s2Query{
			name: "fulltext-denied", statement: "CALL db.index.fulltext.queryNodes('s2_ft', $tok) YIELD node RETURN node.name AS n",
			params: p("tok", strings.ToLower(v)), allowErr: true, check: s2RowCount(0),
		})
	}
	return queries
}

func s2WideQueries() []s2Query {
	return []s2Query{
		{name: "wide-ssn", statement: "MATCH (p:Person) RETURN p.ssn AS s ORDER BY s", check: s2ColumnStrings(0, s2AllSSN())},
		{name: "wide-ssn-seek", statement: "MATCH (p:Person {ssn: $v}) RETURN p.name AS n", params: map[string]any{"v": s2SSN(2)}, check: s2ColumnStrings(0, []string{"n2"})},
		{name: "wide-salary", statement: "MATCH (p:Person) RETURN sum(p.salary) AS s", check: s2ScalarIs(0, 6396014)},
		{name: "wide-person-point", statement: "MATCH (p:Person {name: $n}) RETURN p.ssn AS s, p.salary AS sal, p.name AS n", params: map[string]any{"n": "n2"},
			check: s2All(s2ColumnStrings(0, []string{s2SSN(2)}), s2ScalarIs(1, 913702))},
		{name: "wide-doc", statement: "MATCH (d:Doc) RETURN d.title AS t ORDER BY t", check: s2ColumnStrings(0, []string{"ZQXCLASSIFIED0", "ZQXCLASSIFIED1", "pub-0", "pub-1"})},
		{name: "wide-doc-classified", statement: "MATCH (d:Doc) WHERE d.classified = true RETURN d.title AS t ORDER BY t", check: s2ColumnStrings(0, []string{"ZQXCLASSIFIED0", "ZQXCLASSIFIED1"})},
		{name: "wide-memo", statement: "MATCH (m:Memo) RETURN m.text AS t ORDER BY t", check: s2ColumnStrings(0, []string{"ZQXTOPMEMO", "memo-low-0", "memo-low-1"})},
		{name: "wide-memo-level", statement: "MATCH (m:Memo) WHERE m.level = 'top' RETURN m.text AS t", check: s2ColumnStrings(0, []string{"ZQXTOPMEMO"})},
		{name: "wide-secret", statement: "MATCH (n:Secret) RETURN n.name AS n, n.ssn AS s", check: s2All(s2ColumnStrings(0, []string{"ZQXHIDDENNAME"}), s2ColumnStrings(1, []string{"ZQXSSNHIDDEN"}))},
		{name: "wide-twin", statement: "MATCH (t:Twin) RETURN t.v AS v", check: s2ColumnStrings(0, []string{"ZQXTWINA"})},
		{name: "wide-keys", statement: "MATCH (p:Person {name: $n}) RETURN keys(p) AS k", params: map[string]any{"n": "n3"}, check: func(o s2Outcome) string {
			rows := o.rowsJSON()
			if !strings.Contains(rows, `"ssn"`) || !strings.Contains(rows, `"salary"`) {
				return "wide user missing keys: " + rows
			}
			return ""
		}},
	}
}

// s2WarmStatements read narrow projections without a principal, so that on
// the PR head the storage caches hold partial images (title only, text only,
// name only) whose authorization fields were never decoded.
func s2WarmStatements() []s2Query {
	return []s2Query{
		{statement: "MATCH (d:Doc) RETURN d.title AS t"},
		{statement: "MATCH (m:Memo) RETURN m.text AS t"},
		{statement: "MATCH (p:Person) RETURN p.name AS n"},
		{statement: "MATCH (p:Person) RETURN p.ssn AS s"},
		{statement: "MATCH (p:Person {name: $n}) RETURN p.ssn AS s", params: map[string]any{"n": "n2"}},
		{statement: "MATCH (d:Doc {title: $t}) RETURN d.title AS t", params: map[string]any{"t": "ZQXCLASSIFIED0"}},
		{statement: "MATCH (m:Memo) WHERE m.text = $t RETURN m.text AS t", params: map[string]any{"t": "ZQXTOPMEMO"}},
		{statement: "MATCH (n:Secret) RETURN n.name AS n"},
	}
}

// s2ProjectedReader finds the projected point reader under the managed
// database wrappers (the PR adds the forward; the base may not have it).
func s2ProjectedReader(engine storage.Engine) storage.ProjectedNodeReader {
	for depth := 0; engine != nil && depth < 16; depth++ {
		if reader, ok := engine.(storage.ProjectedNodeReader); ok {
			return reader
		}
		unwrapper, ok := engine.(storage.EngineUnwrapper)
		if !ok {
			return nil
		}
		engine = unwrapper.GetInnerEngine()
	}
	return nil
}

// ---------------------------------------------------------------------------
// Scenario driver.

type s2Env struct {
	t        *testing.T
	server   *Server
	http     *s2HTTP
	admin    string
	narrow   string
	boltURI  string
	adminB   *s2Bolt
	narrowB  *s2Bolt
	failures int
	// nodeIDs maps storage node IDs in s2DB to their warming projection.
	nodeIDs map[storage.NodeID][]string
}

func (env *s2Env) report(kind, where string, q s2Query, o s2Outcome, detail string) {
	env.t.Helper()
	env.failures++
	env.t.Errorf("%s [%s] %s: %s\n  statement: %s\n  params: %v\n  errors: %v\n  raw: %.1500s", kind, where, q.name, detail, q.statement, q.params, o.Errors, o.Raw)
}

// judgeRestricted applies the universal leak test and the query's own check.
func (env *s2Env) judgeRestricted(where string, q s2Query, o s2Outcome) {
	env.t.Helper()
	if o.Hang {
		env.report("HANG", where, q, o, "no answer within deadline")
		return
	}
	if o.Crash {
		env.report("CRASH", where, q, o, "server error or panic")
	}
	if strings.Contains(strings.ToLower(o.Raw), s2Marker) || strings.Contains(strings.ToLower(strings.Join(o.Errors, " ")), s2Marker) {
		env.report("LEAK", where, q, o, "restricted response contains a denied value (marker ZQX)")
		return
	}
	if len(o.Errors) > 0 {
		if !q.allowErr {
			env.report("WRONG", where, q, o, "unexpected error for a permitted read")
		}
		return
	}
	if q.check != nil {
		if problem := q.check(o); problem != "" {
			env.report("WRONG-OR-LEAK", where, q, o, problem)
		}
	}
}

func (env *s2Env) judgeWide(where string, q s2Query, o s2Outcome) {
	env.t.Helper()
	if o.Hang || o.Crash || len(o.Errors) > 0 {
		env.report("WIDE-ERROR", where, q, o, "wide read failed")
		return
	}
	if q.check != nil {
		if problem := q.check(o); problem != "" {
			env.report("WIDE-WRONG", where, q, o, problem)
		}
	}
}

func (env *s2Env) restrictedHTTP(round string) {
	for _, q := range s2RestrictedQueries("") {
		env.judgeRestricted("http-auto "+round, q, env.http.autocommit(s2DB, env.narrow, q.statement, q.params))
	}
	for _, q := range s2RestrictedQueries("USE " + s2Comp + ".a ") {
		if strings.HasPrefix(strings.TrimPrefix(q.statement, "USE "+s2Comp+".a "), "EXPLAIN") {
			continue
		}
		env.judgeRestricted("http-composite "+round, q, env.http.autocommit(s2Comp, env.narrow, q.statement, q.params))
	}
	txPath, err := env.http.begin(s2DB, env.narrow)
	if err != nil {
		env.t.Errorf("WRONG [http-tx %s] restricted user cannot open a transaction: %v", round, err)
		return
	}
	for _, q := range s2RestrictedQueries("") {
		env.judgeRestricted("http-tx "+round, q, env.http.inTx(txPath, env.narrow, q.statement, q.params))
	}
	env.http.rollback(txPath, env.narrow)
}

func (env *s2Env) restrictedBolt(round string) {
	for _, q := range s2RestrictedQueries("") {
		env.judgeRestricted("bolt-auto "+round, q, env.narrowB.autocommit(s2DB, q.statement, q.params))
	}
	for _, q := range s2RestrictedQueries("USE " + s2Comp + ".a ") {
		if strings.HasPrefix(strings.TrimPrefix(q.statement, "USE "+s2Comp+".a "), "EXPLAIN") {
			continue
		}
		env.judgeRestricted("bolt-composite "+round, q, env.narrowB.autocommit(s2Comp, q.statement, q.params))
	}
	queries := s2RestrictedQueries("")
	outs := env.narrowB.explicit(s2DB, queries)
	for i, q := range queries {
		env.judgeRestricted("bolt-tx "+round, q, outs[i])
	}
}

func (env *s2Env) wideAll(round string) {
	store, err := env.server.dbManager.GetStorage(s2DB)
	if err != nil {
		env.t.Fatalf("storage %s: %v", s2DB, err)
	}
	for _, q := range s2WideQueries() {
		env.judgeWide("wide-http-admin "+round, q, env.http.autocommit(s2DB, env.admin, q.statement, q.params))
		env.judgeWide("wide-bolt-admin "+round, q, env.adminB.autocommit(s2DB, q.statement, q.params))
		env.judgeWide("wide-internal "+round, q, s2Internal(store, q.statement, q.params))
	}
}

// warm reads narrow projections without a principal and through the storage
// projected point reader, so caches hold partial images.
func (env *s2Env) warm(round string) {
	store, err := env.server.dbManager.GetStorage(s2DB)
	if err != nil {
		env.t.Fatalf("storage %s: %v", s2DB, err)
	}
	for _, q := range s2WarmStatements() {
		if o := s2Internal(store, q.statement, q.params); len(o.Errors) > 0 {
			env.t.Errorf("WARM-ERROR [%s] %s: %v", round, q.statement, o.Errors)
		}
	}
	reader := s2ProjectedReader(store)
	if reader == nil {
		env.t.Logf("NOTE [%s] no storage.ProjectedNodeReader reachable under %T; direct partial warming skipped", round, store)
		return
	}
	ctx := context.Background()
	warmed := 0
	for id, projection := range env.nodeIDs {
		if _, err := reader.GetNodeProjected(ctx, id, projection); err != nil {
			env.t.Logf("NOTE [%s] direct projected read %s %v: %v", round, id, projection, err)
			continue
		}
		warmed++
	}
	if warmed == 0 {
		env.t.Errorf("WARM-ERROR [%s] no node was warmed through the projected point reader", round)
	}
}

// profileDifferential checks that PROFILE counters do not differ between a
// predicate value that exists (hidden) and one that does not.
func (env *s2Env) profileDifferential(where string, run func(statement string, params map[string]any) s2Outcome) {
	statement := "PROFILE MATCH (p:Person) WHERE p.ssn = $v RETURN p.name AS n"
	hit := run(statement, map[string]any{"v": s2SSN(2)})
	miss := run(statement, map[string]any{"v": "absent-value-0000"})
	q := s2Query{name: "profile-differential", statement: statement}
	env.judgeRestricted(where+" profile-hit", q, hit)
	env.judgeRestricted(where+" profile-miss", q, miss)
	if !hit.HasProfile || !miss.HasProfile {
		env.t.Logf("NOTE [%s] profile not returned (hit=%v miss=%v)", where, hit.HasProfile, miss.HasProfile)
		return
	}
	if !reflect.DeepEqual(hit.ProfileRows, miss.ProfileRows) {
		env.report("LEAK", where, q, hit, fmt.Sprintf("PROFILE row counters differ for an existing hidden value %v vs absent value %v", hit.ProfileRows, miss.ProfileRows))
	}
	if !reflect.DeepEqual(hit.ProfileHits, miss.ProfileHits) {
		env.t.Logf("SIDE-CHANNEL [%s] PROFILE db-hit counters differ for an existing hidden value %v vs absent value %v", where, hit.ProfileHits, miss.ProfileHits)
	}
}

// readYourWrites: the restricted principal changes a permitted property in an
// explicit transaction and reads the same nodes projected inside it.
func (env *s2Env) readYourWritesHTTP(round string) {
	txPath, err := env.http.begin(s2DB, env.narrow)
	if err != nil {
		env.t.Errorf("WRONG [http-ryw %s] begin: %v", round, err)
		return
	}
	defer env.http.rollback(txPath, env.narrow)
	for _, q := range s2ReadYourWrites() {
		env.judgeRestricted("http-ryw "+round, q, env.http.inTx(txPath, env.narrow, q.statement, q.params))
	}
}

func (env *s2Env) readYourWritesBolt(round string) {
	queries := s2ReadYourWrites()
	outs := env.narrowB.explicit(s2DB, queries)
	for i, q := range queries {
		env.judgeRestricted("bolt-ryw "+round, q, outs[i])
	}
}

func s2ReadYourWrites() []s2Query {
	return []s2Query{
		{name: "ryw-set", statement: "MATCH (p:Person {name: $n}) SET p.dept = 'ops' RETURN p.name AS n, p.ssn AS s, properties(p) AS pr", params: map[string]any{"n": "n2"},
			check: s2All(s2RowCount(1), s2ColumnNil(1), s2RowsLack(`"ssn"`))},
		{name: "ryw-read-projected", statement: "MATCH (p:Person) WHERE p.dept = 'ops' RETURN p.name AS n, p.ssn AS s, p.salary AS sal", check: s2All(s2RowCount(1), s2ColumnNil(1), s2ColumnNil(2))},
		{name: "ryw-read-denied-filter", statement: "MATCH (p:Person) WHERE p.dept = 'ops' AND p.ssn = $v RETURN count(p) AS c", params: map[string]any{"v": s2SSN(2)}, check: s2ScalarIs(0, 0)},
		{name: "ryw-read-keys", statement: "MATCH (p:Person {dept: 'ops'}) RETURN keys(p) AS k, p{.*} AS m", check: s2All(s2RowCount(1), s2RowsLack(`"ssn"`, `"salary"`))},
		{name: "ryw-hidden", statement: "MATCH (p:Person) WHERE p.dept = 'eng' RETURN p.name AS n ORDER BY n", check: s2ColumnStrings(0, []string{"n0", "n1", "n3", "n4", "n5"})},
	}
}

func s2SetupEnv(t *testing.T, configureDB func(*nornicdb.Config)) *s2Env {
	t.Helper()
	server, authenticator := setupTestServerWithDatabaseConfig(t, configureDB, nil)
	if _, err := authenticator.CreateUser(context.Background(), s2User, s2Password, []auth.Role{auth.RoleViewer}); err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := server.initializeFineGrainedRuntime(t.Context()); err != nil {
		t.Fatalf("fgac: %v", err)
	}
	env := &s2Env{t: t, server: server, http: &s2HTTP{t: t, router: server.buildRouter()}}
	env.admin = getAuthToken(t, authenticator, "admin")
	env.narrow = getAuthToken(t, authenticator, s2User)
	manager := server.GetDatabaseManager()
	for _, name := range []string{s2DB, s2OtherDB} {
		if err := manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: name}); err != nil {
			t.Fatalf("create database %s: %v", name, err)
		}
	}
	if err := manager.CreateCompositeDatabase(t.Context(), s2Comp, []multidb.ConstituentRef{
		{Alias: "a", DatabaseName: s2DB, Type: "local", AccessMode: "read_write"},
		{Alias: "b", DatabaseName: s2OtherDB, Type: "local", AccessMode: "read_write"},
	}); err != nil {
		t.Fatalf("create composite: %v", err)
	}
	for _, statement := range s2FixtureStatements() {
		if o := env.http.autocommit(s2DB, env.admin, statement, nil); len(o.Errors) > 0 {
			t.Fatalf("fixture %q: %v", statement, o.Errors)
		}
	}
	_ = env.http.autocommit(s2DB, env.admin, "CALL db.awaitIndexes(60)", nil)
	if o := env.http.autocommit(s2OtherDB, env.admin, "CREATE (:Person {name: 'ZQXBNAME0', ssn: 'ZQXBSSN0', dept: 'eng'})", nil); len(o.Errors) > 0 {
		t.Fatalf("fixture b: %v", o.Errors)
	}
	for name, value := range map[string]string{s2DB: "ZQXTWINA", s2OtherDB: "ZQXTWINB"} {
		store, err := manager.GetStorage(name)
		if err != nil {
			t.Fatalf("storage %s: %v", name, err)
		}
		if _, err := store.CreateNode(t.Context(), &storage.Node{ID: s2TwinID, Labels: []string{"Twin"}, Properties: map[string]any{"v": value, "w": "w-" + name}}); err != nil {
			t.Fatalf("twin %s: %v", name, err)
		}
	}
	env.nodeIDs = map[storage.NodeID][]string{}
	if store, err := manager.GetStorage(s2DB); err == nil {
		for label, projection := range map[string][]string{"Doc": {"title"}, "Memo": {"text"}, "Person": {"name"}} {
			nodes, err := store.GetNodesByLabel(t.Context(), label)
			if err != nil {
				t.Fatalf("ids %s: %v", label, err)
			}
			for _, node := range nodes {
				env.nodeIDs[node.ID] = projection
			}
		}
	}
	t.Logf("S2 fixture node ids: %d", len(env.nodeIDs))
	for _, statement := range s2GrantStatements() {
		if o := env.http.autocommit("system", env.admin, statement, nil); len(o.Errors) > 0 {
			t.Errorf("SETUP grant %q not accepted: %v", statement, o.Errors)
		}
	}
	env.boltURI = s2StartBolt(t, server, authenticator)
	env.adminB = s2NewBolt(t, env.boltURI, "admin")
	env.narrowB = s2NewBolt(t, env.boltURI, s2User)
	return env
}

// TestPR1826S2FGACProjectedReads is the S2 main scenario: restricted and wide
// principals over HTTP and Bolt, autocommit, explicit transactions and the
// composite database, in both cache orders, under three storage settings.
func TestPR1826S2FGACProjectedReads(t *testing.T) {
	variants := []struct {
		name      string
		configure func(*nornicdb.Config)
	}{
		{name: "default", configure: nil},
		{name: "small-node-cache", configure: func(c *nornicdb.Config) { c.Database.BadgerNodeCacheMaxEntries = 8 }},
		{name: "async-writes", configure: func(c *nornicdb.Config) { c.Database.AsyncWritesEnabled = true }},
	}
	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			env := s2SetupEnv(t, variant.configure)
			// Control: without a principal the same reads return the marked
			// values, so the leak detector below is able to see a leak.
			if store, err := env.server.dbManager.GetStorage(s2DB); err == nil {
				control := s2Internal(store, s2RestrictedQueries("")[0].statement, nil)
				t.Logf("CONTROL marker visible without access control: %v", strings.Contains(strings.ToLower(control.Raw), s2Marker))
				if !strings.Contains(strings.ToLower(control.Raw), s2Marker) {
					t.Errorf("CONTROL failed: marker not visible without access control: %s", control.Raw)
				}
			}
			// Round 1: restricted principal first on cold caches, then wide.
			env.restrictedHTTP("r1-restricted-first")
			env.restrictedBolt("r1-restricted-first")
			env.wideAll("r1-after-restricted")
			// Round 2: new versions, wide (projected, no principal) warms first.
			for r := 2; r <= 4; r++ {
				round := fmt.Sprintf("r%d", r)
				if o := env.http.autocommit(s2DB, env.admin, "MATCH (n) WHERE n:Person OR n:Doc OR n:Memo SET n.touch = $r", map[string]any{"r": r}); len(o.Errors) > 0 {
					t.Fatalf("touch: %v", o.Errors)
				}
				env.warm(round + "-warm")
				env.restrictedHTTP(round + "-after-partial-warm")
				env.warm(round + "-warm-again")
				env.restrictedBolt(round + "-after-partial-warm")
				env.profileDifferential("http "+round, func(statement string, params map[string]any) s2Outcome {
					return env.http.autocommit(s2DB, env.narrow, statement, params)
				})
				env.profileDifferential("bolt "+round, func(statement string, params map[string]any) s2Outcome {
					return env.narrowB.autocommit(s2DB, statement, params)
				})
				env.readYourWritesHTTP(round)
				env.readYourWritesBolt(round)
				env.wideAll(round + "-after-restricted")
				env.restrictedHTTP(round + "-after-wide")
			}
			// Database-level denial: the other database, directly and through
			// the composite.
			denied := []s2Query{
				{name: "other-db-direct", statement: "MATCH (p:Person) RETURN p.name AS n, p.ssn AS s", allowErr: true, check: s2RowCount(0)},
				{name: "other-db-twin", statement: "MATCH (t:Twin) RETURN t.v AS v", allowErr: true, check: s2RowCount(0)},
			}
			for _, q := range denied {
				env.judgeRestricted("http-other-db", q, env.http.autocommit(s2OtherDB, env.narrow, q.statement, q.params))
				env.judgeRestricted("bolt-other-db", q, env.narrowB.autocommit(s2OtherDB, q.statement, q.params))
				cq := q
				cq.statement = "USE " + s2Comp + ".b " + q.statement
				env.judgeRestricted("http-composite-b", cq, env.http.autocommit(s2Comp, env.narrow, cq.statement, cq.params))
				env.judgeRestricted("bolt-composite-b", cq, env.narrowB.autocommit(s2Comp, cq.statement, cq.params))
			}
			// The restricted user may not read the other database even when
			// the wide user just warmed it.
			otherStore, err := env.server.dbManager.GetStorage(s2OtherDB)
			if err != nil {
				t.Fatal(err)
			}
			_ = s2Internal(otherStore, "MATCH (p:Person) RETURN p.name AS n", nil)
			_ = s2Internal(otherStore, "MATCH (t:Twin) RETURN t.v AS v", nil)
			for _, q := range denied {
				env.judgeRestricted("http-other-db-warm", q, env.http.autocommit(s2OtherDB, env.narrow, q.statement, q.params))
				env.judgeRestricted("bolt-other-db-warm", q, env.narrowB.autocommit(s2OtherDB, q.statement, q.params))
			}
			t.Logf("S2 variant %s finished with %d findings", variant.name, env.failures)
		})
	}
}

// TestPR1826S2DatabaseIsolationAndAliasing: two databases hold a node with
// the same ID and the same keys; alternate projected and full reads must never
// serve one database's image to the other, and a caller's mutation of a
// returned node or map must not reach any later result.
func TestPR1826S2DatabaseIsolationAndAliasing(t *testing.T) {
	server, authenticator := setupTestServer(t)
	manager := server.GetDatabaseManager()
	for _, name := range []string{s2DB, s2OtherDB} {
		if err := manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: name}); err != nil {
			t.Fatalf("create database %s: %v", name, err)
		}
	}
	if err := manager.CreateCompositeDatabase(t.Context(), s2Comp, []multidb.ConstituentRef{
		{Alias: "a", DatabaseName: s2DB, Type: "local", AccessMode: "read_write"},
		{Alias: "b", DatabaseName: s2OtherDB, Type: "local", AccessMode: "read_write"},
	}); err != nil {
		t.Fatalf("create composite: %v", err)
	}
	h := &s2HTTP{t: t, router: server.buildRouter()}
	admin := getAuthToken(t, authenticator, "admin")
	stores := map[string]storage.Engine{}
	expect := map[string]string{s2DB: "A", s2OtherDB: "B"}
	for name, tag := range expect {
		store, err := manager.GetStorage(name)
		if err != nil {
			t.Fatal(err)
		}
		stores[name] = store
		if _, err := store.CreateNode(t.Context(), &storage.Node{ID: s2TwinID, Labels: []string{"Twin"}, Properties: map[string]any{
			"v": "value-" + tag, "w": "wide-" + tag, "k": 1, "list": []any{"l-" + tag}, "nested": map[string]any{"x": "m-" + tag},
		}}); err != nil {
			t.Fatal(err)
		}
		if o := h.autocommit(name, admin, "CREATE (:Keyed {k: 1, v: $v, w: $w})", map[string]any{"v": "kv-" + tag, "w": "kw-" + tag}); len(o.Errors) > 0 {
			t.Fatal(o.Errors)
		}
	}
	uri := s2StartBolt(t, server, authenticator)
	adminB := s2NewBolt(t, uri, "admin")
	failures := 0
	expectValue := func(where, database string, o s2Outcome, want string) {
		t.Helper()
		got := ""
		if len(o.Rows) == 1 && len(o.Rows[0]) > 0 {
			got, _ = o.Rows[0][0].(string)
		}
		if len(o.Errors) > 0 || got != want {
			failures++
			t.Errorf("ISOLATION [%s] database %s expected %q, got rows %s errors %v", where, database, want, o.rowsJSON(), o.Errors)
		}
	}
	ctx := context.Background()
	for i := 0; i < 40; i++ {
		for _, name := range []string{s2DB, s2OtherDB, s2OtherDB, s2DB} {
			tag := expect[name]
			alias := map[string]string{s2DB: "a", s2OtherDB: "b"}[name]
			expectValue("internal-label-projected", name, s2Internal(stores[name], "MATCH (t:Twin) RETURN t.v AS v", nil), "value-"+tag)
			expectValue("internal-keyed-index", name, s2Internal(stores[name], "MATCH (n:Keyed {k: 1}) RETURN n.v AS v", nil), "kv-"+tag)
			expectValue("internal-widen", name, s2Internal(stores[name], "MATCH (t:Twin) RETURN t.w AS w, t.v AS v", nil), "wide-"+tag)
			expectValue("http-admin", name, h.autocommit(name, admin, "MATCH (t:Twin) RETURN t.v AS v", nil), "value-"+tag)
			expectValue("bolt-admin", name, adminB.autocommit(name, "MATCH (n:Keyed) RETURN n.w AS w", nil), "kw-"+tag)
			expectValue("http-composite-alias", name, h.autocommit(s2Comp, admin, "USE "+s2Comp+"."+alias+" MATCH (t:Twin) RETURN t.v AS v", nil), "value-"+tag)
			expectValue("bolt-composite-alias", name, adminB.autocommit(s2Comp, "USE "+s2Comp+"."+alias+" MATCH (n:Keyed {k: 1}) RETURN n.v AS v", nil), "kv-"+tag)
			if reader := s2ProjectedReader(stores[name]); reader != nil {
				for _, projection := range [][]string{{"v"}, {"w"}, {"v", "w", "list", "nested"}} {
					node, err := reader.GetNodeProjected(ctx, s2TwinID, projection)
					if err != nil || node == nil || node.Properties[projection[0]] != map[string]string{"v": "value-", "w": "wide-"}[projection[0]]+tag {
						failures++
						var props map[string]any
						if node != nil {
							props = node.Properties
						}
						t.Errorf("ISOLATION [direct-projected %v] database %s got %v err %v", projection, name, props, err)
					}
				}
			} else if i == 0 {
				t.Logf("NOTE no storage.ProjectedNodeReader reachable under %T", stores[name])
			}
		}
		// Whole-composite read sees each database's value exactly once.
		o := h.autocommit(s2Comp, admin, "UNWIND ['a','b'] AS g CALL { USE graph.byName('"+s2Comp+".' + g) MATCH (t:Twin) RETURN t.v AS v } RETURN v ORDER BY v", nil)
		if len(o.Errors) == 0 && !reflect.DeepEqual(o.Rows, [][]any{{"value-A"}, {"value-B"}}) {
			failures++
			t.Errorf("ISOLATION [composite-all] got %s", o.rowsJSON())
		}
	}

	// Caller mutation of returned values (internal executor and storage).
	for name, tag := range expect {
		exec := cypher.NewStorageExecutor(stores[name])
		for _, statement := range []string{
			"MATCH (t:Twin) RETURN t AS n, properties(t) AS p, t{.*} AS m, t.list AS l, t.nested AS x",
			"MATCH (t:Twin) RETURN t.v AS v, t.list AS l, t.nested AS x, t{.v, .list, .nested} AS m",
		} {
			res, err := exec.Execute(ctx, statement, nil)
			if err != nil {
				t.Fatalf("%s: %v", statement, err)
			}
			for _, row := range res.Rows {
				for _, value := range row {
					s2Mutate(value)
				}
			}
		}
		if reader := s2ProjectedReader(stores[name]); reader != nil {
			if node, err := reader.GetNodeProjected(ctx, s2TwinID, []string{"v", "list", "nested"}); err == nil && node != nil {
				s2Mutate(node)
			}
		}
		if node, err := stores[name].GetNode(ctx, s2TwinID); err == nil && node != nil {
			s2Mutate(node)
		}
		checks := []struct {
			where, statement, want string
		}{
			{"after-mutation-v", "MATCH (t:Twin) RETURN t.v AS v", "value-" + tag},
			{"after-mutation-list", "MATCH (t:Twin) RETURN t.list[0] AS v", "l-" + tag},
			{"after-mutation-nested", "MATCH (t:Twin) RETURN t.nested.x AS v", "m-" + tag},
		}
		for _, check := range checks {
			expectValue("internal-"+check.where, name, s2Internal(stores[name], check.statement, nil), check.want)
			expectValue("http-"+check.where, name, h.autocommit(name, admin, check.statement, nil), check.want)
			expectValue("bolt-"+check.where, name, adminB.autocommit(name, check.statement, nil), check.want)
		}
		for _, statement := range []string{
			"MATCH (t:Twin) RETURN t AS n, properties(t) AS p, t{.*} AS m, t.list AS l, t.nested AS x",
			"MATCH (t:Twin) RETURN t.v AS v, t.list AS l, t.nested AS x, t{.v, .list, .nested} AS m",
		} {
			for where, o := range map[string]s2Outcome{
				"internal-same-executor": func() s2Outcome {
					res, err := exec.Execute(ctx, statement, nil)
					if err != nil {
						return s2Outcome{Errors: []string{err.Error()}}
					}
					out := s2Outcome{}
					for _, row := range res.Rows {
						normalized, _ := s2Normalize(row).([]any)
						out.Rows = append(out.Rows, normalized)
					}
					return out
				}(),
				"internal-new-executor": s2Internal(stores[name], statement, nil),
				"http":                  h.autocommit(name, admin, statement, nil),
				"bolt":                  adminB.autocommit(name, statement, nil),
			} {
				if len(o.Errors) > 0 || len(o.Rows) != 1 || strings.Contains(o.rowsJSON(), "MUTATED") || strings.Contains(o.rowsJSON(), "injected") {
					failures++
					t.Errorf("ALIASING [%s repeat of mutated statement] database %s rows %s errors %v", where, name, o.rowsJSON(), o.Errors)
				}
			}
		}
		if reader := s2ProjectedReader(stores[name]); reader != nil {
			node, err := reader.GetNodeProjected(ctx, s2TwinID, []string{"v", "list", "nested"})
			if err != nil || node == nil || node.Properties["v"] != "value-"+tag || strings.Contains(fmt.Sprint(node.Properties), "MUTATED") {
				failures++
				t.Errorf("ALIASING [direct-projected after mutation] database %s got %v err %v", name, node, err)
			}
		}
		if node, err := stores[name].GetNode(ctx, s2TwinID); err != nil || strings.Contains(fmt.Sprint(node.Properties), "MUTATED") {
			failures++
			t.Errorf("ALIASING [full read after mutation] database %s got %v err %v", name, node, err)
		}
	}
	t.Logf("S2 isolation finished with %d findings", failures)
}

// s2Mutate changes every reachable string inside returned values.
func s2Mutate(value any) {
	switch v := value.(type) {
	case *storage.Node:
		if v == nil {
			return
		}
		for key, inner := range v.Properties {
			if _, isString := inner.(string); isString {
				v.Properties[key] = "MUTATED"
			} else {
				s2Mutate(inner)
			}
		}
		v.Properties["injected"] = "MUTATED"
	case map[string]any:
		for key, inner := range v {
			if _, isString := inner.(string); isString {
				v[key] = "MUTATED"
			} else {
				s2Mutate(inner)
			}
		}
		v["injected"] = "MUTATED"
	case []any:
		for i, inner := range v {
			if _, isString := inner.(string); isString {
				v[i] = "MUTATED"
			} else {
				s2Mutate(inner)
			}
		}
	case []string:
		for i := range v {
			v[i] = "MUTATED"
		}
	}
}
