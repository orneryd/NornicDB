package server

// Shared harness of the v1.5.2 open hunt (grounds H1, H3, H5, H6): a whole
// NornicDB server (database, HTTP router, Bolt listener) over a directory that
// survives a restart, with statement routes that return
// differential.Outcome values, and the lead line format.
//
// Nothing here changes product code. Every ground prints its leads as one line
// each: "HUNT_LEAD <json>", and its coverage as "HUNT_COVERAGE <json>".

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/differential"
)

// huntOptions are the server settings a path pair varies.
type huntOptions struct {
	AsyncWrites bool
	QueryCache  bool
	// TxMemoryMax is db.memory.transaction.max in bytes (0 = unlimited).
	TxMemoryMax int64
}

// huntServer is one whole server over dir.
type huntServer struct {
	dir     string
	options huntOptions
	db      *nornicdb.DB
	server  *Server
	auth    *auth.Authenticator
	token   string
	http    *httptest.Server
	bolt    *bolt.Server
	served  chan error
	driver  neo4jdriver.DriverWithContext
	closeMu sync.Mutex
	closed  bool
}

func huntOpen(t testing.TB, dir string, options huntOptions) *huntServer {
	t.Helper()
	h, err := huntOpenErr(t, dir, options)
	if err != nil {
		t.Fatalf("hunt: %v", err)
	}
	return h
}

// huntOpenErr opens a whole server over dir, or reports why it could not.
func huntOpenErr(t testing.TB, dir string, options huntOptions) (*huntServer, error) {
	t.Helper()
	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Memory.QueryCacheEnabled = options.QueryCache
	config.Database.AsyncWritesEnabled = options.AsyncWrites
	config.Memory.TransactionMax = options.TxMemoryMax
	db, err := nornicdb.Open(dir, config)
	if err != nil {
		return nil, fmt.Errorf("open database %s: %w", dir, err)
	}
	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-key-for-testing-only-32b"),
	}, storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system"))
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("authenticator: %w", err)
	}
	if _, err := authenticator.CreateUser(context.Background(), "admin", "password123", []auth.Role{auth.RoleAdmin}); err != nil && !errors.Is(err, auth.ErrUserExists) {
		_ = db.Close()
		return nil, fmt.Errorf("create user: %w", err)
	}
	serverConfig := DefaultConfig()
	serverConfig.Port = 0
	serverConfig.EmbeddingEnabled = false
	srv, err := New(db, authenticator, serverConfig)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("server: %w", err)
	}
	h := &huntServer{dir: dir, options: options, db: db, server: srv, auth: authenticator}
	tokenResp, _, err := authenticator.Authenticate(context.Background(), "admin", "password123", "127.0.0.1", "HuntAgent")
	if err != nil {
		h.Close(t)
		return nil, fmt.Errorf("token: %w", err)
	}
	h.token = "Bearer " + tokenResp.AccessToken
	h.http = httptest.NewServer(srv.buildRouter())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		h.Close(t)
		return nil, fmt.Errorf("listen: %w", err)
	}
	boltConfig := bolt.DefaultConfig()
	boltConfig.MaxConnections = 64
	h.bolt = bolt.NewWithDatabaseManager(boltConfig, nil, srv.dbManager)
	h.served = make(chan error, 1)
	go func() { h.served <- h.bolt.Serve(listener) }()
	h.driver, err = neo4jdriver.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4jdriver.NoAuth())
	if err != nil {
		h.Close(t)
		return nil, fmt.Errorf("driver: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.driver.VerifyConnectivity(ctx); err != nil {
		h.Close(t)
		return nil, fmt.Errorf("connectivity: %w", err)
	}
	return h, nil
}

// Close stops the server cleanly (driver, Bolt, HTTP, server, database).
func (h *huntServer) Close(t testing.TB) {
	h.closeMu.Lock()
	defer h.closeMu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	if h.driver != nil {
		_ = h.driver.Close(context.Background())
	}
	if h.bolt != nil {
		_ = h.bolt.Close()
		select {
		case <-h.served:
		case <-time.After(10 * time.Second):
			t.Logf("hunt: Bolt server of %s did not stop within 10s", h.dir)
		}
	}
	if h.http != nil {
		h.http.Close()
	}
	if h.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = h.server.Stop(ctx)
		cancel()
	}
	if h.db != nil {
		if err := h.db.Close(); err != nil {
			t.Logf("hunt: close database %s: %v", h.dir, err)
		}
	}
}

// huntRoute names a statement route.
type huntRoute string

const (
	routeBoltAuto     huntRoute = "bolt-auto"
	routeBoltExplicit huntRoute = "bolt-explicit"
	routeHTTPCommit   huntRoute = "http-commit"
)

// Run executes one statement through route with its own deadline. Values are
// returned in the typed Bolt form (huntBoltValue) for Bolt routes and in the
// protocol-neutral form (huntNeutral) for HTTP; compare HTTP only with a Bolt
// outcome passed through huntNeutralOutcome.
func (h *huntServer) Run(route huntRoute, query string, params map[string]any, timeout time.Duration) differential.Outcome {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	switch route {
	case routeHTTPCommit:
		return h.runHTTP(ctx, query, params)
	default:
		return h.runBolt(ctx, route == routeBoltExplicit, query, params)
	}
}

func (h *huntServer) runBolt(ctx context.Context, explicit bool, query string, params map[string]any) differential.Outcome {
	return huntBoltRun(ctx, h.driver, "nornic", explicit, query, params)
}

// huntBoltRun runs one statement through a Bolt driver (NornicDB or the
// Neo4j reference) and returns its typed outcome.
func huntBoltRun(ctx context.Context, driver neo4jdriver.DriverWithContext, database string, explicit bool, query string, params map[string]any) (outcome differential.Outcome) {
	session := driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: database})
	defer session.Close(context.Background())
	collect := func(result neo4jdriver.ResultWithContext) error {
		keys, err := result.Keys()
		if err != nil {
			return err
		}
		records, err := result.Collect(ctx)
		if err != nil {
			return err
		}
		outcome.Columns = keys
		outcome.Rows = make([][]any, len(records))
		for i, record := range records {
			row := make([]any, len(record.Values))
			for c, v := range record.Values {
				row[c] = huntBoltValue(v)
			}
			outcome.Rows[i] = row
		}
		return nil
	}
	var err error
	if explicit {
		var tx neo4jdriver.ExplicitTransaction
		if tx, err = session.BeginTransaction(ctx); err == nil {
			var result neo4jdriver.ResultWithContext
			if result, err = tx.Run(ctx, query, params); err == nil {
				err = collect(result)
			}
			if err == nil {
				err = tx.Commit(ctx)
			} else {
				_ = tx.Rollback(context.Background())
			}
		}
	} else {
		var result neo4jdriver.ResultWithContext
		if result, err = session.Run(ctx, query, params); err == nil {
			err = collect(result)
		}
	}
	if err != nil {
		return huntErrorOutcome(err)
	}
	return outcome
}

func huntErrorOutcome(err error) differential.Outcome {
	var status *neo4jdriver.Neo4jError
	if errors.As(err, &status) {
		return differential.Outcome{Code: status.Code, Message: status.Msg}
	}
	if errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "deadline") || strings.Contains(err.Error(), "timeout") {
		return differential.Outcome{Code: "client:timeout", Message: err.Error()}
	}
	return differential.Outcome{Code: "client:" + huntFirstLine(err.Error()), Message: err.Error()}
}

func huntFirstLine(text string) string {
	if i := strings.IndexAny(text, "\n:"); i > 0 {
		return text[:i]
	}
	return text
}

func (h *huntServer) runHTTP(ctx context.Context, query string, params map[string]any) differential.Outcome {
	statement := map[string]any{"statement": query}
	if len(params) > 0 {
		statement["parameters"] = huntJSONParams(params)
	}
	body, err := json.Marshal(map[string]any{"statements": []any{statement}})
	if err != nil {
		return differential.Outcome{Code: "client:marshal", Message: err.Error()}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, h.http.URL+"/db/nornic/tx/commit", strings.NewReader(string(body)))
	if err != nil {
		return differential.Outcome{Code: "client:request", Message: err.Error()}
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", h.token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return huntErrorOutcome(err)
	}
	defer response.Body.Close()
	var decoded struct {
		Results []struct {
			Columns []string `json:"columns"`
			Data    []struct {
				Row []any `json:"row"`
			} `json:"data"`
		} `json:"results"`
		Errors []struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"errors"`
	}
	decoder := json.NewDecoder(response.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		return differential.Outcome{Code: fmt.Sprintf("client:http %d undecodable", response.StatusCode), Message: err.Error()}
	}
	if len(decoded.Errors) > 0 {
		return differential.Outcome{Code: decoded.Errors[0].Code, Message: decoded.Errors[0].Message}
	}
	if len(decoded.Results) != 1 {
		return differential.Outcome{Code: fmt.Sprintf("client:http %d results=%d", response.StatusCode, len(decoded.Results))}
	}
	outcome := differential.Outcome{Columns: decoded.Results[0].Columns, Rows: make([][]any, len(decoded.Results[0].Data))}
	for i, data := range decoded.Results[0].Data {
		row := make([]any, len(data.Row))
		for c, v := range data.Row {
			row[c] = huntNeutral(v)
		}
		outcome.Rows[i] = row
	}
	return outcome
}

// huntJSONParams converts Bolt parameter values to their JSON form.
func huntJSONParams(params map[string]any) map[string]any {
	out := make(map[string]any, len(params))
	for k, v := range params {
		out[k] = v
	}
	return out
}

// huntBoltValue writes a Bolt value so that equal Cypher values encode equally
// and values of different types never do (as the repository's differential
// harness does): every scalar carries its type, entities lose their internal
// identities, and a node's labels are sorted.
func huntBoltValue(value any) any {
	switch typed := value.(type) {
	case nil, bool, string:
		return typed
	case int64:
		return map[string]string{"Integer": strconv.FormatInt(typed, 10)}
	case float64:
		if math.IsNaN(typed) {
			return map[string]string{"Float": "NaN"}
		}
		return map[string]string{"Float": strconv.FormatFloat(typed, 'g', -1, 64)}
	case []byte:
		return map[string]string{"ByteArray": fmt.Sprintf("%x", typed)}
	case []any:
		items := make([]any, len(typed))
		for i, item := range typed {
			items[i] = huntBoltValue(item)
		}
		return items
	case map[string]any:
		result := make(map[string]any, len(typed))
		for k, item := range typed {
			result[k] = huntBoltValue(item)
		}
		return result
	case dbtype.Node:
		labels := append([]string(nil), typed.Labels...)
		sort.Strings(labels)
		return map[string]any{"Node": labels, "properties": huntBoltValue(typed.Props)}
	case dbtype.Relationship:
		return map[string]any{"Relationship": typed.Type, "properties": huntBoltValue(typed.Props)}
	case dbtype.Path:
		nodes := make([]any, len(typed.Nodes))
		for i, node := range typed.Nodes {
			nodes[i] = huntBoltValue(node)
		}
		rels := make([]any, len(typed.Relationships))
		for i, rel := range typed.Relationships {
			rels[i] = huntBoltValue(rel)
		}
		return map[string]any{"Path": nodes, "relationships": rels}
	case dbtype.Date:
		return map[string]string{"Date": typed.Time().Format("2006-01-02")}
	case dbtype.LocalTime:
		return map[string]string{"LocalTime": typed.Time().Format("15:04:05.999999999")}
	case dbtype.Time:
		return map[string]string{"Time": typed.Time().Format("15:04:05.999999999Z07:00")}
	case dbtype.LocalDateTime:
		return map[string]string{"LocalDateTime": typed.Time().Format("2006-01-02T15:04:05.999999999")}
	case time.Time:
		return map[string]string{"DateTime": typed.Format(time.RFC3339Nano) + "[" + typed.Location().String() + "]"}
	case dbtype.Duration:
		return map[string]string{"Duration": fmt.Sprintf("%dM %dD %dS %dN", typed.Months, typed.Days, typed.Seconds, typed.Nanos)}
	case dbtype.Point2D:
		return map[string]string{"Point": fmt.Sprintf("%d %v %v", typed.SpatialRefId, typed.X, typed.Y)}
	case dbtype.Point3D:
		return map[string]string{"Point": fmt.Sprintf("%d %v %v %v", typed.SpatialRefId, typed.X, typed.Y, typed.Z)}
	}
	return map[string]string{fmt.Sprintf("%T", value): fmt.Sprintf("%v", value)}
}

// huntNeutral is the protocol-neutral form used only to compare Bolt with
// HTTP: numbers by value (integers and floats alike), entities by their
// properties (HTTP's row format carries nothing else), temporal values and
// points by their text, maps and lists recursively.
func huntNeutral(value any) any {
	switch typed := value.(type) {
	case nil, bool, string:
		return typed
	case json.Number:
		f, err := typed.Float64()
		if err != nil {
			return typed.String()
		}
		return map[string]string{"Number": strconv.FormatFloat(f, 'g', -1, 64)}
	case float64:
		return map[string]string{"Number": strconv.FormatFloat(typed, 'g', -1, 64)}
	case []any:
		items := make([]any, len(typed))
		for i, item := range typed {
			items[i] = huntNeutral(item)
		}
		return items
	case map[string]any:
		result := make(map[string]any, len(typed))
		for k, item := range typed {
			result[k] = huntNeutral(item)
		}
		return result
	case map[string]string:
		// a typed Bolt scalar from huntBoltValue
		for kind, text := range typed {
			switch kind {
			case "Integer", "Float":
				if f, err := strconv.ParseFloat(text, 64); err == nil {
					return map[string]string{"Number": strconv.FormatFloat(f, 'g', -1, 64)}
				}
				return map[string]string{"Number": text}
			default:
				return text
			}
		}
	}
	return fmt.Sprintf("%v", value)
}

// huntNeutralOutcome converts a Bolt outcome to the protocol-neutral form.
// Entities: a typed Bolt node/relationship becomes its property map, a path
// the alternating list of property maps, as HTTP's row format writes them.
func huntNeutralOutcome(outcome differential.Outcome) differential.Outcome {
	if outcome.Failed() {
		return outcome
	}
	out := differential.Outcome{Columns: outcome.Columns, Rows: make([][]any, len(outcome.Rows))}
	for i, row := range outcome.Rows {
		values := make([]any, len(row))
		for c, v := range row {
			values[c] = huntNeutral(huntEntityToRowFormat(v))
		}
		out.Rows[i] = values
	}
	return out
}

func huntEntityToRowFormat(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		if _, ok := typed["Node"]; ok {
			return huntEntityToRowFormat(typed["properties"])
		}
		if _, ok := typed["Relationship"]; ok {
			return huntEntityToRowFormat(typed["properties"])
		}
		if nodes, ok := typed["Path"].([]any); ok {
			rels, _ := typed["relationships"].([]any)
			var alternating []any
			for i, node := range nodes {
				alternating = append(alternating, huntEntityToRowFormat(node))
				if i < len(rels) {
					alternating = append(alternating, huntEntityToRowFormat(rels[i]))
				}
			}
			return alternating
		}
		result := make(map[string]any, len(typed))
		for k, v := range typed {
			result[k] = huntEntityToRowFormat(v)
		}
		return result
	case []any:
		items := make([]any, len(typed))
		for i, v := range typed {
			items[i] = huntEntityToRowFormat(v)
		}
		return items
	}
	return value
}

// huntReset empties the default database: every node and relationship,
// every index and constraint. It reports whether the database is empty
// afterwards; a caller reopens a fresh server when it is not.
func (h *huntServer) huntReset() bool { return huntResetWith(h.Run) }

// huntRunner runs one statement (huntServer.Run or huntReference.Run).
type huntRunner func(route huntRoute, query string, params map[string]any, timeout time.Duration) differential.Outcome

// huntResetWith empties a database through run.
func huntResetWith(run huntRunner) bool {
	const timeout = 60 * time.Second
	h := struct{ Run huntRunner }{run}
	if h.Run(routeBoltAuto, "MATCH (n) DETACH DELETE n", nil, timeout).Failed() {
		return false
	}
	for _, kind := range []string{"CONSTRAINTS", "INDEXES"} {
		listed := h.Run(routeBoltAuto, "SHOW "+kind+" YIELD name, type RETURN name, type", nil, timeout)
		if listed.Failed() {
			return false
		}
		for _, row := range listed.Rows {
			name, _ := row[0].(string)
			if kindOf, _ := row[1].(string); name == "" || kindOf == "LOOKUP" {
				// the token lookup indexes are part of an empty Neo4j database
				continue
			}
			drop := "DROP CONSTRAINT `" + strings.ReplaceAll(name, "`", "``") + "` IF EXISTS"
			if kind == "INDEXES" {
				drop = "DROP INDEX `" + strings.ReplaceAll(name, "`", "``") + "` IF EXISTS"
			}
			_ = h.Run(routeBoltAuto, drop, nil, timeout)
		}
	}
	count := h.Run(routeBoltAuto, "MATCH (n) RETURN count(n) AS c", nil, timeout)
	return !count.Failed() && len(count.Rows) == 1 && fmt.Sprint(count.Rows[0][0]) == fmt.Sprint(map[string]string{"Integer": "0"})
}

// huntGraphState reads the whole default graph as two multisets (nodes,
// relationships) in typed Bolt form.
func (h *huntServer) huntGraphState() []differential.Outcome { return huntGraphStateWith(h.Run) }

func huntGraphStateWith(run huntRunner) []differential.Outcome {
	const timeout = 60 * time.Second
	h := struct{ Run huntRunner }{run}
	return []differential.Outcome{
		h.Run(routeBoltAuto, "MATCH (n) RETURN labels(n) AS labels, properties(n) AS properties", nil, timeout),
		h.Run(routeBoltAuto, "MATCH (a)-[r]->(b) RETURN type(r) AS type, properties(r) AS properties, labels(a) AS from, properties(a) AS fromProperties, labels(b) AS to, properties(b) AS toProperties", nil, timeout),
	}
}

// huntLead prints one lead line. Values are truncated to keep logs readable;
// the statement and both error codes are always complete.
func huntLead(t testing.TB, lead map[string]any) {
	t.Helper()
	for _, key := range []string{"a", "b"} {
		if outcome, ok := lead[key].(differential.Outcome); ok {
			lead[key] = huntTrim(outcome)
		}
	}
	data, err := json.Marshal(lead)
	if err != nil {
		data = []byte(fmt.Sprintf("%q", fmt.Sprint(lead)))
	}
	fmt.Fprintf(os.Stdout, "HUNT_LEAD %s\n", data)
}

func huntCoverage(t testing.TB, coverage map[string]any) {
	t.Helper()
	data, _ := json.Marshal(coverage)
	fmt.Fprintf(os.Stdout, "HUNT_COVERAGE %s\n", data)
}

func huntTrim(outcome differential.Outcome) map[string]any {
	rows := outcome.Rows
	truncated := false
	if len(rows) > 20 {
		rows, truncated = rows[:20], true
	}
	text, _ := json.Marshal(rows)
	if len(text) > 4000 {
		text, truncated = append(text[:4000:4000], []byte("...")...), true
	}
	return map[string]any{"code": outcome.Code, "message": outcome.Message, "columns": outcome.Columns, "rowCount": len(outcome.Rows), "rows": string(text), "truncated": truncated}
}

// huntShard parses "i/n" (1-based) and reports whether index belongs to it.
func huntShard(spec string, index int) bool {
	if spec == "" {
		return true
	}
	var i, n int
	if _, err := fmt.Sscanf(spec, "%d/%d", &i, &n); err != nil || n <= 0 || i < 1 || i > n {
		return true
	}
	return index%n == i-1
}

var huntWriteRE = regexp.MustCompile(`(?i)\b(CREATE|MERGE|SET|DELETE|REMOVE|DROP|ALTER|GRANT|DENY|REVOKE|LOAD\s+CSV|FOREACH)\b|CALL\s+(apoc\.(create|merge|refactor|periodic)|db\.(create|index\.fulltext\.create|clearQueryCaches))`)

// huntWrites reports whether a statement may write (conservative: any
// write keyword or write procedure counts).
func huntWrites(query string) bool { return huntWriteRE.MatchString(query) || differential.Writes(query) }

func huntEnv(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func huntEnvInt(name string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return v
	}
	return fallback
}
