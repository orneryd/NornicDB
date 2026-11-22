package server

// Test-only A/B harness for the 18b36a85d -> 1527cc83b latency question.
//
// It starts the real HTTP server and a Bolt listener wired exactly as
// cmd/nornicdb/main.go wires them (authenticated Bolt, the HTTP server's
// per-database executor options, access, procedure-grant and fine-grained
// admission), opens the application's database "library" as the default
// database with the application's durability settings, and measures over the
// wire with the Neo4j Go driver (Bolt) and plain HTTP requests (one TCP
// connection per request, Basic auth, as the application's probe does):
//
//   - BenchmarkRegr18b1527Small: small statements, auto-commit over Bolt and
//     HTTP, and an explicit Bolt read transaction as control.
//   - BenchmarkRegr18b1527Subjects: the application's decisions.SUBJECTS read
//     (row 44) at 8,000 subjects, workaround ("with") and plain ("joined")
//     forms, over an explicit read, an auto-commit and a write transaction.
//   - BenchmarkRegr18b1527Writers: the application's three-statement small
//     write transaction with 1, 2 and 4 concurrent writers.
//
// Every sub-benchmark reports ns/op (mean), p50-ms and p95-ms per operation;
// the writer benchmarks also report tx/s.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	neo4j "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

const regrDatabase = "library"

type regrEnv struct {
	driver neo4j.DriverWithContext
	http   *http.Client
	url    string
	basic  string
}

func regrStart(b *testing.B) *regrEnv {
	b.Helper()
	s, authenticator := setupTestServerWithDatabaseConfig(b, func(c *nornicdb.Config) {
		// The application's deployment settings that bear on these paths.
		c.Database.DefaultDatabase = regrDatabase
		c.Database.AsyncWritesEnabled = false
		c.Database.StrictDurability = true
		c.Database.WALSyncMode = "immediate"
		c.Database.MVCCRetentionMaxVersions = 100
	}, nil)

	// Bolt wiring as cmd/nornicdb/main.go (identical on both builds).
	cfg := bolt.DefaultConfig()
	boltAuth := bolt.NewAuthenticatorAdapter(authenticator)
	boltAuth.SetGetEffectivePermissions(s.GetEffectivePermissions)
	cfg.SetRequestAuthenticator(boltAuth)
	cfg.RequireAuth = true
	cfg.DatabaseExecutorOptions = s.DatabaseScopedExecutorOptions
	boltServer := bolt.NewWithDatabaseManager(cfg, nil, s.GetDatabaseManager())
	boltServer.SetMemoryPolicy(s.db.MemoryPolicy())
	s.SetConnectionLister(boltServer.ConnectionListings)
	tracker := multidb.NewConnectionTracker()
	boltServer.SetDatabaseConnectionAdmission(s.GetDatabaseManager().ResolveConnectionLimitOwner, func(databaseName string) error {
		return tracker.TryIncrementConnection(s.GetDatabaseManager(), databaseName)
	}, tracker.DecrementConnection)
	boltServer.SetDatabaseAccessModeResolver(s.GetDatabaseAccessModeForRoles)
	boltServer.SetResolvedAccessResolver(s.GetResolvedAccessForRoles)
	boltServer.SetProcedureGrantResolver(s.ProcedureGrantChecker)
	boltServer.SetFineGrainedAdmission(s.FineGrainedAdmission())

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- boltServer.Serve(listener) }()
	b.Cleanup(func() { _ = boltServer.Close(); <-served })

	driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.BasicAuth("admin", "password123", ""))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = driver.Close(context.Background()) })

	web := httptest.NewServer(s.buildRouter())
	b.Cleanup(web.Close)
	env := &regrEnv{
		driver: driver,
		http:   &http.Client{Timeout: 5 * time.Minute, Transport: &http.Transport{DisableKeepAlives: true}},
		url:    web.URL,
		basic:  "Basic " + base64.StdEncoding.EncodeToString([]byte("admin:password123")),
	}
	// The default database must be the application's.
	if rows, err := env.httpAuto(context.Background(), "RETURN 1 AS one", nil); err != nil || rows != 1 {
		b.Fatalf("library database: rows=%d err=%v", rows, err)
	}
	return env
}

// regrSchema is the subset of the application's schema these statements use.
var regrSchema = []string{
	"CREATE CONSTRAINT pd_graph_id IF NOT EXISTS FOR (n:PDGraphNode) REQUIRE n.id IS UNIQUE",
	"CREATE CONSTRAINT pd_document_id IF NOT EXISTS FOR (n:PDDocument) REQUIRE n.id IS UNIQUE",
	"CREATE CONSTRAINT pd_source_id IF NOT EXISTS FOR (n:PDSearchSource) REQUIRE n.id IS UNIQUE",
	"CREATE CONSTRAINT pd_collection_id IF NOT EXISTS FOR (n:PDCollection) REQUIRE n.id IS UNIQUE",
	"CREATE CONSTRAINT pd_owed_id IF NOT EXISTS FOR (n:PDOwed) REQUIRE n.id IS UNIQUE",
	"CREATE CONSTRAINT pd_identity_anchor_id IF NOT EXISTS FOR (n:PDIdentityAnchor) REQUIRE n.id IS UNIQUE",
	"CREATE CONSTRAINT pd_relation_id IF NOT EXISTS FOR ()-[r:PD_RELATION]-() REQUIRE r.id IS UNIQUE",
	"CREATE INDEX pd_graph_document IF NOT EXISTS FOR (n:PDGraphNode) ON (n.document_id)",
	"CREATE INDEX pd_graph_kind IF NOT EXISTS FOR (n:PDGraphNode) ON (n.kind)",
	"CREATE INDEX pd_identity_survivor IF NOT EXISTS FOR (n:PDIdentity) ON (n.survivor)",
	"CREATE INDEX pd_identity_anchor_party IF NOT EXISTS FOR (n:PDIdentityAnchor) ON (n.party)",
	"CREATE INDEX pd_owed_kind IF NOT EXISTS FOR (n:PDOwed) ON (n.kind)",
	"CREATE INDEX pd_owed_since IF NOT EXISTS FOR (n:PDOwed) ON (n.since)",
}

func (e *regrEnv) setup(b *testing.B, statements ...string) {
	b.Helper()
	for _, statement := range statements {
		if _, err := e.boltAuto(context.Background(), statement, nil); err != nil {
			b.Fatalf("%v\n%s", err, statement)
		}
	}
}

func (e *regrEnv) fill(b *testing.B, count int, statement string) {
	b.Helper()
	for from := 0; from < count; from += 2000 {
		to := min(from+2000, count) - 1
		if _, err := e.boltWrite(context.Background(), statement, map[string]any{"from": int64(from), "to": int64(to)}); err != nil {
			b.Fatalf("%v\n%s", err, statement)
		}
	}
}

// boltAuto is one auto-commit statement in a session of its own.
func (e *regrEnv) boltAuto(ctx context.Context, query string, params map[string]any) (int, error) {
	session := e.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: regrDatabase})
	defer session.Close(ctx)
	result, err := session.Run(ctx, query, params)
	if err != nil {
		return 0, err
	}
	records, err := result.Collect(ctx)
	return len(records), err
}

func (e *regrEnv) boltTx(ctx context.Context, mode neo4j.AccessMode, queries []string, params map[string]any) (int, error) {
	session := e.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: regrDatabase, AccessMode: mode})
	defer session.Close(ctx)
	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return 0, err
	}
	rows := 0
	for _, query := range queries {
		result, err := tx.Run(ctx, query, params)
		if err == nil {
			var records []*neo4j.Record
			records, err = result.Collect(ctx)
			rows += len(records)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return 0, err
		}
	}
	return rows, tx.Commit(ctx)
}

// boltRead is an explicit Bolt read transaction.
func (e *regrEnv) boltRead(ctx context.Context, query string, params map[string]any) (int, error) {
	return e.boltTx(ctx, neo4j.AccessModeRead, []string{query}, params)
}

// boltWrite is an explicit Bolt write transaction, committed.
func (e *regrEnv) boltWrite(ctx context.Context, query string, params map[string]any) (int, error) {
	return e.boltTx(ctx, neo4j.AccessModeWrite, []string{query}, params)
}

// httpAuto is one HTTP tx/commit request on a new connection.
func (e *regrEnv) httpAuto(ctx context.Context, query string, params map[string]any) (int, error) {
	body, err := json.Marshal(map[string]any{"statements": []map[string]any{{"statement": query, "parameters": params}}})
	if err != nil {
		return 0, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url+"/db/"+regrDatabase+"/tx/commit", strings.NewReader(string(body)))
	if err != nil {
		return 0, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", e.basic)
	response, err := e.http.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, err
	}
	var decoded struct {
		Results []struct {
			Data []json.RawMessage `json:"data"`
		} `json:"results"`
		Errors []json.RawMessage `json:"errors"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return 0, fmt.Errorf("%v: %.300s", err, data)
	}
	if len(decoded.Errors) > 0 {
		return 0, fmt.Errorf("%s", decoded.Errors[0])
	}
	if len(decoded.Results) == 0 {
		return 0, nil
	}
	return len(decoded.Results[0].Data), nil
}

type regrRunner func(context.Context, string, map[string]any) (int, error)

func regrTransports(e *regrEnv) []struct {
	name string
	run  regrRunner
} {
	return []struct {
		name string
		run  regrRunner
	}{{"bolt_auto", e.boltAuto}, {"http_auto", e.httpAuto}, {"bolt_read_tx", e.boltRead}}
}

// regrLatencies reports p50 and p95 of the per-operation times.
func regrLatencies(b *testing.B, times []time.Duration) {
	if len(times) == 0 {
		return
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	b.ReportMetric(ms(times[len(times)/2]), "p50-ms")
	b.ReportMetric(ms(times[len(times)*95/100]), "p95-ms")
}

// regrMeasure runs one statement b.N times after a warm-up, checking its rows.
func regrMeasure(b *testing.B, warm int, run regrRunner, query string, params func(int) map[string]any, rows int) {
	ctx := context.Background()
	for i := range warm {
		got, err := run(ctx, query, params(i))
		if err != nil {
			b.Fatalf("%v\n%.400s", err, query)
		}
		if rows >= 0 && got != rows {
			b.Fatalf("rows %d, want %d\n%.400s", got, rows, query)
		}
	}
	times := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for i := range b.N {
		began := time.Now()
		if _, err := run(ctx, query, params(warm+i)); err != nil {
			b.Fatal(err)
		}
		times = append(times, time.Since(began))
	}
	b.StopTimer()
	regrLatencies(b, times)
}

// regrViewTextRange is the application's view_text_range!("s", "$start", "$end").
const regrViewTextRange = "{start: head(apoc.coll.sort([at IN [i IN range(0, size(coalesce(s.view_starts, [])) - 1) WHERE s.view_starts[i] < $end AND s.view_ends[i] > $start] | head([v IN [s.view_source_starts[at]] WHERE s.view_library[at]] + [s.view_source_starts[at] + last(apoc.coll.sort([$start - s.view_starts[at], 0]))])])), " +
	"end: last(apoc.coll.sort([at IN [i IN range(0, size(coalesce(s.view_starts, [])) - 1) WHERE s.view_starts[i] < $end AND s.view_ends[i] > $start] | head([v IN [s.view_source_ends[at]] WHERE s.view_library[at]] + [s.view_source_starts[at] + head(apoc.coll.sort([$end, s.view_ends[at]])) - s.view_starts[at]])]))}"

// regrPieces is the application's pieces(count): pieces of ten, alternating
// ordinary and viewed.
func regrPieces(count int) map[string]any {
	var starts, ends, sourceStarts, sourceEnds, library []any
	view, text := 0, 0
	for i := range count {
		viewed := i%2 == 1
		long := 10
		if viewed {
			long = 30
		}
		starts = append(starts, int64(view))
		ends = append(ends, int64(view+10))
		sourceStarts = append(sourceStarts, int64(text))
		sourceEnds = append(sourceEnds, int64(text+long))
		library = append(library, viewed)
		view += 11
		text += long + 1
	}
	return map[string]any{"view_starts": starts, "view_ends": ends, "view_source_starts": sourceStarts,
		"view_source_ends": sourceEnds, "view_library": library}
}

const regrSubjectFill = "UNWIND range($from, $to) AS i CREATE (:PDGraphNode:PDEntity {id: 'n' + toString(i), " +
	"document_id: 'd', source_id: 's', origin: 'automatic', name: 'N'})"

func regrTwenty() []any {
	ids := make([]any, 0, 22)
	for i := range 20 {
		ids = append(ids, fmt.Sprintf("n%d", i))
	}
	return ids
}

func BenchmarkRegr18b1527Small(b *testing.B) {
	e := regrStart(b)
	e.setup(b, regrSchema...)
	e.fill(b, 500, regrSubjectFill)
	if _, err := e.boltWrite(context.Background(),
		"CREATE (s:PDSearchSource {id: 's'}) SET s += $pieces CREATE (:PDSearchSource {id: 'none'})",
		map[string]any{"pieces": regrPieces(200)}); err != nil {
		b.Fatal(err)
	}
	fixed := func(p map[string]any) func(int) map[string]any { return func(int) map[string]any { return p } }
	statements := []struct {
		name   string
		query  string
		params func(int) map[string]any
		rows   int
	}{
		// Row 54's "view_text_range! on parameters 12..14", product form, at 200 pieces.
		{"view_range", "MATCH (s:PDSearchSource {id: 's'}) RETURN " + regrViewTextRange + " AS found",
			fixed(map[string]any{"start": int64(12), "end": int64(14)}), 1},
		// decisions.TARGETS over 20 subjects.
		{"targets", regrTargetsStatement, fixed(map[string]any{"targets": regrTwenty()}), 20},
		// One keyed read, the same parameters every time.
		{"keyed", "MATCH (n:PDGraphNode {id: $id}) RETURN n.id AS id, n.name AS name",
			fixed(map[string]any{"id": "n7"}), 1},
		// One keyed read, a different key every time.
		{"keyed_vary", "MATCH (n:PDGraphNode {id: $id}) RETURN n.id AS id, n.name AS name",
			func(i int) map[string]any { return map[string]any{"id": fmt.Sprintf("n%d", i%500)} }, 1},
	}
	for _, statement := range statements {
		for _, transport := range regrTransports(e) {
			b.Run(statement.name+"/"+transport.name, func(b *testing.B) {
				regrMeasure(b, 20, transport.run, statement.query, statement.params, statement.rows)
			})
		}
	}
}

func BenchmarkRegr18b1527Subjects(b *testing.B) {
	e := regrStart(b)
	e.setup(b, regrSchema...)
	e.fill(b, 8000, regrSubjectFill)
	if _, err := e.boltWrite(context.Background(),
		"CREATE (:PDGraphNode:PDDocument {id: 'd'}) "+
			"CREATE (:PDGraphNode:PDEntity {id: 'elsewhere', document_id: 'e', source_id: 's', origin: 'automatic'})", nil); err != nil {
		b.Fatal(err)
	}
	if strings.Count(regrSubjectsStatement, "WITH n\nWHERE n.origin") != 1 {
		b.Fatal("joined form: anchor not found once")
	}
	joined := strings.Replace(regrSubjectsStatement, "WITH n\nWHERE n.origin", "WHERE n.origin", 1)
	targets := append(regrTwenty(), "absent", "elsewhere")
	params := map[string]any{"targets": targets, "document_id": "d", "source_id": "s", "current": true, "items": []any{}}
	transports := []struct {
		name string
		run  regrRunner
	}{{"bolt_read_tx", e.boltRead}, {"bolt_auto", e.boltAuto}, {"bolt_write_tx", e.boltWrite}}
	for _, form := range []struct{ name, query string }{{"with", regrSubjectsStatement}, {"joined", joined}} {
		for _, transport := range transports {
			b.Run(form.name+"/"+transport.name, func(b *testing.B) {
				regrMeasure(b, 3, transport.run, form.query, func(int) map[string]any { return params }, 1)
			})
		}
	}
}

// regrSmallWrites is the application's three keyed writes of one small transaction.
var regrSmallWrites = []string{
	"MATCH (n:PDDocument {id: $id}) SET n.summary_status = $token RETURN n.id AS id",
	"MERGE (o:PDOwed {id: 'holdings:' + $id}) ON CREATE SET o.kind = 'holdings', o.key = $id, o.since = $token SET o.token = $token RETURN o.id AS id",
	"MATCH (n:PDDocument {id: $id}) MATCH (h:PDCollection {id: 'c'}) MERGE (n)-[r:MEMBER_OF]->(h) RETURN type(r) AS id",
}

func BenchmarkRegr18b1527Writers(b *testing.B) {
	e := regrStart(b)
	e.setup(b, regrSchema...)
	e.setup(b, "CREATE (:PDCollection {id:'c'})",
		"UNWIND range(0, 15) AS w UNWIND range(0, 49) AS d CREATE (:PDDocument:PDGraphNode {id: 'd' + toString(w) + '-' + toString(d)})")
	e.fill(b, 40000, "UNWIND range($from, $to) AS i CREATE (:PDGraphNode:PDEntity {id: 'e' + toString(i), name: 'entity ' + toString(i)})")
	ctx := context.Background()
	var turns [16]atomic.Int64
	write := func(writer int) error {
		turn := turns[writer].Add(1) - 1
		params := map[string]any{"id": fmt.Sprintf("d%d-%d", writer, turn%50), "token": fmt.Sprint(turn)}
		_, err := e.boltTx(ctx, neo4j.AccessModeWrite, regrSmallWrites, params)
		return err
	}
	for _, writers := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("writers_%d", writers), func(b *testing.B) {
			for w := range writers {
				for range 10 {
					if err := write(w); err != nil {
						b.Fatal(err)
					}
				}
			}
			var next, refused atomic.Int64
			var mu sync.Mutex
			times := make([]time.Duration, 0, b.N)
			var wg sync.WaitGroup
			b.ResetTimer()
			began := time.Now()
			for w := range writers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					local := make([]time.Duration, 0, b.N/writers+1)
					for next.Add(1) <= int64(b.N) {
						started := time.Now()
						if err := write(w); err != nil {
							refused.Add(1)
						}
						local = append(local, time.Since(started))
					}
					mu.Lock()
					times = append(times, local...)
					mu.Unlock()
				}()
			}
			wg.Wait()
			took := time.Since(began)
			b.StopTimer()
			b.ReportMetric(float64(b.N)/took.Seconds(), "tx/s")
			b.ReportMetric(float64(refused.Load()), "refused")
			regrLatencies(b, times)
		})
	}
}
