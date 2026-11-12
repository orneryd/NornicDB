package server

// Author: hnsw-stall implementation lane (Claude Opus 5.5), 9 Nov 2026.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/require"
)

// A vector index build ends by bringing the new graph up to date with every
// vector changed while it was built. These tests change thousands of vectors
// while a build is held at its start, so that this catch-up takes many
// seconds, and then commit and search while it runs.

// stallPrompt is the longest a COMMIT, a write or a query may take while the
// graph catches up. The catch-up itself is required to last longer.
const stallPrompt = 3 * time.Second

// stallOperation is when a client gives up on one operation. It is far above
// the prompt limit, so that an operation the database holds is measured and
// reported, not cut short.
const stallOperation = 5 * time.Minute

// stallJoin is how long a goroutine may take to end once what it waits on has
// been closed.
const stallJoin = 30 * time.Second

func stallEnvInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

func stallEnv(t *testing.T) {
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	// Each graph insert explores more of the graph, so a catch-up of a few
	// thousand changes lasts seconds; construction is parallel, the catch-up
	// one change at a time.
	t.Setenv("NORNICDB_VECTOR_HNSW_EF_CONSTRUCTION", strconv.Itoa(stallEnvInt("HNSW_STALL_EF_CONSTRUCTION", 800)))
	t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "180s")
}

// hnswStallLog follows the operator log of the search service it observes,
// injected with that service's SetLogger (never the process-wide default
// logger). It can hold the next graph construction at its start, and records
// when a catch-up starts and when a graph is published.
type hnswStallLog struct {
	mu      sync.Mutex
	hold    chan struct{} // armed: the next construction waits on it
	held    chan struct{} // closed when an armed construction is held
	holding chan struct{} // the hold a held construction waits on
	events  map[string]time.Time
	// pending is the catch-up's progress log: changes still to apply.
	pending []stallPending
}

type stallPending struct {
	at    time.Time
	count int
}

func installHNSWStallLog(t *testing.T) *hnswStallLog {
	h := &hnswStallLog{events: map[string]time.Time{}}
	t.Cleanup(h.release)
	return h
}

// observe makes h the operator log of db's search service, for what that
// service logs from now on. A construction h holds is released before db is
// closed, so a failed test cannot leave it held.
func (h *hnswStallLog) observe(t *testing.T, db *nornicdb.DB) {
	t.Helper()
	svc, err := db.GetOrCreateSearchService("nornic", db.GetStorage())
	require.NoError(t, err)
	svc.SetLogger(slog.New(fixtureowner.RawLogHandler(h.Handle)))
	t.Cleanup(h.release)
}

func (h *hnswStallLog) Handle(_ context.Context, r slog.Record) error {
	var kind string
	if rest, ok := strings.CutPrefix(r.Message, "[HNSW] Catching up: "); ok {
		if count, err := strconv.Atoi(strings.Fields(rest)[0]); err == nil {
			h.mu.Lock()
			h.pending = append(h.pending, stallPending{at: time.Now(), count: count})
			h.mu.Unlock()
		}
		return nil
	}
	switch {
	case strings.Contains(r.Message, "[HNSW] Building from"):
		kind = "construction"
	case strings.Contains(r.Message, "[HNSW] Applying"):
		kind = "catch-up"
	case strings.Contains(r.Message, "HNSW index created"), strings.Contains(r.Message, "HNSW index rebuilt"):
		kind = "published"
	default:
		return nil
	}
	h.mu.Lock()
	if _, seen := h.events[kind]; !seen {
		h.events[kind] = time.Now()
	}
	hold, held := h.hold, h.held
	if kind == "construction" && hold != nil {
		h.hold, h.held, h.holding = nil, nil, hold
	}
	h.mu.Unlock()
	if kind == "construction" && hold != nil {
		close(held)
		<-hold
	}
	return nil
}

// holdNextConstruction holds the next graph construction at its start;
// vector changes made while it is held are the ones its catch-up applies.
func (h *hnswStallLog) holdNextConstruction() <-chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = map[string]time.Time{}
	h.pending = nil
	h.hold, h.held = make(chan struct{}), make(chan struct{})
	return h.held
}

// release lets a held construction continue, or disarms the hold.
func (h *hnswStallLog) release() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, hold := range []chan struct{}{h.hold, h.holding} {
		if hold != nil {
			close(hold)
		}
	}
	h.hold, h.held, h.holding = nil, nil, nil
}

// reset forgets the events seen so far.
func (h *hnswStallLog) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = map[string]time.Time{}
	h.pending = nil
}

// pendingBetween is the catch-up's progress log between from and to.
func (h *hnswStallLog) pendingBetween(from, to time.Time) []stallPending {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []stallPending
	for _, p := range h.pending {
		if !p.at.Before(from) && !p.at.After(to) {
			out = append(out, p)
		}
	}
	return out
}

func (h *hnswStallLog) at(kind string) (time.Time, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	at, ok := h.events[kind]
	return at, ok
}

// slowWatch logs where slow operations wait, at most three times per test.
type slowWatch struct {
	t     *testing.T
	dumps atomic.Int32
}

// run runs op. If op is still running after half the prompt limit, it logs
// the goroutines that are blocked inside the database, so that a failure
// names the owner of the wait. It returns only once that log is written or
// can no longer be, so a caller that is joined logs nothing after its test.
func (w *slowWatch) run(name string, op func()) {
	done := make(chan struct{})
	logged := make(chan struct{})
	timer := time.AfterFunc(stallPrompt/2, func() {
		defer close(logged)
		select {
		case <-done:
			return
		default:
		}
		if w.dumps.Add(1) > 3 {
			return
		}
		var dump strings.Builder
		_ = pprof.Lookup("goroutine").WriteTo(&dump, 2)
		var kept []string
		for _, stack := range strings.Split(dump.String(), "\n\n") {
			header, _, _ := strings.Cut(stack, "\n")
			blocked := strings.Contains(header, "semacquire") || strings.Contains(header, "sync.") ||
				strings.Contains(header, "chan receive") || strings.Contains(header, "chan send") || strings.Contains(header, "select")
			// Blocked database goroutines, and every search goroutine whatever
			// its state: the holder of a lock is often running.
			if !(blocked && strings.Contains(stack, "orneryd/nornicdb/pkg/")) && !strings.Contains(stack, "orneryd/nornicdb/pkg/search.") {
				continue
			}
			if strings.Contains(stack, "pkg/server.(*stallClients)") || strings.Contains(stack, "pkg/server.(*slowWatch)") {
				continue
			}
			lines := strings.Split(stack, "\n")
			kept = append(kept, strings.Join(lines[:min(len(lines), 40)], "\n"))
			if len(kept) == 80 {
				break
			}
		}
		w.t.Logf("%s still running after %v; goroutines blocked in the database:\n%s", name, stallPrompt/2, strings.Join(kept, "\n\n"))
	})
	defer func() {
		close(done)
		if !timer.Stop() {
			<-logged
		}
	}()
	op()
}

// joinWithin waits for the goroutines of group, and fails the test if one is
// still running after stallJoin.
func joinWithin(t *testing.T, group *sync.WaitGroup, what string) {
	joined := make(chan struct{})
	go func() {
		group.Wait()
		close(joined)
	}()
	select {
	case <-joined:
	case <-time.After(stallJoin):
		t.Errorf("%s was still running %v after the database was closed", what, stallJoin)
	}
}

func stallVector(r *rand.Rand, dims int) []float64 {
	v := make([]float64, dims)
	var norm float64
	for i := range v {
		v[i] = r.NormFloat64()
		norm += v[i] * v[i]
	}
	norm = math.Sqrt(norm)
	for i := range v {
		v[i] /= norm
	}
	return v
}

type stallStore struct {
	t    *testing.T
	db   *nornicdb.DB
	r    *rand.Rand
	dims int
	ids  []string
	want map[string][]float64
}

func newStallStore(t *testing.T, db *nornicdb.DB, log *hnswStallLog, nodes, dims int) *stallStore {
	s := &stallStore{t: t, db: db, r: rand.New(rand.NewSource(1)), dims: dims, want: map[string][]float64{}}
	s.exec(fmt.Sprintf("CREATE VECTOR INDEX doc_embedding IF NOT EXISTS FOR (d:Doc) ON (d.embedding) OPTIONS {indexConfig: {`vector.dimensions`: %d, `vector.similarity_function`: 'cosine'}}", dims), nil)
	s.exec("CREATE CONSTRAINT doc_id IF NOT EXISTS FOR (d:Doc) REQUIRE d.id IS UNIQUE", nil)
	// The database's initial search build ran on an empty store; once it is
	// done, no build runs until the first vector query starts one.
	s.awaitSearchReady()
	log.observe(t, db)
	for i := 0; i < nodes; i++ {
		s.ids = append(s.ids, fmt.Sprintf("doc-%05d", i))
	}
	s.write("UNWIND $rows AS r CREATE (d:Doc {id: r.id, embedding: r.v})", s.ids)
	return s
}

func (s *stallStore) awaitSearchReady() {
	s.t.Helper()
	require.Eventually(s.t, func() bool {
		svc, err := s.db.GetOrCreateSearchService("nornic", s.db.GetStorage())
		return err == nil && svc.GetBuildProgress().Ready
	}, 2*time.Minute, 20*time.Millisecond, "the search index build did not finish")
}

func (s *stallStore) exec(query string, params map[string]any) *nornicdb.CypherResult {
	s.t.Helper()
	result, err := s.db.ExecuteCypher(context.Background(), query, params)
	require.NoError(s.t, err, query)
	return result
}

// write gives every id a new vector, in batches.
func (s *stallStore) write(query string, ids []string) {
	s.t.Helper()
	for start := 0; start < len(ids); start += 500 {
		end := min(start+500, len(ids))
		rows := make([]any, 0, end-start)
		for _, id := range ids[start:end] {
			v := stallVector(s.r, s.dims)
			s.want[id] = v
			rows = append(rows, map[string]any{"id": id, "v": v})
		}
		s.exec(query, map[string]any{"rows": rows})
	}
}

func (s *stallStore) changeAll() {
	s.write("UNWIND $rows AS r MATCH (d:Doc {id: r.id}) SET d.embedding = r.v", s.ids)
}

func (s *stallStore) query(v []float64) {
	s.exec("CALL db.index.vector.queryNodes('doc_embedding', 3, $v) YIELD node, score RETURN node.id AS id, score", map[string]any{"v": v})
}

// requireServes checks that the index finds each sampled node by its current
// vector. It asks for 100 neighbours: every node changed during the build
// leaves a tombstone, so the published graph is up to half tombstones and an
// approximate top 10 can miss a node that is there (exact membership is
// checked by the search package's catch-up tests).
func (s *stallStore) requireServes(ids []string) {
	s.t.Helper()
	for _, id := range ids {
		result := s.exec(stallNearest, map[string]any{"v": s.want[id]})
		if !stallServed(result.Rows, id) {
			s.diagnoseFirstMiss(id)
		}
		require.True(s.t, stallServed(result.Rows, id), "the vector index does not find %s by its current vector (rows %v)", id, result.Rows)
	}
}

// diagnoseFirstMiss captures the canonical index answer before any settling
// or repeated ANN query. It never replaces the original failed assertion.
func (s *stallStore) diagnoseFirstMiss(id string) {
	s.t.Helper()
	started := time.Now()
	service, err := s.db.GetOrCreateSearchService("nornic", s.db.GetStorage())
	if err != nil {
		s.t.Logf("first-miss service lookup failed: %v", err)
		return
	}
	query := make([]float32, len(s.want[id]))
	for i, v := range s.want[id] {
		query[i] = float32(v)
	}
	exact, exactErr := service.VectorQueryNodes(s.t.Context(), query, search.VectorQuerySpec{
		IndexName: "doc_embedding", Label: "Doc", Property: "embedding", Similarity: "cosine", Limit: 100, Exact: true,
	})
	exactAt := time.Since(started)
	direct, directErr := service.VectorQueryNodes(s.t.Context(), query, search.VectorQuerySpec{
		IndexName: "doc_embedding", Label: "Doc", Property: "embedding", Similarity: "cosine", Limit: 100,
	})
	directAt := time.Since(started)
	nodes, readErr := s.db.GetStorage().GetNodesByLabel(s.t.Context(), "Doc")
	if readErr != nil {
		s.t.Logf("first-miss stored read failed: %v (exact err %v)", readErr, exactErr)
		return
	}
	for _, node := range nodes {
		if node.Properties["id"] != id {
			continue
		}
		raw, encodeErr := json.Marshal(node.Properties["embedding"])
		var stored []float64
		decodeErr := json.Unmarshal(raw, &stored)
		difference := math.Inf(1)
		if len(stored) == len(s.want[id]) {
			difference = 0
			for i, value := range stored {
				difference = max(difference, math.Abs(value-s.want[id][i]))
			}
		}
		found, score := false, float64(0)
		for _, hit := range exact {
			if hit.ID == string(node.ID) {
				found, score = true, hit.Score
			}
		}

		directFound, directScore := false, float64(0)
		for _, hit := range direct {
			if hit.ID == string(node.ID) {
				directFound, directScore = true, hit.Score
			}
		}
		state := s.db.GetDatabaseSearchStatus("nornic")
		s.t.Logf("first-miss id=%s stored_id=%s durable_max_difference=%g durable_dimensions=%d encode_err=%v decode_err=%v exact_found=%v exact_score=%g exact_err=%v exact_elapsed=%v direct_found=%v direct_score=%g direct_err=%v direct_elapsed=%v ready=%v building=%v phase=%s",
			id, node.ID, difference, len(stored), encodeErr, decodeErr, found, score, exactErr, exactAt, directFound, directScore, directErr, directAt, state.Ready, state.Building, state.Phase)
		return
	}
	s.t.Logf("first-miss id=%s absent from durable label population; exact_err=%v", id, exactErr)
}

// stallNearest returns the 100 nodes nearest to $v: each row is a node's id
// and its score.
const stallNearest = "CALL db.index.vector.queryNodes('doc_embedding', 100, $v) YIELD node, score RETURN node.id AS id, score"

// stallServed reports whether rows, the answer of stallNearest for a node's
// current vector, hold that node with a score of at least 0.999. The row
// compared is the node's own. Another node scoring 1 does not count, and
// neither does the node with a lower score: the index then holds another
// vector for it.
func stallServed(rows [][]any, id string) bool {
	for _, row := range rows {
		if len(row) == 2 && row[0] == id {
			score, _ := row[1].(float64)
			return score >= 0.999
		}
	}
	return false
}

func (s *stallStore) sample(n int) []string {
	out := make([]string, 0, n)
	step := max(1, len(s.ids)/n)
	for i := 0; i < len(s.ids) && len(out) < n; i += step {
		out = append(out, s.ids[i])
	}
	return out
}

// holdConstructionAndChangeAll holds the next construction, starts it with
// start, changes every vector while it is held, then lets it continue.
func holdConstructionAndChangeAll(t *testing.T, log *hnswStallLog, store *stallStore, start func()) {
	t.Helper()
	held := log.holdNextConstruction()
	// Released on every return: a construction left held after a failed
	// assertion would keep the database from closing.
	defer log.release()
	start()
	select {
	case <-held:
	case <-time.After(3 * time.Minute):
		t.Fatal("no vector index construction started")
	}
	store.changeAll()
}

type stallClients struct {
	base   string
	token  string
	client *http.Client
	driver neo4jdriver.DriverWithContext
	bolt   string // the Bolt server's URL
}

// newStallClients serves the database over HTTP and Bolt, with a client for
// each. Each part is closed by a cleanup registered as soon as the part
// exists, so a failure part-way leaves nothing open.
func newStallClients(t *testing.T, server *Server, token string) *stallClients {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	boltServer := bolt.NewWithDatabaseManager(&bolt.Config{ReadBufferSize: 8192, WriteBufferSize: 8192}, nil, server.dbManager)
	serveErr := make(chan error, 1)
	go func() { serveErr <- boltServer.Serve(listener) }()
	t.Cleanup(func() {
		_ = boltServer.Close()
		<-serveErr
	})
	address := "bolt://" + listener.Addr().String()
	driver, err := neo4jdriver.NewDriverWithContext(address, neo4jdriver.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	local := httptest.NewServer(server.buildRouter())
	t.Cleanup(local.Close)
	return &stallClients{base: local.URL, token: token, client: &http.Client{Timeout: stallOperation}, driver: driver, bolt: address}
}

// stallHTTPError is a failed HTTP transaction request: its status and the
// Neo4j errors in its response.
type stallHTTPError struct {
	status int
	errors []QueryError
}

func (e *stallHTTPError) Error() string {
	if len(e.errors) == 0 {
		return fmt.Sprintf("HTTP %d", e.status)
	}
	return fmt.Sprintf("HTTP %d: %+v", e.status, e.errors)
}

func (c *stallClients) request(ctx context.Context, method, url string, statements []map[string]any) (TransactionResponse, error) {
	payload, err := json.Marshal(map[string]any{"statements": statements})
	if err != nil {
		return TransactionResponse{}, err
	}
	request, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(payload))
	if err != nil {
		return TransactionResponse{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+c.token)
	response, err := c.client.Do(request)
	if err != nil {
		return TransactionResponse{}, err
	}
	defer response.Body.Close()
	var decoded TransactionResponse
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		return TransactionResponse{}, fmt.Errorf("HTTP %d: %w", response.StatusCode, err)
	}
	if len(decoded.Errors) > 0 || (response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated) {
		return decoded, &stallHTTPError{status: response.StatusCode, errors: decoded.Errors}
	}
	return decoded, nil
}

func (c *stallClients) post(ctx context.Context, url string, statements []map[string]any) (TransactionResponse, error) {
	return c.request(ctx, http.MethodPost, url, statements)
}

// rollback ends an explicit transaction that did not commit, so that it holds
// nothing in the server; the server may have ended it already. commitURL is
// the transaction's commit address, empty if none was opened.
func (c *stallClients) rollback(commitURL string) {
	if commitURL == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), stallPrompt)
	defer cancel()
	_, _ = c.request(ctx, http.MethodDelete, strings.TrimSuffix(commitURL, "/commit"), nil)
}

// stallErrorCodes are the Neo4j status codes of a failed client operation,
// over HTTP or over Bolt.
func stallErrorCodes(err error) []string {
	var overHTTP *stallHTTPError
	if errors.As(err, &overHTTP) {
		codes := make([]string, 0, len(overHTTP.errors))
		for _, failure := range overHTTP.errors {
			codes = append(codes, failure.Code)
		}
		return codes
	}
	var overBolt *neo4jdriver.Neo4jError
	if errors.As(err, &overBolt) {
		return []string{overBolt.Code}
	}
	return nil
}

// transactionConflict reports whether err is one a Neo4j client answers by
// running its transaction again: every error in it is a transient transaction
// error, other than the two the Neo4j drivers treat as final. The 503
// "database unavailable" that a held COMMIT is answered with is a transient
// error of another class and is never retried, whatever code it carries: it is
// a failure these tests exist to find.
func transactionConflict(err error) bool {
	var overHTTP *stallHTTPError
	if errors.As(err, &overHTTP) && overHTTP.status == http.StatusServiceUnavailable {
		return false
	}
	codes := stallErrorCodes(err)
	for _, code := range codes {
		if !strings.HasPrefix(code, "Neo.TransientError.Transaction.") ||
			code == "Neo.TransientError.Transaction.Terminated" || code == "Neo.TransientError.Transaction.LockClientStopped" {
			return false
		}
	}
	return len(codes) > 0
}

// retryConflicts runs one client operation the way a Neo4j client runs a
// transaction: an attempt refused with a transaction conflict is run again.
// attempt returns how long its timed part took. The whole of every refused
// attempt is added to that, so a retry cannot make a slow operation look
// prompt, and the operation gives up once its refused attempts have used the
// prompt limit.
func retryConflicts(ctx context.Context, attempt func() (time.Duration, error)) (time.Duration, error) {
	var refused time.Duration
	for {
		started := time.Now()
		took, err := attempt()
		if err == nil || !transactionConflict(err) || ctx.Err() != nil {
			return refused + took, err
		}
		if refused += time.Since(started); refused >= stallPrompt {
			return refused, err
		}
	}
}

const stallSetVector = "MATCH (d:Doc {id: $id}) SET d.embedding = $v"

// One timed operation: what it does and how long its timed part took. It
// ends when ctx does.
type stallProbe struct {
	name string
	run  func(ctx context.Context, id string, v []float64) (time.Duration, error)
}

func (c *stallClients) probes() []stallProbe {
	return []stallProbe{
		{"HTTP explicit COMMIT", func(ctx context.Context, id string, v []float64) (time.Duration, error) {
			open, err := c.post(ctx, c.base+"/db/nornic/tx", []map[string]any{{"statement": stallSetVector, "parameters": map[string]any{"id": id, "v": v}}})
			commitURL := open.Commit
			if commitURL != "" && !strings.HasPrefix(commitURL, "http") {
				commitURL = c.base + commitURL
			}
			if err != nil {
				c.rollback(commitURL)
				return 0, err
			}
			started := time.Now()
			_, err = c.post(ctx, commitURL, []map[string]any{})
			took := time.Since(started)
			if err != nil {
				c.rollback(commitURL)
			}
			return took, err
		}},
		{"Bolt explicit COMMIT", func(ctx context.Context, id string, v []float64) (time.Duration, error) {
			ctx, cancel := context.WithTimeout(ctx, stallOperation)
			defer cancel()
			session := c.driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: "nornic"})
			defer session.Close(ctx)
			tx, err := session.BeginTransaction(ctx)
			if err != nil {
				return 0, err
			}
			result, err := tx.Run(ctx, stallSetVector, map[string]any{"id": id, "v": v})
			if err == nil {
				_, err = result.Consume(ctx)
			}
			if err != nil {
				_ = tx.Rollback(ctx)
				return 0, err
			}
			started := time.Now()
			err = tx.Commit(ctx)
			return time.Since(started), err
		}},
		{"HTTP auto-commit write", func(ctx context.Context, id string, v []float64) (time.Duration, error) {
			started := time.Now()
			_, err := c.post(ctx, c.base+"/db/nornic/tx/commit", []map[string]any{{"statement": stallSetVector, "parameters": map[string]any{"id": id, "v": v}}})
			return time.Since(started), err
		}},
		{"HTTP vector query", func(ctx context.Context, _ string, v []float64) (time.Duration, error) {
			started := time.Now()
			_, err := c.post(ctx, c.base+"/db/nornic/tx/commit", []map[string]any{{
				"statement":  "CALL db.index.vector.queryNodes('doc_embedding', 3, $v) YIELD node, score RETURN node.id, score",
				"parameters": map[string]any{"v": v},
			}})
			return time.Since(started), err
		}},
	}
}

// commitUntilPublished runs one client per kind of operation, concurrently,
// from the moment the held construction continues until its graph is
// published (and a few operations after), and requires every operation to be
// prompt and to succeed. extra runs once, as soon as the catch-up has started.
// It returns every node changed meanwhile: by the clients and by the
// sustained writer.
func commitUntilPublished(t *testing.T, log *hnswStallLog, store *stallStore, clients *stallClients, extra func(ctx context.Context) (string, time.Duration, error)) []string {
	t.Helper()
	probes := clients.probes()
	type outcome struct {
		name    string
		count   int
		slowest time.Duration
		err     error
		changed map[string][]float64
	}
	outcomes := make([]outcome, len(probes))
	deadline := time.Now().Add(10 * time.Minute)
	watch := &slowWatch{t: t}
	// The clients are stopped and joined on every return, so a failed
	// assertion below leaves none of them running.
	ctx, stop := context.WithCancel(t.Context())
	var running sync.WaitGroup
	defer running.Wait()
	defer stop()
	for k, probe := range probes {
		running.Go(func() {
			r := rand.New(rand.NewSource(int64(100 + k)))
			o := outcome{name: probe.name, changed: map[string][]float64{}}
			defer func() { outcomes[k] = o }()
			for i, after := 0, 0; after < 3; i++ {
				if _, published := log.at("published"); published {
					after++
				}
				if time.Now().After(deadline) {
					o.err = fmt.Errorf("the vector index was not published within 10 minutes")
					return
				}
				if err := ctx.Err(); err != nil {
					o.err = fmt.Errorf("stopped before the vector index was published: %w", err)
					return
				}
				// Each client changes its own nodes, so the last value it
				// committed is each node's current vector.
				id := store.ids[((i*7919)%(len(store.ids)/stallSlots))*stallSlots+k]
				v := stallVector(r, store.dims)
				var took time.Duration
				var err error
				watch.run(probe.name, func() {
					took, err = retryConflicts(ctx, func() (time.Duration, error) { return probe.run(ctx, id, v) })
				})
				o.count++
				o.slowest = max(o.slowest, took)
				if err != nil {
					o.err = err
					return
				}
				if probe.name != "HTTP vector query" {
					o.changed[id] = v
				}
			}
		})
	}
	for {
		if _, catchingUp := log.at("catch-up"); catchingUp {
			break
		}
		if _, published := log.at("published"); published || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if extra != nil {
		if _, catchingUp := log.at("catch-up"); catchingUp {
			name, took, err := extra(ctx)
			require.NoError(t, err, name)
			require.Less(t, took, stallPrompt, "%s took %v while the vector index caught up", name, took)
		}
	}
	changed := sustainVectorWrites(t, log, store)
	running.Wait()
	for _, o := range outcomes {
		t.Logf("%s: %d operations, slowest %v", o.name, o.count, o.slowest)
		require.NoError(t, o.err, "%s while the vector index caught up", o.name)
		require.Less(t, o.slowest, stallPrompt, "%s took %v while the vector index caught up", o.name, o.slowest)
		for id, v := range o.changed {
			store.want[id] = v
			changed = append(changed, id)
		}
	}
	// The clients must have run during a catch-up, and the workload must make
	// that catch-up long, or prompt commits prove nothing.
	catchUp, ok := log.at("catch-up")
	require.True(t, ok, "no catch-up was observed, so nothing was shown about commits during one")
	published, _ := log.at("published")
	t.Logf("catch-up lasted %v", published.Sub(catchUp))
	require.GreaterOrEqual(t, published.Sub(catchUp), stallPrompt, "the catch-up was too short to show a wait; raise HNSW_STALL_NODES")
	return changed
}

// stallSlots partitions the nodes: the probe clients write slots 0-3 of
// every eight, the sustained writer slots 4-7, so each node has one writer
// and its last committed vector is known.
const stallSlots = 8

// sustainVectorWrites, once the catch-up has started, rewrites the vectors of
// half the nodes in batches, without pause, until the graph is published: a
// sustained write load for the whole catch-up, on top of the probes. Each
// write pays for the changes it records, so the load cannot keep the backlog
// from shrinking: the catch-up must still finish while it runs, and the
// probes must stay prompt. The catch-up's progress log over the period is the
// evidence. It returns the nodes it changed.
func sustainVectorWrites(t *testing.T, log *hnswStallLog, store *stallStore) []string {
	t.Helper()
	if _, catchingUp := log.at("catch-up"); !catchingUp {
		return nil
	}
	var owned []string
	for i, id := range store.ids {
		if i%stallSlots >= stallSlots/2 {
			owned = append(owned, id)
		}
	}
	deadline := time.Now().Add(10 * time.Minute)
	started := time.Now()
	writes := 0
	var changed []string
	for next := 0; ; {
		if _, published := log.at("published"); published {
			break
		}
		require.False(t, time.Now().After(deadline), "the catch-up did not finish under sustained writes")
		end := min(next+500, len(owned))
		rows := make([]any, 0, end-next)
		for _, id := range owned[next:end] {
			v := stallVector(store.r, store.dims)
			store.want[id] = v
			rows = append(rows, map[string]any{"id": id, "v": v})
		}
		store.exec(stallSetVectorRows, map[string]any{"rows": rows})
		writes += len(rows)
		if len(changed) < len(owned) {
			changed = append(changed, owned[next:end]...)
		}
		if next = end; next == len(owned) {
			next = 0
		}
	}
	stopped := time.Now()
	var progress []string
	for _, p := range log.pendingBetween(started, stopped) {
		progress = append(progress, fmt.Sprintf("%v:%d", p.at.Sub(started).Round(time.Second), p.count))
	}
	t.Logf("sustained writes until publication: %d vector changes over %v; catch-up pending during them: %s",
		writes, stopped.Sub(started).Round(time.Millisecond), strings.Join(progress, " "))
	require.Positive(t, writes)
	return changed
}

func stallServer(t *testing.T) (*Server, *stallClients) {
	server, authenticator := setupTestServer(t)
	return server, newStallClients(t, server, getAuthToken(t, authenticator, "admin"))
}

// The end of a vector index build or rebuild must not hold COMMIT. While the
// new graph catches up with the vectors changed during its construction,
// explicit HTTP and Bolt COMMITs, auto-commit writes, vector queries and DROP
// INDEX return promptly and succeed (a committed transaction is never answered
// 503), and the published graph holds every vector changed meanwhile.
func TestHNSWBuildEndDoesNotHoldCommits(t *testing.T) {
	nodes := stallEnvInt("HNSW_STALL_NODES", 6000)
	dims := stallEnvInt("HNSW_STALL_DIMS", 128)

	t.Run("first build", func(t *testing.T) {
		stallEnv(t)
		log := installHNSWStallLog(t)
		server, clients := stallServer(t)
		store := newStallStore(t, server.db, log, nodes, dims)
		store.exec(fmt.Sprintf("CREATE VECTOR INDEX aux_embedding IF NOT EXISTS FOR (a:Aux) ON (a.embedding) OPTIONS {indexConfig: {`vector.dimensions`: %d, `vector.similarity_function`: 'cosine'}}", dims), nil)
		// The first vector query starts the build and is answered exactly.
		holdConstructionAndChangeAll(t, log, store, func() { store.query(store.want[store.ids[0]]) })
		changed := commitUntilPublished(t, log, store, clients, func(ctx context.Context) (string, time.Duration, error) {
			// A schema change commits only if no other write committed since
			// it began, and the writers here keep committing. A client retries
			// that transient conflict, as Neo4j drivers do; the retries count
			// towards the prompt limit.
			took, err := retryConflicts(ctx, func() (time.Duration, error) {
				started := time.Now()
				_, err := clients.post(ctx, clients.base+"/db/nornic/tx/commit", []map[string]any{{"statement": "DROP INDEX aux_embedding"}})
				return time.Since(started), err
			})
			return "DROP INDEX", took, err
		})
		store.requireServes(append(store.sample(100), changed...))
	})

	t.Run("tombstone rebuild", func(t *testing.T) {
		stallEnv(t)
		// Rebuild as soon as a tombstone exists, checked often.
		t.Setenv("NORNICDB_HNSW_TOMBSTONE_REBUILD_RATIO", "0.0001")
		t.Setenv("NORNICDB_HNSW_MAINT_INTERVAL_MS", "200")
		log := installHNSWStallLog(t)
		server, clients := stallServer(t)
		store := newStallStore(t, server.db, log, nodes, dims)
		log.reset()
		store.query(store.want[store.ids[0]]) // the first build runs freely
		require.Eventually(t, func() bool { _, ok := log.at("published"); return ok }, 5*time.Minute, 50*time.Millisecond, "first build")
		// A changed vector leaves a tombstone; the maintenance loop rebuilds.
		holdConstructionAndChangeAll(t, log, store, func() { store.write(stallSetVectorRows, store.ids[:10]) })
		changed := commitUntilPublished(t, log, store, clients, nil)
		store.requireServes(append(store.sample(100), changed...))
	})
}

const stallSetVectorRows = "UNWIND $rows AS r MATCH (d:Doc {id: r.id}) SET d.embedding = r.v"

// DB.Close while a vector index catches up returns promptly instead of
// waiting for the catch-up. Nothing committed is lost: after a restart the
// first build serves every vector.
func TestDBCloseDuringHNSWBuildCatchUpReturnsPromptly(t *testing.T) {
	stallEnv(t)
	nodes := stallEnvInt("HNSW_STALL_NODES", 6000)
	dims := stallEnvInt("HNSW_STALL_DIMS", 128)
	log := installHNSWStallLog(t)
	dir := t.TempDir()
	open := func() *nornicdb.DB {
		config := nornicdb.DefaultConfig()
		config.Memory.DecayEnabled = false
		config.Memory.AutoLinksEnabled = false
		config.Database.AsyncWritesEnabled = false
		db, err := nornicdb.Open(dir, config)
		require.NoError(t, err)
		return db
	}
	// The goroutines this test starts on the database are joined once it is
	// closed: closing it is what ends a write, or a Close, that is still
	// running when an assertion fails.
	var background sync.WaitGroup
	t.Cleanup(func() { joinWithin(t, &background, "a write or a Close of the database") })
	db := open()
	// The cleanup closes the open database unless the test has asked it to
	// close: a Close that failed or did not return is not asked again.
	closeRequested := false
	t.Cleanup(func() {
		if !closeRequested {
			_ = db.Close()
		}
	})
	store := newStallStore(t, db, log, nodes, dims)
	holdConstructionAndChangeAll(t, log, store, func() { store.query(store.want[store.ids[0]]) })

	// Wait until the end of the build is under way: its catch-up has started,
	// or a write has been waiting a second for it.
	type write struct {
		id   string
		v    []float64
		done chan error
	}
	var waiting *write
	for waiting == nil {
		if _, ok := log.at("catch-up"); ok {
			break
		}
		w := &write{id: store.ids[len(store.ids)-1], v: stallVector(store.r, dims), done: make(chan error, 1)}
		target := db
		background.Go(func() {
			_, err := target.ExecuteCypher(context.Background(), stallSetVector, map[string]any{"id": w.id, "v": w.v})
			w.done <- err
		})
		select {
		case err := <-w.done:
			require.NoError(t, err)
			store.want[w.id] = w.v
		case <-time.After(time.Second):
			waiting = w
		}
	}
	_, publishedBefore := log.at("published")
	require.False(t, publishedBefore, "the graph was published before Close; the catch-up was too short")
	started := time.Now()
	closeRequested = true
	closing := db
	closeDone := make(chan error, 1)
	background.Go(func() { closeDone <- closing.Close() })
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Minute):
		t.Fatal("DB.Close did not return")
	}
	took := time.Since(started)
	require.Less(t, took, stallPrompt, "DB.Close took %v while the vector index caught up", took)
	_, publishedByClose := log.at("published")
	require.False(t, publishedByClose, "the catch-up finished before DB.Close returned, so Close was not shown to stop one")
	if waiting != nil {
		// The write that was waiting ended with the build's end or with the
		// close. Its vector is the node's current one only if it committed.
		select {
		case err := <-waiting.done:
			if err == nil {
				store.want[waiting.id] = waiting.v
			}
		case <-time.After(stallJoin):
			t.Fatal("a write that waited for the build's end was still running after DB.Close returned")
		}
	}

	// Restart: nothing committed is lost. The startup search build constructs
	// the graph again from the store, and the index then finds every vector
	// committed before the close: the sampled nodes, and the node the waiting
	// write changed, by the vector that write left it with. Writes during the
	// startup build are not timed: a vector write then waits for that build as
	// a whole, which this test does not cover.
	log.reset()
	db = open()
	closeRequested = false
	store.db = db
	log.observe(t, db)
	require.Eventually(t, func() bool { _, ok := log.at("published"); return ok }, 5*time.Minute, 50*time.Millisecond, "no vector index was published after the restart")
	store.awaitSearchReady()
	store.requireServes(append(store.sample(100), store.ids[len(store.ids)-1]))
}

// Clients that rewrite the same nodes many times while a vector index is
// built or rebuilt, until it is published: afterwards the index finds each
// of those nodes by its latest vector. The clients use every write path.
// Some paths refresh the search indexes within the commit, others through the
// database's debounced refresh (250 ms after its last write), so the check
// allows that refresh to run once the writes stop.
func TestHNSWBuildKeepsTheLatestOfRepeatedRewrites(t *testing.T) {
	nodes := stallEnvInt("HNSW_STALL_NODES", 6000)
	dims := stallEnvInt("HNSW_STALL_DIMS", 128)

	t.Run("first build", func(t *testing.T) {
		stallEnv(t)
		log := installHNSWStallLog(t)
		server, clients := stallServer(t)
		store := newStallStore(t, server.db, log, nodes, dims)
		holdConstructionAndChangeAll(t, log, store, func() { store.query(store.want[store.ids[0]]) })
		rewriteUntilPublished(t, log, store, clients)
	})

	t.Run("tombstone rebuild", func(t *testing.T) {
		stallEnv(t)
		t.Setenv("NORNICDB_HNSW_TOMBSTONE_REBUILD_RATIO", "0.0001")
		t.Setenv("NORNICDB_HNSW_MAINT_INTERVAL_MS", "200")
		log := installHNSWStallLog(t)
		server, clients := stallServer(t)
		store := newStallStore(t, server.db, log, nodes, dims)
		log.reset()
		store.query(store.want[store.ids[0]]) // the first build runs freely
		require.Eventually(t, func() bool { _, ok := log.at("published"); return ok }, 5*time.Minute, 50*time.Millisecond, "first build")
		holdConstructionAndChangeAll(t, log, store, func() { store.write(stallSetVectorRows, store.ids[:10]) })
		rewriteUntilPublished(t, log, store, clients)
	})
}

// rewriteUntilPublished runs one client per write path, each rewriting its own
// eight nodes in turn, from the moment the held construction continues until
// the graph is published. It then requires the index to find every rewritten
// node by its latest vector.
func rewriteUntilPublished(t *testing.T, log *hnswStallLog, store *stallStore, clients *stallClients) {
	t.Helper()
	type writer struct {
		name string
		run  func(ctx context.Context, id string, v []float64) error
	}
	var writers []writer
	for _, probe := range clients.probes() {
		if probe.name == "HTTP vector query" {
			continue
		}
		writers = append(writers, writer{probe.name, func(ctx context.Context, id string, v []float64) error {
			_, err := retryConflicts(ctx, func() (time.Duration, error) { return probe.run(ctx, id, v) })
			return err
		}})
	}
	// The second Bolt client has its own driver: two goroutines borrowing
	// connections from one driver race in the driver's connector
	// (neo4j-go-driver v5.28.5, Connector.Connect).
	autoDriver, err := neo4jdriver.NewDriverWithContext(clients.bolt, neo4jdriver.NoAuth())
	require.NoError(t, err)
	defer func() { _ = autoDriver.Close(context.Background()) }()
	writers = append(writers, writer{"Bolt auto-commit write", func(ctx context.Context, id string, v []float64) error {
		_, err := retryConflicts(ctx, func() (time.Duration, error) {
			ctx, cancel := context.WithTimeout(ctx, stallOperation)
			defer cancel()
			session := autoDriver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: "nornic"})
			defer session.Close(ctx)
			result, err := session.Run(ctx, stallSetVector, map[string]any{"id": id, "v": v})
			if err == nil {
				_, err = result.Consume(ctx)
			}
			return 0, err
		})
		return err
	}})
	const perWriter = 8
	started := time.Now()
	deadline := started.Add(10 * time.Minute)
	latest := make([]map[string][]float64, len(writers))
	counts := make([]int, len(writers))
	errs := make([]error, len(writers))
	// The writers are stopped and joined on every return, before the second
	// driver is closed.
	ctx, stop := context.WithCancel(t.Context())
	var running sync.WaitGroup
	defer running.Wait()
	defer stop()
	for k, w := range writers {
		latest[k] = map[string][]float64{}
		running.Go(func() {
			r := rand.New(rand.NewSource(int64(200 + k)))
			for i := 0; ; i++ {
				if _, published := log.at("published"); published {
					return
				}
				if time.Now().After(deadline) {
					errs[k] = fmt.Errorf("%s: the vector index was not published within 10 minutes", w.name)
					return
				}
				if err := ctx.Err(); err != nil {
					errs[k] = fmt.Errorf("%s: stopped before the vector index was published: %w", w.name, err)
					return
				}
				id := store.ids[1+k*perWriter+i%perWriter]
				v := stallVector(r, store.dims)
				if err := w.run(ctx, id, v); err != nil {
					errs[k] = fmt.Errorf("%s: %w", w.name, err)
					return
				}
				latest[k][id] = v
				counts[k]++
			}
		})
	}
	running.Wait()
	stopped := time.Now()
	var rewritten []string
	for k, w := range writers {
		require.NoError(t, errs[k])
		require.Positive(t, counts[k], "%s wrote nothing before publication", w.name)
		t.Logf("%s: %d rewrites of %d nodes", w.name, counts[k], len(latest[k]))
		for id, v := range latest[k] {
			store.want[id] = v
			rewritten = append(rewritten, id)
		}
	}
	published, _ := log.at("published")
	t.Logf("rewrites from construction until publication %v later", published.Sub(started).Round(time.Millisecond))
	// Allow the database's debounced search refresh to run after the writes.
	var missing []string
	for settle := stopped.Add(10 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		missing = missing[:0]
		for _, id := range rewritten {
			if !store.serves(id, store.want[id]) {
				missing = append(missing, id)
			}
		}
		if len(missing) == 0 || time.Now().After(settle) {
			break
		}
	}
	require.Empty(t, missing, "the index does not find these nodes by their latest vectors 10s after the rewrites stopped")
	t.Logf("every rewritten node is found by its latest vector %v after the rewrites stopped", time.Since(stopped).Round(time.Millisecond))
	store.requireServes(append(store.sample(20), rewritten...))
}

// serves reports whether the index finds id among the 100 nearest to v with
// a score of at least 0.999.
func (s *stallStore) serves(id string, v []float64) bool {
	s.t.Helper()
	return stallServed(s.exec(stallNearest, map[string]any{"v": v}).Rows, id)
}

// The helpers the public tests judge by must reject what the tests exist to
// find. Each case names the break.
func TestStallOraclesRejectBrokenResults(t *testing.T) {
	t.Run("the index finds a node by its current vector", func(t *testing.T) {
		require.True(t, stallServed([][]any{{"other", 0.72}, {"doc", 0.9996}}, "doc"), "the node's own row scores 1")
		for _, broken := range []struct {
			name string
			rows [][]any
		}{
			{"the node is missing", [][]any{{"other", 1.0}, {"third", 0.4}}},
			{"the node holds an older vector", [][]any{{"other", 0.8}, {"doc", 0.41}}},
			{"two nodes hold each other's vectors", [][]any{{"other", 1.0}, {"doc", 0.03}}},
			{"the node's score is not a number", [][]any{{"doc", "1.0"}}},
			{"the node's score is just under the bar", [][]any{{"doc", 0.9989}}},
			{"the answer is empty", nil},
		} {
			require.False(t, stallServed(broken.rows, "doc"), broken.name)
		}
	})

	conflict := &stallHTTPError{status: http.StatusOK, errors: []QueryError{{Code: "Neo.TransientError.Transaction.Outdated"}}}
	unavailable := &stallHTTPError{status: http.StatusServiceUnavailable, errors: []QueryError{{Code: "Neo.TransientError.General.DatabaseUnavailable"}}}
	t.Run("only a transaction conflict is retried", func(t *testing.T) {
		require.True(t, transactionConflict(conflict))
		require.True(t, transactionConflict(&neo4jdriver.Neo4jError{Code: "Neo.TransientError.Transaction.DeadlockDetected"}))
		for _, final := range []struct {
			name string
			err  error
		}{
			{"a 503 for an unavailable database", unavailable},
			{"a 503 with no Neo4j error", &stallHTTPError{status: http.StatusServiceUnavailable}},
			{"a 503 that carries only a conflict code", &stallHTTPError{status: http.StatusServiceUnavailable, errors: conflict.errors}},
			{"a conflict together with a 503", &stallHTTPError{status: http.StatusServiceUnavailable, errors: []QueryError{conflict.errors[0], unavailable.errors[0]}}},
			{"a terminated transaction", &neo4jdriver.Neo4jError{Code: "Neo.TransientError.Transaction.Terminated"}},
			{"a stopped lock client", &neo4jdriver.Neo4jError{Code: "Neo.TransientError.Transaction.LockClientStopped"}},
			{"a client error", &neo4jdriver.Neo4jError{Code: "Neo.ClientError.Statement.SyntaxError"}},
			{"an error that is not a Neo4j status", errors.New("connection reset")},
		} {
			require.False(t, transactionConflict(final.err), final.name)
		}
	})

	t.Run("a 503 ends the operation at once", func(t *testing.T) {
		for _, answer := range []*stallHTTPError{unavailable, {status: http.StatusServiceUnavailable, errors: conflict.errors}} {
			attempts := 0
			_, err := retryConflicts(t.Context(), func() (time.Duration, error) {
				if attempts++; attempts > 1 {
					return 0, nil
				}
				return 0, answer
			})
			require.ErrorIs(t, err, answer)
			require.Equal(t, 1, attempts, "a 503 was retried: %v", answer)
		}
	})

	t.Run("refused attempts count towards the operation's time", func(t *testing.T) {
		const refusal = 40 * time.Millisecond
		attempts := 0
		took, err := retryConflicts(t.Context(), func() (time.Duration, error) {
			if attempts++; attempts <= 3 {
				time.Sleep(refusal)
				return 0, conflict
			}
			return time.Millisecond, nil
		})
		require.NoError(t, err)
		require.Equal(t, 4, attempts)
		require.GreaterOrEqual(t, took, 3*refusal, "the time of the refused attempts was dropped, so a slow operation would look prompt")
	})

	t.Run("conflicts for longer than the prompt limit fail the operation", func(t *testing.T) {
		took, err := retryConflicts(t.Context(), func() (time.Duration, error) {
			time.Sleep(stallPrompt / 4)
			return 0, conflict
		})
		require.ErrorIs(t, err, conflict)
		require.GreaterOrEqual(t, took, stallPrompt, "an operation refused for the whole prompt limit was reported as prompt")
	})
}
