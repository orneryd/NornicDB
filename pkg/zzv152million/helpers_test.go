package zzv152million

// Investigation harness (untracked, lab only). One job builds the real
// `nornicdb` binary at b6d8e762a (from git archive) and at this checkout
// (003ac1ded), then:
//   A  b6 creates a store (PDU_NODES nodes) through HTTP, churns it, stops cleanly,
//      restarts (baseline start time and query timings);
//   B  the release binary opens a copy without and with --upgrade-storage,
//      restarts it, and writes to it;
//   C  b6 opens the upgraded copies (with and without release writes) and the
//      untouched original (restore-based rollback);
//   D  the upgrade is SIGKILLed at several points on fresh copies, then resumed.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var vocab = strings.Fields(`company registration office record invoice payment contract tenant lease insurance claim policy
letter statement balance account bank transfer receipt tax return pension salary employer notice council water energy
gas electricity mortgage loan interest rate quarter annual report audit director share dividend property survey
planning permission builder warranty guarantee repair boiler heating window roof garden fence neighbour dispute solicitor
court hearing judgment appeal deadline reminder overdue penalty refund credit debit card mobile phone broadband
subscription renewal cancellation vehicle registration licence insurance service garage mot inspection doctor hospital
appointment prescription dentist school college university tuition grant scholarship passport visa travel booking hotel
flight ticket parcel delivery courier customs import export supplier customer order quote estimate schedule meeting
minutes agenda board trustee charity donation membership club volunteer election vote census survey questionnaire
application form signature witness deed trust estate probate will executor beneficiary inheritance valuation`)

type server struct {
	t       *testing.T
	name    string
	cmd     *exec.Cmd
	http    int
	bolt    int
	start   time.Time
	mu      sync.Mutex
	lines   []string
	exited  chan struct{}
	exitErr error
}

var interesting = regexp.MustCompile(`(?i)upgrade|migrat|wal|storage|version|rebuild|recover|error|panic|fatal|refus|clean|warm|index|ready|listening|started|search`)

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func startServer(t *testing.T, name, bin, dir string, args []string, env []string) *server {
	s := &server{t: t, name: name, http: freePort(t), bolt: freePort(t), exited: make(chan struct{})}
	full := append([]string{"serve", "--data-dir", dir, "--address", "127.0.0.1", "--http-port", strconv.Itoa(s.http),
		"--bolt-port", strconv.Itoa(s.bolt), "--no-auth", "--headless", "--mcp-enabled=false"}, args...)
	s.cmd = exec.Command(bin, full...)
 t.Cleanup(func(){select{case <-s.exited: default: s.kill()}})
	s.cmd.Env = append(append(os.Environ(), "NORNICDB_EMBEDDING_ENABLED=false", "NORNICDB_AUTO_LINKS_ENABLED=false", "NORNICDB_DECAY_ENABLED=false"), env...)
	pr, pw := io.Pipe()
	s.cmd.Stdout, s.cmd.Stderr = pw, pw
	s.start = time.Now()
	if err := s.cmd.Start(); err != nil {
		t.Fatalf("%s: start: %v", name, err)
	}
	go func() {
		sc := bufio.NewScanner(pr)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			s.mu.Lock()
			s.lines = append(s.lines, fmt.Sprintf("+%7.2fs %s", time.Since(s.start).Seconds(), sc.Text()))
			s.mu.Unlock()
		}
	}()
	go func() {
		s.exitErr = s.cmd.Wait()
		_ = pw.Close()
		close(s.exited)
	}()
	t.Logf("[%s] started: %s %s", name, filepath.Base(bin), strings.Join(full, " "))
	return s
}

func (s *server) logLines(all bool, max int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, l := range s.lines {
		if all || interesting.MatchString(l) {
			out = append(out, l)
		}
	}
	if len(out) > max {
		out = append(out[:max/2], append([]string{"..."}, out[len(out)-max/2:]...)...)
	}
	return out
}

func (s *server) firstLine(re *regexp.Regexp) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range s.lines {
		if re.MatchString(l) {
			return l
		}
	}
	return ""
}

func (s *server) dump(all bool, max int) {
	s.t.Logf("[%s] log (%d lines total):\n%s", s.name, len(s.lines), strings.Join(s.logLines(all, max), "\n"))
}

type result struct {
	Columns []string
	Rows    [][]any
	Err     string
}

func (s *server) query(db, stmt string, params map[string]any, timeout time.Duration) result {
	body, _ := json.Marshal(map[string]any{"statements": []any{map[string]any{"statement": stmt, "parameters": params}}})
	client := &http.Client{Timeout: timeout}
	resp, err := client.Post(fmt.Sprintf("http://127.0.0.1:%d/db/%s/tx/commit", s.http, db), "application/json", bytes.NewReader(body))
	if err != nil {
		return result{Err: err.Error()}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var parsed struct {
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
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return result{Err: fmt.Sprintf("status %d body %.300s", resp.StatusCode, raw)}
	}
	var r result
	if len(parsed.Errors) > 0 {
		r.Err = parsed.Errors[0].Code + ": " + parsed.Errors[0].Message
		if len(r.Err) > 400 {
			r.Err = r.Err[:400]
		}
	}
	if len(parsed.Results) > 0 {
		r.Columns = parsed.Results[0].Columns
		for _, d := range parsed.Results[0].Data {
			r.Rows = append(r.Rows, d.Row)
		}
	}
	return r
}

func (s *server) mustQuery(db, stmt string, params map[string]any) result {
	r := s.query(db, stmt, params, 30*time.Minute)
	if r.Err != "" {
		s.dump(false, 60)
		s.t.Fatalf("[%s] %.200s: %s", s.name, stmt, r.Err)
	}
	return r
}

// waitReady returns the time from process start until RETURN 1 succeeds over
// HTTP, or the exit error if the process ended first.
func (s *server) waitReady(limit time.Duration) (time.Duration, error) {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		select {
		case <-s.exited:
			return time.Since(s.start), fmt.Errorf("process exited: %v", s.exitErr)
		default:
		}
		if r := s.query("nornic", "RETURN 1 AS one", nil, 2*time.Second); r.Err == "" && len(r.Rows) == 1 {
			return time.Since(s.start), nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return time.Since(s.start), fmt.Errorf("not ready after %v", limit)
}

func (s *server) stop() time.Duration {
	begin := time.Now()
	_ = s.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-s.exited:
	case <-time.After(10 * time.Minute):
		_ = s.cmd.Process.Kill()
		<-s.exited
		s.t.Logf("[%s] did not stop within 10m; killed", s.name)
	}
	d := time.Since(begin)
	s.t.Logf("[%s] stopped (SIGTERM) in %v, exit=%v", s.name, d.Round(time.Millisecond), s.exitErr)
	return d
}

func (s *server) kill() {
	_ = s.cmd.Process.Signal(syscall.SIGKILL)
	<-s.exited
}

func sh(t *testing.T, dir, script string) string {
	t.Helper()
	cmd := exec.Command("bash", "-c", "set -euo pipefail; " + script)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("$ %s\n%s(err=%v)", script, out, err)
	}
	return string(out)
}

func shLog(t *testing.T, dir, script string) string {
	out := sh(t, dir, script)
	t.Logf("$ %s\n%s", script, out)
	return out
}

func vec(r *rand.Rand, dims int) []float64 {
	v := make([]float64, dims)
	for i := range v {
		v[i] = float64(int(r.Float64()*10000)) / 10000
	}
	return v
}

func text(r *rand.Rand, words int) string {
	parts := make([]string, words)
	for i := range parts {
		parts[i] = vocab[r.Intn(len(vocab))]
	}
	return strings.Join(parts, " ")
}

const dims = 64

type plan struct{ docs, searchables, entities int }

func newPlan(total int) plan {
	p := plan{docs: total / 11, searchables: total * 432 / 1000}
	p.entities = total - p.docs - p.searchables
	return p
}

func load(t *testing.T, s *server, p plan) {
	for _, q := range []string{
		"CREATE CONSTRAINT pd_doc_id IF NOT EXISTS FOR (d:PDDocument) REQUIRE d.id IS UNIQUE",
		"CREATE FULLTEXT INDEX pd_text IF NOT EXISTS FOR (n:PDSearchable) ON EACH [n.text]",
		fmt.Sprintf("CREATE VECTOR INDEX pd_vec IF NOT EXISTS FOR (n:PDSearchable) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: %d, `vector.similarity_function`: 'cosine'}}", dims),
		"CREATE INDEX pd_entity_name IF NOT EXISTS FOR (e:PDEntity) ON (e.name)",
		"CREATE INDEX pd_entity_id IF NOT EXISTS FOR (e:PDEntity) ON (e.id)",
		"CREATE INDEX pd_searchable_id IF NOT EXISTS FOR (s:PDSearchable) ON (s.id)",
	} {
		s.mustQuery("nornic", q, nil)
	}
	s.mustQuery("system", "CREATE DATABASE pdextra IF NOT EXISTS", nil)
	s.mustQuery("pdextra", "CREATE (:Marker {id: 'extra'})", nil)
	r := rand.New(rand.NewSource(11))
	begin := time.Now()
	batch := func(label string, n, size int, stmt string, row func(i int) map[string]any) {
		start := time.Now()
		for i := 0; i < n; i += size {
			rows := make([]any, 0, size)
			for j := i; j < i+size && j < n; j++ {
				rows = append(rows, row(j))
			}
			s.mustQuery("nornic", stmt, map[string]any{"rows": rows})
		}
		t.Logf("[load] %s: %d in %v (%.0f/s)", label, n, time.Since(start).Round(time.Millisecond), float64(n)/time.Since(start).Seconds())
	}
	batch("PDDocument", p.docs, 5000, "UNWIND $rows AS r CREATE (:PDDocument:PDGraphNode {id: r.id, title: r.title, current_version_id: r.v, created: r.c})", func(i int) map[string]any {
		return map[string]any{"id": fmt.Sprintf("d%d", i), "title": text(r, 6), "v": fmt.Sprintf("v%d", i), "c": int64(1700000000 + i)}
	})
	batch("PDSearchable+SOURCE_OF", p.searchables, 2000, "UNWIND $rows AS r MATCH (d:PDDocument {id: r.doc}) CREATE (s:PDSearchable:PDSearchSource:PDGraphNode {id: r.id, document_id: r.doc, text: r.text, current: true, embedding: r.e})-[:SOURCE_OF {ord: r.o}]->(d)", func(i int) map[string]any {
		return map[string]any{"id": fmt.Sprintf("s%d", i), "doc": fmt.Sprintf("d%d", i%p.docs), "text": text(r, 60), "e": vec(r, dims), "o": int64(i / p.docs)}
	})
	batch("PDEntity+MENTIONED_IN", p.entities, 5000, "UNWIND $rows AS r MATCH (d:PDDocument {id: r.doc}) CREATE (e:PDEntity:PDGraphNode {id: r.id, name: r.name, kind: r.kind})-[:MENTIONED_IN]->(d)", func(i int) map[string]any {
		return map[string]any{"id": fmt.Sprintf("e%d", i), "doc": fmt.Sprintf("d%d", (i*7)%p.docs), "name": text(r, 2), "kind": vocab[i%len(vocab)]}
	})
	// MERGE-created relationships (merge IDs), as the application uses.
	batch("MERGE PD_KEYED", p.docs/2, 2000, "UNWIND $rows AS r MATCH (a:PDDocument {id: r.a}), (b:PDDocument {id: r.b}) MERGE (a)-[:PD_KEYED {key: r.k}]->(b)", func(i int) map[string]any {
		return map[string]any{"a": fmt.Sprintf("d%d", i), "b": fmt.Sprintf("d%d", (i+1)%p.docs), "k": fmt.Sprintf("k%d", i)}
	})
	// Churn: updates and deletes, as the live workload does.
	batch("update current", p.searchables/10, 5000, "UNWIND $rows AS r MATCH (s:PDSearchable {id: r.id}) SET s.current = false, s.updated = r.u", func(i int) map[string]any {
		return map[string]any{"id": fmt.Sprintf("s%d", i*10), "u": int64(i)}
	})
	batch("detach delete entities", p.entities/50, 2000, "UNWIND $rows AS r MATCH (e:PDEntity {id: r.id}) DETACH DELETE e", func(i int) map[string]any {
		return map[string]any{"id": fmt.Sprintf("e%d", i*50)}
	})
	t.Logf("[load] total %v", time.Since(begin).Round(time.Second))
}

var countQueries = []string{
	"MATCH (n:PDDocument) RETURN count(n)",
	"MATCH (n:PDSearchable) RETURN count(n)",
	"MATCH (n:PDEntity) RETURN count(n)",
	"MATCH (n) RETURN count(n)",
	"MATCH ()-[r:SOURCE_OF]->() RETURN count(r)",
	"MATCH ()-[r:MENTIONED_IN]->() RETURN count(r)",
	"MATCH ()-[r:PD_KEYED]->() RETURN count(r)",
	"MATCH ()-[r]->() RETURN count(r)",
	"MATCH (n:PDSearchable {current: false}) RETURN count(n)",
	"MATCH (n:PDSearchable {id: 's12345'})-[r:SOURCE_OF]->(d) RETURN n.text = n.text AND size(n.embedding) = 64, d.id",
}

func counts(s *server) []string {
	var out []string
	for _, q := range countQueries {
		r := s.query("nornic", q, nil, 30*time.Minute)
		if r.Err != "" {
			out = append(out, "ERR "+r.Err)
			continue
		}
		b, _ := json.Marshal(r.Rows)
		out = append(out, string(b))
	}
	r := s.query("pdextra", "MATCH (m:Marker) RETURN m.id", nil, time.Minute)
	b, _ := json.Marshal(r.Rows)
	out = append(out, "pdextra:"+string(b)+r.Err)
	return out
}

const (
	qFT     = "CALL db.index.fulltext.queryNodes('pd_text', $q) YIELD node, score RETURN node.id AS id, score LIMIT 300"
	qFTJoin = "CALL db.index.fulltext.queryNodes('pd_text', $q) YIELD node, score WITH node, score LIMIT 300 MATCH (node)-[:SOURCE_OF]->(d:PDDocument) WHERE coalesce(node.current, true) RETURN d.id AS doc, collect(node.id)[0..3] AS hits, max(score) AS best ORDER BY best DESC"
	qVec    = "CALL db.index.vector.queryNodes('pd_vec', 300, $v) YIELD node, score RETURN node.id AS id, score"
)

// waitSearch polls until the full-text and vector queries return rows and
// reports the time since process start plus the first successful call's latency.
func waitSearch(t *testing.T, s *server, limit time.Duration) {
	r := rand.New(rand.NewSource(99))
	deadline := time.Now().Add(limit)
	var ftAt, vecAt time.Duration
	var ftCold, vecCold time.Duration
	var lastFT, lastVec string
	for time.Now().Before(deadline) && (ftAt == 0 || vecAt == 0) {
		if ftAt == 0 {
			b := time.Now()
			res := s.query("nornic", qFT, map[string]any{"q": "invoice payment"}, 10*time.Minute)
			if res.Err == "" && len(res.Rows) > 0 {
				ftAt, ftCold = time.Since(s.start), time.Since(b)
				lastFT = fmt.Sprintf("rows=%d", len(res.Rows))
			} else {
				lastFT = fmt.Sprintf("rows=%d err=%s", len(res.Rows), res.Err)
			}
		}
		if vecAt == 0 {
			b := time.Now()
			res := s.query("nornic", qVec, map[string]any{"v": vec(r, dims)}, 10*time.Minute)
			if res.Err == "" && len(res.Rows) > 0 {
				vecAt, vecCold = time.Since(s.start), time.Since(b)
				lastVec = fmt.Sprintf("rows=%d", len(res.Rows))
			} else {
				lastVec = fmt.Sprintf("rows=%d err=%s", len(res.Rows), res.Err)
			}
		}
		if ftAt == 0 || vecAt == 0 {
			time.Sleep(250 * time.Millisecond)
		}
	}
	t.Logf("[%s] SEARCH-READY fulltext at %v (first ok call %v, %s); vector at %v (first ok call %v, %s)", s.name,
		ftAt.Round(time.Millisecond), ftCold.Round(time.Millisecond), lastFT, vecAt.Round(time.Millisecond), vecCold.Round(time.Millisecond), lastVec)
}

func median(ds []time.Duration) time.Duration {
	c := append([]time.Duration(nil), ds...)
	sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
	return c[len(c)/2]
}

func timings(t *testing.T, s *server, label string) {
	r := rand.New(rand.NewSource(5))
	queries := []string{"invoice payment", "tenant lease contract", "insurance claim policy", "court hearing judgment", "pension salary employer", "boiler repair warranty", "passport visa travel", "tax return refund", "mortgage loan interest", "school tuition grant", "probate will executor"}
	for _, shape := range []struct{ name, q string }{{"fulltext300", qFT}, {"fulltext300+join", qFTJoin}, {"vector300", qVec}} {
		var ds []time.Duration
		rows := 0
		errs := ""
		for i := 0; i < 11; i++ {
			params := map[string]any{"q": queries[i], "v": vec(r, dims)}
			b := time.Now()
			res := s.query("nornic", shape.q, params, 10*time.Minute)
			ds = append(ds, time.Since(b))
			rows = len(res.Rows)
			if res.Err != "" {
				errs = res.Err
			}
		}
		var parts []string
		for _, d := range ds {
			parts = append(parts, strconv.FormatInt(d.Milliseconds(), 10))
		}
		t.Logf("TIMING|%s|%s|first=%dms|median(rest)=%dms|all_ms=[%s]|rows=%d|err=%s", label, shape.name, ds[0].Milliseconds(), median(ds[1:]).Milliseconds(), strings.Join(parts, ","), rows, errs)
	}
}

func manifest(t *testing.T, dir string) string {
	return sh(t, dir, `find . -type f -printf '%P %s %T@\n' | sort | sha256sum | cut -c1-16; du -sb . | cut -f1`)
}

