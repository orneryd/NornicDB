package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	stallDB       = "personal_documents"
	stallPassword = "SyntheticLabProbePassword123"
	stallNodes    = 739888
	stallVectors  = 193295
	stallDims     = 2048
	stallCadence  = 100 * time.Millisecond
	stallTx       = "/db/" + stallDB + "/tx"
)

func TestPDW161StartupStall(t *testing.T) {
	if mode := os.Getenv("PD_STALL_CHILD"); mode != "" {
		if mode == "serve" && os.Getenv("PD_STALL_PRIME") == "1" {
			openCommandDB = func(dir string, cfg *nornicdb.Config) (*nornicdb.DB, error) {
				db, err := nornicdb.Open(dir, cfg)
				if err != nil {
					return nil, err
				}
				engine, ok := storage.UnwrapEngine(db.GetBaseStorageForManager()).(*storage.BadgerEngine)
				if !ok {
					_ = db.Close()
					return nil, fmt.Errorf("namespace primer was unavailable")
				}
				for pass := 1; pass <= 2; pass++ {
					at := time.Now()
					err := engine.EnsureNamespaceMVCC(stallDB)
					fmt.Printf("PD_CAUSE pre_listener_prime pass=%d namespace=%s elapsed_ms=%.3f error=%v\n",
						pass, stallDB, float64(time.Since(at))/float64(time.Millisecond), err)
					if err != nil {
						_ = db.Close()
						return nil, err
					}
				}
				return db, nil
			}
		}
		if mode == "seed" {
			openCommandDB = func(dir string, cfg *nornicdb.Config) (*nornicdb.DB, error) {
				db, err := nornicdb.Open(dir, cfg)
				if err != nil {
					t.Fatal(err)
				}
				stallSeed(t, db)
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				fmt.Println("PD_STAGE seed_closed_cleanly")
				os.Exit(0)
				return nil, nil
			}
		}
		os.Args = append(os.Args[:1], strings.Split(os.Getenv("PD_STALL_ARGS"), "\n")...)
		main()
		return
	}

	if os.Getenv("PD_STALL_RUN") != "1" {
		t.Skip("full disk-backed lab fixture")
	}
	dir := t.TempDir()
	cfg := filepath.Join(dir, "native.yaml")
	if err := os.WriteFile(cfg, []byte("database:\n  mvcc_lifecycle_interval: '0s'\n"), 0600); err != nil {
		t.Fatal(err)
	}

	// Precomputed vectors keep the provider out of fixture construction. The
	// production Voyage settings stay enabled and point only at this stand-in.
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("PD_PROVIDER method=%s path=%s\n", r.Method, r.URL.Path)
		http.Error(w, "unexpected generated embedding in precomputed fixture", http.StatusServiceUnavailable)
	}))
	defer provider.Close()
	env := []string{
		"HOME=" + dir, "PATH=" + os.Getenv("PATH"), "TZ=UTC",
		"NORNICDB_LANGUAGE=en",
		"NORNICDB_DEFAULT_DATABASE=" + stallDB,
		"NORNICDB_UPGRADE_STORAGE=true",
		"NORNICDB_AUTH_ENABLED=true",
		"NORNICDB_AUTH_JWT_SECRET=synthetic-w161-startup-signing-key",
		"NORNICDB_TELEMETRY_ENABLED=false",
		"NORNICDB_HEIMDALL_ENABLED=false",
		"NORNICDB_BOLT_ENABLED=true",
		"NORNICDB_BOLT_MAX_CONNECTIONS=400",
		"NORNICDB_EMBEDDING_ENABLED=true",
		"NORNICDB_EMBEDDING_PROVIDER=voyage",
		"NORNICDB_EMBEDDING_MODE=contextualized",
		"NORNICDB_EMBEDDING_MODEL=voyage-context-4",
		"NORNICDB_EMBEDDING_API_KEY=synthetic-startup-lab",
		"NORNICDB_EMBEDDING_API_URL=" + provider.URL,
		"NORNICDB_EMBEDDING_DIMENSIONS=2048",
		"NORNICDB_EMBED_CHUNK_SIZE=128",
		"NORNICDB_EMBED_WORKER_NUM_WORKERS=16",
		"NORNICDB_EMBEDDING_LABELS_INCLUDE=PDSearchSource,PDSearchable",
		"NORNICDB_EMBEDDING_PROPERTIES_INCLUDE=view_text,search_text",
		"NORNICDB_EMBEDDING_INCLUDE_LABELS=false",
		"NORNICDB_SEARCH_BM25_PROPERTIES=text,search_text",
		"NORNICDB_SEARCH_RELATIONSHIPS_ENABLED=true",
		"NORNICDB_SEARCH_RELATIONSHIP_BM25_PROPERTIES=search_text",
		"NORNICDB_EMBEDDING_RELATIONSHIP_PROPERTIES_INCLUDE=search_text",
		"NORNICDB_SEARCH_RERANK_ENABLED=false",
		"NORNICDB_AUTO_LINKS_ENABLED=false",
		"NORNICDB_AUTO_TLP_ENABLED=false",
		"NORNICDB_MEMORY_DECAY_ENABLED=false",
		"NORNICDB_ASYNC_WRITES_ENABLED=false",
		"NORNICDB_STRICT_DURABILITY=true",
		"NORNICDB_WAL_SYNC_MODE=immediate",
		"NORNICDB_WAL_AUTO_COMPACTION_ENABLED=true",
		"NORNICDB_WAL_SNAPSHOT_RETENTION_MAX_COUNT=3",
		"NORNICDB_MVCC_RETENTION_MAX_VERSIONS=100",
		"NORNICDB_GO_MEMORY_LIMIT_BYTES=21474836480",
	}
	httpAddress, boltAddress, pprofAddress := reservePort(t), reservePort(t), reservePort(t)
	env = append(env, "NORNICDB_PPROF_ENABLED=true", "NORNICDB_PPROF_LISTEN="+pprofAddress)
	port := func(address string) string {
		_, value, _ := net.SplitHostPort(address)
		return value
	}
	args := []string{"serve", "--address=127.0.0.1", "--http-port=" + port(httpAddress),
		"--bolt-port=" + port(boltAddress), "--config=" + cfg, "--data-dir=" + filepath.Join(dir, "store"),
		"--admin-password=" + stallPassword, "--stdio-log-max-kb=0"}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	// PD_STALL_CAUSE=1 adds a third clean start whose namespace MVCC state is
	// primed before the listeners open (test-side bypass of the first-use
	// recovery); the second start is the unprimed reproduction.
	type stallPhase struct{ mode, prime string }
	phases := []stallPhase{{"seed", ""}, {"serve", os.Getenv("PD_STALL_PRIME")}}
	if os.Getenv("PD_STALL_CAUSE") == "1" {
		phases = append(phases, stallPhase{"serve", "1"})
	}
	for index, phase := range phases {
		mode := phase.mode
		fmt.Printf("PD_PHASE index=%d mode=%s prime=%q\n", index, mode, phase.prime)
		process := exec.Command(binary, "-test.run=^TestPDW161StartupStall$")
		process.Dir = dir
		process.Env = append(append([]string{}, env...), "PD_STALL_CHILD="+mode,
			"PD_STALL_ARGS="+strings.Join(args, "\n"), "PD_STALL_PRIME="+phase.prime)
		output, err := process.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		process.Stderr = process.Stdout
		if err := process.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = process.Process.Kill() })
		started := time.Now()
		fmt.Printf("PD_STAGE child=%s pid=%d at=%s\n", mode, process.Process.Pid, started.UTC().Format(time.RFC3339Nano))
		logs := &stallServerLog{}
		logDone := make(chan struct{})
		go func() {
			defer close(logDone)
			scan := bufio.NewScanner(output)
			scan.Buffer(make([]byte, 4096), 1<<20)
			for scan.Scan() {
				logs.add(mode, scan.Text())
			}
		}()
		procDone := make(chan error, 1)
		go func() {
			<-logDone
			procDone <- process.Wait()
		}()
		memoryDone := make(chan struct{})
		go stallMemory(mode, process.Process.Pid, memoryDone)
		var probeErr error
		if mode == "serve" {
			probeErr = stallProbe(httpAddress, boltAddress, pprofAddress, started, logs)
			if err := process.Process.Signal(os.Interrupt); err != nil {
				fmt.Printf("PD_STAGE interrupt=%v\n", err)
			}
		}
		select {
		case err = <-procDone:
		case <-time.After(12 * time.Minute):
			_ = process.Process.Kill()
			err = fmt.Errorf("%s did not exit within its budget", mode)
			<-procDone
		}
		close(memoryDone)
		if err != nil || probeErr != nil {
			t.Fatalf("%s process=%v probe=%v tail:\n%s", mode, err, probeErr, logs.tail())
		}
		if mode == "serve" && !logs.cleanStart() {
			t.Fatalf("server did not confirm a clean restart:\n%s", logs.tail())
		}
	}
}

func stallSeed(t *testing.T, db *nornicdb.DB) {
	ctx := t.Context()
	store := db.GetStorage()
	const sources = 2191
	const summaries = 5074
	families := []struct {
		label string
		count int
	}{
		{"PDDocument", 2204},
		{"PDVersion", 2220},
		{"PDOriginal", 2219},
		{"PDReading", 6873},
		{"PDSummary", summaries},
		{"PDRelevance", 19928},
		{"PDHeld", 1970},
		{"PDCollection", 5},
		{"PDUploadOccurrence", 2873},
		{"PDEntity", 0},
	}
	families[len(families)-1].count = stallNodes - sources
	for i := 0; i < len(families)-1; i++ {
		families[len(families)-1].count -= families[i].count
	}
	for _, statement := range []string{
		"CREATE CONSTRAINT pd_graph_id IF NOT EXISTS FOR (n:PDGraphNode) REQUIRE n.id IS UNIQUE",
		"CREATE CONSTRAINT pd_source_id IF NOT EXISTS FOR (n:PDSearchSource) REQUIRE n.id IS UNIQUE",
		"CREATE INDEX pd_source_document IF NOT EXISTS FOR (n:PDSearchSource) ON (n.document_id)",
		"CREATE INDEX pd_source_version IF NOT EXISTS FOR (n:PDSearchSource) ON (n.version_id)",
		"CREATE FULLTEXT INDEX pd_source_text IF NOT EXISTS FOR (n:PDSearchSource) ON EACH [n.text]",
	} {
		if _, err := db.GetCypherExecutor().Execute(ctx, statement, nil); err != nil {
			t.Fatalf("schema %q: %v", statement, err)
		}
	}

	random := rand.New(rand.NewPCG(24, 739888))
	writeNodes := func(nodes []*storage.Node) {
		if err := store.BulkCreateNodes(ctx, nodes); err != nil {
			t.Fatal(err)
		}
	}
	for _, family := range families {
		for from := 0; from < family.count; from += 1000 {
			nodes := make([]*storage.Node, 0, 1000)
			for i := from; i < min(family.count, from+1000); i++ {
				id := fmt.Sprintf("%s-%06d", family.label, i)
				props := map[string]any{"id": id, "kind": family.label}
				labels := []string{"PDGraphNode", family.label}
				if family.label == "PDEntity" {
					props["value"] = "synthetic catalog entity"
					props["document_id"] = fmt.Sprintf("PDDocument-%06d", i%2204)
					props["family_key"] = "family-" + strconv.Itoa(i%500)
				}
				nodes = append(nodes, &storage.Node{ID: storage.NodeID(id), Labels: labels, Properties: props})
			}
			writeNodes(nodes)
			if from > 0 && from%100000 == 0 {
				fmt.Printf("PD_STAGE seeded family=%s count=%d\n", family.label, from+len(nodes))
			}
		}
	}
	for from := 0; from < sources; from += 24 {
		nodes := make([]*storage.Node, 0, 24)
		edges := make([]*storage.Edge, 0, 24)
		for i := from; i < min(sources, from+24); i++ {
			// A deterministic distribution around the observed 88 chunks per
			// source. Pair offsets cancel so the total stays exact.
			chunks := stallVectors / sources
			if i < stallVectors%sources {
				chunks++
			}
			offset := 7 * ((i / 2) % 10)
			if i%2 != 0 {
				offset = -offset
			}
			if i+1 == sources {
				offset = 0
			}
			chunks += offset
			text := stallText(i, chunks)
			id := fmt.Sprintf("source-%06d", i)
			version := fmt.Sprintf("PDVersion-%06d", i)
			node := &storage.Node{
				ID: storage.NodeID(id), Labels: []string{"PDSearchSource"},
				Properties: map[string]any{"id": id, "document_id": fmt.Sprintf("PDDocument-%06d", i),
					"version_id": version, "content_hash": fmt.Sprintf("synthetic-%06d", i), "text": text,
					"view_text": text, "search_hidden": false},
				ChunkEmbeddings: make([][]float32, chunks),
				EmbedMeta: map[string]any{"has_embedding": true, "has_chunks": true, "chunk_count": int64(chunks),
					"embedding_dimensions": int64(stallDims), "embedding_model": "voyage-context-4"},
			}
			for chunk := range node.ChunkEmbeddings {
				vector := make([]float32, stallDims)
				for d := range vector {
					vector[d] = (random.Float32() - 0.5) * 0.07654655
				}
				vector[i%128] += 0.2
				node.ChunkEmbeddings[chunk] = vector
			}
			nodes = append(nodes, node)
			edges = append(edges, &storage.Edge{ID: storage.EdgeID("has-" + id), StartNode: storage.NodeID(version),
				EndNode: node.ID, Type: "HAS_SOURCE"})
		}
		writeNodes(nodes)
		if err := store.BulkCreateEdges(ctx, edges); err != nil {
			t.Fatal(err)
		}
		if from%240 == 0 {
			fmt.Printf("PD_STAGE seeded sources=%d\n", from+len(nodes))
		}
	}
	count, err := store.NodeCount(ctx)
	if err != nil || count != stallNodes {
		t.Fatalf("fixture nodes=%d want=%d error=%v", count, stallNodes, err)
	}
	fmt.Printf("PD_STAGE fixture nodes=%d source_nodes=%d stored_vectors=%d dims=%d\n", count, sources, stallVectors, stallDims)
}

func stallText(source, chunks int) string {
	var text strings.Builder
	text.Grow(chunks * 480)
	words := []string{"archive", "family", "document", "reference", "application", "record", "application", "identifier",
		"submitted", "naturalisation", "residence", "permit", "issued", "assessment", "statement", "certificate"}
	for c := 0; c < chunks; c++ {
		for j := 0; j < 64; j++ {
			if j%3 == 0 {
				text.WriteString(words[(source/17+c+j)%len(words)])
			} else {
				text.WriteString("term")
				text.WriteString(strconv.FormatInt(int64(((source+1)*131+c*37+j*59+j*c)%8192), 36))
			}
			text.WriteByte(' ')
		}
	}
	return text.String()
}

type stallServerLog struct {
	mu        sync.Mutex
	lines     []string
	clean     bool
	begin     time.Time
	iteration time.Time
	total     int
}

var stallProcessed = regexp.MustCompile(`base index iteration complete \(processed=(\d+)\)`)

func (l *stallServerLog) add(mode, line string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, line)
	if len(l.lines) > 32 {
		l.lines = l.lines[1:]
	}
	if strings.Contains(line, "Clean shutdown detected") {
		l.clean = true
	}
	if strings.Contains(line, "BuildIndexes: building from storage") && l.begin.IsZero() {
		l.begin = time.Now()
	}
	if m := stallProcessed.FindStringSubmatch(line); m != nil {
		count, _ := strconv.Atoi(m[1])
		if count > l.total {
			l.iteration, l.total = time.Now(), count
		}
	}
	for _, marker := range []string{"PD_STAGE", "PD_CAUSE", "Clean shutdown", "MVCC lifecycle manager", "Building BM25", "BuildIndexes",
		"embeddings ready", "queue started", "Warming up vector search pipeline", "server listening", "NornicDB is ready", "signal received", "Shutdown complete",
		"slow query", "fatal error", "panic:"} {
		if strings.Contains(line, marker) {
			fmt.Printf("PD_SERVER mode=%s received=%s %s\n", mode, time.Now().UTC().Format(time.RFC3339Nano), line)
			return
		}
	}
}

func (l *stallServerLog) tail() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

func (l *stallServerLog) cleanStart() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.clean
}

func (l *stallServerLog) timings() (time.Time, time.Time, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.begin, l.iteration, l.total
}

func stallMemory(mode string, pid int, done <-chan struct{}) {
	timer := time.NewTicker(time.Second)
	defer timer.Stop()
	var highest int
	for {
		select {
		case <-done:
			fmt.Printf("PD_MEMORY mode=%s process_peak_kib=%d\n", mode, highest)
			return
		case <-timer.C:
			data, _ := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
			for line := range strings.SplitSeq(string(data), "\n") {
				if strings.HasPrefix(line, "VmHWM:") {
					_, _ = fmt.Sscanf(line, "VmHWM: %d kB", &highest)
					break
				}
			}
		}
	}
}

type stallHTTP struct {
	base   string
	client *http.Client
}

func (h stallHTTP) call(ctx context.Context, method, path, body string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, method, h.base+path, strings.NewReader(body))
	if err != nil {
		return "", err
	}
	if path != "/auth/token" {
		req.SetBasicAuth("admin", stallPassword)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := h.client.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	var reply struct {
		Commit string            `json:"commit"`
		Token  string            `json:"access_token"`
		Errors []json.RawMessage `json:"errors"`
	}
	if err := json.NewDecoder(res.Body).Decode(&reply); err != nil {
		return "", fmt.Errorf("HTTP %d decode: %w", res.StatusCode, err)
	}
	if res.StatusCode/100 != 2 || len(reply.Errors) != 0 {
		return "", fmt.Errorf("HTTP %d errors=%s", res.StatusCode, reply.Errors)
	}
	if path == "/auth/token" && reply.Token == "" {
		return "", fmt.Errorf("HTTP login returned no token")
	}
	return reply.Commit, nil
}

type stallLane struct {
	name  string
	run   func(context.Context, int) (func(context.Context), error)
	mu    sync.Mutex
	since time.Time
	ok    int
	fail  int
	max   time.Duration
	slow  int
}

func (l *stallLane) loop(ctx context.Context) {
	for seq := 0; ctx.Err() == nil; seq++ {
		at := time.Now()
		l.mu.Lock()
		l.since = at
		l.mu.Unlock()
		op, cancel := context.WithTimeout(ctx, 70*time.Second)
		cleanup, err := l.run(op, seq)
		elapsed := time.Since(at)
		l.mu.Lock()
		l.since = time.Time{}
		if err == nil {
			l.ok++
			l.max = max(l.max, elapsed)
			if elapsed >= 25*time.Second {
				l.slow++
			}
		} else {
			l.fail++
		}
		l.mu.Unlock()
		fmt.Printf("PD_PROBE lane=%s seq=%d start=%s end=%s latency_ms=%.3f error=%q\n",
			l.name, seq, at.UTC().Format(time.RFC3339Nano), at.Add(elapsed).UTC().Format(time.RFC3339Nano),
			float64(elapsed)/float64(time.Millisecond), fmt.Sprint(err))
		if cleanup != nil {
			cleanup(op)
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-time.After(max(0, stallCadence-time.Since(at))):
		}
	}
}

func stallProbe(httpAddress, boltAddress, pprofAddress string, started time.Time, logs *stallServerLog) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var group sync.WaitGroup
	lanes := []*stallLane{}
	providers := []struct {
		name, address string
		lanes         []*stallLane
	}{
		{"http", httpAddress, stallHTTPLanes(httpAddress)},
		{"bolt", boltAddress, stallBoltLanes(boltAddress)},
	}
	for _, provider := range providers {
		lanes = append(lanes, provider.lanes...)
		group.Go(func() {
			if err := stallWaitListener(ctx, provider.address); err != nil {
				return
			}
			fmt.Printf("PD_READY wire=%s at=%s elapsed_ms=%d\n", provider.name, time.Now().UTC().Format(time.RFC3339Nano), time.Since(started).Milliseconds())
			var local sync.WaitGroup
			for _, lane := range provider.lanes {
				local.Go(func() { lane.loop(ctx) })
			}
			local.Wait()
		})
	}

	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	thresholds := []time.Duration{3 * time.Second, 10 * time.Second, 22 * time.Second}
	for tick := range ticker.C {
		began, iterated, count := logs.timings()
		if count >= stallNodes && tick.Sub(iterated) >= 20*time.Second && tick.Sub(began) >= 60*time.Second {
			break
		}
		if tick.Sub(started) >= 8*time.Minute {
			break
		}
		if len(thresholds) == 0 || began.IsZero() || !iterated.IsZero() {
			continue
		}
		var stalled []string
		for _, lane := range lanes {
			lane.mu.Lock()
			if !lane.since.IsZero() && tick.Sub(lane.since) >= thresholds[0] {
				stalled = append(stalled, fmt.Sprintf("%s:%dms", lane.name, tick.Sub(lane.since).Milliseconds()))
			}
			lane.mu.Unlock()
		}
		if len(stalled) > 0 {
			mark := thresholds[0]
			thresholds = thresholds[1:]
			group.Go(func() { stallDump(pprofAddress, mark, stalled) })
		}
	}
	cancel()
	group.Wait()
	began, iterated, count := logs.timings()
	fmt.Printf("PD_ITERATION begin=%s complete=%s processed=%d elapsed_ms=%d\n",
		began.UTC().Format(time.RFC3339Nano), iterated.UTC().Format(time.RFC3339Nano), count, iterated.Sub(began).Milliseconds())
	if count < stallNodes {
		return fmt.Errorf("startup index build processed %d of %d nodes", count, stallNodes)
	}
	for _, lane := range lanes {
		fmt.Printf("PD_SUMMARY lane=%s ok=%d failed=%d max_ms=%.3f at_least_25s=%d\n",
			lane.name, lane.ok, lane.fail, float64(lane.max)/float64(time.Millisecond), lane.slow)
		if lane.ok == 0 {
			return fmt.Errorf("%s had no successful probes", lane.name)
		}
	}
	return nil
}

func stallWaitListener(ctx context.Context, address string) error {
	for ctx.Err() == nil {
		conn, err := (&net.Dialer{Timeout: 50 * time.Millisecond}).DialContext(ctx, "tcp", address)
		if err == nil {
			conn.Close()
			return nil
		}
		select {
		case <-ctx.Done():
		case <-time.After(20 * time.Millisecond):
		}
	}
	return ctx.Err()
}

func stallDump(address string, threshold time.Duration, stalled []string) {
	client := &http.Client{Timeout: 5 * time.Second}
	at := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := client.Get("http://" + address + "/debug/pprof/goroutine?debug=2")
	if err != nil {
		fmt.Printf("PD_DUMP at=%s threshold=%s stalled=%v error=%q\n", at, threshold, stalled, err)
		return
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil || res.StatusCode != http.StatusOK {
		fmt.Printf("PD_DUMP at=%s threshold=%s stalled=%v status=%d error=%v\n", at, threshold, stalled, res.StatusCode, err)
		return
	}
	var selected strings.Builder
	for stack := range strings.SplitSeq(string(data), "\n\n") {
		for _, owner := range []string{"pkg/server.(*Server).handle", "pkg/bolt.(*Session).handle", "pkg/auth.(*Authenticator).Authenticate",
			"pkg/search.(*Service).BuildIndexes", "pkg/nornicdb.(*DB).", "pkg/storage.(*Badger", "badger/v4", "ristretto", "pkg/storage.(*WAL"} {
			if strings.Contains(stack, owner) {
				selected.WriteString(stack)
				selected.WriteString("\n\n")
				break
			}
		}
	}
	fmt.Printf("PD_DUMP at=%s threshold=%s stalled=%v raw_bytes=%d selected_bytes=%d\n%sPD_DUMP_END\n",
		at, threshold, stalled, len(data), selected.Len(), selected.String())
}

func stallHTTPLanes(address string) []*stallLane {
	h := stallHTTP{base: "http://" + address, client: &http.Client{}}
	return []*stallLane{
		{name: "http_begin", run: func(ctx context.Context, _ int) (func(context.Context), error) {
			commit, err := h.call(ctx, http.MethodPost, stallTx, `{"statements":[]}`)
			if err != nil {
				return nil, err
			}
			u, err := url.Parse(commit)
			if err != nil || !strings.HasSuffix(u.Path, "/commit") {
				return nil, fmt.Errorf("missing transaction commit URL: %q", commit)
			}
			return func(ctx context.Context) {
				if _, err := h.call(ctx, http.MethodDelete, strings.TrimSuffix(u.Path, "/commit"), ""); err != nil && ctx.Err() == nil {
					fmt.Printf("PD_CLEANUP wire=http error=%q\n", err)
				}
			}, nil
		}},
		{name: "http_write", run: func(ctx context.Context, seq int) (func(context.Context), error) {
			body := fmt.Sprintf(`{"statements":[{"statement":"MERGE (p:PDLabProbe {id:$id}) SET p.at=timestamp(), p.kind='auto'","parameters":{"id":"http-%d"}}]}`, seq)
			_, err := h.call(ctx, http.MethodPost, stallTx+"/commit", body)
			return nil, err
		}},
		{name: "http_login", run: func(ctx context.Context, _ int) (func(context.Context), error) {
			_, err := h.call(ctx, http.MethodPost, "/auth/token", `{"username":"admin","password":"`+stallPassword+`"}`)
			return nil, err
		}},
	}
}

// Use Bolt 5.0 one-message acknowledgements. A driver may pipeline BEGIN
// with the first statement, which would conflate the timings under test.
func stallBoltLanes(address string) []*stallLane {
	db := stallMap("db", stallDB)
	var beginConn, writeConn net.Conn
	ensure := func(ctx context.Context, conn *net.Conn) error {
		if *conn == nil {
			fresh, err := stallBoltHello(ctx, address)
			if err != nil {
				return err
			}
			*conn = fresh
		}
		deadline, _ := ctx.Deadline()
		return (*conn).SetDeadline(deadline)
	}
	closeOnError := func(conn *net.Conn, err error) error {
		if err != nil && *conn != nil {
			(*conn).Close()
			*conn = nil
		}
		return err
	}
	return []*stallLane{
		{name: "bolt_begin", run: func(ctx context.Context, _ int) (func(context.Context), error) {
			err := ensure(ctx, &beginConn)
			if err == nil {
				err = stallBoltMessage(beginConn, append([]byte{0xb1, 0x11}, db...))
			}
			if err != nil {
				return nil, closeOnError(&beginConn, err)
			}
			return func(context.Context) {
				if err := closeOnError(&beginConn, stallBoltMessage(beginConn, []byte{0xb0, 0x13})); err != nil {
					fmt.Printf("PD_CLEANUP wire=bolt error=%q\n", err)
				}
			}, nil
		}},
		{name: "bolt_write", run: func(ctx context.Context, seq int) (func(context.Context), error) {
			if err := ensure(ctx, &writeConn); err != nil {
				return nil, closeOnError(&writeConn, err)
			}
			run := stallString([]byte{0xb3, 0x10}, "MERGE (p:PDLabProbe {id:$id}) SET p.at=timestamp(), p.kind='auto'")
			run = append(run, stallMap("id", fmt.Sprintf("bolt-%d", seq))...)
			run = append(run, db...)
			err := stallBoltMessage(writeConn, run)
			if err == nil {
				err = stallBoltMessage(writeConn, []byte{0xb1, 0x3f, 0xa1, 0x81, 'n', 0xff})
			}
			return nil, closeOnError(&writeConn, err)
		}},
		{name: "bolt_login", run: func(ctx context.Context, _ int) (func(context.Context), error) {
			conn, err := stallBoltHello(ctx, address)
			if err == nil {
				conn.Close()
			}
			return nil, err
		}},
	}
}

func stallBoltHello(ctx context.Context, address string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	deadline, _ := ctx.Deadline()
	conn.SetDeadline(deadline)
	var version [4]byte
	_, err = conn.Write([]byte{0x60, 0x60, 0xb0, 0x17, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	if err == nil {
		_, err = io.ReadFull(conn, version[:])
	}
	if err == nil && version != [4]byte{0, 0, 0, 5} {
		err = fmt.Errorf("unexpected Bolt version %x", version)
	}
	if err == nil {
		err = stallBoltMessage(conn, append([]byte{0xb1, 0x01},
			stallMap("user_agent", "pd-startup-probe/1", "scheme", "basic", "principal", "admin", "credentials", stallPassword)...))
	}
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func stallBoltMessage(conn net.Conn, payload []byte) error {
	wire := binary.BigEndian.AppendUint16(nil, uint16(len(payload)))
	wire = append(append(wire, payload...), 0, 0)
	if _, err := conn.Write(wire); err != nil {
		return err
	}
	var reply bytes.Buffer
	for {
		var header [2]byte
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			return err
		}
		n := int64(binary.BigEndian.Uint16(header[:]))
		if n != 0 {
			if _, err := io.CopyN(&reply, conn, n); err != nil {
				return err
			}
		} else if reply.Len() > 0 {
			data := reply.Bytes()
			if len(data) < 2 || data[1] != 0x70 {
				return fmt.Errorf("Bolt %02x response %q", payload[1], data)
			}
			return nil
		}
	}
}

func stallMap(keysAndValues ...string) []byte {
	data := []byte{byte(0xa0 + len(keysAndValues)/2)}
	for _, text := range keysAndValues {
		data = stallString(data, text)
	}
	return data
}

func stallString(data []byte, text string) []byte {
	if len(text) < 16 {
		data = append(data, byte(0x80+len(text)))
	} else {
		data = append(data, 0xd0, byte(len(text)))
	}
	return append(data, text...)
}
