package main

// Investigation probe (untracked, 13 Nov 2026): where the time of a large
// read statement with nested CALL subqueries goes, over Bolt, on generated
// data. The stack is wired as runServe wires it (copied from the NOR-357
// wire probe): one database, the HTTP server, Bolt on the HTTP server's
// database manager, authentication on. Every statement runs in its own
// explicit READ transaction (BEGIN, RUN, PULL all, COMMIT), timed from BEGIN
// to COMMIT on the client. Observation only: the tests log EFFL lines and
// fail only when a statement errors.

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/server"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type efflStack struct {
	t      *testing.T
	driver neo4j.DriverWithContext
}

func efflStart(t *testing.T) *efflStack {
	t.Helper()
	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Database.AsyncWritesEnabled = false
	db, err := nornicdb.Open(t.TempDir(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{SecurityEnabled: true, JWTSecret: []byte("test-secret-key-for-testing-only-32b")},
		storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system"))
	if err != nil {
		t.Fatal(err)
	}
	create := reflect.ValueOf(authenticator).MethodByName("CreateUser")
	var args []reflect.Value
	if create.Type().NumIn() == 4 {
		args = append(args, reflect.ValueOf(context.Background()))
	}
	args = append(args, reflect.ValueOf("admin"), reflect.ValueOf("password123"), reflect.ValueOf([]auth.Role{auth.RoleAdmin}))
	if out := create.Call(args); !out[1].IsNil() {
		t.Fatal(out[1].Interface())
	}

	httpConfig := server.DefaultConfig()
	httpConfig.Address = "127.0.0.1"
	httpConfig.Port = 0
	httpConfig.EmbeddingEnabled = false
	httpConfig.MCPEnabled = false
	httpConfig.Headless = true
	httpServer, err := server.New(db, authenticator, httpConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := httpServer.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Stop(ctx)
	})

	boltConfig := bolt.DefaultConfig()
	boltConfig.Port = 0
	boltAuth := bolt.NewAuthenticatorAdapter(authenticator)
	boltAuth.SetGetEffectivePermissions(httpServer.GetEffectivePermissions)
	boltConfig.Authenticator = boltAuth
	boltConfig.RequireAuth = true
	if field := reflect.ValueOf(boltConfig).Elem().FieldByName("DatabaseExecutorOptions"); field.IsValid() {
		if method := reflect.ValueOf(httpServer).MethodByName("DatabaseScopedExecutorOptions"); method.IsValid() {
			field.Set(method)
		}
	}
	boltServer := bolt.NewWithDatabaseManager(boltConfig, NewDBQueryExecutor(db), httpServer.GetDatabaseManager())
	boltServer.SetMemoryPolicy(db.MemoryPolicy())
	httpServer.SetConnectionLister(boltServer.ConnectionListings)
	tracker := multidb.NewConnectionTracker()
	boltServer.SetDatabaseConnectionAdmission(httpServer.GetDatabaseManager().ResolveConnectionLimitOwner, func(databaseName string) error {
		return tracker.TryIncrementConnection(httpServer.GetDatabaseManager(), databaseName)
	}, tracker.DecrementConnection)
	boltServer.SetDatabaseAccessModeResolver(httpServer.GetDatabaseAccessModeForRoles)
	boltServer.SetResolvedAccessResolver(httpServer.GetResolvedAccessForRoles)
	boltServer.SetProcedureGrantResolver(httpServer.ProcedureGrantChecker)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- boltServer.Serve(listener) }()
	t.Cleanup(func() {
		_ = boltServer.Close()
		select {
		case <-served:
		case <-time.After(10 * time.Second):
		}
	})
	driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.BasicAuth("admin", "password123", ""))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = driver.Close(ctx)
	})
	return &efflStack{t: t, driver: driver}
}

// write runs one auto-commit statement.
func (s *efflStack) write(query string, params map[string]any) {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	session := s.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	defer session.Close(ctx)
	result, err := session.Run(ctx, query, params)
	if err == nil {
		_, err = result.Consume(ctx)
	}
	if err != nil {
		s.t.Fatalf("write %.80q: %v", query, err)
	}
}

// read runs one statement in its own explicit read transaction, as the
// reporting client does, and returns the records and the BEGIN..COMMIT time.
func (s *efflStack) read(query string, params map[string]any) ([]*neo4j.Record, time.Duration) {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	session := s.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic", AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)
	start := time.Now()
	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		s.t.Fatalf("begin: %v", err)
	}
	result, err := tx.Run(ctx, query, params)
	if err != nil {
		_ = tx.Rollback(ctx)
		s.t.Fatalf("run %.80q: %v", query, err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		_ = tx.Rollback(ctx)
		s.t.Fatalf("collect %.80q: %v", query, err)
	}
	if err := tx.Commit(ctx); err != nil {
		s.t.Fatalf("commit: %v", err)
	}
	return records, time.Since(start)
}

func efflEnvInts(name string, def []int) []int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	var out []int
	for _, f := range strings.Split(v, ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(f)); err == nil {
			out = append(out, n)
		}
	}
	return out
}

func efflEnvInt(name string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return n
	}
	return def
}

// ---------------------------------------------------------------------------
// Data of the reported shape (generated; all ids are ours).
//
// One document node D (:PDGraphNode:PDDocument) and K = round(links*238/1000)
// owned nodes (:PDGraphNode, document_id = D). Links (all :PD_RELATION with
// an id, origin automatic, stage interpretation, current true, document_id D):
//   - about 52.5% start at D and end at owned node i mod K;
//   - each owned node has 2 links to the next two owned nodes.
// A :PDVersion of D exists; no :PDCorrection exists (none is selected by the
// reported calls). Optionally bg further documents of the same shape.
// ---------------------------------------------------------------------------

type efflData struct {
	doc        string
	owned      []string
	pairs      []map[string]any // {from, link} for every link of document 0
	firstOwned string
}

func efflSchema(s *efflStack) {
	for _, q := range []string{
		"CREATE CONSTRAINT pd_graph_id IF NOT EXISTS FOR (n:PDGraphNode) REQUIRE n.id IS UNIQUE",
		"CREATE CONSTRAINT pd_document_id IF NOT EXISTS FOR (n:PDDocument) REQUIRE n.id IS UNIQUE",
		"CREATE CONSTRAINT pd_version_id IF NOT EXISTS FOR (n:PDVersion) REQUIRE n.id IS UNIQUE",
		"CREATE CONSTRAINT pd_definition_id IF NOT EXISTS FOR (n:PDDefinition) REQUIRE n.id IS UNIQUE",
		"CREATE CONSTRAINT pd_correction_id IF NOT EXISTS FOR (n:PDCorrection) REQUIRE n.id IS UNIQUE",
		"CREATE CONSTRAINT pd_ocr_report_id IF NOT EXISTS FOR (n:PDOcrReport) REQUIRE n.id IS UNIQUE",
		"CREATE INDEX pd_version_document IF NOT EXISTS FOR (n:PDVersion) ON (n.document_id)",
		"CREATE INDEX pd_document_current_version IF NOT EXISTS FOR (n:PDDocument) ON (n.current_version_id)",
		"CREATE INDEX pd_graph_document IF NOT EXISTS FOR (n:PDGraphNode) ON (n.document_id)",
		"CREATE INDEX pd_correction_status IF NOT EXISTS FOR (n:PDCorrection) ON (n.status)",
	} {
		s.write(q, nil)
	}
}

func efflLoad(s *efflStack, links, docs int) efflData {
	var out efflData
	for d := 0; d < docs; d++ {
		doc := fmt.Sprintf("doc-%d", d)
		k := (links*238 + 500) / 1000
		if k < 3 {
			k = 3
		}
		fromDoc := links - 2*k
		if fromDoc < 0 {
			fromDoc = 0
		}
		s.write("CREATE (:PDGraphNode:PDDocument {id: $doc, kind: 'document', current: true, current_version_id: $doc + '-v1', text: $text})",
			map[string]any{"doc": doc, "text": strings.Repeat("d", 200)})
		s.write("CREATE (:PDVersion {id: $doc + '-v1', document_id: $doc})", map[string]any{"doc": doc})
		owned := make([]any, k)
		ids := make([]string, k)
		for i := 0; i < k; i++ {
			ids[i] = fmt.Sprintf("%s-n%04d", doc, i)
			owned[i] = map[string]any{"id": ids[i], "name": fmt.Sprintf("entity %d of %s", i, doc)}
		}
		s.write("UNWIND $rows AS row CREATE (:PDGraphNode {id: row.id, document_id: $doc, kind: 'entity', current: true, name: row.name})",
			map[string]any{"rows": owned, "doc": doc})
		var rels []any
		var pairs []map[string]any
		for j := 0; j < fromDoc; j++ {
			id := fmt.Sprintf("%s-l%05d", doc, len(rels))
			rels = append(rels, map[string]any{"a": doc, "b": ids[j%k], "id": id})
			pairs = append(pairs, map[string]any{"from": doc, "link": id})
		}
		for i := 0; i < k; i++ {
			for step := 1; step <= 2; step++ {
				id := fmt.Sprintf("%s-l%05d", doc, len(rels))
				rels = append(rels, map[string]any{"a": ids[i], "b": ids[(i+step)%k], "id": id})
				pairs = append(pairs, map[string]any{"from": ids[i], "link": id})
			}
		}
		for start := 0; start < len(rels); start += 500 {
			end := start + 500
			if end > len(rels) {
				end = len(rels)
			}
			s.write(`UNWIND $rows AS row
MATCH (a:PDGraphNode {id: row.a}), (b:PDGraphNode {id: row.b})
CREATE (a)-[:PD_RELATION {id: row.id, kind: 'same_document', origin: 'automatic', stage: 'interpretation', current: true, document_id: $doc}]->(b)`,
				map[string]any{"rows": rels[start:end], "doc": doc})
		}
		if d == 0 {
			out = efflData{doc: doc, owned: ids, pairs: pairs, firstOwned: ids[0]}
		}
	}
	return out
}

func efflParams(links []map[string]any, nodeIDs, documents []string) map[string]any {
	l := make([]any, len(links))
	for i, m := range links {
		l[i] = m
	}
	toAny := func(xs []string) []any {
		out := make([]any, len(xs))
		for i, x := range xs {
			out[i] = x
		}
		return out
	}
	return map[string]any{"links": l, "node_ids": toAny(nodeIDs), "documents": toAny(documents),
		"correction_ids": []any{}, "gone_ids": []any{}, "selected": []any{}}
}

// The four reported calls, in order, on our data: (1) all link pairs,
// (2) documents [D], (3) node_ids [one owned node], (4) 88% of the pairs.
func efflCalls(d efflData) []struct {
	name   string
	params map[string]any
} {
	cut := len(d.pairs) * 881 / 999
	sub := make([]map[string]any, 0, cut)
	for i, p := range d.pairs {
		if i%9 != 4 && len(sub) < cut {
			sub = append(sub, p)
		}
	}
	return []struct {
		name   string
		params map[string]any
	}{
		{"call1-pairs", efflParams(d.pairs, nil, nil)},
		{"call2-document", efflParams(nil, nil, []string{d.doc})},
		{"call3-one-node", efflParams(nil, []string{d.firstOwned}, nil)},
		{"call4-subset-pairs", efflParams(sub, nil, nil)},
	}
}

func efflStatement(t *testing.T) []string {
	t.Helper()
	b, err := os.ReadFile("v152p_efflink_statement.cypher")
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimRight(string(b), "\n"), "\n")
}

// Cut-down variants of the statement (line numbers of the reported text).
func efflVariants(lines []string) map[string]string {
	head := func(n int) string { return strings.Join(lines[:n], "\n") + "\n" }
	return map[string]string{
		"full": strings.Join(lines, "\n"),
		// lines 1-75: gathering of the links (named, incident, concerned)
		"A-gather": head(75) + "  RETURN size(effl_found_links) AS effl_n\n}\nRETURN effl_n",
		// + per-link rows, nothing evaluated per link
		"B-rows": head(78) + "    RETURN count(effl_r) AS effl_rows\n  }\n  RETURN effl_rows AS effl_n\n}\nRETURN effl_n",
		// + per-link keys and the per-link correction lookup subquery (lines 80-104)
		"C-gathered": head(104) + "  RETURN size(eff_gathered) AS eff_g\n}\n    RETURN count(eff_g) AS effl_rows\n  }\n  RETURN effl_rows AS effl_n\n}\nRETURN effl_n",
		// + the whole per-link view subquery (lines 79-150)
		"D-view": head(150) + "\n    RETURN count(eff_view) AS effl_rows\n  }\n  RETURN effl_rows AS effl_n\n}\nRETURN effl_n",
		// + the per-link row evaluation and collect (lines 151-197)
		"E-perlink": head(197) + "\n  RETURN size(effl_rows) AS effl_n\n}\nRETURN effl_n",
	}
}

type efflStats struct {
	first time.Duration
	reps  []time.Duration
}

func (st efflStats) String() string {
	r := append([]time.Duration(nil), st.reps...)
	sort.Slice(r, func(i, j int) bool { return r[i] < r[j] })
	ms := func(d time.Duration) string { return strconv.FormatFloat(float64(d)/1e6, 'f', 1, 64) }
	var all []string
	for _, d := range st.reps {
		all = append(all, ms(d))
	}
	if len(r) == 0 {
		return "first=" + ms(st.first)
	}
	return fmt.Sprintf("first=%s median=%s min=%s max=%s reps=[%s]", ms(st.first), ms(r[len(r)/2]), ms(r[0]), ms(r[len(r)-1]), strings.Join(all, " "))
}

func efflMeasure(s *efflStack, q string, p map[string]any, reps int) (efflStats, []*neo4j.Record) {
	recs, first := s.read(q, p)
	st := efflStats{first: first}
	for i := 0; i < reps; i++ {
		_, d := s.read(q, p)
		st.reps = append(st.reps, d)
	}
	return st, recs
}

func efflRowsOf(recs []*neo4j.Record) string {
	if len(recs) == 0 {
		return "records=0"
	}
	r := recs[0]
	if v, ok := r.Get("rows"); ok {
		if l, ok := v.([]any); ok {
			return fmt.Sprintf("rows=%d", len(l))
		}
	}
	if v, ok := r.Get("effl_n"); ok {
		return fmt.Sprintf("n=%v", v)
	}
	if v, ok := r.Get("n"); ok {
		return fmt.Sprintf("n=%v", v)
	}
	return fmt.Sprintf("records=%d", len(recs))
}

// efflProfile takes a CPU profile of reps runs of q and prints go tool pprof
// -top (flat and cumulative) for the test binary.
func efflProfile(t *testing.T, s *efflStack, label, q string, p map[string]any, reps int) {
	path := filepath.Join(t.TempDir(), "cpu.pprof")
	f, err := os.Create(path)
	if err != nil {
		t.Logf("EFFL profile %s: %v", label, err)
		return
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		t.Logf("EFFL profile %s: %v", label, err)
		_ = f.Close()
		return
	}
	start := time.Now()
	for i := 0; i < reps; i++ {
		s.read(q, p)
	}
	pprof.StopCPUProfile()
	_ = f.Close()
	t.Logf("EFFL profile %s runs=%d wall=%s", label, reps, time.Since(start).Round(time.Millisecond))
	goBin := filepath.Join(runtime.GOROOT(), "bin", "go")
	for _, mode := range [][]string{{"-top", "-nodecount=45"}, {"-top", "-cum", "-nodecount=70"}} {
		args := append([]string{"tool", "pprof"}, mode...)
		args = append(args, os.Args[0], path)
		cmd := exec.Command(goBin, args...)
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out
		err := cmd.Run()
		t.Logf("EFFL pprof %s %v err=%v\n%s", label, mode, err, out.String())
	}
}

// TestEfflinkReported: the reported statement on data of the reported shape
// at about 1,000 links: the four calls in order on a fresh server (cold), then
// repeated; the cut-down variants; and a CPU profile of call 2.
func TestEfflinkReported(t *testing.T) {
	links := efflEnvInt("EFFL_LINKS", 1000)
	reps := efflEnvInt("EFFL_REPS", 5)
	lines := efflStatement(t)
	variants := efflVariants(lines)
	s := efflStart(t)
	efflSchema(s)
	start := time.Now()
	d := efflLoad(s, links, 1)
	t.Logf("EFFL load links=%d pairs=%d owned=%d in %s", links, len(d.pairs), len(d.owned), time.Since(start).Round(time.Millisecond))
	calls := efflCalls(d)
	full := variants["full"]

	// Cold pass in the reported order, then reps more passes.
	byCall := map[string]*efflStats{}
	for pass := 0; pass <= reps; pass++ {
		for _, c := range calls {
			recs, dur := s.read(full, c.params)
			st := byCall[c.name]
			if st == nil {
				st = &efflStats{first: dur}
				byCall[c.name] = st
				t.Logf("EFFL cold %s %s ms=%.1f", c.name, efflRowsOf(recs), float64(dur)/1e6)
			} else {
				st.reps = append(st.reps, dur)
			}
		}
	}
	for _, c := range calls {
		t.Logf("EFFL full %s links=%d %s", c.name, links, byCall[c.name])
	}
	empty := efflParams(nil, nil, nil)
	st, recs := efflMeasure(s, full, empty, reps)
	t.Logf("EFFL full empty-params %s %s", efflRowsOf(recs), st)
	st, recs = efflMeasure(s, "RETURN 1 AS one", nil, reps)
	t.Logf("EFFL baseline return1 %s %s", efflRowsOf(recs), st)

	for _, name := range []string{"A-gather", "B-rows", "C-gathered", "D-view", "E-perlink"} {
		for _, ci := range []int{1, 0} {
			st, recs := efflMeasure(s, variants[name], calls[ci].params, reps)
			t.Logf("EFFL variant %s %s %s %s", name, calls[ci].name, efflRowsOf(recs), st)
		}
	}
	efflProfile(t, s, "full-call2", full, calls[1].params, efflEnvInt("EFFL_PROF_REPS", 6))
}

// TestEfflinkFirstCall: is the first call's extra time owed to the first
// execution of this statement text in the process, or to the first read
// after the load? Fresh server each case.
func TestEfflinkFirstCall(t *testing.T) {
	links := efflEnvInt("EFFL_LINKS", 1000)
	lines := efflStatement(t)
	full := efflVariants(lines)["full"]
	orders := []string{"call1-first", "call3-then-call1", "other-read-then-call1", "call1-twice-then-variant-text"}
	if v := os.Getenv("EFFL_ORDERS"); v != "" {
		orders = strings.Split(v, ",")
	}
	profileFirst := os.Getenv("EFFL_PROFILE_FIRST") != ""
	for _, order := range orders {
		s := efflStart(t)
		efflSchema(s)
		d := efflLoad(s, links, 1)
		calls := efflCalls(d)
		var log []string
		runs := 0
		run := func(label, q string, p map[string]any) {
			runs++
			if profileFirst && runs <= 2 {
				efflProfile(t, s, fmt.Sprintf("%s-run%d-%s", order, runs, label), q, p, 1)
				return
			}
			recs, dur := s.read(q, p)
			log = append(log, fmt.Sprintf("%s(%s)=%.1f", label, efflRowsOf(recs), float64(dur)/1e6))
		}
		switch order {
		case "call1-first":
			run("call1", full, calls[0].params)
			run("call1", full, calls[0].params)
		case "call3-then-call1":
			run("call3", full, calls[2].params)
			run("call1", full, calls[0].params)
			run("call1", full, calls[0].params)
		case "other-read-then-call1":
			run("scan", "MATCH (n:PDGraphNode)-[r]->(m) RETURN count(r) AS c", nil)
			run("call1", full, calls[0].params)
			run("call1", full, calls[0].params)
		case "call2-first":
			run("call2", full, calls[1].params)
			run("call2", full, calls[1].params)
			run("call1", full, calls[0].params)
		case "call1-twice-then-variant-text":
			run("call1", full, calls[0].params)
			run("call1", full, calls[0].params)
			run("call1-newtext", "// variant\n"+full, calls[0].params)
			run("call1-newtext", "// variant\n"+full, calls[0].params)
		}
		t.Logf("EFFL first %s links=%d %s", order, links, strings.Join(log, " "))
	}
}

// TestEfflinkScale: the statement and its variants at several sizes, with
// and without other documents of the same shape in the database.
func TestEfflinkScale(t *testing.T) {
	sizes := efflEnvInts("EFFL_SIZES", []int{250, 500, 1000, 2000})
	bgs := efflEnvInts("EFFL_BG", []int{1, 5})
	reps := efflEnvInt("EFFL_REPS", 3)
	variants := efflVariants(efflStatement(t))
	for _, docs := range bgs {
		for _, n := range sizes {
			if docs > 1 && n != 1000 {
				continue
			}
			s := efflStart(t)
			efflSchema(s)
			d := efflLoad(s, n, docs)
			calls := efflCalls(d)
			for _, name := range []string{"full", "B-rows", "C-gathered", "D-view", "E-perlink"} {
				st, recs := efflMeasure(s, variants[name], calls[1].params, reps)
				t.Logf("EFFL scale docs=%d links=%d variant=%s call2-document %s %s", docs, n, name, efflRowsOf(recs), st)
			}
			st, recs := efflMeasure(s, variants["full"], calls[0].params, reps)
			t.Logf("EFFL scale docs=%d links=%d variant=full call1-pairs %s %s", docs, n, efflRowsOf(recs), st)
		}
	}
}

// TestEfflinkProfile: CPU profile of call 2 at 1,000 links with the callers
// and callees of the text-reading and storage paths.
func TestEfflinkProfile(t *testing.T) {
	links := efflEnvInt("EFFL_LINKS", 1000)
	full := efflVariants(efflStatement(t))["full"]
	s := efflStart(t)
	efflSchema(s)
	d := efflLoad(s, links, 1)
	calls := efflCalls(d)
	s.read(full, calls[1].params)
	s.read(full, calls[1].params)
	path := filepath.Join(t.TempDir(), "cpu.pprof")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	for i := 0; i < 6; i++ {
		s.read(full, calls[1].params)
	}
	pprof.StopCPUProfile()
	_ = f.Close()
	t.Logf("EFFL profile2 runs=6 wall=%s", time.Since(start).Round(time.Millisecond))
	goBin := filepath.Join(runtime.GOROOT(), "bin", "go")
	for _, mode := range [][]string{
		{"-peek", `evaluateRowValueText$`},
		{"-peek", `topLevelKeywordIndex$|keywordIndexFrom$`},
		{"-top", "-cum", "-nodecount=60", "-focus=evaluateRowValueText"},
		{"-peek", `readRelationshipEndpoint$|relationshipEndpointID$|rowEndpointPropertyNode..eval$`},
		{"-peek", `pipelineApplyCallSubqueryWithMetadata$`},
		{"-top", "-cum", "-nodecount=40", "-focus=gcBgMarkWorker|mallocgc"},
	} {
		args := append([]string{"tool", "pprof"}, mode...)
		args = append(args, os.Args[0], path)
		cmd := exec.Command(goBin, args...)
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out
		err := cmd.Run()
		t.Logf("EFFL pprof2 %v err=%v\n%s", mode, err, out.String())
	}
}

// ---------------------------------------------------------------------------
// Neutral reproduction: our own schema, data and statement.
//
// Items in groups; every group has one hub item and K member items; LINK
// relationships: about 52% from the hub to members, and 2 from each member to
// the next two members. No Note exists. The statement gathers a group's links
// and, for each link, runs a correlated subquery that reads the link's end
// nodes, looks up an owner item by a property of the link, unwinds the end
// nodes, optionally matches notes about them, runs a nested correlated
// subquery per note, and projects maps and lists.
// ---------------------------------------------------------------------------

const efflNeutralFull = `MATCH (seed:Item {grp: $grp})-[r:LINK]-()
WITH collect(DISTINCT r) AS links
CALL (links) {
  UNWIND links AS r
  CALL (r) {
    WITH r, [startNode(r).id + '|' + endNode(r).id + '|' + coalesce(r.kind, ''), startNode(r).id + '|' + endNode(r).id + '|*'] AS keys
    CALL (r) {
      WITH r, startNode(r) AS s, endNode(r) AS e
      OPTIONAL MATCH (owner:Item {id: coalesce(r.grp, '')})
      UNWIND [s, e] + CASE WHEN owner IS NULL THEN [] ELSE [owner] END AS t
      OPTIONAL MATCH (t)<-[:ABOUT]-(note:Note)
      WHERE note.status = 'active'
      WITH DISTINCT note
      CALL (note) {
        OPTIONAL MATCH (note)-[:ABOUT]->(x)
        RETURN collect(DISTINCT x.id) AS about
      }
      RETURN collect(CASE WHEN note IS NULL THEN null ELSE note {.*, about: about} END) AS notes
    }
    RETURN keys, [n IN notes WHERE any(k IN n.keys WHERE k IN keys) | n.id] AS hiding
  }
  WITH r, keys, hiding, startNode(r) AS s, endNode(r) AS e
  RETURN collect({id: r.id, from: s.id, to: e.id, keys: keys, hidden: size(hiding) > 0,
                  live: coalesce(r.live, true) AND coalesce(s.live, true) AND coalesce(e.live, true)}) AS rows
}
RETURN size(rows) AS n`

const efflNeutralRows = `MATCH (seed:Item {grp: $grp})-[r:LINK]-()
WITH collect(DISTINCT r) AS links
CALL (links) {
  UNWIND links AS r
  RETURN count(r) AS rows
}
RETURN rows AS n`

// Per link: only the endpoint reads and the owner lookup, in one subquery.
const efflNeutralLookup = `MATCH (seed:Item {grp: $grp})-[r:LINK]-()
WITH collect(DISTINCT r) AS links
CALL (links) {
  UNWIND links AS r
  CALL (r) {
    WITH r, startNode(r) AS s, endNode(r) AS e
    OPTIONAL MATCH (owner:Item {id: coalesce(r.grp, '')})
    RETURN s.id AS sid, e.id AS eid, owner IS NULL AS lone
  }
  RETURN count(sid) AS rows
}
RETURN rows AS n`

// The same per-link projection with no subquery at all.
const efflNeutralFlat = `MATCH (seed:Item {grp: $grp})-[r:LINK]-()
WITH collect(DISTINCT r) AS links
UNWIND links AS r
WITH r, startNode(r) AS s, endNode(r) AS e
RETURN size(collect({id: r.id, from: s.id, to: e.id,
                     keys: [s.id + '|' + e.id + '|' + coalesce(r.kind, ''), s.id + '|' + e.id + '|*'],
                     live: coalesce(r.live, true) AND coalesce(s.live, true) AND coalesce(e.live, true)})) AS n`

func efflNeutralLoad(s *efflStack, links, groups int) {
	for _, q := range []string{
		"CREATE CONSTRAINT item_id IF NOT EXISTS FOR (n:Item) REQUIRE n.id IS UNIQUE",
		"CREATE CONSTRAINT note_id IF NOT EXISTS FOR (n:Note) REQUIRE n.id IS UNIQUE",
		"CREATE INDEX item_grp IF NOT EXISTS FOR (n:Item) ON (n.grp)",
		"CREATE INDEX note_status IF NOT EXISTS FOR (n:Note) ON (n.status)",
	} {
		s.write(q, nil)
	}
	for g := 0; g < groups; g++ {
		grp := fmt.Sprintf("g%d", g)
		k := (links*238 + 500) / 1000
		hubLinks := links - 2*k
		s.write(`CREATE (:Item {id: $grp + '-hub', grp: $grp, live: true})
WITH 1 AS one UNWIND range(0, $k - 1) AS i
CREATE (:Item {id: $grp + '-m' + toString(i), grp: $grp, live: true, name: 'member ' + toString(i)})`,
			map[string]any{"grp": grp, "k": k})
		var rels []any
		for j := 0; j < hubLinks; j++ {
			rels = append(rels, map[string]any{"a": grp + "-hub", "b": fmt.Sprintf("%s-m%d", grp, j%k), "id": fmt.Sprintf("%s-l%d", grp, len(rels))})
		}
		for i := 0; i < k; i++ {
			for step := 1; step <= 2; step++ {
				rels = append(rels, map[string]any{"a": fmt.Sprintf("%s-m%d", grp, i), "b": fmt.Sprintf("%s-m%d", grp, (i+step)%k), "id": fmt.Sprintf("%s-l%d", grp, len(rels))})
			}
		}
		for start := 0; start < len(rels); start += 500 {
			end := start + 500
			if end > len(rels) {
				end = len(rels)
			}
			s.write(`UNWIND $rows AS row
MATCH (a:Item {id: row.a}), (b:Item {id: row.b})
CREATE (a)-[:LINK {id: row.id, kind: 'peer', src: 'auto', grp: $grp, live: true}]->(b)`,
				map[string]any{"rows": rels[start:end], "grp": grp})
		}
	}
}

// TestEfflinkNeutral: the neutral statement and its cut-downs at several
// sizes, five repetitions each after a first (cold) run.
func TestEfflinkNeutral(t *testing.T) {
	sizes := efflEnvInts("EFFL_SIZES", []int{250, 500, 1000, 2000})
	reps := efflEnvInt("EFFL_REPS", 5)
	for _, n := range sizes {
		s := efflStart(t)
		efflNeutralLoad(s, n, 1)
		p := map[string]any{"grp": "g0"}
		for _, v := range []struct{ name, q string }{
			{"full", efflNeutralFull}, {"rows", efflNeutralRows}, {"lookup", efflNeutralLookup}, {"flat", efflNeutralFlat},
		} {
			st, recs := efflMeasure(s, v.q, p, reps)
			t.Logf("EFFL neutral links=%d variant=%s %s %s", n, v.name, efflRowsOf(recs), st)
		}
	}
}
