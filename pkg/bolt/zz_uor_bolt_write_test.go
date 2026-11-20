package bolt

// Research fixture writer (not for merge), round 2: builds b6d8e762a stores
// with asynchronous writes enabled (NORNICDB_ASYNC_WRITES_ENABLED=true, strict
// durability off) whose journal holds one write-behind commit over 16 MiB,
// written through a real Bolt server and the official driver: one auto-commit
// import batch of 1,000 rows, each with a 1,536-dimension vector. With and
// without a snapshot (old-build compaction) before it, stopped cleanly or by
// SIGKILL. Stores, the acknowledged state and the state the store holds
// without its journal are kept under UOR_ROOT for the new-build reader.

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type uobScenario struct {
	name     string
	kind     string // batch | docs | many
	snapshot bool
	kill     bool
}

func uobScenarios() []uobScenario {
	var out []uobScenario
	for _, kind := range []string{"batch", "docs"} {
	for _, snap := range []bool{false, true} {
		for _, kill := range []bool{false, true} {
			name := map[string]string{"batch": "asyncbolt", "docs": "asyncdocs"}[kind]
			if snap {
				name += "-snap"
			} else {
				name += "-nosnap"
			}
			if kill {
				name += "-kill"
			} else {
				name += "-clean"
			}
			out = append(out, uobScenario{name: name, kind: kind, snapshot: snap, kill: kill})
		}
	}
	}
	out = append(out, uobScenario{name: "asyncmany-nosnap-clean", kind: "many"}, uobScenario{name: "asyncmany-nosnap-kill", kind: "many", kill: true})
	out = append(out, uobScenario{name: "asyncone-nosnap-clean", kind: "one"}, uobScenario{name: "asyncone-nosnap-kill", kind: "one", kill: true})
	// v152 upgrade lane: change events (CDC on, asynchronous writes) and the
	// relationship a pre-#1745 rebuild hides (synchronous writes, killed, then
	// reopened by the old build itself before the new build sees it).
	out = append(out, uobScenario{name: "asynccdc-nosnap-clean", kind: "cdc"}, uobScenario{name: "asynccdc-nosnap-kill", kind: "cdc", kill: true})
	out = append(out, uobScenario{name: "synchidden-nosnap-kill", kind: "hidden", kill: true})
	return out
}

const uobAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

// uobDocsBatch is one auto-commit statement creating nine 2 MiB documents
// (incompressible content, such as encoded attachments): one write-behind
// commit over 16 MiB that touches only low internal entity ids.
func uobDocsBatch(t testing.TB, b *uobLab) {
	r := rand.New(rand.NewSource(43))
	rows := make([]any, 0, 9)
	for i := 0; i < 9; i++ {
		body := make([]byte, 2<<20)
		for j := range body {
			body[j] = uobAlphabet[r.Intn(len(uobAlphabet))]
		}
		rows = append(rows, map[string]any{"uid": fmt.Sprintf("doc-%02d", i), "body": string(body)})
	}
	b.run(t, "UNWIND $rows AS r CREATE (n:AsyncDoc {uid: r.uid, body: r.body})", map[string]any{"rows": rows})
	fmt.Println("UOR_BOLT_DOCS rows=9 bytes=18874368 ok")
}

// uobOneBigNode is one auto-commit statement creating ONE node whose own body
// is over 16 MiB (an 18 MiB property).
func uobOneBigNode(t testing.TB, b *uobLab) {
	r := rand.New(rand.NewSource(44))
	body := make([]byte, 18<<20)
	for j := range body {
		body[j] = uobAlphabet[r.Intn(len(uobAlphabet))]
	}
	b.run(t, "CREATE (n:AsyncBig {uid: 'asyncbig', body: $body})", map[string]any{"body": string(body)})
	fmt.Println("UOR_BOLT_ONE bytes=18874368 ok")
}

// uobManySmall is 300 ordinary auto-commit statements, none near 16 MiB.
func uobManySmall(t testing.TB, b *uobLab) {
	for i := 0; i < 300; i++ {
		b.run(t, "CREATE (n:Many {uid:$uid, n:$i})", map[string]any{"uid": fmt.Sprintf("many-%03d", i), "i": int64(i)})
	}
	fmt.Println("UOR_BOLT_MANY statements=300 ok")
}

func uobRoot() string {
	if r := os.Getenv("UOR_ROOT"); r != "" {
		return r
	}
	return "/work/tnrun/v152-upg/v150/uor"
}

func uobDBConfig(sc uobScenario, snapshotInterval time.Duration, logw io.Writer) *nornicdb.Config {
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.EmbeddingEnabled = false
	cfg.Database.WALAutoCompactionEnabled = true
	cfg.Database.WALSnapshotInterval = snapshotInterval
	if sc.kind == "hidden" {
		// Production defaults for the write mode (asynchronous writes off).
		cfg.Database.AsyncWritesEnabled = false
		cfg.Logger = slog.New(slog.NewTextHandler(logw, &slog.HandlerOptions{Level: slog.LevelInfo}))
		return cfg
	}
	cfg.Database.AsyncWritesEnabled = true
	cfg.Database.StrictDurability = false
	if sc.kill {
		// A long write-behind flush interval keeps acknowledged commits only
		// in the journal until the kill.
		cfg.Database.AsyncFlushInterval = 10 * time.Minute
	}
	cfg.Logger = slog.New(slog.NewTextHandler(logw, &slog.HandlerOptions{Level: slog.LevelInfo}))
	return cfg
}

type uobNodeState struct {
	ID        string   `json:"id"`
	Labels    []string `json:"labels"`
	PropsHash string   `json:"propsHash"`
	PropBytes int      `json:"propBytes"`
	Chunks    int      `json:"chunks"`
	ChunkHash string   `json:"chunkHash"`
	Named     []string `json:"named"`
	NamedHash string   `json:"namedHash"`
}

type uobEdgeState struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Start     string `json:"start"`
	End       string `json:"end"`
	PropsHash string `json:"propsHash"`
}

type uobState struct {
	Nodes []uobNodeState `json:"nodes"`
	Edges []uobEdgeState `json:"edges"`
}

func uobHashVectors(h io.Writer, vs [][]float32) {
	var b [4]byte
	for _, v := range vs {
		binary.LittleEndian.PutUint32(b[:], uint32(len(v)))
		h.Write(b[:])
		for _, f := range v {
			binary.LittleEndian.PutUint32(b[:], math.Float32bits(f))
			h.Write(b[:])
		}
	}
}

func uobHashProps(p map[string]any) (string, int) {
	keys := make([]string, 0, len(p))
	for k := range p {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	n := 0
	for _, k := range keys {
		raw, _ := json.Marshal(p[k])
		n += len(raw)
		fmt.Fprintf(h, "%s=%s;", k, raw)
	}
	return hex.EncodeToString(h.Sum(nil)), n
}

func uobCapture(t testing.TB, engine storage.Engine) uobState {
	var st uobState
	nodes, err := upgAllNodes(engine)
	require.NoError(t, err)
	for _, n := range nodes {
		if strings.HasPrefix(string(n.ID), "system:") {
			continue
		}
		full, err := upgGetNode(engine, n.ID)
		require.NoError(t, err)
		labels := append([]string(nil), full.Labels...)
		sort.Strings(labels)
		ph, pb := uobHashProps(full.Properties)
		ch := sha256.New()
		uobHashVectors(ch, full.ChunkEmbeddings)
		names := make([]string, 0, len(full.NamedEmbeddings))
		for name := range full.NamedEmbeddings {
			names = append(names, name)
		}
		sort.Strings(names)
		nh := sha256.New()
		for _, name := range names {
			fmt.Fprintf(nh, "%s;", name)
			uobHashVectors(nh, [][]float32{full.NamedEmbeddings[name]})
		}
		st.Nodes = append(st.Nodes, uobNodeState{ID: string(n.ID), Labels: labels, PropsHash: ph, PropBytes: pb,
			Chunks: len(full.ChunkEmbeddings), ChunkHash: hex.EncodeToString(ch.Sum(nil)), Named: names, NamedHash: hex.EncodeToString(nh.Sum(nil))})
	}
	edges, err := upgAllEdges(engine)
	require.NoError(t, err)
	for _, e := range edges {
		if strings.HasPrefix(string(e.ID), "system:") {
			continue
		}
		ph, _ := uobHashProps(e.Properties)
		st.Edges = append(st.Edges, uobEdgeState{ID: string(e.ID), Type: e.Type, Start: string(e.StartNode), End: string(e.EndNode), PropsHash: ph})
	}
	sort.Slice(st.Nodes, func(i, j int) bool { return st.Nodes[i].ID < st.Nodes[j].ID })
	sort.Slice(st.Edges, func(i, j int) bool { return st.Edges[i].ID < st.Edges[j].ID })
	return st
}

func uobCompare(tag string, want, got uobState) {
	gotNodes := map[string]uobNodeState{}
	for _, n := range got.Nodes {
		gotNodes[n.ID] = n
	}
	missing, changed := 0, 0
	byLabel := map[string]int{}
	for _, w := range want.Nodes {
		g, ok := gotNodes[w.ID]
		if !ok {
			missing++
			byLabel[strings.Join(w.Labels, ",")]++
			continue
		}
		if g.PropsHash != w.PropsHash || strings.Join(g.Labels, ",") != strings.Join(w.Labels, ",") {
			changed++
		}
	}
	for label, n := range byLabel {
		fmt.Printf("UOR_DIFF %s nodes MISSING labels=%s count=%d\n", tag, label, n)
	}
	gotEdges := map[string]bool{}
	for _, e := range got.Edges {
		gotEdges[e.ID] = true
	}
	for _, w := range want.Edges {
		if !gotEdges[w.ID] {
			missing++
		}
	}
	fmt.Printf("UOR_COUNTS %s nodes want=%d got=%d edges want=%d got=%d missing=%d changed=%d\n",
		tag, len(want.Nodes), len(got.Nodes), len(want.Edges), len(got.Edges), missing, changed)
}

type uobLab struct {
	db   *nornicdb.DB
	d    neo4j.DriverWithContext
	s    *Server
	done chan error
}

func uobOpen(t testing.TB, dir string, cfg *nornicdb.Config) *uobLab {
	db, err := nornicdb.Open(dir, cfg)
	require.NoError(t, err)
	m, err := multidb.NewDatabaseManager(context.Background(), db.GetBaseStorageForManager(), nil)
	require.NoError(t, err)
	s := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 16}, nil, m)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- s.Serve(l) }()
	d, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", l.Addr().(*net.TCPAddr).Port), neo4j.NoAuth())
	require.NoError(t, err)
	return &uobLab{db: db, d: d, s: s, done: done}
}

func (b *uobLab) close(t testing.TB) {
	_ = b.d.Close(context.Background())
	_ = b.s.Close()
	<-b.done
	require.NoError(t, b.db.Close())
}

// run is one auto-commit statement (session.Run), as an import client sends it.
func (b *uobLab) run(t testing.TB, q string, p map[string]any) {
	ctx := context.Background()
	s := b.d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	defer s.Close(ctx)
	r, err := s.Run(ctx, q, p)
	require.NoError(t, err, q)
	_, err = r.Consume(ctx)
	require.NoError(t, err, q)
}

func uobBefore(t testing.TB, b *uobLab) {
	b.run(t, "CREATE INDEX small_name FOR (n:Small) ON (n.name)", nil)
	for i := 0; i < 20; i++ {
		b.run(t, "CREATE (n:Small {uid:$uid, name:$name, n:$i})", map[string]any{"uid": fmt.Sprintf("small-%02d", i), "name": fmt.Sprintf("small-%d", i), "i": int64(i)})
	}
	for i := 0; i < 19; i++ {
		b.run(t, "MATCH (a:Small {uid:$a}), (b:Small {uid:$b}) CREATE (a)-[:NEXT {uid:$uid, w:$i}]->(b)",
			map[string]any{"a": fmt.Sprintf("small-%02d", i), "b": fmt.Sprintf("small-%02d", i+1), "uid": fmt.Sprintf("next-%02d", i), "i": int64(i)})
	}
}

func uobAfter(t testing.TB, b *uobLab) {
	for i := 0; i < 10; i++ {
		b.run(t, "CREATE (n:After {uid:$uid, name:$name})", map[string]any{"uid": fmt.Sprintf("after-%02d", i), "name": fmt.Sprintf("after-%d", i)})
	}
	b.run(t, "MATCH (a:After {uid:'after-00'}), (b:Small {uid:'small-00'}) CREATE (a)-[:LINK {uid:'link-00'}]->(b)", nil)
	b.run(t, "MATCH (n:Small {uid:'small-05'}) SET n.updated = 'after-big', n:Touched", nil)
	b.run(t, "MATCH (n:Small {uid:'small-06'}) REMOVE n.name", nil)
}

// uobImportBatch is the standard import pattern: one auto-commit UNWIND of
// 1,000 rows, each with a 1,536-dimension embedding.
func uobImportBatch(t testing.TB, b *uobLab) {
	r := rand.New(rand.NewSource(42))
	rows := make([]any, 0, 1000)
	for i := 0; i < 1000; i++ {
		vec := make([]any, 1536)
		for j := range vec {
			vec[j] = r.Float64()*2 - 1
		}
		rows = append(rows, map[string]any{"uid": fmt.Sprintf("emb-%04d", i), "title": fmt.Sprintf("document %d", i), "vec": vec})
	}
	b.run(t, "UNWIND $rows AS r CREATE (n:Emb {uid: r.uid, title: r.title, vec: r.vec})", map[string]any{"rows": rows})
	fmt.Println("UOR_BOLT_IMPORT rows=1000 dims=1536 ok")
}

func uobCountFiles(dir, prefix string) int {
	entries, _ := os.ReadDir(dir)
	n := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), prefix) {
			n++
		}
	}
	return n
}

func uobBuild(t testing.TB, sc uobScenario, dataDir string, logw io.Writer) *uobLab {
	if sc.snapshot {
		lab := uobOpen(t, dataDir, uobDBConfig(sc, 2*time.Second, logw))
		uobBefore(t, lab)
		snapDir := filepath.Join(dataDir, "snapshots")
		deadline := time.Now().Add(2 * time.Minute)
		for uobCountFiles(snapDir, "snapshot-") == 0 && time.Now().Before(deadline) {
			time.Sleep(100 * time.Millisecond)
		}
		require.Positive(t, uobCountFiles(snapDir, "snapshot-"), "no snapshot was taken")
		time.Sleep(500 * time.Millisecond)
		lab.close(t)
	}
	lab := uobOpen(t, dataDir, uobDBConfig(sc, time.Hour, logw))
	if sc.kind == "cdc" {
		lab.runOn(t, "system", "ALTER DATABASE nornic SET OPTION txLogEnrichment 'FULL'", nil)
	}
	if !sc.snapshot {
		uobBefore(t, lab)
	}
	switch sc.kind {
	case "docs":
		uobDocsBatch(t, lab)
	case "many":
		uobManySmall(t, lab)
	case "one":
		uobOneBigNode(t, lab)
	case "cdc":
		uobManySmall(t, lab)
	case "hidden":
		uobHidden(t, lab)
	default:
		uobImportBatch(t, lab)
	}
	uobAfter(t, lab)
	if sc.kind == "cdc" {
		uobSaveJSON(t, filepath.Join(filepath.Dir(dataDir), "cdc-writer.json"), upgCDC(lab, "writer "+sc.name))
	}
	return lab
}

// runOn is one auto-commit statement on the named database.
func (b *uobLab) runOn(t testing.TB, database, q string, p map[string]any) {
	ctx := context.Background()
	s := b.d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
	defer s.Close(ctx)
	r, err := s.Run(ctx, q, p)
	require.NoError(t, err, q)
	_, err = r.Consume(ctx)
	require.NoError(t, err, q)
}

// upgOne returns the first record's values of an auto-commit statement on
// nornic, or the error text.
func (b *uobLab) upgOne(q string) string {
	ctx := context.Background()
	s := b.d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	defer s.Close(ctx)
	r, err := s.Run(ctx, q, nil)
	if err != nil {
		return "ERR " + err.Error()
	}
	recs, err := r.Collect(ctx)
	if err != nil {
		return "ERR " + err.Error()
	}
	if len(recs) == 0 {
		return "NOROWS"
	}
	return fmt.Sprint(recs[0].Values)
}

// upgCDC records the change feed's bounds and its event counts from earliest.
func upgCDC(b *uobLab, tag string) map[string]string {
	out := map[string]string{
		"earliest":  b.upgOne("CALL db.cdc.earliest() YIELD id RETURN id"),
		"current":   b.upgOne("CALL db.cdc.current() YIELD id RETURN id"),
		"count":     b.upgOne("CALL db.cdc.earliest() YIELD id CALL db.cdc.query(id) YIELD id AS change RETURN count(*) AS n"),
		"countMany": b.upgOne("CALL db.cdc.earliest() YIELD id CALL db.cdc.query(id) YIELD event WITH event WHERE 'Many' IN event.state.after.labels RETURN count(*) AS n"),
	}
	for _, k := range []string{"earliest", "current", "count", "countMany"} {
		fmt.Printf("UPG_CDC %s %s=%s\n", tag, k, out[k])
	}
	return out
}

// uobHidden: MERGE, DELETE, MERGE again under the same merge key, plus a
// control relationship (the #1745 / NOR-405 shape).
func uobHidden(t testing.TB, b *uobLab) {
	b.run(t, "CREATE (:HA {k:'a'}), (:HB {k:'b'}), (:HC {k:'c'}), (:HD {k:'d'})", nil)
	b.run(t, "MATCH (a:HA {k:'a'}), (b:HB {k:'b'}) MERGE (a)-[r:HID {merge_id:'m1'}]->(b) ON CREATE SET r.gen = 1", nil)
	b.run(t, "MATCH (:HA {k:'a'})-[r:HID]->(:HB {k:'b'}) DELETE r", nil)
	b.run(t, "MATCH (a:HA {k:'a'}), (b:HB {k:'b'}) MERGE (a)-[r:HID {merge_id:'m1'}]->(b) ON CREATE SET r.gen = 2", nil)
	b.run(t, "MATCH (c:HC {k:'c'}), (d:HD {k:'d'}) CREATE (c)-[:HCTL {k:'ctl'}]->(d)", nil)
	fmt.Println("UPG_HIDDEN written")
}

// upgCensus prints the HID relationship as auto-commit and an explicit
// (rolled back) transaction see it, and the explicit write form's count.
func upgCensus(b *uobLab, tag string) {
	ctx := context.Background()
	auto := b.upgOne("MATCH ()-[r:HID]->() RETURN count(r) AS n, collect(r.gen) AS gens")
	s := b.d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	defer s.Close(ctx)
	explicit, write := "?", "?"
	if tx, err := s.BeginTransaction(ctx); err != nil {
		explicit = "ERR " + err.Error()
	} else {
		if r, err := tx.Run(ctx, "MATCH ()-[r:HID]->() RETURN count(r) AS n, collect(r.gen) AS gens", nil); err != nil {
			explicit = "ERR " + err.Error()
		} else if recs, err := r.Collect(ctx); err != nil {
			explicit = "ERR " + err.Error()
		} else if len(recs) > 0 {
			explicit = fmt.Sprint(recs[0].Values)
		}
		if r, err := tx.Run(ctx, "MATCH (:HA {k:'a'})-[r:HID]->(:HB {k:'b'}) SET r.probe = 1 RETURN count(r) AS n", nil); err != nil {
			write = "ERR " + err.Error()
		} else if recs, err := r.Collect(ctx); err != nil {
			write = "ERR " + err.Error()
		} else if len(recs) > 0 {
			write = fmt.Sprint(recs[0].Values)
		}
		_ = tx.Rollback(ctx)
	}
	fmt.Printf("UPG_HIDDEN_CENSUS %s auto=%s explicit=%s writeform=%s\n", tag, auto, explicit, write)
}

func uobListing(dir string) []string {
	var out []string
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info == nil {
			return nil
		}
		rel, _ := filepath.Rel(dir, p)
		if info.IsDir() {
			out = append(out, rel+"/")
		} else {
			out = append(out, fmt.Sprintf("%s %d", rel, info.Size()))
		}
		return nil
	})
	return out
}

func uobSaveJSON(t testing.TB, path string, v any) {
	raw, err := json.MarshalIndent(v, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, raw, 0o644))
}

func uobCopyDir(t testing.TB, src, dst string) {
	require.NoError(t, filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm()|0o700)
		}
		in, err := os.Open(p)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm())
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			out.Close()
			return err
		}
		return out.Close()
	}))
}

func uobScan(tag, walDir string) {
	scanner, _ := filepath.Abs(filepath.Join("testdata", "zz_uor", "wal_frame_scan.py"))
	out, err := exec.Command("python3", "-I", scanner, "--quiet", walDir).CombinedOutput()
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		code = -1
	}
	for _, line := range strings.Split(strings.TrimRight(string(out), "\n"), "\n") {
		fmt.Printf("UOR_SCAN %s | %s\n", tag, line)
	}
	fmt.Printf("UOR_SCAN_EXIT %s %d\n", tag, code)
}

// uobStoreOnly opens a copy of the store without its journal.
func uobStoreOnly(t testing.TB, base, name string, want uobState) {
	copyDir := filepath.Join(base, "store-only-copy")
	require.NoError(t, os.RemoveAll(copyDir))
	uobCopyDir(t, filepath.Join(base, "data"), copyDir)
	defer os.RemoveAll(copyDir)
	engine, err := storage.NewBadgerEngine(copyDir)
	if err != nil {
		fmt.Printf("UOR_STORE_ONLY %s open err=%v\n", name, err)
		return
	}
	got := uobCapture(t, engine)
	_ = engine.Close()
	uobCompare(name+" store-only", want, got)
}

// uobOldRestart opens a copy with the old build itself, with its journal, to
// show what the old build's own restart keeps.
func uobOldRestart(t testing.TB, base, name string, sc uobScenario, want uobState) {
	copyDir := filepath.Join(base, "b6-restart-copy")
	require.NoError(t, os.RemoveAll(copyDir))
	uobCopyDir(t, filepath.Join(base, "data"), copyDir)
	defer os.RemoveAll(copyDir)
	logf, err := os.Create(filepath.Join(base, "b6-restart.log"))
	require.NoError(t, err)
	defer logf.Close()
	cfg := uobDBConfig(uobScenario{name: sc.name, kind: sc.kind, snapshot: sc.snapshot}, time.Hour, logf)
	db, err := nornicdb.Open(copyDir, cfg)
	if err != nil {
		fmt.Printf("UOR_B6_RESTART %s open err=%v\n", name, err)
		return
	}
	got := uobCapture(t, db.GetBaseStorageForManager())
	_ = db.Close()
	uobCompare(name+" b6-restart", want, got)
	raw, _ := os.ReadFile(filepath.Join(base, "b6-restart.log"))
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.Contains(line, "repair") || strings.Contains(line, "truncat") || strings.Contains(line, "Recovered") || strings.Contains(line, "WARN") {
			fmt.Printf("UOR_B6_RESTART_LOG %s | %s\n", name, line)
		}
	}
}

func TestUORBoltWriteChild(t *testing.T) {
	name := os.Getenv("UOR_CHILD")
	if name == "" {
		t.Skip("child only")
	}
	var sc uobScenario
	for _, s := range uobScenarios() {
		if s.name == name {
			sc = s
		}
	}
	require.Equal(t, name, sc.name)
	base := filepath.Join(uobRoot(), "stores", sc.name)
	logf, err := os.Create(filepath.Join(base, "write.log"))
	require.NoError(t, err)
	lab := uobBuild(t, sc, filepath.Join(base, "data"), logf)
	uobSaveJSON(t, filepath.Join(base, "expected.json"), uobCapture(t, lab.db.GetBaseStorageForManager()))
	fmt.Println("UOR_READY")
	select {}
}

func TestUORBoltWrite(t *testing.T) {
	root := uobRoot()
	only := os.Getenv("UOR_ONLY")
	for _, sc := range uobScenarios() {
		sc := sc
		if only != "" && !strings.Contains(","+only+",", ","+sc.name+",") {
			continue
		}
		t.Run(sc.name, func(t *testing.T) {
			base := filepath.Join(root, "stores", sc.name)
			require.NoError(t, os.RemoveAll(base))
			require.NoError(t, os.MkdirAll(base, 0o755))
			if !sc.kill {
				logf, err := os.Create(filepath.Join(base, "write.log"))
				require.NoError(t, err)
				defer logf.Close()
				lab := uobBuild(t, sc, filepath.Join(base, "data"), logf)
				uobSaveJSON(t, filepath.Join(base, "expected.json"), uobCapture(t, lab.db.GetBaseStorageForManager()))
				lab.close(t)
			} else {
				cmd := exec.Command(os.Args[0], "-test.run=^TestUORBoltWriteChild$", "-test.v", "-test.timeout=30m")
				cmd.Env = append(os.Environ(), "UOR_CHILD="+sc.name, "UOR_ROOT="+root)
				stdout, err := cmd.StdoutPipe()
				require.NoError(t, err)
				cmd.Stderr = os.Stderr
				require.NoError(t, cmd.Start())
				ready := make(chan bool, 1)
				go func() {
					scanner := bufio.NewScanner(stdout)
					scanner.Buffer(make([]byte, 1<<20), 1<<20)
					ok := false
					for scanner.Scan() {
						line := scanner.Text()
						fmt.Println("child:", line)
						if strings.Contains(line, "UOR_READY") && !ok {
							ok = true
							ready <- true
						}
					}
					if !ok {
						ready <- false
					}
				}()
				require.True(t, <-ready, "child did not reach the stop point")
				require.NoError(t, cmd.Process.Kill())
				_ = cmd.Wait()
				fmt.Printf("UOR_KILLED %s state=%v\n", sc.name, cmd.ProcessState)
				if sc.kind == "hidden" {
					// The old build reopens the store itself (its rebuild after
					// the unclean stop) and stops cleanly: the store the new
					// build then receives.
					logf, err := os.Create(filepath.Join(base, "oldbuild-reopen.log"))
					require.NoError(t, err)
					lab := uobOpen(t, filepath.Join(base, "data"), uobDBConfig(uobScenario{name: sc.name, kind: sc.kind}, time.Hour, logf))
					upgCensus(lab, sc.name+" oldbuild-reopen")
					lab.close(t)
					logf.Close()
					raw, _ := os.ReadFile(filepath.Join(base, "oldbuild-reopen.log"))
					for _, line := range strings.Split(string(raw), "\n") {
						if strings.Contains(line, "Rebuilding MVCC") || strings.Contains(line, "Clean shutdown") {
							fmt.Printf("UPG_HIDDEN_LOG %s | %s\n", sc.name, line)
						}
					}
				}
			}
			fmt.Printf("UOR_WAL_LISTING %s %s\n", sc.name, strings.Join(uobListing(filepath.Join(base, "data", "wal")), " | "))
			uobScan(sc.name, filepath.Join(base, "data", "wal"))
			raw, err := os.ReadFile(filepath.Join(base, "expected.json"))
			require.NoError(t, err)
			var want uobState
			require.NoError(t, json.Unmarshal(raw, &want))
			uobStoreOnly(t, base, sc.name, want)
			uobOldRestart(t, base, sc.name, sc, want)
		})
	}
}
