package bolt

// v152 upgrade lane (research reader, not for merge). Opens copies of stores
// written by older builds (b6d8e762a, v1.5.0 c99e77131, v1.5.1 cb0a574f7;
// writers zz_uor_write_test.go, zz_uor_bolt_write_test.go and
// zz_ei5_torn_audit_write_test.go on those builds, kept under
// /work/tnrun/v152-upg/<build>/) with this build:
//   1. first open WITHOUT storage upgrade allowed (an old-format store must be
//      refused and left unchanged; a current-format store opens);
//   2. if refused, open WITH storage upgrade allowed (the in-place upgrade);
//   3. a second open after that.
// After every successful open, every node and relationship (labels, every
// property, every chunk and named vector) is compared with the state the old
// build acknowledged (expected.json). Change-event stores: the change feed is
// read and compared with what the old build reported, and on the last open one
// new write must add one event. Hidden-relationship store: the relationship
// is counted by auto-commit and by an explicit transaction, then an explicit
// MERGE ON MATCH must commit.

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
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

const uprRoot = "/work/tnrun/v152-upg"

var uprBuilds = []string{"b6", "v150", "v151"}

// uprReadDir is this reader build's scratch area on the lab dataset.
var uprReadDir = "read-v151"

type uprNode struct {
	ID        string   `json:"id"`
	Labels    []string `json:"labels"`
	PropsHash string   `json:"propsHash"`
	PropBytes int      `json:"propBytes"`
	Chunks    int      `json:"chunks"`
	ChunkHash string   `json:"chunkHash"`
	Named     []string `json:"named"`
	NamedHash string   `json:"namedHash"`
}

type uprEdge struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Start     string `json:"start"`
	End       string `json:"end"`
	PropsHash string `json:"propsHash"`
}

type uprState struct {
	Nodes []uprNode `json:"nodes"`
	Edges []uprEdge `json:"edges"`
}

func uprHashVectors(h io.Writer, vs [][]float32) {
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

func uprHashProps(p map[string]any) (string, int) {
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

func uprCapture(t testing.TB, engine storage.Engine) uprState {
	ctx := context.Background()
	var st uprState
	nodes, err := engine.AllNodes(ctx)
	require.NoError(t, err)
	for _, n := range nodes {
		if strings.HasPrefix(string(n.ID), "system:") {
			continue
		}
		full, err := engine.GetNode(ctx, n.ID)
		require.NoError(t, err)
		labels := append([]string(nil), full.Labels...)
		sort.Strings(labels)
		ph, pb := uprHashProps(full.Properties)
		ch := sha256.New()
		uprHashVectors(ch, full.ChunkEmbeddings)
		names := make([]string, 0, len(full.NamedEmbeddings))
		for name := range full.NamedEmbeddings {
			names = append(names, name)
		}
		sort.Strings(names)
		nh := sha256.New()
		for _, name := range names {
			fmt.Fprintf(nh, "%s;", name)
			uprHashVectors(nh, [][]float32{full.NamedEmbeddings[name]})
		}
		st.Nodes = append(st.Nodes, uprNode{ID: string(n.ID), Labels: labels, PropsHash: ph, PropBytes: pb,
			Chunks: len(full.ChunkEmbeddings), ChunkHash: hex.EncodeToString(ch.Sum(nil)), Named: names, NamedHash: hex.EncodeToString(nh.Sum(nil))})
	}
	edges, err := engine.AllEdges(ctx)
	require.NoError(t, err)
	for _, e := range edges {
		if strings.HasPrefix(string(e.ID), "system:") {
			continue
		}
		ph, _ := uprHashProps(e.Properties)
		st.Edges = append(st.Edges, uprEdge{ID: string(e.ID), Type: e.Type, Start: string(e.StartNode), End: string(e.EndNode), PropsHash: ph})
	}
	sort.Slice(st.Nodes, func(i, j int) bool { return st.Nodes[i].ID < st.Nodes[j].ID })
	sort.Slice(st.Edges, func(i, j int) bool { return st.Edges[i].ID < st.Edges[j].ID })
	return st
}

// uprCompare prints every difference and returns missing, changed, extra.
func uprCompare(tag string, want, got uprState) (missing, changed, extra int) {
	gotNodes := map[string]uprNode{}
	for _, n := range got.Nodes {
		gotNodes[n.ID] = n
	}
	byLabel := map[string]int{}
	for _, w := range want.Nodes {
		g, ok := gotNodes[w.ID]
		if !ok {
			missing++
			byLabel[strings.Join(w.Labels, ",")]++
			continue
		}
		delete(gotNodes, w.ID)
		if strings.Join(w.Labels, ",") != strings.Join(g.Labels, ",") || w.PropsHash != g.PropsHash || w.Chunks != g.Chunks ||
			w.ChunkHash != g.ChunkHash || strings.Join(w.Named, ",") != strings.Join(g.Named, ",") || w.NamedHash != g.NamedHash {
			changed++
			fmt.Printf("UPR_DIFF %s node %s CHANGED labels %v->%v propBytes %d->%d chunks %d->%d named %v->%v\n", tag, w.ID, w.Labels, g.Labels, w.PropBytes, g.PropBytes, w.Chunks, g.Chunks, w.Named, g.Named)
		}
	}
	for label, n := range byLabel {
		fmt.Printf("UPR_DIFF %s nodes MISSING labels=%s count=%d\n", tag, label, n)
	}
	for id, n := range gotNodes {
		extra++
		fmt.Printf("UPR_DIFF %s node %s EXTRA labels=%v\n", tag, id, n.Labels)
	}
	gotEdges := map[string]uprEdge{}
	for _, e := range got.Edges {
		gotEdges[e.ID] = e
	}
	for _, w := range want.Edges {
		g, ok := gotEdges[w.ID]
		if !ok {
			missing++
			fmt.Printf("UPR_DIFF %s edge %s %s MISSING\n", tag, w.ID, w.Type)
			continue
		}
		delete(gotEdges, w.ID)
		if g != w {
			changed++
			fmt.Printf("UPR_DIFF %s edge %s %s CHANGED\n", tag, w.ID, w.Type)
		}
	}
	for id, e := range gotEdges {
		extra++
		fmt.Printf("UPR_DIFF %s edge %s %s EXTRA\n", tag, id, e.Type)
	}
	fmt.Printf("UPR_COUNTS %s nodes want=%d got=%d edges want=%d got=%d missing=%d changed=%d extra=%d\n",
		tag, len(want.Nodes), len(got.Nodes), len(want.Edges), len(got.Edges), missing, changed, extra)
	return
}

func uprHashes(dir string) string {
	var out []string
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		f, err := os.Open(p)
		if err != nil {
			return nil
		}
		h := sha256.New()
		_, _ = io.Copy(h, f)
		f.Close()
		rel, _ := filepath.Rel(dir, p)
		out = append(out, fmt.Sprintf("%s:%d:%s", rel, info.Size(), hex.EncodeToString(h.Sum(nil))[:16]))
		return nil
	})
	sort.Strings(out)
	return strings.Join(out, ",")
}

func uprCopy(t testing.TB, src, dst string) {
	require.NoError(t, os.MkdirAll(filepath.Dir(dst), 0o755))
	out, err := exec.Command("cp", "-a", "--sparse=always", src, dst).CombinedOutput()
	require.NoError(t, err, string(out))
}

// uprConfig: the write mode of the store's producer, as the frozen report's
// reader used it; storage upgrade allowed only when asked.
func uprConfig(name string, upgrade bool, logw io.Writer) *nornicdb.Config {
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.EmbeddingEnabled = false
	cfg.Database.WALAutoCompactionEnabled = true
	cfg.Database.WALSnapshotInterval = time.Hour
	cfg.Database.AllowStorageUpgrade = upgrade
	switch {
	case strings.HasPrefix(name, "synchidden"):
		cfg.Database.AsyncWritesEnabled = false
	case strings.HasPrefix(name, "async"):
		cfg.Database.AsyncWritesEnabled = true
		cfg.Database.StrictDurability = false
	default:
		cfg.Database.StrictDurability = true
		cfg.Database.WALSyncMode = "immediate"
	}
	cfg.Logger = slog.New(slog.NewTextHandler(logw, &slog.HandlerOptions{Level: slog.LevelInfo}))
	return cfg
}

type uprLab struct {
	db   *nornicdb.DB
	d    neo4j.DriverWithContext
	s    *Server
	done chan error
}

func uprServe(t testing.TB, db *nornicdb.DB) *uprLab {
	m, err := multidb.NewDatabaseManager(context.Background(), db.GetBaseStorageForManager(), nil)
	require.NoError(t, err)
	s := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 16}, nil, m)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- s.Serve(l) }()
	d, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", l.Addr().(*net.TCPAddr).Port), neo4j.NoAuth())
	require.NoError(t, err)
	return &uprLab{db: db, d: d, s: s, done: done}
}

func (b *uprLab) close(t testing.TB) {
	_ = b.d.Close(context.Background())
	_ = b.s.Close()
	<-b.done
	require.NoError(t, b.db.Close())
}

func (b *uprLab) one(q string, p map[string]any) string {
	ctx := context.Background()
	s := b.d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	defer s.Close(ctx)
	r, err := s.Run(ctx, q, p)
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

func (b *uprLab) cdc(tag string) map[string]string {
	out := map[string]string{
		"earliest":  b.one("CALL db.cdc.earliest() YIELD id RETURN id", nil),
		"current":   b.one("CALL db.cdc.current() YIELD id RETURN id", nil),
		"count":     b.one("CALL db.cdc.earliest() YIELD id CALL db.cdc.query(id) YIELD id AS change RETURN count(*) AS n", nil),
		"countMany": b.one("CALL db.cdc.earliest() YIELD id CALL db.cdc.query(id) YIELD event WITH event WHERE 'Many' IN event.state.after.labels RETURN count(*) AS n", nil),
	}
	for _, k := range []string{"earliest", "current", "count", "countMany"} {
		fmt.Printf("UPR_CDC %s %s=%s\n", tag, k, out[k])
	}
	return out
}

func (b *uprLab) census(tag string) (auto, explicit, write string) {
	ctx := context.Background()
	const q = "MATCH ()-[r:HID]->() RETURN count(r) AS n, collect(r.gen) AS gens"
	auto = b.one(q, nil)
	s := b.d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	defer s.Close(ctx)
	explicit, write = "?", "?"
	tx, err := s.BeginTransaction(ctx)
	if err != nil {
		explicit = "ERR " + err.Error()
	} else {
		if r, err := tx.Run(ctx, q, nil); err != nil {
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
	fmt.Printf("UPR_HIDDEN_CENSUS %s auto=%s explicit=%s writeform=%s\n", tag, auto, explicit, write)
	return
}

// mergeExplicit is the client's MERGE over the relationship, in an explicit
// transaction that commits.
func (b *uprLab) mergeExplicit(tag string) error {
	ctx := context.Background()
	s := b.d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	defer s.Close(ctx)
	tx, err := s.BeginTransaction(ctx)
	if err == nil {
		var r neo4j.ResultWithContext
		r, err = tx.Run(ctx, "MATCH (a:HA {k:'a'}), (b:HB {k:'b'}) MERGE (a)-[r:HID {merge_id:'m1'}]->(b) ON CREATE SET r.c = 1 ON MATCH SET r.m = 1 RETURN r.gen, r.c, r.m", nil)
		if err == nil {
			var recs []*neo4j.Record
			recs, err = r.Collect(ctx)
			if err == nil && len(recs) > 0 {
				fmt.Printf("UPR_HIDDEN_MERGE %s row=%v\n", tag, recs[0].Values)
			}
		}
		if err == nil {
			err = tx.Commit(ctx)
		} else {
			_ = tx.Rollback(ctx)
		}
	}
	fmt.Printf("UPR_HIDDEN_MERGE %s err=%v\n", tag, err)
	return err
}

func uprGrepLog(tag, path string) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(raw), "\n") {
		for _, k := range []string{"upgrade", "Upgrade", "migrat", "repair", "corrupt", "truncat", "recover", "Recover", "oversized", "Rebuilding MVCC", "Clean shutdown", "ERROR", "WARN"} {
			if strings.Contains(line, k) {
				if len(line) > 600 {
					line = line[:600] + "..."
				}
				fmt.Printf("UPR_LOG %s | %s\n", tag, line)
				break
			}
		}
	}
}

func uprOpen(t testing.TB, tag, name, data string, upgrade bool) (*nornicdb.DB, error) {
	logPath := data + "." + strings.NewReplacer("/", "_", " ", "_").Replace(tag) + ".log"
	logf, err := os.Create(logPath)
	require.NoError(t, err)
	start := time.Now()
	db, openErr := nornicdb.Open(data, uprConfig(name, upgrade, logf))
	elapsed := time.Since(start)
	logf.Close()
	uprGrepLog(tag, logPath)
	fmt.Printf("UPR_OPEN %s upgrade=%v err=%v elapsed=%s\n", tag, upgrade, openErr, elapsed.Round(time.Millisecond))
	return db, openErr
}

// uprCheck compares, reads the change feed, counts the hidden relationship;
// on the last open it also writes (one change event; the client's MERGE).
func uprCheck(t *testing.T, tag, name string, db *nornicdb.DB, want uprState, writerCDC map[string]string, last bool) {
	got := uprCapture(t, db.GetBaseStorageForManager())
	missing, changed, extra := uprCompare(tag, want, got)
	if missing+changed+extra != 0 {
		t.Errorf("%s: acknowledged state differs: missing=%d changed=%d extra=%d", tag, missing, changed, extra)
	}
	if !strings.Contains(name, "cdc") && !strings.Contains(name, "hidden") {
		return
	}
	lab := uprServe(t, db)
	defer func() { _ = lab.d.Close(context.Background()); _ = lab.s.Close(); <-lab.done }()
	if strings.Contains(name, "cdc") {
		now := lab.cdc(tag)
		for _, k := range []string{"count", "countMany"} {
			if writerCDC != nil && now[k] != writerCDC[k] {
				t.Errorf("%s: change feed %s %s, the old build reported %s", tag, k, now[k], writerCDC[k])
				fmt.Printf("UPR_CDC_DIFF %s %s now=%s writer=%s\n", tag, k, now[k], writerCDC[k])
			}
		}
		if last {
			if out := lab.one("CREATE (:UpgAfter {k: 1}) RETURN 1", nil); strings.HasPrefix(out, "ERR") {
				t.Errorf("%s: write after upgrade: %s", tag, out)
			}
			after := lab.cdc(tag + " after-write")
			fmt.Printf("UPR_CDC_WRITE %s before=%s after=%s\n", tag, now["count"], after["count"])
			if after["count"] == now["count"] {
				t.Errorf("%s: a write after the upgrade added no change event (%s)", tag, after["count"])
			}
		}
	}
	if strings.Contains(name, "hidden") {
		auto, explicit, write := lab.census(tag)
		if auto != explicit {
			t.Errorf("%s: hidden relationship: auto-commit %s, explicit %s, write form %s", tag, auto, explicit, write)
		}
		if last {
			if err := lab.mergeExplicit(tag); err != nil {
				t.Errorf("%s: MERGE over the relationship failed: %v", tag, err)
			}
			lab.census(tag + " after-merge")
		}
	}
}

func TestV152UpgradeRead(t *testing.T) {
	only := os.Getenv("UPR_ONLY")
	for _, build := range uprBuilds {
		storesDir := filepath.Join(uprRoot, build, "uor", "stores")
		entries, err := os.ReadDir(storesDir)
		if err != nil {
			fmt.Printf("UPR_MISSING %s %v\n", build, err)
			continue
		}
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		sort.Strings(names)
		for _, name := range names {
			build, name := build, name
			if only != "" && !strings.Contains(","+only+",", ","+build+"/"+name+",") {
				continue
			}
			t.Run(build+"/"+name, func(t *testing.T) {
				src := filepath.Join(storesDir, name)
				raw, err := os.ReadFile(filepath.Join(src, "expected.json"))
				if err != nil {
					t.Fatalf("no expected state for %s/%s: %v", build, name, err)
				}
				var want uprState
				require.NoError(t, json.Unmarshal(raw, &want))
				var writerCDC map[string]string
				if raw, err := os.ReadFile(filepath.Join(src, "cdc-writer.json")); err == nil {
					_ = json.Unmarshal(raw, &writerCDC)
				}
				work := filepath.Join(uprRoot, uprReadDir, build, name)
				require.NoError(t, os.RemoveAll(work))
				defer os.RemoveAll(work)
				data := filepath.Join(work, "data")
				uprCopy(t, filepath.Join(src, "data"), data)
				tag := build + "/" + name

				before := uprHashes(data)
				db, err := uprOpen(t, tag+" first-noflag", name, data, false)
				upgraded := false
				if err != nil {
					unchanged := before == uprHashes(data)
					fmt.Printf("UPR_REFUSED_NOFLAG %s unchanged=%v\n", tag, unchanged)
					if !unchanged {
						t.Errorf("%s: a refused open changed the store's files", tag)
					}
					db, err = uprOpen(t, tag+" upgrade", name, data, true)
					if err != nil {
						fmt.Printf("UPR_UNCHANGED_AFTER_UPGRADE_REFUSAL %s %v\n", tag, before == uprHashes(data))
						fmt.Printf("UPR_RESULT %s upgrade=REFUSED err=%v\n", tag, err)
						t.Errorf("%s: in-place upgrade refused: %v", tag, err)
						return
					}
					upgraded = true
				}
				uprCheck(t, tag+" open1", name, db, want, writerCDC, false)
				require.NoError(t, db.Close())

				db, err = uprOpen(t, tag+" second", name, data, upgraded)
				if err != nil {
					fmt.Printf("UPR_RESULT %s upgraded=%v second=FAILED err=%v\n", tag, upgraded, err)
					t.Errorf("%s: second open failed: %v", tag, err)
					return
				}
				uprCheck(t, tag+" open2", name, db, want, writerCDC, true)
				require.NoError(t, db.Close())
				fmt.Printf("UPR_RESULT %s upgraded=%v second=ok failed=%v\n", tag, upgraded, t.Failed())
			})
		}
	}
}

// Torn tails of an oversized embedding-audit record (report entry 5).
func uprTornConfig(logw io.Writer) *nornicdb.Config {
	c := nornicdb.DefaultConfig()
	c.Database.AsyncWritesEnabled = false
	c.Database.StrictDurability = true
	c.Database.WALSyncMode = "immediate"
	c.Database.WALAutoCompactionEnabled = false
	c.Database.AllowStorageUpgrade = true
	c.Memory.DecayEnabled = false
	c.Memory.AutoLinksEnabled = false
	c.Memory.EmbeddingEnabled = false
	c.Logger = slog.New(slog.NewTextHandler(logw, &slog.HandlerOptions{Level: slog.LevelInfo}))
	return c
}

func uprTornState(db *nornicdb.DB) string {
	store := db.GetStorage()
	ctx := context.Background()
	docs, err1 := store.GetNodesByLabel(ctx, "Doc")
	after, err2 := store.GetNodesByLabel(ctx, "After")
	chunks := -1
	if len(docs) == 1 {
		if full, err := store.GetNode(ctx, docs[0].ID); err == nil {
			chunks = len(full.ChunkEmbeddings)
		}
	}
	return fmt.Sprintf("docs=%d docChunks=%d after=%d errs=%v/%v", len(docs), chunks, len(after), err1, err2)
}

func TestV152TornAuditRead(t *testing.T) {
	for _, build := range uprBuilds {
		for _, name := range []string{"intact", "torn-audit", "torn-ordinary"} {
			build, name := build, name
			t.Run(build+"/"+name, func(t *testing.T) {
				src := filepath.Join(uprRoot, build, "torn-audit", name, "data")
				if _, err := os.Stat(src); err != nil {
					fmt.Printf("UPR_TORN_MISSING %s/%s %v\n", build, name, err)
					t.Skipf("no store %s", src)
				}
				work := filepath.Join(t.TempDir(), "data")
				uprCopy(t, src, work)
				tag := build + "/" + name
				before := uprHashes(work)
				for i, step := range []string{"first", "second"} {
					logPath := work + "." + step + ".log"
					logf, err := os.Create(logPath)
					require.NoError(t, err)
					start := time.Now()
					db, err := nornicdb.Open(work, uprTornConfig(logf))
					elapsed := time.Since(start)
					logf.Close()
					uprGrepLog(tag+" "+step, logPath)
					fmt.Printf("UPR_TORN_OPEN %s %s err=%v elapsed=%s\n", tag, step, err, elapsed.Round(time.Millisecond))
					if err != nil {
						if i == 0 {
							fmt.Printf("UPR_TORN_UNCHANGED_AFTER_REFUSAL %s %v\n", tag, before == uprHashes(work))
						}
						t.Errorf("%s %s open refused: %v", tag, step, err)
						continue
					}
					fmt.Printf("UPR_TORN_STATE %s %s %s\n", tag, step, uprTornState(db))
					require.NoError(t, db.Close())
				}
			})
		}
	}
}
