package server

// Store written by one build, opened by another (test only, not for merge).
//
// TF_MODE=write (run on the writer build): writes a chunk-vector store through the
// same server path as TestChunkVectorCarryCost (nodes with chunk vectors, a range
// index, relationships, then one explicit HTTP transaction of the four update
// statements, COMMIT, a clean stop) into $TF_ROOT/data, and the acknowledged state
// into $TF_ROOT/expected.json.
// TF_MODE=read (run on the reader build): copies $TF_ROOT/data (never opens the
// original), opens the copy without storage upgrade, then (only if refused) with it,
// compares every node, label, property and chunk vector and every relationship with
// expected.json, runs Cypher reads and one write over HTTP, stops, opens a second
// time and compares again. Lines start "HS TFSTORE".

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type tfNode struct {
	ID        string   `json:"id"`
	Labels    []string `json:"labels"`
	PropsHash string   `json:"props_hash"`
	Chunks    int      `json:"chunks"`
	ChunkHash string   `json:"chunk_hash"`
}
type tfEdge struct {
	ID, Type, Start, End, PropsHash string
}
type tfState struct {
	Nodes []tfNode `json:"nodes"`
	Edges []tfEdge `json:"edges"`
}

func tfHashProps(p map[string]any) string {
	keys := make([]string, 0, len(p))
	for k := range p {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		raw, _ := json.Marshal(p[k])
		fmt.Fprintf(h, "%s=%s;", k, raw)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func tfCapture(t *testing.T, e storage.Engine) tfState {
	ctx := context.Background()
	var st tfState
	nodes, err := e.AllNodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		full, err := e.GetNode(ctx, n.ID)
		if err != nil {
			t.Fatal(err)
		}
		labels := append([]string(nil), full.Labels...)
		sort.Strings(labels)
		ch := sha256.New()
		var b [4]byte
		for _, v := range full.ChunkEmbeddings {
			fmt.Fprintf(ch, "%d;", len(v))
			for _, f := range v {
				binary.LittleEndian.PutUint32(b[:], math.Float32bits(f))
				ch.Write(b[:])
			}
		}
		st.Nodes = append(st.Nodes, tfNode{ID: string(n.ID), Labels: labels, PropsHash: tfHashProps(full.Properties),
			Chunks: len(full.ChunkEmbeddings), ChunkHash: hex.EncodeToString(ch.Sum(nil))})
	}
	edges, err := e.AllEdges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, ed := range edges {
		st.Edges = append(st.Edges, tfEdge{ID: string(ed.ID), Type: ed.Type, Start: string(ed.StartNode), End: string(ed.EndNode), PropsHash: tfHashProps(ed.Properties)})
	}
	sort.Slice(st.Nodes, func(i, j int) bool { return st.Nodes[i].ID < st.Nodes[j].ID })
	sort.Slice(st.Edges, func(i, j int) bool { return st.Edges[i].ID < st.Edges[j].ID })
	return st
}

func tfCompare(tag string, want, got tfState) (missing, changed, extra int) {
	g := map[string]tfNode{}
	for _, n := range got.Nodes {
		g[n.ID] = n
	}
	w := map[string]bool{}
	for _, n := range want.Nodes {
		w[n.ID] = true
		x, ok := g[n.ID]
		switch {
		case !ok:
			missing++
			hsLog("TFSTORE %s MISSING node %s", tag, n.ID)
		case strings.Join(x.Labels, ",") != strings.Join(n.Labels, ",") || x.PropsHash != n.PropsHash || x.Chunks != n.Chunks || x.ChunkHash != n.ChunkHash:
			changed++
			hsLog("TFSTORE %s CHANGED node %s labels %v->%v props_same=%v chunks %d->%d chunk_hash_same=%v", tag, n.ID, n.Labels, x.Labels, x.PropsHash == n.PropsHash, n.Chunks, x.Chunks, x.ChunkHash == n.ChunkHash)
		}
	}
	for _, n := range got.Nodes {
		if !w[n.ID] {
			extra++
			hsLog("TFSTORE %s EXTRA node %s labels %v", tag, n.ID, n.Labels)
		}
	}
	ge := map[string]tfEdge{}
	for _, e := range got.Edges {
		ge[e.ID] = e
	}
	we := map[string]bool{}
	for _, e := range want.Edges {
		we[e.ID] = true
		x, ok := ge[e.ID]
		if !ok {
			missing++
			hsLog("TFSTORE %s MISSING edge %s", tag, e.ID)
		} else if x != e {
			changed++
			hsLog("TFSTORE %s CHANGED edge %s", tag, e.ID)
		}
	}
	for _, e := range got.Edges {
		if !we[e.ID] {
			extra++
			hsLog("TFSTORE %s EXTRA edge %s", tag, e.ID)
		}
	}
	return
}

func tfConfig(upgrade bool) *nornicdb.Config {
	c := nornicdb.DefaultConfig()
	c.Memory.DecayEnabled = false
	c.Memory.AutoLinksEnabled = false
	c.Memory.EmbeddingEnabled = false
	c.Memory.SearchCursorMax = 128
	c.Memory.EmbeddingDimensions = 2048
	c.Database.AsyncWritesEnabled = false
	c.Database.MVCCRetentionMaxVersions = 100
	c.Database.WALSyncMode = "immediate"
	c.Database.AllowStorageUpgrade = upgrade
	c.EmbeddingWorker.PropertiesInclude = []string{"view_text", "search_text"}
	return c
}

type tfServer struct {
	db *nornicdb.DB
	s  *Server
	cl *hsClient
	st storage.Engine
}

func tfServe(t *testing.T, db *nornicdb.DB) *tfServer {
	systemStorage := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system")
	a, err := auth.NewAuthenticator(auth.AuthConfig{SecurityEnabled: true, JWTSecret: []byte("test-secret-key-for-testing-only-32b")}, systemStorage)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.CreateUser(context.Background(), "admin", "password123", []auth.Role{auth.RoleAdmin}); err != nil {
		hsLog("TFSTORE create admin: %v (expected when the store already has it)", err)
	}
	sc := DefaultConfig()
	sc.Port = 0
	sc.EmbeddingEnabled = false
	s, err := New(db, a, sc)
	if err != nil {
		t.Fatal(err)
	}
	cl := &hsClient{h: s.buildRouter(), token: "Bearer " + getAuthToken(t, a, "admin"), db: s.dbManager.DefaultDatabaseName()}
	st, err := s.dbManager.GetStorage(s.dbManager.DefaultDatabaseName())
	if err != nil {
		t.Fatal(err)
	}
	return &tfServer{db, s, cl, st}
}

func (x *tfServer) stop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = x.s.Stop(ctx)
	if err := x.db.Close(); err != nil {
		hsLog("TFSTORE CLOSE_ERROR %v", err)
		t.Errorf("close: %v", err)
	}
}

func (x *tfServer) query(t *testing.T, q string) string {
	code, body, _, err := x.cl.do(http.MethodPost, "/db/"+x.cl.db+"/tx/commit", hsStmts(q, nil))
	if err != nil {
		t.Errorf("query %q: %v", q, err)
		return ""
	}
	if _, err := x.cl.check(code, body); err != nil {
		t.Errorf("query %q: %v", q, err)
	}
	s := string(body)
	if len(s) > 400 {
		s = s[:400]
	}
	return s
}

func tfRoot() string { return hsEnvS("TF_ROOT", "/work/tnrun/v152-tipfix/51a-cvstore") }

func TestTipfixStore(t *testing.T) {
	switch os.Getenv("TF_MODE") {
	case "write":
		tfWrite(t)
	case "read":
		tfRead(t)
	default:
		t.Skip("set TF_MODE=write|read")
	}
}

func tfWrite(t *testing.T) {
	hsStart = time.Now()
	root := tfRoot()
	data := filepath.Join(root, "data")
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY", "test-remote-credential-key-32b!")
	db, err := nornicdb.Open(data, tfConfig(false))
	if err != nil {
		t.Fatal(err)
	}
	x := tfServe(t, db)
	ctx := context.Background()
	if err := x.cl.autocommitErr("CREATE INDEX cv_source_id IF NOT EXISTS FOR (s:Source) ON (s.id)"); err != nil {
		t.Fatal(err)
	}
	const nSources, vecPer, chunkChars = 16, 64, 512
	emb := &hsEmbedder{dims: 2048}
	r := rand.New(rand.NewSource(7))
	vocab := hsVocab(r, 30000)
	var sigs, prof, versions, copies []interface{}
	vectors := 0
	for i := 0; i < nSources; i++ {
		id := fmt.Sprintf("src-%04d", i)
		text := vocab.text(r, vecPer*chunkChars)
		var chunks [][]float32
		for c0 := 0; c0 < len(text); c0 += chunkChars {
			chunks = append(chunks, emb.vec(text[c0:min(len(text), c0+chunkChars)]))
		}
		vectors += len(chunks)
		props := map[string]interface{}{"id": id, "document_id": "doc-" + id, "text": text, "view_text": text,
			"set_keys": hs2Strs("k", 50, r), "set_weights": hs2Floats(50, r), "record_profiled": int64(1), "profiled_run_layout": "L", "set_version": "v1"}
		if _, err := x.st.CreateNode(ctx, &storage.Node{ID: storage.NodeID(id), Labels: []string{"Source"}, Properties: props, ChunkEmbeddings: chunks}); err != nil {
			t.Fatal(err)
		}
		sigs = append(sigs, map[string]interface{}{"source_id": id, "keys": hs2Strs("k", 50, r), "weights": hs2Floats(50, r), "total": r.Float64(),
			"pool_runs": hs2Strs("p", 5, r), "pool_keys": hs2Strs("q", 10, r), "pool_records": hs2Strs("w", 10, r), "current": true})
		prof = append(prof, map[string]interface{}{"source_id": id, "generation": int64(2), "layout": "L2"})
		versions = append(versions, map[string]interface{}{"source_id": id, "version": "v2"})
		copies = append(copies, map[string]interface{}{"source_id": id, "runs": []interface{}{int64(1)}, "of": "x", "documents": []interface{}{"doc-" + id}, "whole": false, "version": "v2"})
	}
	if err := x.cl.autocommitErr("MATCH (a:Source {id: 'src-0000'}), (b:Source) WHERE b.id <> 'src-0000' CREATE (a)-[:COVERS {w: 1}]->(b)"); err != nil {
		t.Fatal(err)
	}
	tx, _, _, err := x.cl.begin(hs2SIGNATURES, map[string]interface{}{"sources": sigs})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []struct {
		q string
		p map[string]interface{}
	}{{hs2PROFILED, map[string]interface{}{"profiled": prof}}, {cvVERSION, map[string]interface{}{"versions": versions}}, {hs2COPIES, map[string]interface{}{"sources": copies}}} {
		if _, _, err := tx.run(s.q, s.p); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.commit(); err != nil {
		t.Fatal(err)
	}
	st := tfCapture(t, x.st)
	x.stop(t)
	raw, _ := json.MarshalIndent(st, "", " ")
	if err := os.WriteFile(filepath.Join(root, "expected.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	out, _ := exec.Command("du", "-sh", data).CombinedOutput()
	hsLog("TFSTORE WRITE root=%s nodes=%d edges=%d vectors=%d du=%s", root, len(st.Nodes), len(st.Edges), vectors, strings.TrimSpace(string(out)))
}

func tfRead(t *testing.T) {
	hsStart = time.Now()
	root := tfRoot()
	raw, err := os.ReadFile(filepath.Join(root, "expected.json"))
	if err != nil {
		t.Fatal(err)
	}
	var want tfState
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	work := filepath.Join(t.TempDir(), "data")
	if out, err := exec.Command("cp", "-a", "--sparse=always", filepath.Join(root, "data"), work).CombinedOutput(); err != nil {
		t.Fatalf("copy: %v %s", err, out)
	}
	t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY", "test-remote-credential-key-32b!")
	for pass := 1; pass <= 2; pass++ {
		upgraded := false
		db, err := nornicdb.Open(work, tfConfig(false))
		if err != nil {
			hsLog("TFSTORE pass=%d open_without_upgrade REFUSED: %v", pass, err)
			db, err = nornicdb.Open(work, tfConfig(true))
			if err != nil {
				hsLog("TFSTORE pass=%d open_with_upgrade FAILED: %v", pass, err)
				t.Fatalf("pass %d: store does not open: %v", pass, err)
			}
			upgraded = true
		}
		x := tfServe(t, db)
		got := tfCapture(t, x.st)
		m, c, e := tfCompare(fmt.Sprintf("pass%d", pass), want, got)
		vecs := 0
		for _, n := range got.Nodes {
			vecs += n.Chunks
		}
		hsLog("TFSTORE pass=%d opened=true upgrade_needed=%v nodes=%d edges=%d vectors=%d missing=%d changed=%d extra=%d", pass, upgraded, len(got.Nodes), len(got.Edges), vecs, m, c, e)
		if m+c+e > 0 {
			t.Errorf("pass %d: %d missing, %d changed, %d extra", pass, m, c, e)
		}
		hsLog("TFSTORE pass=%d cypher count: %s", pass, x.query(t, "MATCH (s:Source) RETURN count(s) AS n, sum(size(s.text)) AS chars, count(s.set_copy_version) AS copied"))
		hsLog("TFSTORE pass=%d cypher index read: %s", pass, x.query(t, "MATCH (s:Source {id:'src-0007'})-[r:COVERS]-() RETURN s.set_version, s.record_profiled, count(r)"))
		if pass == 1 {
			hsLog("TFSTORE pass=1 cypher write: %s", x.query(t, "MATCH (s:Source {id:'src-0003'}) SET s.tipfix = 1 RETURN s.tipfix"))
			// the write is part of the expected state of pass 2
			for i := range want.Nodes {
				if want.Nodes[i].ID == "src-0003" {
					full, err := x.st.GetNode(context.Background(), "src-0003")
					if err == nil {
						want.Nodes[i].PropsHash = tfHashProps(full.Properties)
					}
				}
			}
		}
		x.stop(t)
	}
}

func (c *hsClient) autocommitErr(q string) error {
	_, _, err := c.autocommit(q, nil)
	return err
}
