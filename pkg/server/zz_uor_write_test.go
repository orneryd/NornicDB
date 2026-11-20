package server

// Research fixture writer (not for merge): builds b6d8e762a stores whose
// journals hold one record over 16 MiB that is NOT an embedding audit, each
// written through a public path of the old build:
//   api    - the embedded Go API (DB.CreateNode, then DB.UpdateNode) with a
//            node carrying an 18 MiB property: create_node + update_node.
//   qdrant - the Qdrant-compatible gRPC API: one Upsert of 1,000 points of
//            3,072 dimensions: one bulk_create_nodes record.
//   async  - asynchronous writes (write-behind): one explicit HTTP transaction
//            creating nine 2 MiB documents: one buffered_commit record.
// With and without a snapshot (old-build compaction) before the record,
// stopped cleanly or by SIGKILL. The stores, the state each one acknowledged
// and the state its store holds without its journal are kept under UOR_ROOT
// for the reader run with the new build.

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
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type uorScenario struct {
	name     string
	path     string // api | qdrant | async
	snapshot bool
	kill     bool
}

func uorScenarios() []uorScenario {
	var out []uorScenario
	for _, path := range []string{"api", "qdrant", "async"} {
		for _, snap := range []bool{false, true} {
			for _, kill := range []bool{false, true} {
				name := path
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
				out = append(out, uorScenario{name: name, path: path, snapshot: snap, kill: kill})
			}
		}
	}
	return out
}

func uorRoot() string {
	if r := os.Getenv("UOR_ROOT"); r != "" {
		return r
	}
	return "/work/tnrun/v152-upg/v151/uor"
}

func uorDBConfig(sc uorScenario, snapshotInterval time.Duration, logw io.Writer) *nornicdb.Config {
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.EmbeddingEnabled = false
	cfg.Database.WALAutoCompactionEnabled = true
	cfg.Database.WALSnapshotInterval = snapshotInterval
	if sc.path == "async" {
		cfg.Database.AsyncWritesEnabled = true
		cfg.Database.StrictDurability = false
		if sc.kill {
			// A long write-behind flush interval keeps the acknowledged
			// commit only in the journal until the kill.
			cfg.Database.AsyncFlushInterval = 10 * time.Minute
		}
	} else {
		cfg.Database.StrictDurability = true
		cfg.Database.WALSyncMode = "immediate"
	}
	cfg.Logger = slog.New(slog.NewTextHandler(logw, &slog.HandlerOptions{Level: slog.LevelInfo}))
	return cfg
}

type uorNodeState struct {
	ID        string   `json:"id"`
	Labels    []string `json:"labels"`
	PropsHash string   `json:"propsHash"`
	PropBytes int      `json:"propBytes"`
	Chunks    int      `json:"chunks"`
	ChunkHash string   `json:"chunkHash"`
	Named     []string `json:"named"`
	NamedHash string   `json:"namedHash"`
}

type uorEdgeState struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Start     string `json:"start"`
	End       string `json:"end"`
	PropsHash string `json:"propsHash"`
}

type uorState struct {
	Nodes []uorNodeState `json:"nodes"`
	Edges []uorEdgeState `json:"edges"`
}

func uorHashVectors(h io.Writer, vs [][]float32) {
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

func uorHashProps(p map[string]any) (string, int) {
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

// uorCapture reads every user-database node and relationship of the base
// engine (all namespaces except system), with every vector.
func uorCapture(t testing.TB, engine storage.Engine) uorState {
	var st uorState
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
		ph, pb := uorHashProps(full.Properties)
		ch := sha256.New()
		uorHashVectors(ch, full.ChunkEmbeddings)
		names := make([]string, 0, len(full.NamedEmbeddings))
		for name := range full.NamedEmbeddings {
			names = append(names, name)
		}
		sort.Strings(names)
		nh := sha256.New()
		for _, name := range names {
			fmt.Fprintf(nh, "%s;", name)
			uorHashVectors(nh, [][]float32{full.NamedEmbeddings[name]})
		}
		st.Nodes = append(st.Nodes, uorNodeState{ID: string(n.ID), Labels: labels, PropsHash: ph, PropBytes: pb,
			Chunks: len(full.ChunkEmbeddings), ChunkHash: hex.EncodeToString(ch.Sum(nil)), Named: names, NamedHash: hex.EncodeToString(nh.Sum(nil))})
	}
	edges, err := upgAllEdges(engine)
	require.NoError(t, err)
	for _, e := range edges {
		if strings.HasPrefix(string(e.ID), "system:") {
			continue
		}
		ph, _ := uorHashProps(e.Properties)
		st.Edges = append(st.Edges, uorEdgeState{ID: string(e.ID), Type: e.Type, Start: string(e.StartNode), End: string(e.EndNode), PropsHash: ph})
	}
	sort.Slice(st.Nodes, func(i, j int) bool { return st.Nodes[i].ID < st.Nodes[j].ID })
	sort.Slice(st.Edges, func(i, j int) bool { return st.Edges[i].ID < st.Edges[j].ID })
	return st
}

func uorCompare(tag string, want, got uorState) (missing, changed int) {
	gotNodes := map[string]uorNodeState{}
	for _, n := range got.Nodes {
		gotNodes[n.ID] = n
	}
	byLabelMissing := map[string]int{}
	for _, w := range want.Nodes {
		g, ok := gotNodes[w.ID]
		if !ok {
			missing++
			byLabelMissing[strings.Join(w.Labels, ",")]++
			continue
		}
		delete(gotNodes, w.ID)
		if strings.Join(w.Labels, ",") != strings.Join(g.Labels, ",") || w.PropsHash != g.PropsHash || w.Chunks != g.Chunks ||
			w.ChunkHash != g.ChunkHash || strings.Join(w.Named, ",") != strings.Join(g.Named, ",") || w.NamedHash != g.NamedHash {
			changed++
			fmt.Printf("UOR_DIFF %s node %s CHANGED: labels %v->%v props %d->%d chunks %d->%d named %v->%v\n", tag, w.ID, w.Labels, g.Labels, w.PropBytes, g.PropBytes, w.Chunks, g.Chunks, w.Named, g.Named)
		}
	}
	for label, n := range byLabelMissing {
		fmt.Printf("UOR_DIFF %s nodes MISSING labels=%s count=%d\n", tag, label, n)
	}
	for id := range gotNodes {
		fmt.Printf("UOR_DIFF %s node %s EXTRA\n", tag, id)
	}
	gotEdges := map[string]uorEdgeState{}
	for _, e := range got.Edges {
		gotEdges[e.ID] = e
	}
	for _, w := range want.Edges {
		g, ok := gotEdges[w.ID]
		if !ok {
			missing++
			fmt.Printf("UOR_DIFF %s edge %s MISSING\n", tag, w.ID)
			continue
		}
		delete(gotEdges, w.ID)
		if g != w {
			changed++
			fmt.Printf("UOR_DIFF %s edge %s CHANGED\n", tag, w.ID)
		}
	}
	for id := range gotEdges {
		fmt.Printf("UOR_DIFF %s edge %s EXTRA\n", tag, id)
	}
	fmt.Printf("UOR_COUNTS %s nodes want=%d got=%d edges want=%d got=%d missing=%d changed=%d\n",
		tag, len(want.Nodes), len(got.Nodes), len(want.Edges), len(got.Edges), missing, changed)
	return missing, changed
}

func uorCypher(t testing.TB, db *nornicdb.DB, q string, params map[string]any) {
	_, err := db.ExecuteCypher(context.Background(), q, params)
	require.NoError(t, err, q)
}

func uorBefore(t testing.TB, db *nornicdb.DB) {
	uorCypher(t, db, "CREATE INDEX small_name FOR (n:Small) ON (n.name)", nil)
	for i := 0; i < 20; i++ {
		uorCypher(t, db, "CREATE (n:Small {uid:$uid, name:$name, n:$i})",
			map[string]any{"uid": fmt.Sprintf("small-%02d", i), "name": fmt.Sprintf("small-%d", i), "i": int64(i)})
	}
	for i := 0; i < 19; i++ {
		uorCypher(t, db, "MATCH (a:Small {uid:$a}), (b:Small {uid:$b}) CREATE (a)-[:NEXT {uid:$uid, w:$i}]->(b)",
			map[string]any{"a": fmt.Sprintf("small-%02d", i), "b": fmt.Sprintf("small-%02d", i+1), "uid": fmt.Sprintf("next-%02d", i), "i": int64(i)})
	}
}

func uorAfter(t testing.TB, db *nornicdb.DB) {
	for i := 0; i < 10; i++ {
		uorCypher(t, db, "CREATE (n:After {uid:$uid, name:$name})", map[string]any{"uid": fmt.Sprintf("after-%02d", i), "name": fmt.Sprintf("after-%d", i)})
	}
	uorCypher(t, db, "MATCH (a:After {uid:'after-00'}), (b:Small {uid:'small-00'}) CREATE (a)-[:LINK {uid:'link-00'}]->(b)", nil)
	uorCypher(t, db, "MATCH (n:Small {uid:'small-05'}) SET n.updated = 'after-big', n:Touched", nil)
	uorCypher(t, db, "MATCH (n:Small {uid:'small-06'}) REMOVE n.name", nil)
}

const uorAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

// uorRandomText is document-like content that does not compress below 6 bits
// per character (base64-encoded attachments, for example).
func uorRandomText(seed int64, n int) string {
	r := rand.New(rand.NewSource(seed))
	b := make([]byte, n)
	for i := range b {
		b[i] = uorAlphabet[r.Intn(len(uorAlphabet))]
	}
	return string(b)
}

func uorVector(seed int64, dims int) []float32 {
	r := rand.New(rand.NewSource(seed))
	v := make([]float32, dims)
	for i := range v {
		v[i] = r.Float32()*2 - 1
	}
	return v
}

type uorServer struct {
	server *Server
	auth   *auth.Authenticator
	token  string
}

func uorStartServer(t testing.TB, db *nornicdb.DB, qdrant bool) *uorServer {
	authConfig := auth.AuthConfig{SecurityEnabled: true, JWTSecret: []byte("test-secret-key-for-testing-only-32b")}
	systemStorage := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system")
	authenticator, err := auth.NewAuthenticator(authConfig, systemStorage)
	require.NoError(t, err)
	if _, err := authenticator.CreateUser(context.Background(), "admin", "password123", []auth.Role{auth.RoleAdmin}); err != nil {
		require.Contains(t, strings.ToLower(err.Error()), "exist")
	}
	serverConfig := DefaultConfig()
	serverConfig.Port = 0
	serverConfig.EmbeddingEnabled = false
	if qdrant {
		serverConfig.Features = &nornicConfig.FeatureFlagsConfig{
			QdrantGRPCEnabled:        true,
			QdrantGRPCListenAddr:     "127.0.0.1:0",
			QdrantGRPCMaxVectorDim:   4096,
			QdrantGRPCMaxBatchPoints: 1000,
			QdrantGRPCMaxTopK:        1000,
		}
	}
	server, err := New(db, authenticator, serverConfig)
	require.NoError(t, err)
	require.NoError(t, server.Start())
	tokenResp, _, err := authenticator.Authenticate(context.Background(), "admin", "password123", "127.0.0.1", "TestAgent")
	require.NoError(t, err)
	return &uorServer{server: server, auth: authenticator, token: tokenResp.AccessToken}
}

func (s *uorServer) stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = s.server.Stop(ctx)
}

// uorQdrantUpsert creates a 3,072-dimension collection and upserts 1,000
// points in ONE Upsert call over the real gRPC listener.
func uorQdrantUpsert(t testing.TB, s *uorServer) {
	conn, err := grpc.Dial(s.server.qdrantGRPCServer.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(64<<20), grpc.MaxCallRecvMsgSize(64<<20)))
	require.NoError(t, err)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+s.token))
	collections, points := qpb.NewCollectionsClient(conn), qpb.NewPointsClient(conn)
	_, err = collections.Create(ctx, &qpb.CreateCollection{
		CollectionName: "big_col",
		VectorsConfig:  &qpb.VectorsConfig{Config: &qpb.VectorsConfig_Params{Params: &qpb.VectorParams{Size: 3072, Distance: qpb.Distance_Cosine}}},
	})
	require.NoError(t, err)
	batch := make([]*qpb.PointStruct, 0, 1000)
	for i := 0; i < 1000; i++ {
		batch = append(batch, &qpb.PointStruct{
			Id:      &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: uint64(i + 1)}},
			Vectors: &qpb.Vectors{VectorsOptions: &qpb.Vectors_Vector{Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: uorVector(int64(1000+i), 3072)}}}}},
			Payload: map[string]*qpb.Value{"uid": {Kind: &qpb.Value_StringValue{StringValue: fmt.Sprintf("qp-%04d", i)}}},
		})
	}
	_, err = points.Upsert(ctx, &qpb.UpsertPoints{CollectionName: "big_col", Points: batch})
	require.NoError(t, err)
	fmt.Println("UOR_QDRANT_UPSERT points=1000 dims=3072 ok")
}

func uorHTTP(t testing.TB, s *uorServer, path string, body any) map[string]any {
	resp := makeRequest(t, s.server, "POST", path, body, "Bearer "+s.token)
	require.Contains(t, []int{http.StatusOK, http.StatusCreated, http.StatusAccepted}, resp.Code, resp.Body.String())
	var payload map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &payload))
	if errs, _ := payload["errors"].([]any); len(errs) > 0 {
		t.Fatalf("HTTP %s errors: %v", path, errs)
	}
	return payload
}

// uorAsyncTransaction writes nine 2 MiB documents in one explicit HTTP
// transaction (three requests below the 10 MB request limit, then commit).
func uorAsyncTransaction(t testing.TB, s *uorServer) {
	stmt := func(first int) map[string]any {
		rows := make([]any, 0, 3)
		for i := first; i < first+3; i++ {
			rows = append(rows, map[string]any{"uid": fmt.Sprintf("asyncdoc-%02d", i), "body": uorRandomText(int64(5000+i), 2<<20)})
		}
		return map[string]any{"statements": []any{map[string]any{
			"statement":  "UNWIND $rows AS r CREATE (n:AsyncDoc {uid: r.uid, body: r.body})",
			"parameters": map[string]any{"rows": rows},
		}}}
	}
	open := uorHTTP(t, s, "/db/nornic/tx", stmt(0))
	commit, _ := open["commit"].(string)
	require.NotEmpty(t, commit)
	idx := strings.Index(commit, "/db/nornic/tx/")
	require.GreaterOrEqual(t, idx, 0, commit)
	txPath := strings.TrimSuffix(commit[idx:], "/commit")
	uorHTTP(t, s, txPath, stmt(3))
	uorHTTP(t, s, txPath, stmt(6))
	uorHTTP(t, s, txPath+"/commit", map[string]any{"statements": []any{}})
	fmt.Printf("UOR_ASYNC_COMMIT docs=9 bytes=%d ok\n", 9*(2<<20))
}

func uorAPIWrite(t testing.TB, db *nornicdb.DB) {
	ctx := context.Background()
	node, err := db.CreateNode(ctx, []string{"ApiDoc"}, map[string]any{"uid": "apidoc", "body": uorRandomText(77, 18<<20)})
	require.NoError(t, err)
	_, err = db.UpdateNode(ctx, node.ID, map[string]any{"note": "updated through the API"})
	require.NoError(t, err)
	fmt.Printf("UOR_API_WRITE id=%s bytes=%d ok\n", node.ID, 18<<20)
}

func uorCountFiles(dir, prefix string) int {
	entries, _ := os.ReadDir(dir)
	n := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), prefix) {
			n++
		}
	}
	return n
}

type uorRunning struct {
	db  *nornicdb.DB
	srv *uorServer
}

func (r *uorRunning) stop(t testing.TB) {
	if r.srv != nil {
		r.srv.stop()
	}
	require.NoError(t, r.db.Close())
}

func uorBuild(t testing.TB, sc uorScenario, dataDir string, logw io.Writer) *uorRunning {
	if sc.snapshot {
		db, err := nornicdb.Open(dataDir, uorDBConfig(sc, 2*time.Second, logw))
		require.NoError(t, err)
		uorBefore(t, db)
		snapDir := filepath.Join(dataDir, "snapshots")
		deadline := time.Now().Add(2 * time.Minute)
		for uorCountFiles(snapDir, "snapshot-") == 0 && time.Now().Before(deadline) {
			time.Sleep(100 * time.Millisecond)
		}
		require.Positive(t, uorCountFiles(snapDir, "snapshot-"), "no snapshot was taken")
		time.Sleep(500 * time.Millisecond)
		require.NoError(t, db.Close())
	}
	db, err := nornicdb.Open(dataDir, uorDBConfig(sc, time.Hour, logw))
	require.NoError(t, err)
	run := &uorRunning{db: db}
	if !sc.snapshot {
		uorBefore(t, db)
	}
	switch sc.path {
	case "api":
		uorAPIWrite(t, db)
	case "qdrant":
		run.srv = uorStartServer(t, db, true)
		uorQdrantUpsert(t, run.srv)
	case "async":
		run.srv = uorStartServer(t, db, false)
		uorAsyncTransaction(t, run.srv)
	}
	uorAfter(t, db)
	return run
}

func uorListing(dir string) []string {
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

func uorSaveJSON(t testing.TB, path string, v any) {
	raw, err := json.MarshalIndent(v, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, raw, 0o644))
}

func uorCopyDir(t testing.TB, src, dst string) {
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

func uorScan(tag, walDir string) {
	scanner, _ := filepath.Abs(filepath.Join("testdata", "zz_uor", "wal_frame_scan.py"))
	cmd := exec.Command("python3", "-I", scanner, "--quiet", walDir)
	out, err := cmd.CombinedOutput()
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

// uorStoreOnly opens a copy of the store without its journal and reports
// which acknowledged entities the store itself holds.
func uorStoreOnly(t testing.TB, base, name string, want uorState) {
	copyDir := filepath.Join(base, "store-only-copy")
	require.NoError(t, os.RemoveAll(copyDir))
	uorCopyDir(t, filepath.Join(base, "data"), copyDir)
	defer os.RemoveAll(copyDir)
	engine, err := storage.NewBadgerEngine(copyDir)
	if err != nil {
		fmt.Printf("UOR_STORE_ONLY %s open err=%v\n", name, err)
		return
	}
	got := uorCapture(t, engine)
	_ = engine.Close()
	uorCompare(name+" store-only", want, got)
}

// TestUORWriteChild is the killed process of a kill scenario.
func TestUORWriteChild(t *testing.T) {
	name := os.Getenv("UOR_CHILD")
	if name == "" {
		t.Skip("child only")
	}
	var sc uorScenario
	for _, s := range uorScenarios() {
		if s.name == name {
			sc = s
		}
	}
	require.Equal(t, name, sc.name)
	base := filepath.Join(uorRoot(), "stores", sc.name)
	logf, err := os.Create(filepath.Join(base, "write.log"))
	require.NoError(t, err)
	run := uorBuild(t, sc, filepath.Join(base, "data"), logf)
	st := uorCapture(t, run.db.GetBaseStorageForManager())
	uorSaveJSON(t, filepath.Join(base, "expected.json"), st)
	fmt.Println("UOR_READY")
	select {}
}

func TestUORWrite(t *testing.T) {
	root := uorRoot()
	only := os.Getenv("UOR_ONLY")
	for _, sc := range uorScenarios() {
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
				run := uorBuild(t, sc, filepath.Join(base, "data"), logf)
				st := uorCapture(t, run.db.GetBaseStorageForManager())
				uorSaveJSON(t, filepath.Join(base, "expected.json"), st)
				run.stop(t)
			} else {
				cmd := exec.Command(os.Args[0], "-test.run=^TestUORWriteChild$", "-test.v", "-test.timeout=30m")
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
			}
			fmt.Printf("UOR_WAL_LISTING %s %s\n", sc.name, strings.Join(uorListing(filepath.Join(base, "data", "wal")), " | "))
			uorScan(sc.name, filepath.Join(base, "data", "wal"))
			raw, err := os.ReadFile(filepath.Join(base, "expected.json"))
			require.NoError(t, err)
			var want uorState
			require.NoError(t, json.Unmarshal(raw, &want))
			uorStoreOnly(t, base, sc.name, want)
		})
	}
}
