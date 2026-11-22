package nornicdb

// Hunt ground H3, part 2: stores of older builds opened by the candidate.
//
// The same file runs on two kinds of checkout:
//   - on an older build (b6d8e762a, v1.5.0 c99e77131, v1.5.1 cb0a574f7) as an
//     untracked overlay: TestHuntH3StoreWrite generates the 8 store shapes from
//     seed 20261120 under /work/tnrun/v152-hunt-h3/<build>/<shape>/, and writes
//     next to each store the old build's own view of it (model.json);
//   - on the candidate: TestHuntH3StoreRead copies each store (the original is
//     never opened) and opens the copy without, then with, storage upgrade
//     allowed, compares the whole graph with model.json, writes one node,
//     reopens and reads it back.
//
// It uses only the API every one of these builds has: Open, DefaultConfig,
// ExecuteCypher, Close.
//
// Oracle: the store opens (with upgrade allowed if the first open refuses, and
// a refused open leaves the files unchanged); every node and relationship of
// the old build's view is present with the same labels, type and properties,
// and nothing else; a write after the open persists across a reopen.
//
// Shapes (seeded generator, config of the writer):
//
//	s1 durable, mixed graph: 500 nodes of 5 labels, 1,000 relationships, every property type
//	s2 s1 plus range, composite, unique and full-text indexes and constraints
//	s3 asynchronous writes, 2,000 nodes, clean close
//	s4 durable churn: 1,000 nodes created, half updated, a quarter deleted, relationships re-pointed
//	s5 durable large values: 64 KiB strings, 10,000-integer lists, 300 nodes
//	s6 durable 128-dimension float lists on 1,000 nodes plus a vector index
//	s7 durable, written in many small transactions (5,000 auto-commits) then closed
//	s8 the s1 content written by a child process that exits without closing the database
//	   (model: the same content written and closed cleanly in a sibling directory)
//
// Run (old build, overlay): HUNT_H3_BUILD=<b6|v150|v151> go test ./pkg/nornicdb -run '^TestHuntH3StoreWrite$' -count=1 -timeout=60m -v
// Run (candidate):          go test ./pkg/nornicdb -run '^TestHuntH3StoreRead$' -count=1 -timeout=60m -v

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

const (
	huntH3Root = "/work/tnrun/v152-hunt-h3"
	huntH3Seed = 20261120
)

var huntH3Shapes = []string{"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"}

func huntH3RootDir() string {
	if v := os.Getenv("HUNT_H3_ROOT"); v != "" {
		return v
	}
	return huntH3Root
}

func huntH3Config(shape string, upgrade bool) *Config {
	cfg := DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.EmbeddingEnabled = false
	cfg.Database.AllowStorageUpgrade = upgrade
	cfg.Database.AsyncWritesEnabled = shape == "s3"
	return cfg
}

func huntH3Exec(t testing.TB, db *DB, query string, params map[string]interface{}) *CypherResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	result, err := db.ExecuteCypher(ctx, query, params)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return result
}

// huntH3Generate writes shape's content (deterministic from the seed).
func huntH3Generate(t testing.TB, db *DB, shape string) {
	rnd := rand.New(rand.NewSource(huntH3Seed + int64(shape[1])))
	labels := []string{"HA", "HB", "HC", "HD", "HE"}
	value := func(i int) map[string]interface{} {
		return map[string]interface{}{
			"i": int64(i), "f": rnd.Float64() * 1e6, "s": fmt.Sprintf("v-%d-%x", i, rnd.Int63()),
			"b": rnd.Intn(2) == 0, "li": []interface{}{int64(i), int64(i * 2), int64(-i)},
			"ls": []interface{}{"x", fmt.Sprint(i)}, "neg": -float64(i) / 3,
		}
	}
	mixed := func(n, rels int) {
		for i := 0; i < n; i++ {
			p := value(i)
			p["id"] = int64(i)
			huntH3Exec(t, db, fmt.Sprintf("CREATE (n:%s:HAll) SET n = $p, n.d = date('2026-01-01') + duration({days: $i}), n.t = datetime('2026-01-01T00:00:00Z') + duration({seconds: $i})", labels[i%len(labels)]), map[string]interface{}{"p": p, "i": int64(i)})
		}
		for r := 0; r < rels; r++ {
			a, b := rnd.Intn(n), rnd.Intn(n)
			huntH3Exec(t, db, "MATCH (a:HAll {id: $a}), (b:HAll {id: $b}) CREATE (a)-[:HR {k: $k, w: $w}]->(b)", map[string]interface{}{"a": int64(a), "b": int64(b), "k": int64(r), "w": rnd.Float64()})
		}
	}
	switch shape {
	case "s1", "s8":
		mixed(500, 1000)
	case "s2":
		huntH3Exec(t, db, "CREATE CONSTRAINT h3_id IF NOT EXISTS FOR (n:HA) REQUIRE n.id IS UNIQUE", nil)
		huntH3Exec(t, db, "CREATE INDEX h3_s IF NOT EXISTS FOR (n:HB) ON (n.s)", nil)
		huntH3Exec(t, db, "CREATE INDEX h3_comp IF NOT EXISTS FOR (n:HC) ON (n.i, n.s)", nil)
		huntH3Exec(t, db, "CREATE FULLTEXT INDEX h3_ft IF NOT EXISTS FOR (n:HD) ON EACH [n.s]", nil)
		mixed(500, 1000)
	case "s3":
		for i := 0; i < 2000; i += 100 {
			huntH3Exec(t, db, "UNWIND range($from, $from + 99) AS i CREATE (:HAll:HA {id: i, s: 'a' + toString(i)})", map[string]interface{}{"from": int64(i)})
		}
	case "s4":
		mixed(1000, 1500)
		huntH3Exec(t, db, "MATCH (n:HAll) WHERE n.id % 2 = 0 SET n.updated = true, n.s = n.s + '-u'", nil)
		huntH3Exec(t, db, "MATCH (n:HAll) WHERE n.id % 4 = 1 DETACH DELETE n", nil)
		huntH3Exec(t, db, "MATCH (a:HAll)-[r:HR]->(b:HAll) WHERE r.k % 3 = 0 WITH a, b, r, r.k AS k DELETE r CREATE (b)-[:HR {k: k, repointed: true}]->(a)", nil)
	case "s5":
		big := strings.Repeat("0123456789abcdef", 4096)
		list := make([]interface{}, 10000)
		for i := range list {
			list[i] = int64(i)
		}
		for i := 0; i < 300; i++ {
			huntH3Exec(t, db, "CREATE (:HAll:HBig {id: $i, big: $big, list: $list})", map[string]interface{}{"i": int64(i), "big": big + fmt.Sprint(i), "list": list})
		}
	case "s6":
		for i := 0; i < 1000; i++ {
			vec := make([]interface{}, 128)
			for d := range vec {
				vec[d] = rnd.Float64()*2 - 1
			}
			huntH3Exec(t, db, "CREATE (:HAll:HVec {id: $i, embedding: $v})", map[string]interface{}{"i": int64(i), "v": vec})
		}
		huntH3Exec(t, db, "CREATE VECTOR INDEX h3_vec IF NOT EXISTS FOR (n:HVec) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 128, `vector.similarity_function`: 'cosine'}}", nil)
	case "s7":
		for i := 0; i < 5000; i++ {
			huntH3Exec(t, db, "CREATE (:HAll:HSmall {id: $i})", map[string]interface{}{"i": int64(i)})
		}
	}
}

// huntH3Dump is a build's view of the whole graph: canonical JSON rows,
// sorted (multisets), plus the index and constraint names.
type huntH3Dump struct {
	Nodes         []string `json:"nodes"`
	Relationships []string `json:"relationships"`
	Schema        []string `json:"schema"`
}

func huntH3Canon(t testing.TB, db *DB, query string) []string {
	result := huntH3Exec(t, db, query, nil)
	out := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		data, err := json.Marshal(row)
		if err != nil {
			data = []byte(fmt.Sprintf("%#v", row))
		}
		out = append(out, string(data))
	}
	sort.Strings(out)
	return out
}

func huntH3Read(t testing.TB, db *DB) huntH3Dump {
	var schema []string
	for _, q := range []string{"SHOW INDEXES YIELD name RETURN name", "SHOW CONSTRAINTS YIELD name RETURN name"} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		if result, err := db.ExecuteCypher(ctx, q, nil); err == nil {
			for _, row := range result.Rows {
				schema = append(schema, fmt.Sprint(row[0]))
			}
		}
		cancel()
	}
	sort.Strings(schema)
	return huntH3Dump{
		Nodes:         huntH3Canon(t, db, "MATCH (n) RETURN labels(n) AS l, properties(n) AS p"),
		Relationships: huntH3Canon(t, db, "MATCH (a)-[r]->(b) RETURN type(r) AS t, properties(r) AS p, a.id AS src, b.id AS dst"),
		Schema:        schema,
	}
}

func huntH3Write(t testing.TB, dir, shape string) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := Open(filepath.Join(dir, "store"), huntH3Config(shape, false))
	if err != nil {
		t.Fatalf("%s: open: %v", dir, err)
	}
	huntH3Generate(t, db, shape)
	dump := huntH3Read(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("%s: close: %v", dir, err)
	}
	data, _ := json.Marshal(dump)
	if err := os.WriteFile(filepath.Join(dir, "model.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("HUNT_H3_WRITTEN %s nodes=%d rels=%d schema=%d\n", dir, len(dump.Nodes), len(dump.Relationships), len(dump.Schema))
}

// TestHuntH3StoreWriteChild writes s8's content and exits without Close.
func TestHuntH3StoreWriteChild(t *testing.T) {
	dir := os.Getenv("HUNT_H3_CHILD_STORE")
	if dir == "" {
		t.Skip("child of TestHuntH3StoreWrite only")
	}
	db, err := Open(dir, huntH3Config("s8", false))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	huntH3Generate(t, db, "s8")
	fmt.Println("HUNT_H3_CHILD_WRITTEN")
	os.Exit(0) // no Close: the store is left as a crash leaves it
}

func TestHuntH3StoreWrite(t *testing.T) {
	build := os.Getenv("HUNT_H3_BUILD")
	if build == "" {
		t.Skip("set HUNT_H3_BUILD (b6, v150, v151) on an older build's checkout")
	}
	root := filepath.Join(huntH3RootDir(), build)
	if _, err := os.Stat(root); err == nil {
		t.Fatalf("%s exists: stores are written once (frozen population)", root)
	}
	for _, shape := range huntH3Shapes {
		dir := filepath.Join(root, shape)
		if shape != "s8" {
			huntH3Write(t, dir, shape)
			continue
		}
		// model: the same content closed cleanly
		huntH3Write(t, filepath.Join(root, "s8-model"), shape)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestHuntH3StoreWriteChild$", "-test.count=1", "-test.v")
		cmd.Env = append(os.Environ(), "HUNT_H3_CHILD_STORE="+filepath.Join(dir, "store"))
		out, err := cmd.CombinedOutput()
		if err != nil || !strings.Contains(string(out), "HUNT_H3_CHILD_WRITTEN") {
			t.Fatalf("s8 child: %v\n%s", err, out)
		}
		model, err := os.ReadFile(filepath.Join(root, "s8-model", "model.json"))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "model.json"), model, 0o644); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("HUNT_H3_WRITTEN %s (exit without close; model from s8-model)\n", dir)
	}
}

func huntH3Fingerprint(dir string) string {
	var parts []string
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(dir, path)
		parts = append(parts, fmt.Sprintf("%s:%d:%d", rel, info.Size(), info.ModTime().UnixNano()))
		return nil
	})
	sort.Strings(parts)
	return strings.Join(parts, "\n")
}

func huntH3Diff(want, got []string) (missing, extra []string) {
	count := map[string]int{}
	for _, w := range want {
		count[w]++
	}
	for _, g := range got {
		if count[g] > 0 {
			count[g]--
			continue
		}
		extra = append(extra, g)
	}
	for k, n := range count {
		for ; n > 0; n-- {
			missing = append(missing, k)
		}
	}
	sort.Strings(missing)
	return missing, extra
}

func huntH3Lead(entry map[string]interface{}) {
	entry["ground"] = "H3"
	entry["part"] = "old-build-store"
	data, _ := json.Marshal(entry)
	fmt.Printf("HUNT_LEAD %s\n", data)
}

func TestHuntH3StoreRead(t *testing.T) {
	root := huntH3RootDir()
	builds := strings.Split(os.Getenv("HUNT_H3_BUILDS"), ",")
	if os.Getenv("HUNT_H3_BUILDS") == "" {
		builds = []string{"b6", "v150", "v151"}
	}
	if _, err := os.Stat(root); err != nil {
		t.Skipf("no stores under %s: run TestHuntH3StoreWrite on the older builds first", root)
	}
	scratch, err := os.MkdirTemp(os.Getenv("HUNT_TMP"), "hunt-h3-read-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(scratch)
	opened, leads := 0, 0
	for _, build := range builds {
		for _, shape := range huntH3Shapes {
			tag := build + "/" + shape
			src := filepath.Join(root, build, shape)
			modelData, err := os.ReadFile(filepath.Join(src, "model.json"))
			if err != nil {
				huntH3Lead(map[string]interface{}{"store": tag, "verdict": "store or model missing (writer did not produce it)", "error": err.Error()})
				leads++
				continue
			}
			var model huntH3Dump
			_ = json.Unmarshal(modelData, &model)
			dst := filepath.Join(scratch, build+"-"+shape)
			if out, err := exec.Command("cp", "-a", "--sparse=always", filepath.Join(src, "store"), dst).CombinedOutput(); err != nil {
				t.Fatalf("%s: copy: %v %s", tag, err, out)
			}
			before := huntH3Fingerprint(dst)
			upgraded := false
			db, err := Open(dst, huntH3Config(shape, false))
			if err != nil {
				if after := huntH3Fingerprint(dst); after != before {
					huntH3Lead(map[string]interface{}{"store": tag, "verdict": "open without upgrade refused but changed the files", "error": err.Error()})
					leads++
				}
				fmt.Printf("HUNT_H3_OPEN %s upgrade=false refused: %v\n", tag, err)
				db, err = Open(dst, huntH3Config(shape, true))
				upgraded = true
				if err != nil {
					huntH3Lead(map[string]interface{}{"store": tag, "verdict": "store does not open, even with upgrade allowed", "error": err.Error()})
					leads++
					continue
				}
			}
			opened++
			got := huntH3Read(t, db)
			for kind, pair := range map[string][2][]string{"nodes": {model.Nodes, got.Nodes}, "relationships": {model.Relationships, got.Relationships}, "schema": {model.Schema, got.Schema}} {
				missing, extra := huntH3Diff(pair[0], pair[1])
				if len(missing)+len(extra) > 0 {
					first := func(v []string) []string {
						if len(v) > 5 {
							return v[:5]
						}
						return v
					}
					huntH3Lead(map[string]interface{}{"store": tag, "upgraded": upgraded, "verdict": kind + " differ from the old build's view",
						"missing": len(missing), "extra": len(extra), "missingFirst5": first(missing), "extraFirst5": first(extra)})
					leads++
				}
			}
			ctx := context.Background()
			if _, err := db.ExecuteCypher(ctx, "CREATE (:HuntH3After {store: $s})", map[string]interface{}{"s": tag}); err != nil {
				huntH3Lead(map[string]interface{}{"store": tag, "verdict": "write after open refused", "error": err.Error()})
				leads++
			}
			if err := db.Close(); err != nil {
				huntH3Lead(map[string]interface{}{"store": tag, "verdict": "close after open failed", "error": err.Error()})
				leads++
			}
			db, err = Open(dst, huntH3Config(shape, upgraded))
			if err != nil {
				huntH3Lead(map[string]interface{}{"store": tag, "verdict": "second open failed", "error": err.Error()})
				leads++
				continue
			}
			result, err := db.ExecuteCypher(ctx, "MATCH (n:HuntH3After) RETURN count(n) AS c", nil)
			if err != nil || len(result.Rows) != 1 || fmt.Sprint(result.Rows[0][0]) != "1" {
				huntH3Lead(map[string]interface{}{"store": tag, "verdict": "write after open not present after reopen", "error": fmt.Sprint(err), "rows": fmt.Sprint(result)})
				leads++
			}
			_ = db.Close()
			_ = os.RemoveAll(dst)
		}
	}
	data, _ := json.Marshal(map[string]interface{}{"ground": "H3", "part": "old-build-store", "builds": builds, "storesOpened": opened, "leads": leads})
	fmt.Printf("HUNT_COVERAGE %s\n", data)
}
