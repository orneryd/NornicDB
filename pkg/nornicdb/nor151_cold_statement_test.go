package nornicdb_test

// NOR-151 reproduction: the first execution of a long application statement in
// a fresh process.
//
// The three statements in testdata/nor151_statements.json are archived
// application statements, byte for byte, keyed by the first 16 hex digits of
// their SHA-256:
//
//	42f785d4d9dbeb87  67,845 bytes  register validation read
//	dd6661ce13671934  41,072 bytes  freshness/register read
//	588c584d45dab5f8  32,047 bytes  freshness/register read
//
// The application's original parameters and data were not kept. The fixture and
// parameters below are the synthetic ones every earlier measurement of this
// claim used: 100 documents, 100 readings, 100 identities, one active
// correction, no relationships.
//
// Each statement runs in its own child process (a fresh process: no parser or
// plan state from an earlier statement of the same text). The child opens a
// disk database through the public nornicdb.Open path, loads the fixture,
// then executes the statement three times and prints one NOR151 line per
// execution. The parent asserts:
//   - every execution succeeds and the three results are identical;
//   - the first execution finishes within NOR151_FIRST_MAX (default 1s, the
//     acceptance bound written on the ticket: "First execution takes under one
//     second on both parser paths ... with identical results").
//
// Run (both parsers):
//
//	go test -tags=noui,nolocalllm ./pkg/nornicdb -run '^TestNOR151FreshProcessFirstExecution$' -count=1 -v -timeout=40m
//	NORNICDB_PARSER=antlr go test -tags=noui,nolocalllm ./pkg/nornicdb -run '^TestNOR151FreshProcessFirstExecution$' -count=1 -v -timeout=40m

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	nconfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

const nor151ChildEnv = "NOR151_CHILD_STATEMENT"

var nor151Keys = []string{"42f785d4d9dbeb87", "dd6661ce13671934", "588c584d45dab5f8"}

type nor151Record struct {
	Key          string `json:"key"`
	Parser       string `json:"parser"`
	Bytes        int    `json:"bytes"`
	Run          int    `json:"run"`
	Nanoseconds  int64  `json:"ns"`
	Rows         int    `json:"rows"`
	ResultSHA256 string `json:"result_sha256"`
	Allocated    uint64 `json:"allocated_bytes"`
	GCCycles     uint32 `json:"gc_cycles"`
	Error        string `json:"error,omitempty"`
	Go           string `json:"go"`
	GOMAXPROCS   int    `json:"gomaxprocs"`
}

func TestNOR151FreshProcessFirstExecution(t *testing.T) {
	if os.Getenv(nor151ChildEnv) != "" {
		t.Skip("parent only")
	}
	bound := time.Second
	if v := os.Getenv("NOR151_FIRST_MAX"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("NOR151_FIRST_MAX: %v", err)
		}
		bound = d
	}
	for _, key := range nor151Keys {
		cmd := exec.Command(os.Args[0], "-test.run=^TestNOR151Child$", "-test.v", "-test.count=1", "-test.timeout=30m")
		cmd.Env = append(os.Environ(), nor151ChildEnv+"="+key)
		out, err := cmd.CombinedOutput()
		var records []nor151Record
		scanner := bufio.NewScanner(bytes.NewReader(out))
		scanner.Buffer(make([]byte, 1<<20), 1<<26)
		for scanner.Scan() {
			line := scanner.Text()
			if i := strings.Index(line, "NOR151 "); i >= 0 {
				t.Log(strings.TrimSpace(line[i:]))
				var r nor151Record
				if json.Unmarshal([]byte(line[i+len("NOR151 "):]), &r) == nil {
					records = append(records, r)
				}
			}
		}
		if err != nil || len(records) != 3 {
			t.Errorf("statement %s: child failed (err=%v, records=%d)\n%s", key, err, len(records), tail(out, 4000))
			continue
		}
		for _, r := range records {
			if r.Error != "" {
				t.Errorf("statement %s run %d: %s", key, r.Run, r.Error)
			}
			if r.ResultSHA256 != records[0].ResultSHA256 {
				t.Errorf("statement %s run %d: result %s differs from first run %s", key, r.Run, r.ResultSHA256, records[0].ResultSHA256)
			}
		}
		first := time.Duration(records[0].Nanoseconds)
		t.Logf("NOR151_SUMMARY key=%s bytes=%d parser=%s first=%s repeat1=%s repeat2=%s rows=%d result=%s bound=%s",
			key, records[0].Bytes, records[0].Parser, first, time.Duration(records[1].Nanoseconds), time.Duration(records[2].Nanoseconds),
			records[0].Rows, records[0].ResultSHA256, bound)
		if first > bound {
			t.Errorf("statement %s (%d bytes, parser %s): first execution in a fresh process took %s, over the %s bound; repeats took %s and %s",
				key, records[0].Bytes, records[0].Parser, first, bound, time.Duration(records[1].Nanoseconds), time.Duration(records[2].Nanoseconds))
		}
	}
}

func tail(b []byte, n int) string {
	if len(b) > n {
		b = b[len(b)-n:]
	}
	return string(b)
}

func TestNOR151Child(t *testing.T) {
	key := os.Getenv(nor151ChildEnv)
	if key == "" {
		t.Skip("child only")
	}
	raw, err := os.ReadFile("testdata/nor151_statements.json")
	if err != nil {
		t.Fatal(err)
	}
	var texts map[string]string
	if err := json.Unmarshal(raw, &texts); err != nil {
		t.Fatal(err)
	}
	q, ok := texts[key]
	if !ok {
		t.Fatalf("unknown statement %s", key)
	}
	if sum := sha256.Sum256([]byte(q)); hex.EncodeToString(sum[:])[:16] != key {
		t.Fatalf("statement %s: bytes do not match their key", key)
	}
	parser := "default"
	if nconfig.IsANTLRParser() {
		parser = "antlr"
	}

	cfg := nornicdb.DefaultConfig()
	cfg.Database.AsyncWritesEnabled = false
	cfg.Database.StrictDurability = true
	cfg.Database.WALSyncMode = "immediate"
	db, err := nornicdb.Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	run := func(q string, p map[string]interface{}) *nornicdb.CypherResult {
		r, err := db.ExecuteCypher(ctx, q, p)
		if err != nil {
			t.Fatalf("%s: %v", q[:min(len(q), 80)], err)
		}
		return r
	}

	// Fixture: identical to the earlier measurements of this claim.
	ids := []interface{}{}
	for i := 0; i < 100; i++ {
		id, rid, gid := fmt.Sprintf("d%d", i), fmt.Sprintf("r%d", i), fmt.Sprintf("g%d", i)
		ids = append(ids, id, gid)
		run("CREATE (d:PDDocument {id:$id,current_version_id:'v1',read_reading_id:$rid,due_at:'2026-01-01T00:00:00Z',read_knowledge_at:'2026-01-02T00:00:00Z'}) CREATE (r:PDReading {id:$rid,identity_ids:[$gid],identity_revisions:[1],subject_ids:[],subject_versions:[],used_ids:[$gid]}) CREATE (g:PDGraphNode:PDIdentity {id:$gid,revision:1})",
			map[string]interface{}{"id": id, "rid": rid, "gid": gid})
	}
	run("CREATE (c:PDCorrection {id:'c0',status:'active',revision:1,scope:['v1']})", nil)
	if got := fmt.Sprint(run("MATCH (d:PDDocument) RETURN count(d) AS n", nil).Rows); got != "[[100]]" {
		t.Fatalf("fixture documents: %s", got)
	}
	params := map[string]interface{}{"full": true, "ids": ids, "correction_ids": []interface{}{"c0"}, "mentions": []interface{}{}, "edges": []interface{}{},
		"creatable": map[string]interface{}{}, "document_id": "d0", "source_id": "s0", "version_id": "v1", "separator": "|"}

	for run := 0; run < 3; run++ {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		start := time.Now()
		r, err := db.ExecuteCypher(ctx, q, params)
		elapsed := time.Since(start)
		runtime.ReadMemStats(&after)
		rec := nor151Record{Key: key, Parser: parser, Bytes: len(q), Run: run, Nanoseconds: elapsed.Nanoseconds(),
			Allocated: after.TotalAlloc - before.TotalAlloc, GCCycles: after.NumGC - before.NumGC, Go: runtime.Version(), GOMAXPROCS: runtime.GOMAXPROCS(0)}
		if err != nil {
			rec.Error = err.Error()
		} else {
			value, merr := json.Marshal(struct {
				Columns []string        `json:"columns"`
				Rows    [][]interface{} `json:"rows"`
			}{r.Columns, r.Rows})
			if merr != nil {
				t.Fatal(merr)
			}
			sum := sha256.Sum256(value)
			rec.ResultSHA256 = hex.EncodeToString(sum[:])
			rec.Rows = len(r.Rows)
		}
		line, _ := json.Marshal(rec)
		fmt.Println("NOR151 " + string(line))
	}
}
