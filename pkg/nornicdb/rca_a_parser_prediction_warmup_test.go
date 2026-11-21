package nornicdb_test

// Root-cause diagnostic (campaign v1.5.2, lane C, group G-parser-prediction-warmup; run by lane A).
// Test-side only. Same fixture and statements as TestNOR151FreshProcessFirstExecution.
// Two fresh-process experiments:
//   cross:   run statement B (dd6661ce, 41 KB) first, then statement A
//            (42f785d4, 68 KB) for the first time. If A's first-call excess
//            is a process-wide warm-up, it shrinks after B; if it is
//            per-text work, it stays.
//   profile: CPU profile of A's first and second execution, printed with
//            `go tool pprof -top` (flat and cumulative).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

const rcaNor151Env = "RCA_NOR151_MODE"

func TestRCANOR151FirstCall(t *testing.T) {
	if os.Getenv(rcaNor151Env) != "" {
		t.Skip("parent only")
	}
	for _, mode := range []string{"cross", "profile"} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestRCANOR151Child$", "-test.v", "-test.count=1", "-test.timeout=20m")
		cmd.Env = append(os.Environ(), rcaNor151Env+"="+mode)
		out, err := cmd.CombinedOutput()
		for _, line := range strings.Split(string(out), "\n") {
			if i := strings.Index(line, "RCA_"); i >= 0 {
				t.Log(strings.TrimSpace(line[i:]))
			}
		}
		if err != nil {
			t.Logf("RCA_NOR151 child %s err=%v\n%s", mode, err, tail(out, 3000))
		}
	}
}

func TestRCANOR151Child(t *testing.T) {
	mode := os.Getenv(rcaNor151Env)
	if mode == "" {
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
	ids := []interface{}{}
	for i := 0; i < 100; i++ {
		id, rid, gid := fmt.Sprintf("d%d", i), fmt.Sprintf("r%d", i), fmt.Sprintf("g%d", i)
		ids = append(ids, id, gid)
		if _, err := db.ExecuteCypher(ctx, "CREATE (d:PDDocument {id:$id,current_version_id:'v1',read_reading_id:$rid,due_at:'2026-01-01T00:00:00Z',read_knowledge_at:'2026-01-02T00:00:00Z'}) CREATE (r:PDReading {id:$rid,identity_ids:[$gid],identity_revisions:[1],subject_ids:[],subject_versions:[],used_ids:[$gid]}) CREATE (g:PDGraphNode:PDIdentity {id:$gid,revision:1})",
			map[string]interface{}{"id": id, "rid": rid, "gid": gid}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecuteCypher(ctx, "CREATE (c:PDCorrection {id:'c0',status:'active',revision:1,scope:['v1']})", nil); err != nil {
		t.Fatal(err)
	}
	params := map[string]interface{}{"full": true, "ids": ids, "correction_ids": []interface{}{"c0"}, "mentions": []interface{}{}, "edges": []interface{}{},
		"creatable": map[string]interface{}{}, "document_id": "d0", "source_id": "s0", "version_id": "v1", "separator": "|"}
	timed := func(label, key string) {
		start := time.Now()
		_, err := db.ExecuteCypher(ctx, texts[key], params)
		fmt.Printf("RCA_NOR151 mode=%s step=%s key=%s seconds=%.6f err=%v\n", mode, label, key, time.Since(start).Seconds(), err)
	}
	const a, b = "42f785d4d9dbeb87", "dd6661ce13671934"
	switch mode {
	case "cross":
		timed("B-first", b)
		timed("B-second", b)
		timed("A-first-after-B", a)
		timed("A-second", a)
	case "profile":
		for _, step := range []string{"A-first", "A-second"} {
			path := filepath.Join(t.TempDir(), step+".pprof")
			f, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := pprof.StartCPUProfile(f); err != nil {
				t.Fatal(err)
			}
			timed(step, a)
			pprof.StopCPUProfile()
			_ = f.Close()
			bin, _ := os.Executable()
			for _, flags := range [][]string{{"-top"}, {"-top", "-cum"}} {
				args := append(append([]string{"tool", "pprof"}, flags...), "-nodecount=35", bin, path)
				var out bytes.Buffer
				cmd := exec.Command("go", args...)
				cmd.Stdout, cmd.Stderr = &out, &out
				runErr := cmd.Run()
				for _, line := range strings.Split(out.String(), "\n") {
					if strings.TrimSpace(line) != "" {
						fmt.Printf("RCA_PROF %s %s | %s\n", step, strings.Join(flags, " "), line)
					}
				}
				if runErr != nil {
					fmt.Printf("RCA_PROF %s err=%v\n", step, runErr)
				}
			}
		}
	}
}
