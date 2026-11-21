package nornicdb_test

// Root-cause diagnostic (campaign v1.5.2, group G-parser-prediction-warmup, fix round 1).
// Test-side only. Same fixture and statement A as TestRCANOR151FirstCall.
// Question: is the first-call premium of statement A the ANTLR syntax gate
// (cypherantlr.ParseSyntax, statement_syntax.go:207, cached per text)?
// Each mode runs in a fresh child process, default parser:
//   control: A-first, A-second, each with a CPU profile; antlr share printed.
//   bypass:  the gate's parse of A is done BEFORE timing by calling
//            cypherantlr.ParseSyntax directly with the keys the execution path
//            uses (StatementEntry and FragmentEntry, AcceptedSource, raw and
//            trailing-delimiter-trimmed text). Then A-first and A-second are
//            timed and profiled. If the gate is the premium, A-first drops to
//            the A-second level and the prewarm parse takes the difference.
//   other:   same as bypass, but the prewarm parses statement B only (control
//            for "any ANTLR warm-up helps").

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

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

const rcaGateEnv = "RCA_GATE_MODE"

func TestRCAGateBypass(t *testing.T) {
	if os.Getenv(rcaGateEnv) != "" {
		t.Skip("parent only")
	}
	for _, mode := range []string{"control", "bypass", "other"} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestRCAGateBypassChild$", "-test.v", "-test.count=1", "-test.timeout=20m")
		cmd.Env = append(os.Environ(), rcaGateEnv+"="+mode)
		out, err := cmd.CombinedOutput()
		for _, line := range strings.Split(string(out), "\n") {
			if i := strings.Index(line, "RCA_"); i >= 0 {
				t.Log(strings.TrimSpace(line[i:]))
			}
		}
		if err != nil {
			t.Logf("RCA_GATE child %s err=%v\n%s", mode, err, tail(out, 3000))
		}
	}
}

func TestRCAGateBypassChild(t *testing.T) {
	mode := os.Getenv(rcaGateEnv)
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
	const a, b = "42f785d4d9dbeb87", "dd6661ce13671934"

	prewarm := func(key string) {
		src := texts[key]
		trimmed := strings.TrimRight(src, " \t\r\n;")
		for _, s := range []string{src, trimmed} {
			for _, e := range []struct {
				name  string
				entry cypherantlr.SyntaxEntry
			}{{"statement", cypherantlr.StatementEntry}, {"fragment", cypherantlr.FragmentEntry}} {
				start := time.Now()
				_, perr := cypherantlr.ParseSyntax(ctx, s, e.entry, false, cypherantlr.AcceptedSource)
				fmt.Printf("RCA_GATE mode=%s step=prewarm key=%s entry=%s trimmed=%v seconds=%.6f err=%v\n", mode, key, e.name, s != src, time.Since(start).Seconds(), perr)
			}
		}
	}
	profiled := func(step string) {
		path := filepath.Join(t.TempDir(), step+".pprof")
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		_, xerr := db.ExecuteCypher(ctx, texts[a], params)
		secs := time.Since(start).Seconds()
		pprof.StopCPUProfile()
		_ = f.Close()
		fmt.Printf("RCA_GATE mode=%s step=%s key=%s seconds=%.6f err=%v\n", mode, step, a, secs, xerr)
		bin, _ := os.Executable()
		for _, flags := range [][]string{{"-top", "-nodecount=5"}, {"-top", "-cum", "-nodecount=12", "-focus=antlr"}} {
			args := append(append([]string{"tool", "pprof"}, flags...), bin, path)
			var out bytes.Buffer
			cmd := exec.Command("go", args...)
			cmd.Stdout, cmd.Stderr = &out, &out
			runErr := cmd.Run()
			for _, line := range strings.Split(out.String(), "\n") {
				if strings.TrimSpace(line) != "" {
					fmt.Printf("RCA_GPROF mode=%s %s %s | %s\n", mode, step, strings.Join(flags, " "), line)
				}
			}
			if runErr != nil {
				fmt.Printf("RCA_GPROF mode=%s %s err=%v\n", mode, step, runErr)
			}
		}
	}
	switch mode {
	case "control":
	case "bypass":
		prewarm(a)
	case "other":
		prewarm(b)
	}
	profiled("A-first")
	profiled("A-second")
}
