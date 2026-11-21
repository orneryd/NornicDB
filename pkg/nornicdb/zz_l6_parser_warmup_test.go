package nornicdb_test

// Lane L6 (closeout): G-parser-prediction-warmup, the parts the gate bypass
// (TestRCAGateBypass) left open, same fixture and statement A (67,845 bytes).
// Each mode runs in a fresh child process (parser from the job's environment,
// so a --parser both job covers default and ANTLR):
//   control:  A-first, A-second (CPU profile).
//   bypass:   gate parse of A prewarmed (as TestRCAGateBypass); A-first
//             profiled with -peek on antlr.ParseSyntax (which callers still
//             parse, and how long) and -top -cum -ignore=antlr (time outside ANTLR).
//   otherdb:  A executed once on a SECOND database in the same process (empty
//             store, same params) before A-first on the fixture database.
//             If the remaining first-call premium is process-wide per-text
//             state, A-first drops to the A-second level.

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

const zzL6WarmEnv = "ZZL6_WARM_MODE"

func TestL6ParserWarmupAttribution(t *testing.T) {
	if os.Getenv(zzL6WarmEnv) != "" {
		t.Skip("parent only")
	}
	t.Logf("L6_WARM parser_env=%q", os.Getenv("NORNICDB_PARSER"))
	for _, mode := range []string{"control", "bypass", "otherdb"} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestL6ParserWarmupChild$", "-test.v", "-test.count=1", "-test.timeout=20m")
		cmd.Env = append(os.Environ(), zzL6WarmEnv+"="+mode)
		out, err := cmd.CombinedOutput()
		for _, line := range strings.Split(string(out), "\n") {
			if i := strings.Index(line, "L6_"); i >= 0 {
				t.Log(strings.TrimSpace(line[i:]))
			}
		}
		if err != nil {
			t.Logf("L6_WARM child %s err=%v\n%s", mode, err, tail(out, 3000))
		}
	}
}

func zzL6OpenFixture(t *testing.T, seed bool) *nornicdb.DB {
	cfg := nornicdb.DefaultConfig()
	cfg.Database.AsyncWritesEnabled = false
	cfg.Database.StrictDurability = true
	cfg.Database.WALSyncMode = "immediate"
	db, err := nornicdb.Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !seed {
		return db
	}
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		id, rid, gid := fmt.Sprintf("d%d", i), fmt.Sprintf("r%d", i), fmt.Sprintf("g%d", i)
		if _, err := db.ExecuteCypher(ctx, "CREATE (d:PDDocument {id:$id,current_version_id:'v1',read_reading_id:$rid,due_at:'2026-01-01T00:00:00Z',read_knowledge_at:'2026-01-02T00:00:00Z'}) CREATE (r:PDReading {id:$rid,identity_ids:[$gid],identity_revisions:[1],subject_ids:[],subject_versions:[],used_ids:[$gid]}) CREATE (g:PDGraphNode:PDIdentity {id:$gid,revision:1})",
			map[string]interface{}{"id": id, "rid": rid, "gid": gid}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecuteCypher(ctx, "CREATE (c:PDCorrection {id:'c0',status:'active',revision:1,scope:['v1']})", nil); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestL6ParserWarmupChild(t *testing.T) {
	mode := os.Getenv(zzL6WarmEnv)
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
	db := zzL6OpenFixture(t, true)
	defer db.Close()
	ctx := context.Background()
	ids := []interface{}{}
	for i := 0; i < 100; i++ {
		ids = append(ids, fmt.Sprintf("d%d", i), fmt.Sprintf("g%d", i))
	}
	params := map[string]interface{}{"full": true, "ids": ids, "correction_ids": []interface{}{"c0"}, "mentions": []interface{}{}, "edges": []interface{}{},
		"creatable": map[string]interface{}{}, "document_id": "d0", "source_id": "s0", "version_id": "v1", "separator": "|"}
	const a = "42f785d4d9dbeb87"

	profiled := func(step string, views [][]string) {
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
		fmt.Printf("L6_WARM mode=%s step=%s seconds=%.6f err=%v\n", mode, step, secs, xerr)
		bin, _ := os.Executable()
		for _, flags := range views {
			args := append(append([]string{"tool", "pprof"}, flags...), bin, path)
			var out bytes.Buffer
			cmd := exec.Command("go", args...)
			cmd.Stdout, cmd.Stderr = &out, &out
			runErr := cmd.Run()
			for _, line := range strings.Split(out.String(), "\n") {
				if strings.TrimSpace(line) != "" {
					fmt.Printf("L6_WPROF mode=%s %s %s | %s\n", mode, step, strings.Join(flags, " "), line)
				}
			}
			if runErr != nil {
				fmt.Printf("L6_WPROF mode=%s %s err=%v\n", mode, step, runErr)
			}
		}
	}
	firstViews := [][]string{
		{"-top", "-nodecount=8"},
		{"-peek=antlr\\.ParseSyntax$"},
		{"-peek=antlr\\.parseStatementSyntaxAttempt$"},
		{"-top", "-cum", "-nodecount=30", "-ignore=antlr"},
		{"-top", "-cum", "-nodecount=12", "-focus=antlr"},
	}
	switch mode {
	case "control":
	case "bypass":
		src := texts[a]
		trimmed := strings.TrimRight(src, " \t\r\n;")
		for _, s := range []string{src, trimmed} {
			for _, e := range []cypherantlr.SyntaxEntry{cypherantlr.StatementEntry, cypherantlr.FragmentEntry} {
				start := time.Now()
				_, perr := cypherantlr.ParseSyntax(ctx, s, e, false, cypherantlr.AcceptedSource)
				fmt.Printf("L6_WARM mode=%s step=prewarm trimmed=%v seconds=%.6f err=%v\n", mode, s != src, time.Since(start).Seconds(), perr)
			}
		}
	case "otherdb":
		other := zzL6OpenFixture(t, false)
		start := time.Now()
		_, oerr := other.ExecuteCypher(ctx, texts[a], params)
		fmt.Printf("L6_WARM mode=%s step=prewarm-otherdb seconds=%.6f err=%v\n", mode, time.Since(start).Seconds(), oerr)
		start = time.Now()
		_, oerr = other.ExecuteCypher(ctx, texts[a], params)
		fmt.Printf("L6_WARM mode=%s step=prewarm-otherdb-second seconds=%.6f err=%v\n", mode, time.Since(start).Seconds(), oerr)
		_ = other.Close()
	}
	profiled("A-first", firstViews)
	profiled("A-second", [][]string{{"-top", "-nodecount=5"}})
}
