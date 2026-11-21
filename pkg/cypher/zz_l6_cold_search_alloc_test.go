package cypher

// Lane L6 (closeout): G-parser-prediction-warmup member E38.5 (first call of
// the ranked-search statement allocates ~275 MB, a second call ~9 MB).
// Same fixture and statement as TestV152PortColdRankedSearch. Each mode in a
// fresh child process:
//   control: call 1 and call 2, with an allocation profile diff of call 1
//            (alloc_space, top and focus=antlr).
//   bypass:  the ANTLR syntax gate's parse of the statement text (statement and
//            fragment entries, raw and trimmed text; antlr/statement_syntax.go:207
//            caches per text) is done before call 1. If the gate's parse is the
//            premium, call 1 allocates about what call 2 does.

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const zzL6ColdEnv = "ZZL6_COLD_MODE"

func TestL6ColdSearchAllocation(t *testing.T) {
	if os.Getenv(zzL6ColdEnv) != "" {
		t.Skip("parent only")
	}
	for _, mode := range []string{"control", "bypass"} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestL6ColdSearchAllocationChild$", "-test.v", "-test.count=1", "-test.timeout=20m")
		cmd.Env = append(os.Environ(), zzL6ColdEnv+"="+mode)
		out, err := cmd.CombinedOutput()
		for _, line := range strings.Split(string(out), "\n") {
			if i := strings.Index(line, "L6_"); i >= 0 {
				t.Log(strings.TrimSpace(line[i:]))
			}
		}
		if err != nil {
			s := string(out)
			if len(s) > 3000 {
				s = s[len(s)-3000:]
			}
			t.Logf("L6_COLD child %s err=%v\n%s", mode, err, s)
		}
	}
}

func TestL6ColdSearchAllocationChild(t *testing.T) {
	mode := os.Getenv(zzL6ColdEnv)
	if mode == "" {
		t.Skip("child only")
	}
	ctx := context.Background()
	_, s, e := pdvStore(t)
	for i := 0; i < 40; i++ {
		id, doc, ver := fmt.Sprintf("source%d", i), fmt.Sprintf("doc%d", i), fmt.Sprintf("version%d", i)
		pdvNode(t, s, doc, []string{"PDGraphNode", "PDDocument"}, map[string]any{"id": doc, "current_version_id": ver, "archived": false})
		pdvNode(t, s, ver, []string{"PDVersion"}, map[string]any{"id": ver, "document_id": doc, "current_source_id": id})
		chunks := make([][]float32, 128)
		for j := range chunks {
			chunks[j] = make([]float32, 2048)
			chunks[j][0] = 1
		}
		pdvCall(t, s, "CreateNode", &storage.Node{ID: storage.NodeID(id), Labels: []string{"PDSearchSource"}, Properties: map[string]any{"id": id, "document_id": doc, "version_id": ver, "content": "zephyrine synthetic document", "search_text": "zephyrine synthetic document", "current": true, "search_hidden": false}, ChunkEmbeddings: chunks})
	}
	pdvNode(t, s, "settings", []string{"PDSearchSettings"}, map[string]any{"id": "search", "rerank_top_k": int64(40)})
	svc := pdvbService(t, s, 2048)
	t.Cleanup(func() { svc.Close() })
	if err := svc.BuildIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	e.SetSearchService(svc)
	e.SetEmbedder(pdvbEmbedder{})
	svc.SetReranker(pdvbReranker{})
	query := pdvQueries(t)["queries.SEARCH:38"]
	p := map[string]any{"query": "zephyrine", "qid": nil, "page_size": int64(10), "history": false, "anchors": []any{"source0"}, "min_similarity": nil, "rerank_min_score": nil, "eff_view": map[string]any{}}

	if mode == "bypass" {
		trimmed := strings.TrimRight(query, " \t\r\n;")
		for _, text := range []string{query, trimmed, strings.TrimSpace(query)} {
			for _, entry := range []cypherantlr.SyntaxEntry{cypherantlr.StatementEntry, cypherantlr.FragmentEntry} {
				runtime.GC()
				var b0, b1 runtime.MemStats
				runtime.ReadMemStats(&b0)
				start := time.Now()
				_, perr := cypherantlr.ParseSyntax(ctx, text, entry, false, cypherantlr.AcceptedSource)
				runtime.ReadMemStats(&b1)
				fmt.Printf("L6_COLD mode=%s step=prewarm seconds=%.6f allocated_bytes=%d err=%v\n", mode, time.Since(start).Seconds(), b1.TotalAlloc-b0.TotalAlloc, perr)
			}
		}
	}
	dir := t.TempDir()
	for attempt := 1; attempt <= 2; attempt++ {
		runtime.GC()
		base := filepath.Join(dir, fmt.Sprintf("before-%d.pprof", attempt))
		if f, err := os.Create(base); err == nil {
			_ = pprof.Lookup("allocs").WriteTo(f, 0)
			_ = f.Close()
		}
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		start := time.Now()
		result := pdvExec(t, e, query, p)
		elapsed := time.Since(start)
		runtime.ReadMemStats(&after)
		fmt.Printf("L6_COLD mode=%s call=%d elapsed=%s allocated_bytes=%d rows=%d\n", mode, attempt, elapsed, after.TotalAlloc-before.TotalAlloc, len(result.Rows))
		if attempt != 1 {
			continue
		}
		runtime.GC()
		post := filepath.Join(dir, "after-1.pprof")
		if f, err := os.Create(post); err == nil {
			_ = pprof.Lookup("allocs").WriteTo(f, 0)
			_ = f.Close()
		}
		bin, _ := os.Executable()
		for _, flags := range [][]string{
			{"-sample_index=alloc_space", "-top", "-cum", "-nodecount=15"},
			{"-sample_index=alloc_space", "-top", "-cum", "-nodecount=10", "-focus=antlr"},
			{"-sample_index=alloc_space", "-peek=antlr\\.ParseSyntax$"},
		} {
			args := append(append([]string{"tool", "pprof"}, flags...), "-diff_base="+base, bin, post)
			var out bytes.Buffer
			cmd := exec.Command("go", args...)
			cmd.Stdout, cmd.Stderr = &out, &out
			runErr := cmd.Run()
			for _, line := range strings.Split(out.String(), "\n") {
				if strings.TrimSpace(line) != "" {
					fmt.Printf("L6_CPROF mode=%s %s | %s\n", mode, strings.Join(flags, " "), line)
				}
			}
			if runErr != nil {
				fmt.Printf("L6_CPROF mode=%s err=%v\n", mode, runErr)
			}
		}
	}
}
