package cypher

// Root-cause diagnostic (campaign v1.5.2, lane C, group G-label-scan-projected; run by lane A).
// Test-side only. Seeds the same shape population as TestZZR12ShapesScale on a
// persistent Badger store (no HTTP layer), then runs each growing statement
// once under a CPU profile and prints `go tool pprof -top` (flat and
// cumulative) for it, plus its PROFILE plan. Sizes from RCA_SHAPE_SIZES
// (default 10000,40000).

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func rcaPprofTop(t *testing.T, name string, run func()) {
	t.Helper()
	path := filepath.Join(t.TempDir(), name+".pprof")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		t.Logf("RCA_PROF %s profile unavailable: %v", name, err)
		run()
		return
	}
	run()
	pprof.StopCPUProfile()
	_ = f.Close()
	bin, _ := os.Executable()
	for _, mode := range []string{"-top", "-top -cum"} {
		args := append([]string{"tool", "pprof"}, strings.Fields(mode)...)
		args = append(args, "-nodecount=30", bin, path)
		var out bytes.Buffer
		cmd := exec.Command("go", args...)
		cmd.Stdout, cmd.Stderr = &out, &out
		err := cmd.Run()
		for _, line := range strings.Split(out.String(), "\n") {
			if strings.TrimSpace(line) != "" {
				t.Logf("RCA_PROF %s %s | %s", name, mode, line)
			}
		}
		if err != nil {
			t.Logf("RCA_PROF %s %s err=%v", name, mode, err)
		}
	}
}

func TestRCALabelScanShapesProfile(t *testing.T) {
	ctx := context.Background()
	base, err := storage.NewBadgerEngine(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = base.Close() })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"))
	run := func(q string, p map[string]any) (*ExecuteResult, error) { return exec.Execute(ctx, q, p) }
	must := func(q string, p map[string]any) {
		if _, err := run(q, p); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for _, label := range []string{"PDGraphNode", "PDDocument", "PDPoint", "PDContext"} {
		must("CREATE INDEX ss_"+label+" FOR (n:"+label+") ON (n.id)", nil)
	}
	rows := []any{}
	for i := 0; i < 100; i++ {
		rows = append(rows, map[string]any{"id": fmt.Sprintf("d%04d", i), "ctx": fmt.Sprintf("c%04d", i)})
	}
	must(`UNWIND $rows AS r CREATE (d:PDGraphNode:PDDocument {id:r.id,document_id:r.id}) CREATE (c:PDGraphNode:PDContext {id:r.ctx,updated_at:1})`, map[string]any{"rows": rows})
	kinds := map[string]any{"kinds": []any{"mentions", "in_context"}, "after": nil, "limit": int64(100)}
	shapes := []struct {
		name, q string
		p       map[string]any
	}{
		{"relation-count", `MATCH (:PDGraphNode)-[n:PD_RELATION]->(:PDGraphNode) WHERE n.kind IN $kinds AND n.updated_at IS NOT NULL AND n.correction_id IS NULL RETURN count(DISTINCT n.id) AS count`, kinds},
		{"relation-count-unlabelled", `MATCH ()-[n:PD_RELATION]->() WHERE n.kind IN $kinds AND n.updated_at IS NOT NULL AND n.correction_id IS NULL RETURN count(DISTINCT n.id) AS count`, kinds},
		{"orphan-count", `MATCH (n:PDGraphNode) OPTIONAL MATCH (ue_owner:PDDocument {id:n.document_id}) WITH n,ue_owner WHERE NOT n:PDDocument AND ue_owner IS NULL RETURN count(DISTINCT n.id) AS count`, nil},
		{"projection-count", `MATCH (n:PDGraphNode) WHERE n.assessed_projection IS NOT NULL AND NOT n:PDContext RETURN count(DISTINCT n.id) AS count`, nil},
		{"label-count-only", `MATCH (n:PDGraphNode) RETURN count(n) AS count`, nil},
	}
	spec := os.Getenv("RCA_SHAPE_SIZES")
	if spec == "" {
		spec = "10000,40000"
	}
	have := 0
	for _, s := range strings.Split(spec, ",") {
		n, _ := strconv.Atoi(strings.TrimSpace(s))
		for b := have; b < n; b += 500 {
			batch := []any{}
			for i := b; i < b+500 && i < n; i++ {
				batch = append(batch, map[string]any{"id": fmt.Sprintf("p%07d", i), "doc": fmt.Sprintf("d%04d", i%100), "ctx": fmt.Sprintf("c%04d", i%100), "body": strings.Repeat("research body ", 80)})
			}
			must(`UNWIND $rows AS r CREATE (n:PDGraphNode:PDPoint {id:r.id,document_id:r.doc,search_text:r.body,assessed_projection:'old',updated_at:1})`, map[string]any{"rows": batch})
			must(`UNWIND $rows AS r MATCH (p:PDPoint {id:r.id}) MATCH (d:PDDocument {id:r.doc}) MATCH (c:PDContext {id:r.ctx}) CREATE (d)-[:PD_RELATION {id:r.id+'d',kind:'mentions',updated_at:1,eff_usable:true}]->(p) CREATE (p)-[:PD_RELATION {id:r.id+'c',kind:'in_context',updated_at:1,eff_usable:true}]->(c)`, map[string]any{"rows": batch})
		}
		have = n
		for _, shape := range shapes {
			var res *ExecuteResult
			var err error
			start := time.Now()
			rcaPprofTop(t, fmt.Sprintf("%s-n%d", shape.name, n), func() { res, err = run(shape.q, shape.p) })
			var first any
			if res != nil && len(res.Rows) > 0 && len(res.Rows[0]) > 0 {
				first = res.Rows[0][0]
			}
			t.Logf("RCA_SHAPE shape=%s points=%d seconds=%.6f value=%v err=%v", shape.name, n, time.Since(start).Seconds(), first, err)
			if n == have && strings.HasPrefix(shape.name, "relation-count") {
				if prof, perr := run("PROFILE "+shape.q, shape.p); perr == nil && prof != nil {
					t.Logf("RCA_PLAN shape=%s points=%d metadata=%v", shape.name, n, prof.Metadata)
				}
			}
		}
	}
}
