package server

// Close-out lane L8, round 2 (2026-11-22): L-H5-001 deterministic sequences
// and stress replay with miss diagnostics; L-H9-018 procedure calls and Go
// API names across pkg/. Test-only, not for merge.

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/testing/cypher/differential"
)

// L-H5-001: a cached plan and an index dropped or re-created between two
// reads of the same statement text.
func TestL8_H5_001_PlanAcrossIndexDDL(t *testing.T) {
	l8ForParsers(t, func(t *testing.T) {
		h := l8Server(t)
		defer h.Close(t)
		read := "MATCH (n:H5RB {key: $k}) RETURN count(n) AS c"
		results := map[string]string{}
		step := func(name string, key string) {
			results[name] = l8One(l8Q(t, h, "L-H5-001", read, map[string]any{"k": key}))
		}
		txCreate := func(key string) {
			errs := &huntH5Errors{}
			_ = huntTx(h, errs, "w", func(ctx context.Context, tx neo4jdriver.ExplicitTransaction) error {
				_, err := huntRun(ctx, tx, "CREATE (:H5RB {key: $k})", map[string]any{"k": key})
				return err
			})
		}
		l8Q(t, h, "L-H5-001", "CREATE INDEX h5_rb IF NOT EXISTS FOR (n:H5RB) ON (n.key)", nil)
		l8Q(t, h, "L-H5-001", "CREATE (:H5RB {key: 'k1'})", nil)
		step("1 index, k1", "k1")
		l8Q(t, h, "L-H5-001", "DROP INDEX h5_rb IF EXISTS", nil)
		l8Q(t, h, "L-H5-001", "CREATE (:H5RB {key: 'k2'})", nil)
		step("2 dropped, k2 created after drop", "k2")
		step("2 dropped, k1", "k1")
		l8Q(t, h, "L-H5-001", "CREATE INDEX h5_rb IF NOT EXISTS FOR (n:H5RB) ON (n.key)", nil)
		step("3 re-created, k1", "k1")
		step("3 re-created, k2", "k2")
		l8Q(t, h, "L-H5-001", "CREATE (:H5RB {key: 'k3'})", nil)
		step("4 re-created, k3 created after", "k3")
		txCreate("k4")
		step("5 explicit-tx k4", "k4")
		l8Q(t, h, "L-H5-001", "DROP INDEX h5_rb IF EXISTS", nil)
		txCreate("k5")
		l8Q(t, h, "L-H5-001", "CREATE INDEX h5_rb IF NOT EXISTS FOR (n:H5RB) ON (n.key)", nil)
		step("6 tx k5 between drop and create", "k5")
		l8Log(t, "L8_H5_SEQ", map[string]any{"results": results})
		for name, got := range results {
			if !strings.Contains(got, "Integer:1") {
				t.Errorf("%s: count %s", name, got)
			}
		}
	})
}

// L-H5-001: the lead's C6 scenario (16 clients; client 0 drops and
// re-creates the index every 50 ms; the others commit a CREATE in an
// explicit transaction and read it back by key) with diagnostics at a miss.
// HUNT_H5_SECONDS per parser (default 600); L8_H5_STRESS_PARSER runs one parser.
func TestL8_H5_001_Stress(t *testing.T) {
	seconds := huntEnvInt("HUNT_H5_SECONDS", 600)
	only := os.Getenv("L8_H5_STRESS_PARSER")
	l8ForParsers(t, func(t *testing.T) {
		if only != "" && only != nornicConfig.GetParserType() {
			t.Skip("other parser")
		}
		h := l8Server(t)
		defer h.Close(t)
		l8Q(t, h, "L-H5-001", "CREATE INDEX h5_rb IF NOT EXISTS FOR (n:H5RB) ON (n.key)", nil)
		var phase, acked, misses, attempted int64
		var mu sync.Mutex
		details := []map[string]any{}
		errs := &huntH5Errors{}
		stop := time.Now().Add(time.Duration(seconds) * time.Second)
		var wg sync.WaitGroup
		for c := 0; c < 16; c++ {
			wg.Add(1)
			go func(client int) {
				defer wg.Done()
				for i := 0; time.Now().Before(stop); i++ {
					if client == 0 {
						atomic.StoreInt64(&phase, 1)
						h.Run(routeBoltAuto, "DROP INDEX h5_rb IF EXISTS", nil, time.Minute)
						atomic.StoreInt64(&phase, 2)
						h.Run(routeBoltAuto, "CREATE INDEX h5_rb IF NOT EXISTS FOR (n:H5RB) ON (n.key)", nil, time.Minute)
						atomic.StoreInt64(&phase, 0)
						time.Sleep(50 * time.Millisecond)
						continue
					}
					atomic.AddInt64(&attempted, 1)
					key := fmt.Sprintf("k%d-%d", client, i)
					phaseWrite := atomic.LoadInt64(&phase)
					err := huntTx(h, errs, "readback write", func(ctx context.Context, tx neo4jdriver.ExplicitTransaction) error {
						_, err := huntRun(ctx, tx, "CREATE (:H5RB {key: $k})", map[string]any{"k": key})
						return err
					})
					if err != nil {
						continue
					}
					atomic.AddInt64(&acked, 1)
					phaseRead := atomic.LoadInt64(&phase)
					params := map[string]any{"k": key}
					o := h.Run(routeBoltAuto, "MATCH (n:H5RB {key: $k}) RETURN count(n) AS c", params, time.Minute)
					if o.Failed() || !strings.Contains(l8One(o), "Integer:1") {
						atomic.AddInt64(&misses, 1)
						phaseEnd := atomic.LoadInt64(&phase)
						again := h.Run(routeBoltAuto, "MATCH (n:H5RB {key: $k}) RETURN count(n) AS c", params, time.Minute)
						scan := h.Run(routeBoltAuto, "MATCH (n:H5RB) WHERE n.key + '' = $k RETURN count(n) AS c", params, time.Minute)
						time.Sleep(time.Second)
						later := h.Run(routeBoltAuto, "MATCH (n:H5RB {key: $k}) RETURN count(n) AS c", params, time.Minute)
						laterScan := h.Run(routeBoltAuto, "MATCH (n:H5RB) WHERE n.key + '' = $k RETURN count(n) AS c", params, time.Minute)
						mu.Lock()
						if len(details) < 20 {
							details = append(details, map[string]any{"key": key, "first": l8One(o), "first_code": o.Code, "first_msg": o.Message,
								"phase_write": phaseWrite, "phase_read_start": phaseRead, "phase_read_end": phaseEnd,
								"reread": l8One(again), "scan": l8One(scan), "after_1s": l8One(later), "scan_after_1s": l8One(laterScan)})
						}
						mu.Unlock()
					}
				}
			}(c)
		}
		wg.Wait()
		final := l8Q(t, h, "L-H5-001", "MATCH (n:H5RB) RETURN count(n) AS c", nil)
		l8Log(t, "L8_H5_STRESS", map[string]any{"seconds": seconds, "attempted": attempted, "acknowledged": acked, "misses": misses,
			"nodes_at_end": l8One(final), "miss_details": details, "phases": "0 idle/sleep, 1 dropping, 2 creating"})
		if misses > 0 {
			t.Errorf("%d of %d acknowledged writes not found by the next read", misses, acked)
		}
	})
}

func l8Cut(s string) string {
	if len(s) > 160 {
		return s[:160]
	}
	return s
}

// L-H9-018: every documented procedure or function name, called as a
// function and as a procedure; the error decides whether the name exists.
func TestL8_H9_018_ProcedureCalls(t *testing.T) {
	h := l8Server(t)
	defer h.Close(t)
	counts := map[string]int{}
	missing := func(o differential.Outcome) bool {
		m := strings.ToLower(o.Message)
		return strings.Contains(o.Code, "NotFound") || strings.Contains(m, "unknown function") || strings.Contains(m, "no procedure with the name") || strings.Contains(m, "no function with the name") || strings.Contains(m, "not registered") || strings.Contains(m, "unknown procedure")
	}
	for _, row := range l8RawRows(t, "procedure") {
		name := row["name"]
		parts := strings.Split(name, ".")
		if last := parts[len(parts)-1]; last != "" && last[0] >= 'A' && last[0] <= 'Z' {
			continue
		}
		fn := h.Run(routeBoltAuto, "RETURN "+name+"() AS v", nil, 30*time.Second)
		proc := h.Run(routeBoltAuto, "CALL "+name+"()", nil, 30*time.Second)
		class := "exists (function or procedure answers)"
		if missing(fn) && missing(proc) {
			class = "absent (neither function nor procedure)"
		}
		counts[class]++
		l8Log(t, "L8_PROC_CALL", map[string]any{"item": row["item"], "name": name, "documented": row["documented"], "class": class,
			"function_code": fn.Code, "function_msg": l8Cut(fn.Message), "procedure_code": proc.Code, "procedure_msg": l8Cut(proc.Message), "doc_line": l8DocLine(row["documented"])})
	}
	l8Log(t, "L8_PROC_CALL_SUMMARY", map[string]any{"counts": counts})
}

// L-H9-018: the Go API names, looked up in every package under pkg/.
func TestL8_H9_018_GoNamesAllPackages(t *testing.T) {
	defined := map[string][]string{}
	_ = filepath.WalkDir("..", func(path string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() || strings.Contains(path, "testdata") {
			return nil
		}
		fset := token.NewFileSet()
		pkgs, err := parser.ParseDir(fset, path, func(info os.FileInfo) bool { return !strings.HasSuffix(info.Name(), "_test.go") }, 0)
		if err != nil {
			return nil
		}
		for _, pkg := range pkgs {
			for _, file := range pkg.Files {
				for _, decl := range file.Decls {
					fn, ok := decl.(*ast.FuncDecl)
					if !ok || !fn.Name.IsExported() {
						continue
					}
					owner := pkg.Name
					if fn.Recv != nil && len(fn.Recv.List) > 0 {
						recv := fn.Recv.List[0].Type
						if star, ok := recv.(*ast.StarExpr); ok {
							recv = star.X
						}
						if ident, ok := recv.(*ast.Ident); ok {
							owner = pkg.Name + "." + ident.Name
						}
					}
					defined[fn.Name.Name] = append(defined[fn.Name.Name], filepath.ToSlash(path)+":"+owner)
				}
			}
		}
		return nil
	})
	for _, row := range l8RawRows(t, "procedure") {
		name := row["name"]
		parts := strings.Split(name, ".")
		last := parts[len(parts)-1]
		if last == "" || last[0] < 'A' || last[0] > 'Z' {
			continue
		}
		where := defined[last]
		if len(where) > 6 {
			where = where[:6]
		}
		l8Log(t, "L8_GONAME", map[string]any{"item": row["item"], "name": name, "documented": row["documented"], "defined_as": where, "doc_line": l8DocLine(row["documented"])})
	}
}
