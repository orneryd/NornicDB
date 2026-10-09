package cypher

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	antlr "github.com/antlr4-go/antlr/v4"
	antlrparser "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// Asymptotic parser study (Nornic hand-written parser vs ANTLR).
//
// Each scaling family grows a query along ONE dimension n. For every
// (family, n, parser, mode) we record hardware-independent cost (allocs/op,
// B/op) next to wall time, plus the ANTLR token count of the input as a
// common size measure. scripts/parser_scaling_report.py fits log-log growth
// exponents and renders the report.
//
// Run:
//
//	NORNIC_RUN_PARSER_SCALING=1 go test ./pkg/cypher -run 'TestParserScaling|TestParserTailLatency' \
//	    -count=1 -v -timeout 60m -test.benchtime=200ms
//
// Env: NORNIC_PARSER_SCALING_OUT (output dir), NORNIC_PARSER_SCALING_MAXN,
// NORNIC_PARSER_TAIL_ITERS.

type scalingFamily struct {
	name string
	desc string
	gen  func(n int) string
}

func joinN(n int, sep string, f func(i int) string) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = f(i)
	}
	return strings.Join(parts, sep)
}

var scalingFamilies = []scalingFamily{
	{"return_items", "n projected items in RETURN", func(n int) string {
		return "MATCH (n:Person) RETURN " + joinN(n, ", ", func(i int) string { return fmt.Sprintf("n.p%d AS a%d", i, i) })
	}},
	{"where_and", "n AND-ed predicates in WHERE", func(n int) string {
		return "MATCH (n:Person) WHERE " + joinN(n, " AND ", func(i int) string { return fmt.Sprintf("n.p%d = %d", i, i) }) + " RETURN n"
	}},
	{"arith_chain", "n-term left-associative arithmetic expression", func(n int) string {
		return "RETURN " + joinN(n, " + ", func(i int) string { return strconv.Itoa(i + 1) })
	}},
	{"list_literal", "n-element list literal", func(n int) string {
		return "RETURN [" + joinN(n, ", ", func(i int) string { return strconv.Itoa(i) }) + "]"
	}},
	{"map_literal", "n-entry map literal", func(n int) string {
		return "RETURN {" + joinN(n, ", ", func(i int) string { return fmt.Sprintf("k%d: %d", i, i) }) + "}"
	}},
	{"match_hops", "single MATCH pattern with n relationship hops", func(n int) string {
		var sb strings.Builder
		sb.WriteString("MATCH (a0)")
		for i := 1; i <= n; i++ {
			fmt.Fprintf(&sb, "-[:R]->(a%d)", i)
		}
		sb.WriteString(" RETURN a0")
		return sb.String()
	}},
	{"with_chain", "n chained WITH clauses (clause count)", func(n int) string {
		var sb strings.Builder
		sb.WriteString("MATCH (n0) ")
		for i := 1; i <= n; i++ {
			fmt.Fprintf(&sb, "WITH n%d AS n%d ", i-1, i)
		}
		fmt.Fprintf(&sb, "RETURN n%d", n)
		return sb.String()
	}},
	{"case_branches", "CASE with n WHEN branches", func(n int) string {
		return "RETURN CASE " + joinN(n, " ", func(i int) string { return fmt.Sprintf("WHEN %d = 1 THEN %d", i, i) }) + " ELSE 0 END"
	}},
	{"nested_parens", "n levels of parenthesis nesting", func(n int) string {
		return "RETURN " + strings.Repeat("(", n) + "1" + strings.Repeat(")", n)
	}},
}

type scalingRow struct {
	Family      string  `json:"family"`
	N           int     `json:"n"`
	Bytes       int     `json:"input_bytes"`
	Tokens      int     `json:"tokens"`
	Parser      string  `json:"parser"`
	Mode        string  `json:"mode"`
	NsPerOp     float64 `json:"ns_per_op"`
	AllocsPerOp int64   `json:"allocs_per_op"`
	BytesPerOp  int64   `json:"bytes_per_op"`
	Iterations  int     `json:"iterations"`
	Status      string  `json:"status"`
	Clauses     int     `json:"clauses,omitempty"`
}

func antlrTokenCount(q string) int {
	lexer := antlrparser.NewCypherLexer(antlr.NewInputStream(q))
	lexer.RemoveErrorListeners()
	n := 0
	for _, tok := range lexer.GetAllTokens() {
		if tok.GetTokenType() != antlr.TokenEOF {
			n++
		}
	}
	return n
}

// parserOps returns the operation under test for (parser, mode). The Nornic
// validate path memoises successful validations per executor, so the cache is
// cleared before every call to measure the real work, not a cache hit. (The
// existing 5-sample median report hits this cache on samples 2-5.)
func parserOp(parser, mode string) func(q string) error {
	switch parser + "/" + mode {
	case "nornic/validate":
		e := &StorageExecutor{}
		return func(q string) error {
			c := e.ensureSyntaxValidationCache()
			c.mu.Lock()
			clear(c.cache)
			c.mu.Unlock()
			return e.validateSyntaxNornic(q)
		}
	case "nornic/parse":
		b := NewASTBuilder()
		return func(q string) error { _, err := b.Build(q); return err }
	case "antlr/validate":
		return antlrparser.Validate
	case "antlr/parse":
		return func(q string) error { _, err := antlrparser.Parse(q); return err }
	}
	panic("unknown parser/mode " + parser + "/" + mode)
}

func scalingOutDir(t *testing.T) string {
	dir := os.Getenv("NORNIC_PARSER_SCALING_OUT")
	if dir == "" {
		dir = filepath.Join("..", "..", "scripts", "parser_scaling_reports", time.Now().Format("20060102_150405"))
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

func writeJSON(t *testing.T, path string, v any) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %s", path)
}

func TestParserScaling(t *testing.T) {
	if os.Getenv("NORNIC_RUN_PARSER_SCALING") == "" {
		t.Skip("asymptotic parser study; set NORNIC_RUN_PARSER_SCALING=1 to run")
	}
	maxN := 512
	if v, err := strconv.Atoi(os.Getenv("NORNIC_PARSER_SCALING_MAXN")); err == nil && v >= 8 {
		maxN = v
	}
	var sizes []int
	for n := 8; n <= maxN; n *= 2 {
		sizes = append(sizes, n)
	}

	var rows []scalingRow
	for _, fam := range scalingFamilies {
		for _, n := range sizes {
			q := fam.gen(n)
			tokens := antlrTokenCount(q)
			for _, mode := range []string{"validate", "parse"} {
				for _, parser := range []string{"nornic", "antlr"} {
					op := parserOp(parser, mode)
					row := scalingRow{Family: fam.name, N: n, Bytes: len(q), Tokens: tokens, Parser: parser, Mode: mode, Status: "ok"}
					if err := op(q); err != nil {
						row.Status = "error: " + err.Error()
						if len(row.Status) > 160 {
							row.Status = row.Status[:160]
						}
						rows = append(rows, row)
						t.Logf("%-14s n=%-4d %-6s %-8s %s", fam.name, n, parser, mode, row.Status)
						continue
					}
					if parser == "nornic" && mode == "parse" {
						if ast, err := NewASTBuilder().Build(q); err == nil {
							row.Clauses = len(ast.Clauses)
						}
					}
					res := testing.Benchmark(func(b *testing.B) {
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							_ = op(q)
						}
					})
					row.NsPerOp = float64(res.T.Nanoseconds()) / float64(res.N)
					row.AllocsPerOp = res.AllocsPerOp()
					row.BytesPerOp = res.AllocedBytesPerOp()
					row.Iterations = res.N
					rows = append(rows, row)
					t.Logf("%-14s n=%-4d tok=%-5d %-6s %-8s %12.0f ns %8d allocs %10d B", fam.name, n, tokens, parser, mode, row.NsPerOp, row.AllocsPerOp, row.BytesPerOp)
				}
			}
		}
	}

	dir := scalingOutDir(t)
	fams := make([]map[string]string, 0, len(scalingFamilies))
	for _, f := range scalingFamilies {
		fams = append(fams, map[string]string{"name": f.name, "desc": f.desc})
	}
	writeJSON(t, filepath.Join(dir, "scaling.json"), map[string]any{
		"goos": runtime.GOOS, "goarch": runtime.GOARCH, "go": runtime.Version(),
		"cpus": runtime.NumCPU(), "families": fams, "sizes": sizes, "rows": rows,
	})
}

type tailRow struct {
	Query      string  `json:"query"`
	Parser     string  `json:"parser"`
	Mode       string  `json:"mode"`
	Iterations int     `json:"iterations"`
	P50Ns      int64   `json:"p50_ns"`
	P95Ns      int64   `json:"p95_ns"`
	P99Ns      int64   `json:"p99_ns"`
	P999Ns     int64   `json:"p999_ns"`
	MaxNs      int64   `json:"max_ns"`
	MeanNs     float64 `json:"mean_ns"`
	NumGC      uint32  `json:"num_gc"`
	GCPauseNs  uint64  `json:"gc_pause_total_ns"`
	GCPerOp    float64 `json:"gc_per_1k_ops"`
}

func pct(sorted []int64, p float64) int64 {
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

// TestParserTailLatency measures per-call latency distribution and GC activity
// for fixed inputs, to test the claim that allocation pressure drives tail
// latency (p95/p99/max) rather than the median.
func TestParserTailLatency(t *testing.T) {
	if os.Getenv("NORNIC_RUN_PARSER_SCALING") == "" {
		t.Skip("asymptotic parser study; set NORNIC_RUN_PARSER_SCALING=1 to run")
	}
	iters := 20000
	if v, err := strconv.Atoi(os.Getenv("NORNIC_PARSER_TAIL_ITERS")); err == nil && v > 100 {
		iters = v
	}
	var rows []tailRow
	for _, fam := range scalingFamilies {
		q := fam.gen(32)
		for _, mode := range []string{"parse"} {
			for _, parser := range []string{"nornic", "antlr"} {
				op := parserOp(parser, mode)
				if err := op(q); err != nil {
					continue
				}
				for i := 0; i < 500; i++ { // warm caches / DFA
					_ = op(q)
				}
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				lat := make([]int64, iters)
				var sum int64
				for i := 0; i < iters; i++ {
					s := time.Now()
					_ = op(q)
					lat[i] = time.Since(s).Nanoseconds()
					sum += lat[i]
				}
				runtime.ReadMemStats(&after)
				sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
				r := tailRow{
					Query: fam.name + "/n=32", Parser: parser, Mode: mode, Iterations: iters,
					P50Ns: pct(lat, 0.50), P95Ns: pct(lat, 0.95), P99Ns: pct(lat, 0.99), P999Ns: pct(lat, 0.999), MaxNs: lat[len(lat)-1],
					MeanNs: float64(sum) / float64(iters),
					NumGC:  after.NumGC - before.NumGC, GCPauseNs: after.PauseTotalNs - before.PauseTotalNs,
				}
				r.GCPerOp = float64(r.NumGC) / float64(iters) * 1000
				rows = append(rows, r)
				t.Logf("%-18s %-6s p50=%d p99=%d max=%d gc=%d", r.Query, parser, r.P50Ns, r.P99Ns, r.MaxNs, r.NumGC)
			}
		}
	}
	writeJSON(t, filepath.Join(scalingOutDir(t), "tail.json"), map[string]any{"rows": rows})
}
