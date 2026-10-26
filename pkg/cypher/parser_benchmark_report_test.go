package cypher

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	antlrparser "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

type parserBenchmarkResult struct {
	name   string
	time   time.Duration
	status string
}

func measureMedianParserDuration(samples int, run func() error) (time.Duration, error) {
	if samples < 1 {
		samples = 1
	}

	durations := make([]time.Duration, 0, samples)
	for i := 0; i < samples; i++ {
		start := time.Now()
		if err := run(); err != nil {
			return time.Since(start), err
		}
		durations = append(durations, time.Since(start))
	}

	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})
	return durations[len(durations)/2], nil
}

func printParserReportComparison(title, mode string, nornicResults, antlrResults []parserBenchmarkResult, totalNornic, totalANTLR time.Duration, successNornic, successANTLR int) {
	fmt.Println("\n" + strings.Repeat("=", 96))
	fmt.Println(title)
	fmt.Println(strings.Repeat("=", 96))
	fmt.Printf("\n%-30s | %-12s | %-12s | %-8s | %s\n", "Query", "Nornic", "ANTLR", "Ratio", "Status")
	fmt.Println(strings.Repeat("-", 96))

	for i := range nornicResults {
		nr := nornicResults[i]
		ar := antlrResults[i]

		status := "ok"
		if nr.status != "ok" || ar.status != "ok" {
			switch {
			case nr.status != "ok" && ar.status != "ok":
				status = "both failed"
			case nr.status != "ok":
				status = "nornic failed"
			default:
				status = "antlr failed"
			}
		}

		ratio := 0.0
		if nr.time > 0 {
			ratio = float64(ar.time) / float64(nr.time)
		}

		name := nr.name
		if len(name) > 30 {
			name = name[:27] + "..."
		}
		fmt.Printf("%-30s | %12s | %12s | %7.1fx | %s\n", name, nr.time, ar.time, ratio, status)
	}

	fmt.Println(strings.Repeat("-", 96))
	avgRatio := 0.0
	if totalNornic > 0 {
		avgRatio = float64(totalANTLR) / float64(totalNornic)
	}
	fmt.Printf("%-30s | %12s | %12s | %7.1fx |\n", "TOTAL", totalNornic, totalANTLR, avgRatio)
	fmt.Println(strings.Repeat("=", 96))
	fmt.Printf("\nSummary: Nornic %.1fx faster than ANTLR for %s on this corpus\n", avgRatio, mode)
	fmt.Printf("PARSER_REPORT_SUMMARY mode=%s parser=Nornic total_ns=%d success=%d total_cases=%d\n", mode, totalNornic.Nanoseconds(), successNornic, len(nornicResults))
	fmt.Printf("PARSER_REPORT_SUMMARY mode=%s parser=ANTLR total_ns=%d success=%d total_cases=%d\n\n", mode, totalANTLR.Nanoseconds(), successANTLR, len(antlrResults))
}

// TestParserBenchmarkReports prints machine-readable validation and full parse
// reports. Run one mode with NORNIC_RUN_PARSER_REPORTS=1 go test
// -run 'TestParserBenchmarkReports/validate' ./pkg/cypher -count=1 -v.
func TestParserBenchmarkReports(t *testing.T) {
	if os.Getenv("NORNIC_RUN_PARSER_REPORTS") == "" {
		t.Skip("benchmark-style parser report; set NORNIC_RUN_PARSER_REPORTS=1 to run")
	}
	exec := &StorageExecutor{}
	astBuilder := NewASTBuilder()
	for _, mode := range []struct {
		name, title, action string
		parsers             [2]func(string) error
	}{
		{"validate", "CYPHER VALIDATION BENCHMARK REPORT", "validate", [2]func(string) error{
			func(query string) error {
				return exec.validateSyntax(context.Background(), originalBraceView(query), false, syntaxNativeCore)
			},
			antlrparser.Validate,
		}},
		{"parse", "CYPHER PARSE BENCHMARK REPORT", "parse", [2]func(string) error{
			func(query string) error { _, err := astBuilder.Build(query); return err },
			func(query string) error { _, err := antlrparser.Parse(query); return err },
		}},
	} {
		t.Run(mode.name, func(t *testing.T) {
			var results [2][]parserBenchmarkResult
			var totals [2]time.Duration
			var successes [2]int
			for index := range results {
				results[index] = make([]parserBenchmarkResult, 0, len(testQueries))
			}
			for _, tc := range testQueries {
				for index, parse := range mode.parsers {
					duration, err := measureMedianParserDuration(5, func() error { return parse(tc.query) })
					status := "ok"
					if err != nil {
						status = "failed: " + err.Error()
					} else {
						totals[index] += duration
						successes[index]++
					}
					results[index] = append(results[index], parserBenchmarkResult{name: tc.name, time: duration, status: status})
				}
			}
			printParserReportComparison(mode.title, mode.name, results[0], results[1], totals[0], totals[1], successes[0], successes[1])
			if successes[0] == 0 || successes[1] == 0 {
				t.Fatalf("expected both parsers to %s at least one query (nornic=%d antlr=%d)", mode.action, successes[0], successes[1])
			}
		})
	}
}
