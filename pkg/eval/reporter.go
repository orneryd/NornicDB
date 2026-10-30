package eval

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// Reporter formats and outputs evaluation results.
type Reporter struct {
	writer io.Writer
}

// NewReporter creates a new reporter that writes to the given writer.
func NewReporter(w io.Writer) *Reporter {
	if w == nil {
		w = os.Stdout
	}
	return &Reporter{writer: w}
}

// PrintSummary prints a human-readable summary of results.
func (r *Reporter) PrintSummary(result *EvalResult) {
	w := r.writer

	fmt.Fprintln(w)
	fmt.Fprintln(w, "╔════════════════════════════════════════════════════════════════╗")
	fmt.Fprintln(w, "║           NornicDB Search Evaluation Results                   ║")
	fmt.Fprintln(w, "╚════════════════════════════════════════════════════════════════╝")
	fmt.Fprintln(w)

	// Summary
	fmt.Fprintf(w, "📊 Suite: %s\n", result.SuiteName)
	fmt.Fprintf(w, "📅 Time:  %s\n", result.Timestamp.Format(time.RFC3339))
	fmt.Fprintf(w, "⏱️  Duration: %v\n", result.Duration.Round(time.Millisecond))
	fmt.Fprintln(w)

	// Pass/Fail summary
	passRate := float64(result.PassedTests) / float64(result.TotalTests) * 100
	statusIcon := "✅"
	if result.FailedTests > 0 {
		statusIcon = "⚠️"
	}
	if passRate < 50 {
		statusIcon = "❌"
	}

	fmt.Fprintf(w, "%s Tests: %d/%d passed (%.1f%%)\n",
		statusIcon, result.PassedTests, result.TotalTests, passRate)
	fmt.Fprintln(w)

	// Aggregate metrics
	fmt.Fprintln(w, "┌─────────────────────────────────────────────────────────────────┐")
	fmt.Fprintln(w, "│                     Aggregate Metrics                           │")
	fmt.Fprintln(w, "├─────────────────────────────────────────────────────────────────┤")

	r.printMetricRow(w, "Precision@1", result.Aggregate.Precision1, -1)
	r.printMetricRow(w, "Precision@5", result.Aggregate.Precision5, -1)
	r.printMetricRow(w, "Precision@10", result.Aggregate.Precision10, result.Thresholds.Precision10)
	fmt.Fprintln(w, "├─────────────────────────────────────────────────────────────────┤")
	r.printMetricRow(w, "Recall@5", result.Aggregate.Recall5, -1)
	r.printMetricRow(w, "Recall@10", result.Aggregate.Recall10, result.Thresholds.Recall10)
	r.printMetricRow(w, "Recall@50", result.Aggregate.Recall50, -1)
	fmt.Fprintln(w, "├─────────────────────────────────────────────────────────────────┤")
	r.printMetricRow(w, "MRR", result.Aggregate.MRR, result.Thresholds.MRR)
	r.printMetricRow(w, "NDCG@5", result.Aggregate.NDCG5, -1)
	r.printMetricRow(w, "NDCG@10", result.Aggregate.NDCG10, result.Thresholds.NDCG10)
	r.printMetricRow(w, "MAP", result.Aggregate.MAP, -1)
	fmt.Fprintln(w, "├─────────────────────────────────────────────────────────────────┤")
	r.printMetricRow(w, "Hit Rate", result.Aggregate.HitRate, result.Thresholds.HitRate)

	fmt.Fprintln(w, "└─────────────────────────────────────────────────────────────────┘")
	fmt.Fprintln(w)
}

// printMetricRow prints a single metric row with optional threshold comparison.
func (r *Reporter) printMetricRow(w io.Writer, name string, value float64, threshold float64) {
	bar := r.progressBar(value, 20)
	status := " "
	if threshold >= 0 {
		if value >= threshold {
			status = "✓"
		} else {
			status = "✗"
		}
	}

	threshStr := ""
	if threshold >= 0 {
		threshStr = fmt.Sprintf(" (target: %.2f)", threshold)
	}

	fmt.Fprintf(w, "│ %s %-14s %s %.3f%s\n", status, name, bar, value, threshStr)
}

// progressBar creates a visual progress bar.
func (r *Reporter) progressBar(value float64, width int) string {
	filled := int(value * float64(width))
	if filled > width {
		filled = width
	}
	if filled < 0 {
		filled = 0
	}

	bar := strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
	return fmt.Sprintf("[%s]", bar)
}

// PrintDetails prints detailed per-test results.
func (r *Reporter) PrintDetails(result *EvalResult) {
	w := r.writer

	fmt.Fprintln(w)
	fmt.Fprintln(w, "┌─────────────────────────────────────────────────────────────────┐")
	fmt.Fprintln(w, "│                     Per-Test Results                            │")
	fmt.Fprintln(w, "└─────────────────────────────────────────────────────────────────┘")
	fmt.Fprintln(w)

	for i, tr := range result.Results {
		status := "✅"
		if tr.Error != "" {
			status = "❌"
		} else if tr.Metrics.HitRate == 0 {
			status = "⚠️"
		}

		fmt.Fprintf(w, "%s Test %d: %s\n", status, i+1, tr.TestCase.Name)
		fmt.Fprintf(w, "   Query: %q\n", truncate(tr.TestCase.Query, 50))
		fmt.Fprintf(w, "   Method: %s | Duration: %v\n", tr.SearchMethod, tr.Duration.Round(time.Microsecond))

		if tr.Error != "" {
			fmt.Fprintf(w, "   Error: %s\n", tr.Error)
		} else {
			fmt.Fprintf(w, "   P@10: %.2f | R@10: %.2f | MRR: %.2f | NDCG@10: %.2f\n",
				tr.Metrics.Precision10, tr.Metrics.Recall10, tr.Metrics.MRR, tr.Metrics.NDCG10)
			fmt.Fprintf(w, "   Expected: %d | Returned: %d | Hits: %.0f\n",
				len(tr.TestCase.Expected), len(tr.Returned), tr.Metrics.HitRate)
		}
		fmt.Fprintln(w)
	}
}

// PrintJSON outputs results as JSON.
func (r *Reporter) PrintJSON(result *EvalResult) error {
	encoder := json.NewEncoder(r.writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

// SaveJSON saves results to a JSON file.
func (r *Reporter) SaveJSON(result *EvalResult, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

// PrintCompact prints a one-line summary.
func (r *Reporter) PrintCompact(result *EvalResult) {
	status := "PASS"
	if result.FailedTests > 0 {
		status = "FAIL"
	}

	fmt.Fprintf(r.writer, "[%s] %d/%d tests | P@10=%.2f R@10=%.2f MRR=%.2f NDCG=%.2f HitRate=%.2f | %v\n",
		status,
		result.PassedTests, result.TotalTests,
		result.Aggregate.Precision10,
		result.Aggregate.Recall10,
		result.Aggregate.MRR,
		result.Aggregate.NDCG10,
		result.Aggregate.HitRate,
		result.Duration.Round(time.Millisecond),
	)
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}
