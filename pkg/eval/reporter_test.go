package eval

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeTestEvalResult() *EvalResult {
	return &EvalResult{
		SuiteName:   "test-suite",
		TotalTests:  3,
		PassedTests: 2,
		FailedTests: 1,
		Duration:    150 * time.Millisecond,
		Timestamp:   time.Now(),
		Thresholds:  DefaultThresholds(),
		Aggregate: Metrics{
			Precision1:  0.8,
			Precision5:  0.75,
			Precision10: 0.7,
			Recall5:     0.65,
			Recall10:    0.6,
			Recall50:    0.55,
			MRR:         0.72,
			NDCG5:       0.68,
			NDCG10:      0.65,
			MAP:         0.63,
			HitRate:     0.80,
		},
		Results: []TestResult{
			{
				TestCase: TestCase{
					Name:     "test-1",
					Query:    "find nodes",
					Expected: []string{"n1", "n2"},
				},
				Returned:     []string{"n1", "n2", "n3"},
				SearchMethod: "hybrid",
				Duration:     10 * time.Millisecond,
				Metrics: Metrics{
					Precision10: 0.8,
					Recall10:    0.9,
					MRR:         1.0,
					NDCG10:      0.85,
					HitRate:     1.0,
				},
			},
			{
				TestCase: TestCase{
					Name:     "test-2",
					Query:    "search edges",
					Expected: []string{"e1"},
				},
				Returned:     []string{},
				SearchMethod: "vector",
				Duration:     5 * time.Millisecond,
				Metrics: Metrics{
					HitRate: 0.0,
				},
			},
			{
				TestCase: TestCase{
					Name:     "test-3",
					Query:    "error case",
					Expected: []string{"n1"},
				},
				SearchMethod: "hybrid",
				Duration:     1 * time.Millisecond,
				Error:        "connection timeout",
			},
		},
	}
}

func TestNewReporter_NilWriter(t *testing.T) {
	r := NewReporter(nil)
	require.NotNil(t, r)
	// Should not panic
	result := makeTestEvalResult()
	r.PrintCompact(result)
}

func TestNewReporter_CustomWriter(t *testing.T) {
	var buf bytes.Buffer
	r := NewReporter(&buf)
	require.NotNil(t, r)

	result := makeTestEvalResult()
	r.PrintCompact(result)
	assert.NotEmpty(t, buf.String())
}

func TestReporter_PrintSummary(t *testing.T) {
	var buf bytes.Buffer
	r := NewReporter(&buf)

	result := makeTestEvalResult()
	r.PrintSummary(result)

	out := buf.String()
	assert.Contains(t, out, "NornicDB Search Evaluation Results")
	assert.Contains(t, out, "test-suite")
	assert.Contains(t, out, "Precision@10")
	assert.Contains(t, out, "Recall@10")
	assert.Contains(t, out, "MRR")
	assert.Contains(t, out, "NDCG@10")
	assert.Contains(t, out, "Hit Rate")
}

func TestReporter_PrintSummary_AllFailed(t *testing.T) {
	var buf bytes.Buffer
	r := NewReporter(&buf)

	result := makeTestEvalResult()
	result.PassedTests = 0
	result.FailedTests = 3

	r.PrintSummary(result)
	out := buf.String()
	assert.Contains(t, out, "❌")
}

func TestReporter_PrintSummary_AllPassed(t *testing.T) {
	var buf bytes.Buffer
	r := NewReporter(&buf)

	result := makeTestEvalResult()
	result.PassedTests = 3
	result.FailedTests = 0

	r.PrintSummary(result)
	out := buf.String()
	assert.Contains(t, out, "✅")
}

func TestReporter_PrintDetails(t *testing.T) {
	var buf bytes.Buffer
	r := NewReporter(&buf)

	result := makeTestEvalResult()
	r.PrintDetails(result)

	out := buf.String()
	assert.Contains(t, out, "Per-Test Results")
	assert.Contains(t, out, "test-1")
	assert.Contains(t, out, "test-2")
	assert.Contains(t, out, "test-3")
	assert.Contains(t, out, "connection timeout")
}

func TestReporter_PrintDetails_LongQuery(t *testing.T) {
	var buf bytes.Buffer
	r := NewReporter(&buf)

	result := makeTestEvalResult()
	// Make a very long query to trigger truncation
	result.Results[0].TestCase.Query = strings.Repeat("word ", 20)

	r.PrintDetails(result)
	out := buf.String()
	assert.Contains(t, out, "...")
}

func TestReporter_PrintJSON(t *testing.T) {
	var buf bytes.Buffer
	r := NewReporter(&buf)

	result := makeTestEvalResult()
	err := r.PrintJSON(result)
	require.NoError(t, err)

	// Should be valid JSON
	var parsed map[string]interface{}
	err = json.Unmarshal(buf.Bytes(), &parsed)
	require.NoError(t, err)
	assert.Equal(t, "test-suite", parsed["suite_name"])
}

func TestReporter_SaveJSON(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "results.json")

	var buf bytes.Buffer
	r := NewReporter(&buf)

	result := makeTestEvalResult()
	err := r.SaveJSON(result, path)
	require.NoError(t, err)

	// File should exist and be valid JSON
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var parsed map[string]interface{}
	err = json.Unmarshal(data, &parsed)
	require.NoError(t, err)
	assert.Equal(t, "test-suite", parsed["suite_name"])
}

func TestReporter_SaveJSON_InvalidPath(t *testing.T) {
	var buf bytes.Buffer
	r := NewReporter(&buf)

	result := makeTestEvalResult()
	err := r.SaveJSON(result, filepath.Join(t.TempDir(), "nonexistent", "results.json"))
	assert.Error(t, err)
}

func TestReporter_PrintCompact_Pass(t *testing.T) {
	var buf bytes.Buffer
	r := NewReporter(&buf)

	result := makeTestEvalResult()
	result.FailedTests = 0
	result.PassedTests = 3
	r.PrintCompact(result)

	assert.Contains(t, buf.String(), "PASS")
}

func TestReporter_PrintCompact_Fail(t *testing.T) {
	var buf bytes.Buffer
	r := NewReporter(&buf)

	result := makeTestEvalResult()
	r.PrintCompact(result)

	assert.Contains(t, buf.String(), "FAIL")
}

func TestTruncate(t *testing.T) {
	tests := []struct {
		input    string
		max      int
		expected string
	}{
		{"hello", 10, "hello"},
		{"hello world", 5, "he..."},
		{"hi", 2, "hi"},
		{"exact", 5, "exact"},
		{"longer than max", 8, "longe..."},
	}

	for _, tt := range tests {
		result := truncate(tt.input, tt.max)
		assert.Equal(t, tt.expected, result, "truncate(%q, %d)", tt.input, tt.max)
	}
}
