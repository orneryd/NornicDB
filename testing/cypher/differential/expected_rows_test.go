package differential

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func loadExpectedRowsCase(t *testing.T, query, annotation string) []IssueCase {
	t.Helper()
	path := filepath.Join(t.TempDir(), "issues.json")
	content := fmt.Sprintf(`[{"issue":12,"statements":[{"id":"expected","query":%q%s}]}]`, query, annotation)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	issues, err := LoadIssues(path)
	require.NoError(t, err)
	return issues
}

func TestIssueExpectedRows(t *testing.T) {
	failed := Outcome{Code: "Neo.ClientError.Statement.SyntaxError"}
	for _, test := range []struct {
		name       string
		query      string
		annotation string
		reference  Outcome
		actual     Outcome
		wantError  bool
	}{
		{"matching errors cannot satisfy empty rows", "RETURN x", `,"expected_rows":[]`, failed, failed, true},
		{"matching wrong rows", "RETURN 1 AS v", `,"expected_rows":[[1]]`, rows([]any{2}), rows([]any{2}), true},
		{"wrong reference", "RETURN 1 AS v", `,"expected_rows":[[1]]`, rows([]any{2}), rows([]any{1}), true},
		{"wrong actual", "RETURN 1 AS v", `,"expected_rows":[[1]]`, rows([]any{1}), rows([]any{2}), true},
		{"reference error", "RETURN 1 AS v", `,"expected_rows":[[1]]`, failed, rows([]any{1}), true},
		{"actual error", "RETURN 1 AS v", `,"expected_rows":[[1]]`, rows([]any{1}), failed, true},
		{"empty success", "MATCH (n) RETURN n.v AS v", `,"expected_rows":[]`, rows(), rows(), false},
		{"empty expectation rejects nonempty", "RETURN 1 AS v", `,"expected_rows":[]`, rows([]any{1}), rows([]any{1}), true},
		{"omitted annotation preserves agreement", "RETURN x", "", failed, failed, false},
		{"null annotation is omitted", "RETURN x", `,"expected_rows":null`, failed, failed, false},
		{"unordered rows", "UNWIND [1,2] AS v RETURN v", `,"expected_rows":[[1],[2]]`, rows([]any{2}, []any{1}), rows([]any{1}, []any{2}), false},
		{"ordered rows reject same reversed answers", "UNWIND [1,2] AS v RETURN v ORDER BY v", `,"expected_rows":[[1],[2]]`, rows([]any{2}, []any{1}), rows([]any{2}, []any{1}), true},
		{"explicit values survive arbitrary limit rule", "MATCH (n) RETURN n.v AS v LIMIT 1", `,"expected_rows":[[1]]`, rows([]any{2}), rows([]any{2}), true},
		{"row multiplicity", "UNWIND [1,1] AS v RETURN v", `,"expected_rows":[[1],[1]]`, rows([]any{1}), rows([]any{1}), true},
		{"large integer exact", "RETURN 9007199254740993 AS v", `,"expected_rows":[[9007199254740993]]`, rows([]any{int64(9007199254740993)}), rows([]any{int64(9007199254740993)}), false},
		{"large integer differs by one", "RETURN 9007199254740993 AS v", `,"expected_rows":[[9007199254740993]]`, rows([]any{int64(9007199254740992)}), rows([]any{int64(9007199254740992)}), true},
		{"fraction and exponent", "RETURN 1000.5 AS v", `,"expected_rows":[[1.0005e3]]`, rows([]any{1000.5}), rows([]any{1000.5}), false},
		{"short row with returned sort key", "RETURN 1 AS v ORDER BY v", `,"expected_rows":[[]]`, rows([]any{1}), rows([]any{1}), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			issues := loadExpectedRowsCase(t, test.query, test.annotation)
			pair := &Pair{
				Neo4j: &fakeServer{answers: map[string]Outcome{test.query: test.reference}},
				NornicDB: &fakeServer{answers: map[string]Outcome{test.query: test.actual}},
			}
			results, err := RunIssues(context.Background(), issues, pair, Options{StatementTimeout: time.Second})
			if test.wantError {
				require.ErrorContains(t, err, "expected_rows")
				require.ErrorContains(t, err, "issue #12")
				require.ErrorContains(t, err, "expected")
			} else {
				require.NoError(t, err)
				require.Len(t, results, 1)
				require.True(t, results[0].Match)
			}
		})
	}
}

func TestIssueExpectedRowsRoundTrip(t *testing.T) {
	for _, annotation := range []string{`,"expected_rows":[]`, `,"expected_rows":[[9007199254740993,{"n":9007199254740993,"xs":[true,null,1.5]}]]`} {
		issues := loadExpectedRowsCase(t, "RETURN 1 AS v", annotation)
		encoded, err := json.Marshal(issues[0].Statements[0])
		require.NoError(t, err)
		require.Contains(t, string(encoded), annotation[1:], "preserve explicit empty rows and exact integers")
	}
}

func TestIssueExpectedRowsCannotBeRatchetWaived(t *testing.T) {
	for _, update := range []string{"0", "1"} {
		t.Run(update, func(t *testing.T) {
			t.Setenv(UpdateRatchetEnv, update)
			path := filepath.Join(t.TempDir(), "known.jsonl")
			known := []byte("{\"id\":\"expected\",\"route\":\"r\",\"issue\":12,\"unstable\":true}\n")
			require.NoError(t, os.WriteFile(path, known, 0o644))
			issues := loadExpectedRowsCase(t, "RETURN 1 AS v", `,"expected_rows":[[1]]`)
			pair := &Pair{Neo4j: &fakeServer{}, NornicDB: &fakeServer{}}
			record := assertRecorded(t, path, "r", func() ([]Result, int, error) {
				results, err := RunIssues(context.Background(), issues, pair, Options{StatementTimeout: time.Second})
				return results, pair.ResetRetries, err
			})
			require.Contains(t, record.fatal, "expected_rows")
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, known, after, "an explicit assertion cannot become a known mismatch")
		})
	}
}
