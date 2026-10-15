package cypher

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The same ordered fixture runs through Bolt auto-commit, Bolt transactions
// and HTTP in the differential corpus. expected_rows records the separate
// Neo4j 5.26.30 oracle for both fulltext and four-dimensional vector indexes.
func TestProcedureYieldWhereSearchEligibility(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testing", "cypher", "tck", "testdata", "differential", "issues.json"))
	require.NoError(t, err)
	var issues []struct {
		Issue      int `json:"issue"`
		Statements []struct {
			ID           string          `json:"id"`
			Query        string          `json:"query"`
			ExpectedRows [][]interface{} `json:"expected_rows"`
		} `json:"statements"`
	}
	require.NoError(t, json.Unmarshal(data, &issues))
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "search_eligibility"))
	for _, issue := range issues {
		if issue.Issue != 1226 {
			continue
		}
		for _, statement := range issue.Statements {
			t.Run(statement.ID, func(t *testing.T) {
				result, err := exec.Execute(context.Background(), statement.Query, nil)
				require.NoError(t, err, statement.Query)
				if statement.ExpectedRows == nil {
					return
				}
				require.Equal(t, []string{"id", "positive"}, result.Columns)
				if len(statement.ExpectedRows) == 0 {
					require.Empty(t, result.Rows, statement.Query)
				} else {
					require.Equal(t, statement.ExpectedRows, result.Rows, statement.Query)
				}
			})
			if t.Failed() {
				return
			}
		}
		return
	}
	t.Fatal("missing procedure eligibility differential fixture for #1226")
}
