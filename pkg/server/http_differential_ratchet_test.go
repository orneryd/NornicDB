package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/testing/cypher/differential"
)

// TestDifferentialRatchetHTTPMatchesPinnedNeo4j runs the sweep and the issue
// reproductions through HTTP /db/<db>/tx/commit on NornicDB and the pinned
// Neo4j, and fails when a statement differs that isn't in
// known_mismatches.jsonl under the "http" route (differential.Assert, #754).
func TestDifferentialRatchetHTTPMatchesPinnedNeo4j(t *testing.T) {
	referenceURL := os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI")
	if referenceURL == "" {
		t.Skip("set NORNICDB_NEO4J_REFERENCE_HTTP_URI to run the differential ratchet against the pinned Neo4j")
	}
	corpusDir := filepath.Join("..", "..", "testing", "cypher", "tck", "testdata", "differential")
	sweep, err := differential.LoadSweep(filepath.Join(corpusDir, "sweep.json.gz"))
	if err != nil {
		t.Fatal(err)
	}
	issues, err := differential.LoadIssues(filepath.Join(corpusDir, "issues.json"))
	if err != nil {
		t.Fatal(err)
	}
	server, authenticator := setupTestServer(t)
	local := httptest.NewServer(server.buildRouter())
	defer local.Close()
	client := &http.Client{Timeout: differential.DefaultOptions.StatementTimeout}
	pair := &differential.Pair{
		Neo4j:    httpDifferentialExecutor{client: client, endpoint: referenceURL + "/db/neo4j/tx/commit"},
		NornicDB: httpDifferentialExecutor{client: client, endpoint: local.URL + "/db/nornic/tx/commit", authorization: "Bearer " + getAuthToken(t, authenticator, "admin")},
	}
	differential.Assert(t, filepath.Join(corpusDir, "known_mismatches.jsonl"), "http", func() ([]differential.Result, int, error) {
		return differential.RunCorpora(context.Background(), pair, sweep, issues, t.Logf)
	})
}

// httpDifferentialExecutor runs each statement as one /tx/commit request.
// Rows are the response's row values with numbers kept as their JSON text, so
// 1 and 1.0 stay distinct.
type httpDifferentialExecutor struct {
	client        *http.Client
	endpoint      string
	authorization string
}

func (executor httpDifferentialExecutor) post(ctx context.Context, statement string, into any) error {
	payload, err := json.Marshal(map[string]any{"statements": []map[string]any{{"statement": statement}}})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, executor.endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	if executor.authorization != "" {
		request.Header.Set("Authorization", executor.authorization)
	}
	response, err := executor.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		return fmt.Errorf("HTTP %d", response.StatusCode)
	}
	decoder := json.NewDecoder(response.Body)
	decoder.UseNumber()
	return decoder.Decode(into)
}

func (executor httpDifferentialExecutor) Execute(ctx context.Context, query string) differential.Outcome {
	var response struct {
		Results []struct {
			Columns []string `json:"columns"`
			Data    []struct {
				Row []any `json:"row"`
			} `json:"data"`
		} `json:"results"`
		Errors []struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := executor.post(ctx, query, &response); err != nil {
		return differential.Outcome{Code: "client:" + err.Error()}
	}
	if len(response.Errors) > 0 {
		return differential.Outcome{Code: response.Errors[0].Code, Message: response.Errors[0].Message}
	}
	if len(response.Results) != 1 {
		return differential.Outcome{Code: fmt.Sprintf("client:%d results", len(response.Results))}
	}
	outcome := differential.Outcome{Columns: response.Results[0].Columns, Rows: make([][]any, len(response.Results[0].Data))}
	for index, data := range response.Results[0].Data {
		outcome.Rows[index] = data.Row
	}
	return outcome
}

func (executor httpDifferentialExecutor) Reset(ctx context.Context) error {
	var failure error
	err := resetHTTPDifferentialBackend(func(statement string) TransactionResponse {
		var response TransactionResponse
		if err := executor.post(ctx, statement, &response); err != nil && failure == nil {
			failure = err
		}
		return response
	})
	if failure != nil {
		return failure
	}
	return err
}
