package server

// v1.5.2 close-out lane L4, H-375: after an HTTP tx/commit read times out
// with 503, the next tx/commit requests (RETURN 1, SHOW TRANSACTIONS) answer
// promptly: a timed-out read does not block other requests. Two blockers: a
// long Cypher read, and a procedure that ignores its context entirely.
// Test file only; not for merge.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

func TestCloseoutL4H375TimedOutReadDoesNotBlockLaterCommits(t *testing.T) {
	t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "1s")
	s, a := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, a, "admin")
	require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: "l4.ignoreContext", Mode: cypher.ProcedureModeRead, MinArgs: 0, MaxArgs: 0},
		func(ctx context.Context, _ *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
			time.Sleep(8 * time.Second) // ignores ctx on purpose
			return &cypher.ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
		}))
	defer cypher.ClearUserProcedures()
	h := httptest.NewServer(s.buildRouter())
	defer h.Close()
	post := func(statement string) (int, string, time.Duration) {
		body, _ := json.Marshal(map[string]any{"statements": []map[string]any{{"statement": statement}}})
		req, err := http.NewRequest(http.MethodPost, h.URL+"/db/nornic/tx/commit", strings.NewReader(string(body)))
		require.NoError(t, err)
		req.Header.Set("Authorization", token)
		req.Header.Set("Content-Type", "application/json")
		start := time.Now()
		response, err := h.Client().Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		raw, _ := io.ReadAll(response.Body)
		return response.StatusCode, string(raw), time.Since(start)
	}
	code, body, _ := post("UNWIND range(1, 30000) AS k CREATE (:H375 {v: k})")
	require.Equal(t, http.StatusOK, code, body)
	for name, blocker := range map[string]string{
		"long-cypher-read":       "UNWIND range(1, 30000) AS i OPTIONAL MATCH (n:H375) WHERE n.v = -i RETURN count(n) AS n",
		"procedure-ignoring-ctx": "CALL l4.ignoreContext()",
	} {
		blockCode, blockBody, blockTime := post(blocker)
		detail := map[string]any{"blocker": name, "blockerStatus": blockCode, "blockerSeconds": blockTime.Seconds(), "blockerBody": truncateL4(blockBody)}
		ok := true // the follow-up requests decide; the blocker's own outcome is recorded
		for _, follow := range []string{"RETURN 1 AS one", "SHOW TRANSACTIONS"} {
			followCode, followBody, followTime := post(follow)
			detail[follow+" status"] = followCode
			detail[follow+" seconds"] = followTime.Seconds()
			detail[follow+" body"] = truncateL4(followBody)
			ok = ok && followCode == http.StatusOK && followTime < 3*time.Second && !strings.Contains(followBody, `"errors":[{`)
		}
		detail["item"] = "H-375"
		detail["check"] = name
		detail["pass"] = ok
		data, _ := json.Marshal(detail)
		fmt.Fprintf(os.Stdout, "L4_RESULT %s\n", data)
		if !ok {
			t.Errorf("H-375 %s: expected behaviour does not hold: %s", name, data)
		}
		time.Sleep(9 * time.Second) // let the blocker end before the next case
	}
}

func truncateL4(s string) string {
	if len(s) > 400 {
		return s[:400]
	}
	return s
}
