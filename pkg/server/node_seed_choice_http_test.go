package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// NOR-414 over HTTP, auto-commit and in an explicit transaction: a lookup by
// an indexed id whose WHERE also filters on an indexed status reads the id
// index, not every node holding the status. Reading the 20,000 pending nodes
// once per id allocated over a gigabyte; the id lookups allocate a few
// megabytes at most.
func TestHTTPNodeSeedChoiceReadsSelectiveIndex(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	for _, statement := range []string{
		"CREATE INDEX http_case_id FOR (n:Case) ON (n.id)",
		"CREATE INDEX http_case_status FOR (n:Case) ON (n.status)",
		"UNWIND range(0, 29999) AS i CREATE (:Case {id: 'c-' + toString(i), status: CASE WHEN i % 3 = 0 THEN 'done' ELSE 'pending' END})",
	} {
		_, err := server.db.ExecuteCypher(t.Context(), statement, nil)
		require.NoError(t, err, statement)
	}

	ids := make([]any, 0, 20)
	var want []string
	for i := 0; i < 20; i++ {
		n := i*50 + 1
		ids = append(ids, fmt.Sprintf("c-%d", n))
		if n%3 != 0 {
			want = append(want, fmt.Sprintf("c-%d", n))
		}
	}
	sort.Strings(want)

	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			for _, statement := range []string{
				"UNWIND $ids AS id MATCH (c:Case {id: id}) WHERE c.status = 'pending' RETURN c.id",
				"UNWIND $ids AS id MATCH (c:Case) WHERE c.status = 'pending' AND c.id = id RETURN c.id",
			} {
				endpoint := "/db/nornic/tx/commit"
				if explicit {
					opened := makeRequest(t, server, http.MethodPost, "/db/nornic/tx", map[string]any{"statements": []any{}}, token)
					var tx TransactionResponse
					require.NoError(t, json.Unmarshal(opened.Body.Bytes(), &tx))
					endpoint = tx.Commit
				}
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				response := makeRequest(t, server, http.MethodPost, endpoint, map[string]any{"statements": []any{map[string]any{"statement": statement, "parameters": map[string]any{"ids": ids}}}}, token)
				runtime.ReadMemStats(&after)
				var result map[string]any
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
				require.Empty(t, result["errors"], statement)
				var got []string
				for _, raw := range result["results"].([]any)[0].(map[string]any)["data"].([]any) {
					got = append(got, fmt.Sprint(raw.(map[string]any)["row"].([]any)[0]))
				}
				sort.Strings(got)
				require.Equal(t, want, got, statement)
				require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(64<<20), "%s: allocated %d bytes; the id index must seed the MATCH", statement, after.TotalAlloc-before.TotalAlloc)
			}
		})
	}
}
