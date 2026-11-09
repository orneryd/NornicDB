package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A ranked db.retrieve / db.rretrieve continuation over HTTP checks its where
// on every page. Each later page is its own request, served after the request
// that started the search has completed and its context is cancelled: a
// pattern in the where must keep matching then, so no page returns a source
// the where excludes and none is lost.
func TestHTTPRetrieveRetainedWhereOnLaterPages(t *testing.T) {
	const sources = 240
	s, users := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, users, "admin")
	database := s.dbManager.DefaultDatabaseName()
	network := httptest.NewServer(s.buildRouter())
	t.Cleanup(network.Close)

	// statement runs one auto-commit statement in a request of its own and
	// returns its first row.
	statement := func(query string, params map[string]any) []any {
		t.Helper()
		body, err := json.Marshal(map[string]any{"statements": []any{map[string]any{"statement": query, "parameters": params}}})
		require.NoError(t, err)
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, network.URL+"/db/"+database+"/tx/commit", bytes.NewReader(body))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", token)
		response, err := network.Client().Do(request)
		require.NoError(t, err)
		defer response.Body.Close()
		var decoded TransactionResponse
		require.NoError(t, json.NewDecoder(response.Body).Decode(&decoded))
		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Empty(t, decoded.Errors, query)
		require.Len(t, decoded.Results, 1)
		if len(decoded.Results[0].Data) == 0 {
			return nil
		}
		return decoded.Results[0].Data[0].Row
	}
	statement("CREATE (:Collection {archived: true}), (:Collection {archived: false})", nil)
	statement(`UNWIND range(0, $last) AS i
		MATCH (c:Collection {archived: i % 2 = 1})
		CREATE (:Src {id: 'src' + toString(i), content: 'walrus notes ' + toString(i)})-[:MEMBER_OF {hidden: i % 2 = 1}]->(c)`,
		map[string]any{"last": sources - 1})
	rebuild := makeRequest(t, s, http.MethodPost, "/nornicdb/search/rebuild", map[string]any{"database": database}, token)
	require.Equal(t, http.StatusOK, rebuild.Code, rebuild.Body.String())
	require.Eventually(t, func() bool {
		status := s.db.GetDatabaseSearchStatus(database)
		return status.Ready && !status.Building
	}, 10*time.Second, 10*time.Millisecond)

	const project = " YIELD page RETURN [r IN page.results | r.node.id] AS ids, page.qid AS qid, page.has_more AS more"
	for _, procedure := range []string{"db.retrieve", "db.rretrieve"} {
		for _, tc := range []struct {
			where    string
			archived bool
		}{
			{where: "NOT EXISTS { (node)-[:MEMBER_OF]->(:Collection {archived: $archived}) }"},
			{where: "EXISTS { (node)-[:MEMBER_OF]->(:Collection {archived: $archived}) }", archived: true},
			{where: "NOT EXISTS { (node)-[m:MEMBER_OF]->(:Collection) WHERE m.hidden = $archived }"},
		} {
			t.Run(procedure+"/"+tc.where, func(t *testing.T) {
				row := statement("CALL "+procedure+"($request)"+project, map[string]any{
					"request":  map[string]any{"query": "walrus", "types": []any{"Src"}, "n": 10, "limit": 20, "where": tc.where},
					"archived": true,
				})
				seen := map[string]int{}
				for number := 1; ; number++ {
					require.Len(t, row, 3)
					ids, _ := row[0].([]any)
					if number == 1 {
						require.Len(t, ids, 10, "first page")
					}
					for _, value := range ids {
						id := value.(string)
						slot, err := strconv.Atoi(strings.TrimPrefix(id, "src"))
						require.NoError(t, err, id)
						require.Equal(t, tc.archived, slot%2 == 1, "page %d returned %s, which the where excludes", number, id)
						_, repeated := seen[id]
						require.False(t, repeated, "page %d repeated %s", number, id)
						seen[id] = number
					}
					if more, _ := row[2].(bool); !more {
						require.Greater(t, number, 3, "the fixture must page past the initial depth")
						break
					}
					require.Less(t, number, sources, "paging does not end")
					row = statement("CALL "+procedure+"($request)"+project, map[string]any{
						"request": map[string]any{"qid": row[1], "n": 10},
					})
				}
				require.Len(t, seen, sources/2, "every admitted source, on some page")
			})
		}
	}
}
