package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestPR1826S1HTTP runs property-only reads over the transactional HTTP
// endpoint against the whole node the same endpoint returns, with and without
// asynchronous writes, auto-commit and explicit, across two databases that use
// the same node ids, and around writes.
func TestPR1826S1HTTP(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async=%v", async), func(t *testing.T) {
			s, authenticator := setupTestServerWithDatabaseConfig(t, func(c *nornicdb.Config) {
				c.Database.AsyncWritesEnabled = async
				if async {
					c.Database.StrictDurability = false
				}
			}, nil)
			token := "Bearer " + getAuthToken(t, authenticator, "admin")
			post := func(path, statement string, params map[string]any) string {
				t.Helper()
				response := makeRequest(t, s, http.MethodPost, path, map[string]any{"statements": []any{map[string]any{"statement": statement, "parameters": params}}}, token)
				var result TransactionResponse
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result), response.Body.String())
				if len(result.Errors) > 0 {
					return fmt.Sprintf("ERR %v", result.Errors)
				}
				rows := []string{}
				if len(result.Results) > 0 {
					for _, d := range result.Results[0].Data {
						encoded, err := json.Marshal(d.Row)
						require.NoError(t, err)
						rows = append(rows, string(encoded))
					}
				}
				if !strings.Contains(statement, "ORDER BY") {
					sort.Strings(rows)
				}
				return strings.Join(rows, "\n")
			}
			digest := sha256.New()
			for _, db := range []string{"s1a", "s1b"} {
				require.NoError(t, s.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: db}))
				store, err := s.dbManager.GetStorage(db)
				require.NoError(t, err)
				require.NotContains(t, post("/db/"+db+"/tx/commit", "CREATE CONSTRAINT s1k FOR (n:Doc) REQUIRE n.key IS UNIQUE", nil), "ERR")
				require.NotContains(t, post("/db/"+db+"/tx/commit", "CREATE INDEX s1r FOR (n:Doc) ON (n.rank)", nil), "ERR")
				for i := 0; i < 10; i++ {
					_, err := store.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i)), Labels: []string{"Doc"}, Properties: map[string]any{
						"key": fmt.Sprintf("k%d", i), "owner": db, "rank": int64(i), "body": strings.Repeat(db, 30000), "bytes": []byte{byte(i), 200}, "list": []any{"a", "b"}, "f": 1.25, "flag": true, "empty": "",
					}})
					require.NoError(t, err)
				}
				require.NotContains(t, post("/db/"+db+"/tx/commit", "MATCH (n:Doc) SET n.d=date('2026-01-02'), n.dt=datetime('2026-11-22T10:11:12+02:00'), n.p=point({x:1.0,y:2.0}), n.dur=duration('P1DT2H')", nil), "ERR")
			}
			check := func(stage string) {
				for round := 0; round < 2; round++ {
					for _, db := range []string{"s1a", "s1b"} {
						other := map[string]string{"s1a": "s1b", "s1b": "s1a"}[db]
						for _, explicit := range []bool{false, true} {
							path := "/db/" + db + "/tx/commit"
							if explicit {
								opened := makeRequest(t, s, http.MethodPost, "/db/"+db+"/tx", map[string]any{"statements": []any{}}, token)
								location, err := url.Parse(opened.Header().Get("Location"))
								require.NoError(t, err)
								path = location.Path
							}
							var whole []map[string]any
							require.NoError(t, json.Unmarshal([]byte(post(path, "MATCH (n:Doc {key:$k}) RETURN n", map[string]any{"k": "k4"})), &whole))
							require.Len(t, whole, 1)
							names := make([]string, 0, len(whole[0]))
							for name := range whole[0] {
								names = append(names, name)
							}
							sort.Strings(names)
							for _, name := range names {
								encoded, _ := json.Marshal([]any{whole[0][name]})
								for _, form := range []string{"MATCH (n:Doc {key:$k}) RETURN n.`%s`", "MATCH (n:Doc) WHERE n.key = $k RETURN n.`%s`", "MATCH (n:Doc) WHERE n.rank = 4 RETURN n.`%s`", "UNWIND [$k] AS k MATCH (n:Doc {key:k}) WITH n AS q RETURN q.`%s`", "MATCH (n:Doc {key:$k}) CALL (n) { RETURN n.`%s` AS v } RETURN v"} {
									got := post(path, fmt.Sprintf(form, name), map[string]any{"k": "k4"})
									fmt.Fprintln(digest, stage, db, form, name, got)
									if got != string(encoded) {
										t.Errorf("S1MISMATCH http %s %s explicit=%v %s\nwhole %.200s\ngot   %.200s", stage, db, explicit, fmt.Sprintf(form, name), encoded, got)
									}
									if strings.Contains(got, other) {
										t.Errorf("S1MISMATCH http %s: %s shows %s data: %s", stage, db, other, form)
									}
								}
							}
							for _, q := range []string{"MATCH (n:Doc) RETURN n.key, n.owner ORDER BY n.key", "MATCH (n:Doc) WHERE n.rank > 3 RETURN n.key, n.missing, n.f ORDER BY n.rank", "MATCH (n:Doc) RETURN count(n.owner), collect(DISTINCT n.owner)", "MATCH (n:Doc {key:$k}) RETURN n{.key,.owner}", "MATCH (n:Doc {key:$k}) RETURN keys(n)"} {
								got := post(path, q, map[string]any{"k": "k4"})
								fmt.Fprintln(digest, stage, db, q, got)
								if strings.Contains(got, other) || strings.HasPrefix(got, "ERR") {
									t.Errorf("S1MISMATCH http %s %s %s: %s", stage, db, q, got)
								}
							}
							if explicit {
								makeRequest(t, s, http.MethodDelete, path, nil, token)
							}
						}
					}
				}
			}
			check("cold")
			// narrow, wide, write, narrow, wide
			path := "/db/s1a/tx/commit"
			k := map[string]any{"k": "k4"}
			narrow, wide := "MATCH (n:Doc {key:$k}) RETURN n.rank, n.added", "MATCH (n:Doc {key:$k}) RETURN n.rank, n.added, n.owner, n.list"
			for i := 1; i <= 8; i++ {
				require.NotContains(t, post(path, "MATCH (n:Doc {key:$k}) SET n.added = $v, n.list = [$v]", map[string]any{"k": "k4", "v": i}), "ERR")
				require.Equal(t, fmt.Sprintf("[4,%d]", i), post(path, narrow, k))
				require.Equal(t, fmt.Sprintf(`[4,%d,"s1a",[%d]]`, i, i), post(path, wide, k))
				require.Equal(t, fmt.Sprintf("[4,%d]", i), post(path, narrow, k))
				require.Equal(t, `[4,null]`, post("/db/s1b/tx/commit", narrow, k))
			}
			// an explicit transaction's own writes, invisible elsewhere, then rolled back
			opened := makeRequest(t, s, http.MethodPost, "/db/s1a/tx", map[string]any{"statements": []any{}}, token)
			location, err := url.Parse(opened.Header().Get("Location"))
			require.NoError(t, err)
			require.NotContains(t, post(location.Path, "MATCH (n:Doc {key:$k}) SET n.added = 'tx' REMOVE n.list", k), "ERR")
			require.Equal(t, `[4,"tx","s1a",null]`, post(location.Path, wide, k))
			require.Equal(t, `[4,8,"s1a",[8]]`, post(path, wide, k))
			makeRequest(t, s, http.MethodDelete, location.Path, nil, token)
			require.Equal(t, `[4,8,"s1a",[8]]`, post(path, wide, k))
			require.NotContains(t, post(path, "MATCH (n:Doc {key:'k5'}) DETACH DELETE n", nil), "ERR")
			require.Equal(t, "", post(path, narrow, map[string]any{"k": "k5"}))
			check("after")
			t.Logf("S1ROW http async=%v %s", async, hex.EncodeToString(digest.Sum(nil)[:8]))
		})
	}
}
