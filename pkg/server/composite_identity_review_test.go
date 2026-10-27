package server

import (
	"context"
	"encoding/json"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/url"
	"testing"
)

func TestHTTPCompositeReviewRetrieval(t *testing.T) {
	s, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	for _, database := range []string{"review_one", "review_two"} {
		require.NoError(t, s.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: database}))
		store, err := s.dbManager.GetStorage(database)
		require.NoError(t, err)
		_, err = store.CreateNode(&storage.Node{ID: "same", Labels: []string{"Review"}, Properties: map[string]interface{}{"content": "identityprobe"}})
		require.NoError(t, err)
		svc, err := s.db.GetOrCreateSearchService(database, store)
		require.NoError(t, err)
		require.NoError(t, svc.BuildIndexes(context.Background()))
	}
	require.NoError(t, s.dbManager.CreateCompositeDatabase(t.Context(), "review_comp", []multidb.ConstituentRef{{Alias: "one", DatabaseName: "review_one", Type: "local", AccessMode: "read_write"}, {Alias: "two", DatabaseName: "review_two", Type: "local", AccessMode: "read_write"}}))
	for _, explicit := range []bool{false, true} {
		for _, procedure := range []string{"db.retrieve", "db.retrieveGraph"} {
			t.Run(procedure+map[bool]string{false: "/auto", true: "/explicit"}[explicit], func(t *testing.T) {
				path := "/db/review_comp/tx/commit"
				if explicit {
					opened := makeRequest(t, s, http.MethodPost, "/db/review_comp/tx", map[string]interface{}{"statements": []interface{}{}}, token)
					require.Equal(t, http.StatusCreated, opened.Code, opened.Body.String())
					location, err := url.Parse(opened.Header().Get("Location"))
					require.NoError(t, err)
					path = location.Path
					defer makeRequest(t, s, http.MethodDelete, path, nil, token)
				}
				response := makeRequest(t, s, http.MethodPost, path, map[string]interface{}{"statements": []interface{}{map[string]interface{}{"statement": "USE review_comp CALL { USE review_comp.one CALL " + procedure + "({query:'identityprobe',limit:10}) YIELD node RETURN node UNION ALL USE review_comp.two CALL " + procedure + "({query:'identityprobe',limit:10}) YIELD node RETURN node } RETURN elementId(node) AS id ORDER BY id"}}}, token)
				var result TransactionResponse
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
				require.Empty(t, result.Errors, response.Body.String())
				require.Len(t, result.Results, 1)
				require.Len(t, result.Results[0].Data, 2, response.Body.String())
				require.Equal(t, []interface{}{"4:review_one:same"}, result.Results[0].Data[0].Row)
				require.Equal(t, []interface{}{"4:review_two:same"}, result.Results[0].Data[1].Row)
			})
		}
	}
}
