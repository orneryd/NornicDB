package server

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRuntimeMapWireContract(t *testing.T) {
	server, _ := setupTestServer(t)
	db := server.dbManager.DefaultDatabaseName()
	ctx := context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{Username: "admin", Roles: []string{"admin"}})
	run := func(q string, p map[string]interface{}) TransactionResponse {
		payload, err := json.Marshal(TransactionRequest{Statements: []StatementRequest{{Statement: q, Parameters: p}}})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/db/"+db+"/tx/commit", bytes.NewReader(payload)).WithContext(ctx)
		rec := httptest.NewRecorder()
		server.handleImplicitTransaction(rec, req, db)
		var response TransactionResponse
		if err = json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatalf("%v body=%s", err, rec.Body.String())
		}
		return response
	}
	for _, input := range []struct {
		name, expr string
		p          map[string]interface{}
	}{
		{"json-null", "$p", map[string]interface{}{"p": nil}},
		{"json-typed-nil-map", "$p", map[string]interface{}{"p": map[string]interface{}(nil)}},
		{"json-empty-map", "$p", map[string]interface{}{"p": map[string]interface{}{}}},
		{"json-decoded-null", "apoc.convert.fromJsonMap($text)", map[string]interface{}{"text": "null"}},
		{"json-nested-null", "$row.value", map[string]interface{}{"row": map[string]interface{}{"value": nil}}},
	} {
		for _, entity := range []string{"n", "r"} {
			for _, op := range []string{"=", "+="} {
				if r := run("MATCH (n:R73HTTP) DETACH DELETE n", nil); len(r.Errors) > 0 {
					t.Fatal(r.Errors)
				}
				if r := run("CREATE (:R73HTTP {keep:1,x:7})-[:R73HTTPR {keep:2,x:8}]->(:R73HTTP)", nil); len(r.Errors) > 0 {
					t.Fatal(r.Errors)
				}
				write := run("MATCH (n:R73HTTP)-[r:R73HTTPR]->() SET "+entity+" "+op+" "+input.expr, input.p)
				state := run("MATCH (n:R73HTTP)-[r:R73HTTPR]->() RETURN properties(n),properties(r)", nil)
				if len(state.Errors) > 0 {
					t.Fatal(state.Errors)
				}
				valid := input.name == "json-empty-map"
				if valid {
					require.Empty(t, write.Errors)
				} else {
					require.NotEmpty(t, write.Errors, input.name)
				}
				wantNode := map[string]interface{}{"keep": float64(1), "x": float64(7)}
				wantRel := map[string]interface{}{"keep": float64(2), "x": float64(8)}
				if valid && op == "=" {
					if entity == "n" {
						wantNode = map[string]interface{}{}
					} else {
						wantRel = map[string]interface{}{}
					}
				}
				require.Len(t, state.Results, 1)
				require.Len(t, state.Results[0].Data, 1)
				require.Equal(t, []interface{}{wantNode, wantRel}, state.Results[0].Data[0].Row, input.name)
			}
		}
	}
}
