package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGh775_HTTPTypedNodeIndexRouting(t *testing.T) {
	for _, testCase := range []struct {
		kind, name, property string
	}{
		{"TEXT", "ti", "name"},
		{"POINT", "pi", "loc"},
	} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/explicit=%v", testCase.kind, explicit), func(t *testing.T) {
				server := setupAsyncUnwindServer(t)
				token := "Bearer " + getAuthToken(t, server.auth, "admin")
				endpoint := "/db/nornic/tx/commit"
				if explicit {
					endpoint = "/db/nornic/tx"
				}
				query := "CREATE " + testCase.kind + " INDEX " + testCase.name + " FOR (n:P) ON (n." + testCase.property + ")"
				created := makeRequest(t, server, http.MethodPost, endpoint, map[string]any{"statements": []map[string]any{{"statement": query}}}, token)
				var creation TransactionResponse
				require.NoError(t, json.Unmarshal(created.Body.Bytes(), &creation))
				require.Empty(t, creation.Errors)
				if explicit {
					require.NotEmpty(t, creation.Commit)
					endpoint = creation.Commit
				}
				shown := makeRequest(t, server, http.MethodPost, endpoint, map[string]any{"statements": []map[string]any{{"statement": "SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties WHERE name = '" + testCase.name + "' RETURN type, entityType, labelsOrTypes, properties"}}}, token)
				var result TransactionResponse
				require.NoError(t, json.Unmarshal(shown.Body.Bytes(), &result))
				require.Empty(t, result.Errors)
				require.Len(t, result.Results, 1)
				require.Len(t, result.Results[0].Data, 1)
				require.Equal(t, []any{testCase.kind, "NODE", []any{"P"}, []any{testCase.property}}, result.Results[0].Data[0].Row)
			})
		}
	}
}

func TestResidualHTTPProfileEnvelope(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	response := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{{"statement": "PROFILE MATCH (n) RETURN n"}},
	}, token)
	require.Equal(t, http.StatusOK, response.Code)
	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &decoded))
	result := decoded["results"].([]interface{})[0].(map[string]interface{})
	require.Contains(t, result, "plan")
	require.NotContains(t, result, "profile")
	require.Contains(t, result["plan"].(map[string]interface{}), "rows")
}

func TestResidualTransactionBodyLifecycle(t *testing.T) {
	server, authenticator := setupTestServer(t)
	local := httptest.NewServer(server.buildRouter())
	defer local.Close()
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	client := &http.Client{Timeout: 15 * time.Second}
	for _, backend := range []struct {
		name string
		url  string
	}{
		{"nornicdb", local.URL + "/db/nornic/tx"},
		{"neo4j", strings.TrimSuffix(os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI"), "/") + "/db/neo4j/tx"},
	} {
		t.Run(backend.name, func(t *testing.T) {
			if backend.name == "neo4j" && os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI") == "" {
				t.Skip("set NORNICDB_NEO4J_REFERENCE_HTTP_URI to compare lifecycle with Neo4j")
			}
			post := func(endpoint, body string) (int, TransactionResponse) {
				t.Helper()
				request, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(body))
				require.NoError(t, err)
				request.Header.Set("Content-Type", "application/json")
				if backend.name == "nornicdb" {
					request.Header.Set("Authorization", token)
				}
				response, err := client.Do(request)
				require.NoError(t, err)
				defer response.Body.Close()
				var decoded TransactionResponse
				require.NoError(t, json.NewDecoder(response.Body).Decode(&decoded))
				return response.StatusCode, decoded
			}
			for _, testCase := range []struct {
				name     string
				body     string
				commit   bool
				implicit bool
				invalid  bool
			}{
				{"truncated execute", `{"statements":[{"statement":"CREATE (:Z2)"`, false, false, true},
				{"array execute", `[1]`, false, false, true},
				{"object commit", `{}`, true, false, true},
				{"empty commit", ``, true, false, false},
				{"empty implicit", ``, true, true, false},
				{"truncated implicit", `{"statements":[{"statement":"CREATE (:Z2)"`, true, true, true},
				{"array implicit", `[1]`, true, true, true},
				{"object implicit", `{}`, true, true, true},
			} {
				t.Run(testCase.name, func(t *testing.T) {
					_, cleared := post(backend.url+"/commit", `{"statements":[{"statement":"MATCH (n) DETACH DELETE n"}]}`)
					require.Empty(t, cleared.Errors)
					endpoint := backend.url + "/commit"
					if !testCase.implicit {
						statusCode, opened := post(backend.url, `{"statements":[{"statement":"CREATE (:Z1)"}]}`)
						require.Equal(t, http.StatusCreated, statusCode)
						require.Empty(t, opened.Errors)
						endpoint = strings.TrimSuffix(opened.Commit, "/commit")
						if testCase.commit {
							endpoint += "/commit"
						}
					}
					statusCode, result := post(endpoint, testCase.body)
					t.Logf("LIFECYCLE_RESULT backend=%s case=%q status=%d errors=%+v", backend.name, testCase.name, statusCode, result.Errors)
					if testCase.invalid {
						require.Len(t, result.Errors, 1)
						require.Equal(t, "Neo.ClientError.Request.InvalidFormat", result.Errors[0].Code)
						require.Equal(t, http.StatusOK, statusCode)
						if !testCase.implicit {
							commitURL := endpoint
							if !testCase.commit {
								commitURL += "/commit"
							}
							statusCode, committed := post(commitURL, `{"statements":[]}`)
							require.Equal(t, http.StatusNotFound, statusCode)
							require.Len(t, committed.Errors, 1)
							require.Equal(t, "Neo.ClientError.Transaction.TransactionNotFound", committed.Errors[0].Code)
						}
					} else {
						require.Equal(t, http.StatusOK, statusCode)
						require.Empty(t, result.Errors)
						require.Empty(t, result.Results)
					}
					_, stored := post(backend.url+"/commit", `{"statements":[{"statement":"MATCH (n) RETURN count(n)"}]}`)
					require.Empty(t, stored.Errors)
					count := float64(0)
					if !testCase.invalid && !testCase.implicit {
						count = 1
					}
					require.Equal(t, count, stored.Results[0].Data[0].Row[0])
				})
			}
		})
	}
}

func TestResidualTrailingTransactionBodies(t *testing.T) {
	server, authenticator := setupTestServer(t)
	local := httptest.NewServer(server.buildRouter())
	defer local.Close()
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	for _, backend := range []struct{ name, url string }{
		{"nornicdb", local.URL + "/db/nornic/tx"},
		{"neo4j", strings.TrimSuffix(os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI"), "/") + "/db/neo4j/tx"},
	} {
		t.Run(backend.name, func(t *testing.T) {
			if backend.name == "neo4j" && os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI") == "" {
				t.Skip("set NORNICDB_NEO4J_REFERENCE_HTTP_URI to compare trailing-body behavior")
			}
			post := func(endpoint, body string) (int, TransactionResponse) {
				t.Helper()
				request, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(body))
				require.NoError(t, err)
				request.Header.Set("Content-Type", "application/json")
				if backend.name == "nornicdb" {
					request.Header.Set("Authorization", token)
				}
				response, err := (&http.Client{Timeout: 15 * time.Second}).Do(request)
				require.NoError(t, err)
				defer response.Body.Close()
				var result TransactionResponse
				require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
				return response.StatusCode, result
			}
			for _, suffix := range []string{" xyz", `{"statements":[{"statement":"CREATE (:Ignored)"}]}`} {
				for _, route := range []string{"open", "execute", "commit", "implicit"} {
					t.Run(route+suffix, func(t *testing.T) {
						_, cleared := post(backend.url+"/commit", `{"statements":[{"statement":"MATCH (n) DETACH DELETE n"}]}`)
						require.Empty(t, cleared.Errors)
						endpoint := backend.url
						count := float64(1)
						if route == "execute" || route == "commit" {
							statusCode, opened := post(backend.url, `{"statements":[{"statement":"CREATE (:Z1)"}]}`)
							require.Equal(t, http.StatusCreated, statusCode)
							require.Empty(t, opened.Errors)
							endpoint = strings.TrimSuffix(opened.Commit, "/commit")
							count = 2
						}
						if route == "commit" || route == "implicit" {
							endpoint += "/commit"
						}
						statusCode, result := post(endpoint, `{"statements":[{"statement":"CREATE (:Z2)"}]}`+suffix)
						wantStatus := http.StatusOK
						if route == "open" {
							wantStatus = http.StatusCreated
						}
						require.Equal(t, wantStatus, statusCode)
						require.Empty(t, result.Errors)
						require.Len(t, result.Results, 1)
						if route == "open" || route == "execute" {
							commitURL := result.Commit
							if route == "execute" {
								commitURL = endpoint + "/commit"
							}
							statusCode, committed := post(commitURL, `{"statements":[]}`)
							require.Equal(t, http.StatusOK, statusCode)
							require.Empty(t, committed.Errors)
						}
						_, stored := post(backend.url+"/commit", `{"statements":[{"statement":"MATCH (n) RETURN labels(n)[0] AS label ORDER BY label"}]}`)
						require.Empty(t, stored.Errors)
						require.Len(t, stored.Results[0].Data, int(count))
						if count == 2 {
							require.Equal(t, []interface{}{"Z1"}, stored.Results[0].Data[0].Row)
						}
						require.Equal(t, []interface{}{"Z2"}, stored.Results[0].Data[int(count)-1].Row)
					})
				}
			}
		})
	}
}

func TestGh776_TransactionBodiesRejectInvalidRequests(t *testing.T) {
	server, authenticator := setupTestServerWithConfig(t, func(config *Config) {
		config.MaxRequestSize = 256
	})
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	valid := `{"statements":[{"statement":"CREATE (:Body776 {final: true})"}]}`
	for _, body := range []struct {
		name string
		text string
	}{
		{"oversized parameter", `{"statements":[{"statement":"CREATE (:Body776 {final: true})","parameters":{"unused":"` + strings.Repeat("x", 512) + `"}}]}`},
		{"truncated JSON", `{"statements":[{"statement":"CREATE (:Body776 {final: true})"`},
		{"oversized trailing whitespace", valid + strings.Repeat(" ", 512)},
	} {
		for _, route := range []string{"open", "execute", "commit", "implicit"} {
			t.Run(body.name+"/"+route, func(t *testing.T) {
				cleanup := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]interface{}{
					"statements": []map[string]interface{}{{"statement": "MATCH (n:Body776) DETACH DELETE n"}},
				}, token)
				require.Equal(t, http.StatusOK, cleanup.Code)
				endpoint := "/db/nornic/tx"
				if route == "execute" || route == "commit" {
					opened := makeRequest(t, server, http.MethodPost, endpoint, map[string]interface{}{
						"statements": []map[string]interface{}{{"statement": "CREATE (:Body776 {prior: true})"}},
					}, token)
					require.Equal(t, http.StatusCreated, opened.Code)
					var transaction TransactionResponse
					require.NoError(t, json.Unmarshal(opened.Body.Bytes(), &transaction))
					require.Empty(t, transaction.Errors)
					endpoint = strings.TrimSuffix(transaction.Commit, "/commit")
					t.Cleanup(func() {
						makeRequest(t, server, http.MethodDelete, strings.TrimSuffix(transaction.Commit, "/commit"), nil, token)
					})
				}
				if route == "commit" || route == "implicit" {
					endpoint += "/commit"
				}
				request := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(body.text))
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("Authorization", token)
				recorder := httptest.NewRecorder()
				server.buildRouter().ServeHTTP(recorder, request)
				var response TransactionResponse
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response), recorder.Body.String())
				require.Len(t, response.Errors, 1, recorder.Body.String())
				require.Equal(t, "Neo.ClientError.Request.InvalidFormat", response.Errors[0].Code)
				require.Empty(t, response.Results)
				statusCode := http.StatusBadRequest
				if body.name == "truncated JSON" && route != "open" {
					statusCode = http.StatusOK
				}
				require.Equal(t, statusCode, recorder.Code)
				if route == "commit" {
					attempt := makeRequest(t, server, http.MethodPost, endpoint, nil, token)
					require.Equal(t, http.StatusNotFound, attempt.Code)
				}
				stored := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]interface{}{
					"statements": []map[string]interface{}{{"statement": "MATCH (n:Body776) RETURN count(n)"}},
				}, token)
				require.Equal(t, int64(0), extractCountFromTxResponse(t, stored))
			})
		}
	}
}

func TestGh776_ReadTransactionRequestBoundaries(t *testing.T) {
	valid := `{"statements":[{"statement":"RETURN $value","parameters":{"value":9007199254740993}}]}`
	for _, testCase := range []struct {
		name    string
		body    string
		limit   int64
		wantErr bool
	}{
		{"exact limit", valid, int64(len(valid)), false},
		{"below limit", valid + " \n", int64(len(valid) + 2), false},
		{"second document", valid + `{}`, int64(len(valid) + 2), false},
		{"trailing text", valid + " xyz", int64(len(valid) + 4), false},
		{"truncated by limit", valid, int64(len(valid) - 1), true},
		{"chunked oversized suffix", valid + strings.Repeat(" ", 512), int64(len(valid)), true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			server := &Server{config: &Config{MaxRequestSize: testCase.limit}}
			request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(testCase.body))
			request.ContentLength = -1
			var decoded TransactionRequest
			err := server.readTransactionRequest(request, &decoded)
			if testCase.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, int64(9007199254740993), decoded.Statements[0].Parameters["value"])
		})
	}
	server := &Server{config: DefaultConfig()}
	for _, empty := range []string{"", " \n\t"} {
		var decoded TransactionRequest
		require.ErrorIs(t, server.readTransactionRequest(httptest.NewRequest(http.MethodPost, "/", strings.NewReader(empty)), &decoded), io.EOF)
	}
}

func TestGh776_DefaultBodyLimit(t *testing.T) {
	body := `{"statements":[{"statement":"RETURN 1 AS result","parameters":{"unused":"` + strings.Repeat("x", 11<<20) + `"}}]}`
	server := &Server{config: DefaultConfig()}
	require.Equal(t, int64(10<<20), server.config.MaxRequestSize)
	var decoded TransactionRequest
	err := server.readTransactionRequest(httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)), &decoded)
	var limitError *http.MaxBytesError
	require.ErrorAs(t, err, &limitError)
	require.Equal(t, int64(10<<20), limitError.Limit)
	server.config.MaxRequestSize = int64(len(body))
	require.NoError(t, server.readTransactionRequest(httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)), &decoded))
	require.Len(t, decoded.Statements[0].Parameters["unused"], 11<<20)
}

func TestGh776_HTTPMalformedCommitRollback(t *testing.T) {
	server, authenticator := setupTestServer(t)
	local := httptest.NewServer(server.buildRouter())
	defer local.Close()
	backends := []struct{ name, endpoint, token string }{{"nornicdb", local.URL + "/db/nornic/tx", "Bearer " + getAuthToken(t, authenticator, "admin")}}
	if reference := os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI"); reference != "" {
		backends = append(backends, struct{ name, endpoint, token string }{"neo4j", reference + "/db/neo4j/tx", ""})
	}
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			client := &http.Client{Timeout: 30 * time.Second}
			post := func(endpoint, body string) (TransactionResponse, int) {
				req, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(body))
				require.NoError(t, err)
				req.Header.Set("Content-Type", "application/json")
				if backend.token != "" {
					req.Header.Set("Authorization", backend.token)
				}
				response, err := client.Do(req)
				require.NoError(t, err)
				defer response.Body.Close()
				var result TransactionResponse
				require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
				return result, response.StatusCode
			}
			cleaned, _ := post(backend.endpoint+"/commit", `{"statements":[{"statement":"MATCH (n:Body776Reference) DETACH DELETE n"}]}`)
			require.Empty(t, cleaned.Errors)
			opened, code := post(backend.endpoint, `{"statements":[{"statement":"CREATE (:Body776Reference {prior:true})"}]}`)
			require.Equal(t, http.StatusCreated, code)
			require.Empty(t, opened.Errors)
			require.NotEmpty(t, opened.Commit)
			failed, code := post(opened.Commit, `{"statements":[{"statement":"CREATE (:Body776Reference {final:true})"`)
			require.Len(t, failed.Errors, 1)
			require.Equal(t, "Neo.ClientError.Request.InvalidFormat", failed.Errors[0].Code)
			require.Empty(t, failed.Results)
			require.Equal(t, http.StatusOK, code)
			stored, _ := post(backend.endpoint+"/commit", `{"statements":[{"statement":"MATCH (n:Body776Reference) RETURN count(n) AS count"}]}`)
			require.Empty(t, stored.Errors)
			require.Len(t, stored.Results, 1)
			require.Len(t, stored.Results[0].Data, 1)
			require.Equal(t, []interface{}{float64(0)}, stored.Results[0].Data[0].Row)
			evidence, err := json.Marshal(map[string]interface{}{"backend": backend.name, "status": code, "errors": failed.Errors, "rows": failed.Results, "durable_rows": stored.Results[0].Data[0].Row})
			require.NoError(t, err)
			t.Logf("ISSUE776_RESULT %s", evidence)
		})
	}
}

func TestGh776_OptionalEmptyBodies(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	for _, empty := range []string{"", " \n\t"} {
		t.Run(fmt.Sprintf("body=%q", empty), func(t *testing.T) {
			post := func(endpoint string) TransactionResponse {
				request := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(empty))
				request.Header.Set("Authorization", token)
				recorder := httptest.NewRecorder()
				server.buildRouter().ServeHTTP(recorder, request)
				require.Contains(t, []int{http.StatusCreated, http.StatusOK}, recorder.Code)
				var response TransactionResponse
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
				require.Empty(t, response.Errors)
				return response
			}
			opened := post("/db/nornic/tx")
			require.NotEmpty(t, opened.Commit)
			written := makeRequest(t, server, http.MethodPost, strings.TrimSuffix(opened.Commit, "/commit"), map[string]interface{}{
				"statements": []map[string]interface{}{{"statement": "CREATE (:EmptyBody776)"}},
			}, token)
			require.Equal(t, http.StatusOK, written.Code)
			post(opened.Commit)
		})
	}
	stored := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{{"statement": "MATCH (n:EmptyBody776) RETURN count(n)"}},
	}, token)
	require.Equal(t, int64(2), extractCountFromTxResponse(t, stored))
}

func TestGh809_HTTPTransactionIndexVisibility(t *testing.T) {
	server, authenticator := setupTestServer(t)
	local := httptest.NewServer(server.buildRouter())
	defer local.Close()
	backends := []struct{ name, endpoint, token string }{{"nornicdb", local.URL + "/db/nornic/tx", "Bearer " + getAuthToken(t, authenticator, "admin")}}
	if reference := os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI"); reference != "" {
		backends = append(backends, struct{ name, endpoint, token string }{"neo4j", reference + "/db/neo4j/tx", ""})
	}
	rows := make([]interface{}, 0, 48)
	ids := make([]interface{}, 0, 16)
	for document := 0; document < 16; document++ {
		documentID := fmt.Sprintf("doc-%d", document)
		ids = append(ids, documentID)
		for _, kind := range []string{"document", "version", "origin"} {
			rows = append(rows, map[string]interface{}{"properties": map[string]interface{}{
				"id": documentID + "-" + kind, "document_id": documentID, "kind": kind,
				"revision": int64(1), "body": "body", "created_at": "created", "updated_at": "updated",
			}})
		}
	}
	write := map[string]interface{}{"statement": "UNWIND $rows AS row CREATE (n:GH809) SET n = row.properties RETURN n.id AS id", "parameters": map[string]interface{}{"rows": rows}}
	read := map[string]interface{}{"statement": "MATCH (n:GH809) WHERE n.document_id IN $ids AND n.kind IN $kinds RETURN n.id AS id,n.kind AS kind,n.revision AS revision,n.body AS body,n.created_at AS created_at,n.updated_at AS updated_at ORDER BY id", "parameters": map[string]interface{}{"ids": ids, "kinds": []interface{}{"version", "origin", "unused"}}}
	observed := make(map[string][][]interface{})
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			client := &http.Client{Timeout: 30 * time.Second}
			request := func(method, endpoint string, statements ...map[string]interface{}) TransactionResponse {
				if statements == nil {
					statements = []map[string]interface{}{}
				}
				payload, err := json.Marshal(map[string]interface{}{"statements": statements})
				require.NoError(t, err)
				req, err := http.NewRequest(method, endpoint, bytes.NewReader(payload))
				require.NoError(t, err)
				req.Header.Set("Content-Type", "application/json")
				if backend.token != "" {
					req.Header.Set("Authorization", backend.token)
				}
				response, err := client.Do(req)
				require.NoError(t, err)
				defer response.Body.Close()
				require.Contains(t, []int{http.StatusOK, http.StatusCreated}, response.StatusCode)
				var result TransactionResponse
				require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
				require.Empty(t, result.Errors)
				return result
			}
			for _, property := range []string{"", "document_id", "kind"} {
				for _, mode := range []string{"single request", "separate requests", "committed"} {
					t.Run("index="+property+"/"+mode, func(t *testing.T) {
						request(http.MethodPost, backend.endpoint+"/commit", map[string]interface{}{"statement": "MATCH (n:GH809) DETACH DELETE n"})
						for _, indexed := range []string{"document_id", "kind"} {
							request(http.MethodPost, backend.endpoint+"/commit", map[string]interface{}{"statement": "DROP INDEX gh809_" + indexed + " IF EXISTS"})
						}
						if property != "" {
							request(http.MethodPost, backend.endpoint+"/commit", map[string]interface{}{"statement": "CREATE INDEX gh809_" + property + " FOR (n:GH809) ON (n." + property + ")"})
							if backend.name == "neo4j" {
								request(http.MethodPost, backend.endpoint+"/commit", map[string]interface{}{"statement": "CALL db.awaitIndexes(30)"})
							}
						}
						statements := []map[string]interface{}{write}
						if mode == "single request" {
							statements = append(statements, read)
						}
						opened := request(http.MethodPost, backend.endpoint, statements...)
						require.Len(t, opened.Results[0].Data, 48)
						require.NotEmpty(t, opened.Commit)
						closed := false
						t.Cleanup(func() {
							if !closed {
								request(http.MethodDelete, strings.TrimSuffix(opened.Commit, "/commit"))
							}
						})
						result := opened
						if mode == "separate requests" {
							result = request(http.MethodPost, strings.TrimSuffix(opened.Commit, "/commit"), read)
						} else if mode == "committed" {
							request(http.MethodPost, opened.Commit)
							closed = true
							result = request(http.MethodPost, backend.endpoint+"/commit", read)
						}
						selected := result.Results[len(result.Results)-1]
						require.Len(t, selected.Data, 32)
						actual := make([][]interface{}, 0, 32)
						for _, data := range selected.Data {
							actual = append(actual, data.Row)
							for _, meta := range data.Meta {
								require.Nil(t, meta)
							}
						}
						key := property + "/" + mode
						if backend.name == "nornicdb" {
							observed[key] = actual
						} else {
							require.Equal(t, observed[key], actual)
						}
						evidence, err := json.Marshal(map[string]interface{}{"backend": backend.name, "route": "http", "index": property, "mode": mode, "columns": selected.Columns, "rows": actual})
						require.NoError(t, err)
						t.Logf("ISSUE809_RESULT %s", evidence)
						if mode != "committed" {
							request(http.MethodDelete, strings.TrimSuffix(opened.Commit, "/commit"))
							closed = true
						}
					})
				}
			}
		})
	}
}

func TestGh810_HTTPNullPropertyMaps(t *testing.T) {
	server, authenticator := setupTestServer(t)
	local := httptest.NewServer(server.buildRouter())
	defer local.Close()
	backends := []struct{ name, endpoint, token string }{{"nornicdb", local.URL + "/db/nornic/tx", "Bearer " + getAuthToken(t, authenticator, "admin")}}
	if reference := os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI"); reference != "" {
		backends = append(backends, struct{ name, endpoint, token string }{"neo4j", reference + "/db/neo4j/tx", ""})
	}
	observed := make(map[string][][]interface{})
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			client := &http.Client{Timeout: 30 * time.Second}
			post := func(endpoint string, statements ...map[string]interface{}) TransactionResponse {
				if statements == nil {
					statements = []map[string]interface{}{}
				}
				payload, err := json.Marshal(map[string]interface{}{"statements": statements})
				require.NoError(t, err)
				request, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(payload))
				require.NoError(t, err)
				request.Header.Set("Content-Type", "application/json")
				if backend.token != "" {
					request.Header.Set("Authorization", backend.token)
				}
				response, err := client.Do(request)
				require.NoError(t, err)
				defer response.Body.Close()
				require.Contains(t, []int{http.StatusOK, http.StatusCreated}, response.StatusCode)
				var result TransactionResponse
				require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
				require.Empty(t, result.Errors)
				return result
			}
			for _, schema := range []string{"", "CREATE INDEX gh810_ix FOR (n:PDRecord) ON (n.id)", "CREATE CONSTRAINT gh810_uq FOR (n:PDRecord) REQUIRE n.id IS UNIQUE"} {
				t.Run("schema="+schema, func(t *testing.T) {
					for _, query := range []string{"MATCH (n:PDRecord) DETACH DELETE n", "DROP CONSTRAINT gh810_uq IF EXISTS", "DROP INDEX gh810_ix IF EXISTS", "CREATE (a:PDRecord {id:'r1',kind:'a'})-[:R]->(b:PDRecord {id:'r2',kind:'b'}), (:PDRecord {id:'r3',kind:'a'}), (:PDRecord {kind:'noid'})"} {
						post(backend.endpoint+"/commit", map[string]interface{}{"statement": query})
					}
					if schema != "" {
						post(backend.endpoint+"/commit", map[string]interface{}{"statement": schema})
						if backend.name == "neo4j" {
							post(backend.endpoint+"/commit", map[string]interface{}{"statement": "CALL db.awaitIndexes(30)"})
						}
					}
					opened := post(backend.endpoint)
					require.NotEmpty(t, opened.Commit)
					endpoint := strings.TrimSuffix(opened.Commit, "/commit")
					for _, query := range []string{
						"MATCH (n:PDRecord {id:$id}) RETURN n.id",
						"MATCH (n:PDRecord {id:null}) RETURN n.id",
						"MATCH (a:PDRecord {id:$id})-[:R]->(b) RETURN b.id",
						"MATCH (n:PDRecord {id:$id}) SET n.touched=true RETURN count(n)",
						"MATCH (n:PDRecord {id:$id}) DETACH DELETE n",
						"MATCH (n {id:$id}) DETACH DELETE n RETURN count(*)",
					} {
						result := post(endpoint, map[string]interface{}{"statement": query, "parameters": map[string]interface{}{"id": nil}})
						require.Len(t, result.Results, 1)
						actual := make([][]interface{}, 0, len(result.Results[0].Data))
						for _, row := range result.Results[0].Data {
							actual = append(actual, row.Row)
						}
						if strings.Contains(query, "count(") {
							require.Equal(t, [][]interface{}{{float64(0)}}, actual)
						} else {
							require.Empty(t, actual)
						}
						key := schema + "/" + query
						if backend.name == "nornicdb" {
							observed[key] = actual
						} else {
							require.Equal(t, observed[key], actual)
						}
						evidence, err := json.Marshal(map[string]interface{}{"backend": backend.name, "route": "http/explicit", "schema": schema, "query": query, "parameters": map[string]interface{}{"id": nil}, "rows": actual})
						require.NoError(t, err)
						t.Logf("ISSUE810_RESULT %s", evidence)
					}
					post(opened.Commit)
					stored := post(backend.endpoint+"/commit", map[string]interface{}{"statement": "MATCH (n:PDRecord) RETURN n.id,n.kind,n.touched ORDER BY n.kind,n.id"})
					require.Len(t, stored.Results[0].Data, 4)
					for _, row := range stored.Results[0].Data {
						require.Nil(t, row.Row[2])
					}
					edges := post(backend.endpoint+"/commit", map[string]interface{}{"statement": "MATCH ()-[r:R]->() RETURN count(r)"})
					require.Equal(t, []interface{}{float64(1)}, edges.Results[0].Data[0].Row)
				})
			}
		})
	}
}

func TestTransactionHTTPValueBoundaries(t *testing.T) {
	server := &Server{}
	node := &storage.Node{ID: "node"}
	edge := &storage.Edge{ID: "edge"}
	path := cypher.PathResult{Nodes: []*storage.Node{node, node}, Relationships: []*storage.Edge{edge}}
	for _, testCase := range []struct {
		name      string
		value     interface{}
		row       interface{}
		metaCount int
	}{
		{"nil", nil, nil, 1},
		{"nil node", (*storage.Node)(nil), nil, 1},
		{"nil edge", (*storage.Edge)(nil), nil, 1},
		{"nil path", (*cypher.PathResult)(nil), nil, 1},
		{"nil duration", (*cypher.CypherDuration)(nil), nil, 1},
		{"empty node properties", node, map[string]interface{}{}, 1},
		{"empty edge properties", edge, map[string]interface{}{}, 1},
		{"typed nodes", []*storage.Node{node}, []interface{}{map[string]interface{}{}}, 1},
		{"array", [2]int{1, 2}, []interface{}{1, 2}, 2},
		{"path pointer", &path, []interface{}{map[string]interface{}{}, map[string]interface{}{}, map[string]interface{}{}}, 1},
		{"path marker", map[string]interface{}{"_pathResult": &path}, []interface{}{map[string]interface{}{}, map[string]interface{}{}, map[string]interface{}{}}, 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			row, metadata := server.transactionHTTPValue(testCase.value, "nornic")
			require.Equal(t, testCase.row, row)
			require.Len(t, metadata, testCase.metaCount)
		})
	}
}

func TestGh668_HTTPFloatTokens(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	for _, endpoint := range []string{"/db/nornic/tx/commit", "/db/nornic/tx"} {
		t.Run(endpoint, func(t *testing.T) {
			response := makeRequest(t, server, http.MethodPost, endpoint, map[string]any{
				"statements": []map[string]any{{"statement": "RETURN 5.0, toFloat(5), 1.5"}},
			}, token)
			var result TransactionResponse
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result), response.Body.String())
			require.Empty(t, result.Errors)
			require.Contains(t, response.Body.String(), `"row":[5.0,5.0,1.5]`)
			if result.Commit != "" {
				rollback := makeRequest(t, server, http.MethodDelete, strings.TrimSuffix(result.Commit, "/commit"), nil, token)
				require.Equal(t, http.StatusOK, rollback.Code)
			}
		})
	}
}

func TestGh668_HTTPNonfiniteRecursiveProperties(t *testing.T) {
	server := &Server{}
	values := []interface{}{math.Inf(1), math.Inf(-1), math.NaN(), float32(5), float64(5)}
	properties := map[string]interface{}{"values": values, "nested": map[string]interface{}{"values": values}}
	node := &storage.Node{ID: "n", Properties: properties}
	edge := &storage.Edge{ID: "r", StartNode: "n", EndNode: "n", Properties: properties}
	path := cypher.PathResult{Nodes: []*storage.Node{node, node}, Relationships: []*storage.Edge{edge}}
	for _, value := range []interface{}{values, properties, node, edge, path} {
		response := TransactionResponse{}
		server.appendStatementResult(&response, &cypher.ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{{value}}}, "nornic", false, []string{"row", "graph"})
		encoded, err := json.Marshal(response)
		require.NoError(t, err)
		require.True(t, json.Valid(encoded))
		require.Contains(t, string(encoded), `["Infinity","-Infinity","NaN",5.0,5.0]`)
	}
}

func TestGh668_HTTPNonfiniteEndpoints(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	values := []interface{}{"Infinity", "-Infinity", "NaN"}
	properties := map[string]interface{}{"values": values}
	for _, endpoint := range []string{"/db/nornic/tx/commit", "/db/nornic/tx"} {
		for _, query := range []string{
			"WITH [toFloat('Infinity'),toFloat('-Infinity'),toFloat('NaN')] AS values RETURN values, {values:values}",
			"WITH [toFloat('Infinity'),toFloat('-Infinity'),toFloat('NaN')] AS values CREATE p=(a:NonfiniteHTTP {values:values})-[r:NONFINITE_HTTP {values:values}]->(b:NonfiniteHTTP {values:values}) RETURN values, {values:values}, a, r, [a,r], {node:a,edge:r}, p",
		} {
			t.Run(endpoint+query, func(t *testing.T) {
				recorder := makeRequest(t, server, http.MethodPost, endpoint, map[string]any{
					"statements": []map[string]any{{"statement": query, "resultDataContents": []string{"row", "graph"}}},
				}, token)
				var response TransactionResponse
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response), recorder.Body.String())
				require.Empty(t, response.Errors)
				require.Len(t, response.Results[0].Data, 1)
				data := response.Results[0].Data[0]
				if !strings.Contains(query, "CREATE") {
					require.Equal(t, []interface{}{values, properties}, data.Row)
					require.Len(t, data.Meta, 6)
					require.Empty(t, data.Graph.Nodes)
					require.Empty(t, data.Graph.Relationships)
				} else {
					require.Equal(t, []interface{}{values, properties, properties, properties, []interface{}{properties, properties}, map[string]interface{}{"node": properties, "edge": properties}, []interface{}{properties, properties, properties}}, data.Row)
					require.Len(t, data.Meta, 13)
					require.Len(t, data.Graph.Nodes, 2)
					require.Len(t, data.Graph.Relationships, 1)
					for _, node := range data.Graph.Nodes {
						require.Equal(t, properties, node.Properties)
					}
					require.Equal(t, properties, data.Graph.Relationships[0].Properties)
				}
				if response.Commit != "" {
					rollback := makeRequest(t, server, http.MethodDelete, strings.TrimSuffix(response.Commit, "/commit"), nil, token)
					require.Equal(t, http.StatusOK, rollback.Code)
				}
			})
		}
	}
}

func TestGh668_HTTPRecursiveTemporalEntities(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	for _, endpoint := range []string{"/db/nornic/tx/commit", "/db/nornic/tx"} {
		for _, testCase := range []struct{ expression, text string }{
			{"date('2020-12-31')", "2020-12-31"},
			{"duration('P1Y2M')", "P1Y2M"},
			{"localtime('10:00:05')", "10:00:05"},
			{"time('10:00:00+01:00')", "10:00+01:00"},
			{"localdatetime('2020-01-01T10:00:00')", "2020-01-01T10:00"},
			{"datetime('2020-01-01T10:00:00Z')", "2020-01-01T10:00Z"},
		} {
			t.Run(endpoint+testCase.expression, func(t *testing.T) {
				query := "WITH " + testCase.expression + " AS value CREATE p=(a:TemporalHTTP {v:value,nested:[value]})-[r:TEMPORAL_HTTP {v:value,nested:[value]}]->(b:TemporalHTTP {v:value,nested:[value]}) RETURN value, [value], {nested:value}, a, r, [a,r], {node:a,edge:r}, p"
				recorder := makeRequest(t, server, http.MethodPost, endpoint, map[string]any{
					"statements": []map[string]any{{"statement": query, "resultDataContents": []string{"row", "graph"}}},
				}, token)
				var response TransactionResponse
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response), recorder.Body.String())
				require.Empty(t, response.Errors)
				require.Len(t, response.Results[0].Data, 1)
				data := response.Results[0].Data[0]
				properties := map[string]interface{}{"v": testCase.text, "nested": []interface{}{testCase.text}}
				require.Equal(t, []interface{}{testCase.text, []interface{}{testCase.text}, map[string]interface{}{"nested": testCase.text}, properties, properties, []interface{}{properties, properties}, map[string]interface{}{"node": properties, "edge": properties}, []interface{}{properties, properties, properties}}, data.Row)
				require.Len(t, data.Meta, 10)
				require.Equal(t, []interface{}{nil, nil, nil}, data.Meta[:3])
				require.Len(t, data.Meta[9], 3)
				require.Len(t, data.Graph.Nodes, 2)
				require.Len(t, data.Graph.Relationships, 1)
				for _, node := range data.Graph.Nodes {
					require.Equal(t, properties, node.Properties)
				}
				require.Equal(t, properties, data.Graph.Relationships[0].Properties)
				require.NotContains(t, recorder.Body.String(), `"Time"`)
				require.NotContains(t, recorder.Body.String(), `"Months"`)
				if response.Commit != "" {
					rollback := makeRequest(t, server, http.MethodDelete, strings.TrimSuffix(response.Commit, "/commit"), nil, token)
					require.Equal(t, http.StatusOK, rollback.Code)
				}
			})
		}
	}
}

func TestGh738_HTTPStatementAdmission(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	for _, endpoint := range []string{"/db/nornic/tx/commit", "/db/nornic/tx"} {
		for _, testCase := range []struct{ query, code string }{
			{"USE system CREATE (:U717 {v:2})", "Neo.ClientError.Statement.SemanticError"},
			{"USE system MATCH (n) RETURN count(n) AS c", "Neo.ClientError.Statement.SemanticError"},
			{"USE system OPTIONAL MATCH (n) RETURN n", "Neo.ClientError.Statement.SemanticError"},
			{"USE system CALL { MATCH (n) RETURN n } RETURN n", "Neo.ClientError.Statement.SemanticError"},
			{"USE nosuchdb RETURN 1", "Neo.ClientError.Database.DatabaseNotFound"},
		} {
			t.Run(endpoint+testCase.query, func(t *testing.T) {
				response := makeRequest(t, server, http.MethodPost, endpoint, map[string]any{"statements": []map[string]any{{"statement": testCase.query}}}, token)
				expectedStatus := http.StatusOK
				if endpoint == "/db/nornic/tx" {
					expectedStatus = http.StatusCreated
				}
				require.Equal(t, expectedStatus, response.Code)
				var result TransactionResponse
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
				require.Len(t, result.Errors, 1)
				require.Equal(t, testCase.code, result.Errors[0].Code)
			})
		}
	}
	response := makeRequest(t, server, http.MethodPost, "/db/nosuchdb/tx/commit", map[string]any{"statements": []map[string]any{{"statement": "RETURN 1"}}}, token)
	require.Equal(t, http.StatusNotFound, response.Code)
	store, err := server.dbManager.GetStorage("system")
	require.NoError(t, err)
	nodes, err := store.GetNodesByLabel("U717")
	require.NoError(t, err)
	require.Empty(t, nodes)
}

func TestGh668_HTTPTemporalText(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	for _, endpoint := range []string{"/db/nornic/tx/commit", "/db/nornic/tx"} {
		for _, testCase := range []struct{ expression, text string }{
			{"date('2020-12-31')", "2020-12-31"},
			{"duration('P1Y2M')", "P1Y2M"},
			{"localtime('10:00:05')", "10:00:05"},
			{"time('10:00:00+01:00')", "10:00+01:00"},
			{"localdatetime('2020-01-01T10:00:00')", "2020-01-01T10:00"},
			{"datetime('2020-01-01T10:00:00Z')", "2020-01-01T10:00Z"},
		} {
			t.Run(endpoint+testCase.expression, func(t *testing.T) {
				query := "WITH " + testCase.expression + " AS value RETURN value, [value], {nested:value}"
				response := makeRequest(t, server, http.MethodPost, endpoint, map[string]any{"statements": []map[string]any{{"statement": query}}}, token)
				var result TransactionResponse
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
				require.Empty(t, result.Errors)
				require.Len(t, result.Results, 1)
				require.Len(t, result.Results[0].Data, 1)
				if result.Commit != "" {
					commitPath := result.Commit[strings.Index(result.Commit, "/db/"):]
					commit := makeRequest(t, server, http.MethodPost, commitPath, map[string]any{"statements": []any{}}, token)
					var committed TransactionResponse
					require.NoError(t, json.Unmarshal(commit.Body.Bytes(), &committed))
					require.Empty(t, committed.Errors)
				}
				require.Equal(t, []interface{}{testCase.text, []interface{}{testCase.text}, map[string]interface{}{"nested": testCase.text}}, result.Results[0].Data[0].Row)
			})
		}
	}
}

func TestGh668_HTTPTemporalEntityProperties(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	for _, endpoint := range []string{"/db/nornic/tx/commit", "/db/nornic/tx"} {
		for _, testCase := range []struct {
			query string
			want  interface{}
		}{
			{"CREATE (n:D {d: date('2020-01-02')}) RETURN n", map[string]interface{}{"d": "2020-01-02"}},
			{"CREATE ()-[r:R {d: date('2020-01-02')}]->() RETURN r", map[string]interface{}{"d": "2020-01-02"}},
			{"CREATE (n:D {t: datetime('2020-01-02T03:04:05Z')}) RETURN [n] AS l", []interface{}{map[string]interface{}{"t": "2020-01-02T03:04:05Z"}}},
			{"CREATE (n:D {d: [date('2020-01-02')]}) RETURN n", map[string]interface{}{"d": []interface{}{"2020-01-02"}}},
		} {
			t.Run(endpoint+testCase.query, func(t *testing.T) {
				response := makeRequest(t, server, http.MethodPost, endpoint, map[string]any{"statements": []map[string]any{{"statement": testCase.query, "resultDataContents": []string{"row", "graph"}}}}, token)
				var result TransactionResponse
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
				require.Empty(t, result.Errors)
				require.Equal(t, []interface{}{testCase.want}, result.Results[0].Data[0].Row)
				require.Len(t, result.Results[0].Data[0].Meta, 1)
				require.NotContains(t, response.Body.String(), `"Time"`)
				if result.Commit != "" {
					commitPath := result.Commit[strings.Index(result.Commit, "/db/"):]
					commit := makeRequest(t, server, http.MethodPost, commitPath, map[string]any{"statements": []any{}}, token)
					var committed TransactionResponse
					require.NoError(t, json.Unmarshal(commit.Body.Bytes(), &committed))
					require.Empty(t, committed.Errors)
				}
			})
		}
	}
}

func TestTransactionHTTPOrderedMapAndGraphState(t *testing.T) {
	for _, testCase := range []struct {
		value     transactionHTTPOrderedMap
		want      string
		wantError bool
	}{
		{transactionHTTPOrderedMap{}, `{}`, false},
		{transactionHTTPOrderedMap{keys: []string{"node", "k"}, values: map[string]interface{}{"node": 1, "k": 2}}, `{"node":1,"k":2}`, false},
		{transactionHTTPOrderedMap{keys: []string{"bad"}, values: map[string]interface{}{"bad": make(chan int)}}, "", true},
	} {
		encoded, err := testCase.value.MarshalJSON()
		if testCase.wantError {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, testCase.want, string(encoded))
		}
	}
	state := &transactionHTTPValueState{graph: &GraphResult{}}
	for _, identity := range []string{"4:a:n", "4:a:n", "4:b:n"} {
		state.addNode(GraphNode{ElementID: identity})
	}
	for _, identity := range []string{"5:a:r", "5:a:r", "5:b:r"} {
		state.addEdge(GraphRelationship{ElementID: identity})
	}
	require.Len(t, state.graph.Nodes, 2)
	require.Len(t, state.graph.Relationships, 2)
}

func TestHTTPTransactionEntityRows(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	response := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{
		"statements": []map[string]any{{"statement": "CREATE (a:Person {name:'Alice'})-[r:KNOWS {since:2020}]->(b:Person {name:'Bob'}) RETURN a, r, [a, r], {entity:a}, {elementId:'user', labels:['custom'], properties:{value:1}}, null"}},
	}, token)
	var result TransactionResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
	require.Empty(t, result.Errors)
	data := result.Results[0].Data[0]
	node := map[string]interface{}{"name": "Alice"}
	edge := map[string]interface{}{"since": float64(2020)}
	require.Equal(t, []interface{}{node, edge, []interface{}{node, edge}, map[string]interface{}{"entity": node}, map[string]interface{}{"elementId": "user", "labels": []interface{}{"custom"}, "properties": map[string]interface{}{"value": float64(1)}}, nil}, data.Row)
	require.Equal(t, "node", data.Meta[0].(map[string]interface{})["type"])
	require.Equal(t, "relationship", data.Meta[1].(map[string]interface{})["type"])
}

func TestRemoteHTTPGraphRoundTrip(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	temporal := ", d:date('2020-12-31'), du:duration('P1Y2M'), lt:localtime('10:00:05'), t:time('10:00:00+01:00'), ldt:localdatetime('2020-01-01T10:00:00'), dt:datetime('2020-01-01T10:00:00Z')"
	wantTemporal := map[string]interface{}{"d": "2020-12-31", "du": "P1Y2M", "lt": "10:00:05", "t": "10:00+01:00", "ldt": "2020-01-01T10:00", "dt": "2020-01-01T10:00Z"}
	response := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{
		"statements": []map[string]any{{"statement": "CREATE (:RP {name:'x', v:1" + temporal + "})-[:LINK {weight:2" + temporal + "}]->(:Target {" + strings.TrimPrefix(temporal, ", ") + "})"}},
	}, token)
	var result TransactionResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
	require.Empty(t, result.Errors)
	host := httptest.NewServer(server.buildRouter())
	defer host.Close()
	remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: host.URL, Database: "nornic", AuthToken: token})
	require.NoError(t, err)
	defer remote.Close()
	nodes, err := remote.GetNodesByLabel("RP")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.NotEmpty(t, nodes[0].ID)
	require.Equal(t, []string{"RP"}, nodes[0].Labels)
	require.Equal(t, "x", nodes[0].Properties["name"])
	require.Equal(t, float64(1), nodes[0].Properties["v"])
	for key, expected := range wantTemporal {
		require.Equal(t, expected, nodes[0].Properties[key])
	}
	edges, err := remote.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 1)
	require.NotEmpty(t, edges[0].ID)
	require.Equal(t, "LINK", edges[0].Type)
	require.Equal(t, nodes[0].ID, edges[0].StartNode)
	require.NotEmpty(t, edges[0].EndNode)
	require.Equal(t, float64(2), edges[0].Properties["weight"])
	for key, expected := range wantTemporal {
		require.Equal(t, expected, edges[0].Properties[key])
	}
	transaction, err := remote.BeginCypherTx(context.Background())
	require.NoError(t, err)
	defer transaction.Rollback(context.Background())
	_, rows, err := transaction.QueryCypher(context.Background(), "MATCH (n:RP) RETURN n", nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "x", rows[0][0].(map[string]interface{})["properties"].(map[string]interface{})["name"])
	for _, query := range []string{
		"MATCH (n:RP) RETURN {node:n} AS m, n",
		"MATCH (n:RP) RETURN {node:n,k:1} AS m, n, [n,1] AS mixed",
	} {
		_, rows, err := transaction.QueryCypher(context.Background(), query, nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		ordinary := rows[0][0].(map[string]interface{})
		require.Contains(t, ordinary, "node", query)
		require.Contains(t, ordinary["node"], "properties", query)
		require.Contains(t, rows[0][1], "properties", query)
	}
	query := "MATCH p=(a:RP)-[r:LINK]->(b:Target) RETURN a,r,b,[a,r],{node:a,edge:r},p,elementId(a),elementId(r),elementId(b),elementId(startNode(r)),elementId(endNode(r))"
	for _, mode := range []struct {
		name  string
		query func(context.Context, string, map[string]interface{}) ([]string, [][]interface{}, error)
	}{
		{"autocommit", remote.QueryCypher},
		{"explicit", transaction.QueryCypher},
	} {
		t.Run(mode.name, func(t *testing.T) {
			columns, rows, err := mode.query(context.Background(), query, nil)
			require.NoError(t, err)
			require.Len(t, columns, 11)
			require.Len(t, rows, 1)
			row := rows[0]
			require.Len(t, row, 11)
			for index := 0; index < 3; index++ {
				entity := row[index].(map[string]interface{})
				require.Equal(t, row[6+index], entity["elementId"])
				properties := entity["properties"].(map[string]interface{})
				for key, expected := range wantTemporal {
					require.Equal(t, expected, properties[key])
				}
			}
			require.Equal(t, string(nodes[0].ID), row[6])
			require.Equal(t, string(edges[0].ID), row[7])
			require.Equal(t, string(edges[0].StartNode), row[9])
			require.Equal(t, string(edges[0].EndNode), row[10])
			relationship := row[1].(map[string]interface{})
			require.Equal(t, row[9], relationship["startNodeElementId"])
			require.Equal(t, row[10], relationship["endNodeElementId"])
			require.Equal(t, []interface{}{row[0], row[1]}, row[3])
			require.Equal(t, map[string]interface{}{"node": row[0], "edge": row[1]}, row[4])
			require.Equal(t, []interface{}{row[0], row[1], row[2]}, row[5])
		})
	}
}

func TestRemoteHTTPPinnedNeo4j(t *testing.T) {
	endpoint := os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI")
	if endpoint == "" {
		t.Skip("set pinned reference HTTP URI to validate remote transport")
	}
	response, err := http.Post(endpoint+"/db/neo4j/tx/commit", "application/json", strings.NewReader(`{"statements":[{"statement":"MATCH (n:RemoteHTTPParity) DETACH DELETE n"},{"statement":"CREATE (:RemoteHTTPParity:RemoteHTTPParitySource {name:'x',v:1})-[:REMOTE_HTTP_PARITY {weight:2}]->(:RemoteHTTPParity)"}]}`))
	require.NoError(t, err)
	defer response.Body.Close()
	var setup TransactionResponse
	require.NoError(t, json.NewDecoder(response.Body).Decode(&setup))
	require.Empty(t, setup.Errors)
	remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: endpoint, Database: "neo4j"})
	require.NoError(t, err)
	defer remote.Close()
	nodes, err := remote.GetNodesByLabel("RemoteHTTPParitySource")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.NotEmpty(t, nodes[0].ID)
	require.Contains(t, nodes[0].Labels, "RemoteHTTPParitySource")
	require.Equal(t, "x", nodes[0].Properties["name"])
	edges, err := remote.GetEdgesByType("REMOTE_HTTP_PARITY")
	require.NoError(t, err)
	require.Len(t, edges, 1)
	require.NotEmpty(t, edges[0].ID)
	require.Equal(t, nodes[0].ID, edges[0].StartNode)
	require.NotEmpty(t, edges[0].EndNode)
	transaction, err := remote.BeginCypherTx(context.Background())
	require.NoError(t, err)
	defer transaction.Rollback(context.Background())
	_, rows, err := transaction.QueryCypher(context.Background(), "MATCH (n:RemoteHTTPParitySource) RETURN {node:n,k:1} AS m, n, [n,1] AS mixed", nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Contains(t, rows[0][0].(map[string]interface{})["node"], "properties")
	require.Contains(t, rows[0][1], "properties")
}

func TestHTTPMapMetadataRetainsValueOrder(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	for _, query := range []string{
		"CREATE (n:H {a:1}) RETURN {node:n,k:1} AS m",
		"CREATE (n:H {a:1}) WITH {node:n,k:1} AS m RETURN m",
		"CREATE (n:H {a:1}) RETURN [{node:n,k:1}] AS m",
	} {
		t.Run(query, func(t *testing.T) {
			response := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{{"statement": query}}}, token)
			var result TransactionResponse
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
			require.Empty(t, result.Errors)
			metadata := result.Results[0].Data[0].Meta
			require.Len(t, metadata, 2)
			require.IsType(t, map[string]interface{}{}, metadata[0], "%s", response.Body.String())
			require.Equal(t, "node", metadata[0].(map[string]interface{})["type"])
			require.Nil(t, metadata[1])
			require.Contains(t, response.Body.String(), `"node":{"a":1},"k":1`)
		})
	}
}

func resetHTTPDifferentialBackend(post func(string) TransactionResponse) error {
	execute := func(statement string) (TransactionResponse, error) {
		response := post(statement)
		if len(response.Errors) != 0 {
			return response, fmt.Errorf("%s: %v", statement, response.Errors)
		}
		return response, nil
	}
	if _, err := execute("MATCH (n) DETACH DELETE n"); err != nil {
		return err
	}
	for _, schema := range []struct{ show, drop string }{
		{"SHOW CONSTRAINTS", "DROP CONSTRAINT"},
		{"SHOW INDEXES", "DROP INDEX"},
	} {
		response, err := execute(schema.show)
		if err != nil {
			return err
		}
		if len(response.Results) != 1 {
			return fmt.Errorf("%s returned %d results, want one", schema.show, len(response.Results))
		}
		result := response.Results[0]
		nameIndex := -1
		for index, column := range result.Columns {
			if column == "name" {
				nameIndex = index
				break
			}
		}
		if nameIndex < 0 {
			return fmt.Errorf("%s did not return column name", schema.show)
		}
		for _, data := range result.Data {
			if nameIndex >= len(data.Row) {
				return fmt.Errorf("%s returned a row without column name", schema.show)
			}
			name, ok := data.Row[nameIndex].(string)
			if !ok || strings.TrimSpace(name) == "" {
				return fmt.Errorf("%s returned invalid schema object name %v", schema.show, data.Row[nameIndex])
			}
			if _, err := execute(schema.drop + " `" + strings.ReplaceAll(name, "`", "``") + "` IF EXISTS"); err != nil {
				return err
			}
		}
	}
	for _, statement := range []string{
		"CREATE LOOKUP INDEX differential_node_labels IF NOT EXISTS FOR (n) ON EACH labels(n)",
		"CREATE LOOKUP INDEX differential_relationship_types IF NOT EXISTS FOR ()-[r]-() ON EACH type(r)",
		"CALL db.clearQueryCaches()",
	} {
		if _, err := execute(statement); err != nil {
			return err
		}
	}
	return nil
}

func TestHTTPDifferentialResetClearsSchemaArtifacts(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	backends := map[string]string{"nornicdb": ""}
	if referenceURL := os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI"); referenceURL != "" {
		backends["neo4j"] = referenceURL + "/db/neo4j/tx/commit"
	}
	for name, endpoint := range backends {
		t.Run(name, func(t *testing.T) {
			post := func(statement string) TransactionResponse {
				if endpoint != "" {
					payload, err := json.Marshal(map[string]any{"statements": []map[string]any{{"statement": statement}}})
					require.NoError(t, err)
					response, err := (&http.Client{Timeout: 15 * time.Second}).Post(endpoint, "application/json", bytes.NewReader(payload))
					require.NoError(t, err)
					defer response.Body.Close()
					require.Equal(t, http.StatusOK, response.StatusCode)
					var result TransactionResponse
					require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
					return result
				}
				response := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{
					"statements": []map[string]any{{"statement": statement}},
				}, token)
				var result TransactionResponse
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
				return result
			}
			require.NoError(t, resetHTTPDifferentialBackend(post))
			for iteration := 0; iteration < 2; iteration++ {
				for _, statement := range []string{
					"CREATE CONSTRAINT reset_unique FOR (n:ResetProbe) REQUIRE n.id IS UNIQUE",
					"CREATE INDEX reset_value FOR (n:ResetProbe) ON (n.value)",
					"CREATE (:ResetProbe {id:1, value:2})",
				} {
					require.Empty(t, post(statement).Errors, statement)
				}
				require.NoError(t, resetHTTPDifferentialBackend(post))
				constraints := post("SHOW CONSTRAINTS")
				require.Empty(t, constraints.Errors)
				require.Len(t, constraints.Results, 1)
				require.Empty(t, constraints.Results[0].Data)
				graph := post("MATCH (n) RETURN count(n)")
				require.Empty(t, graph.Errors)
				require.Equal(t, []interface{}{float64(0)}, graph.Results[0].Data[0].Row)
				indexes := post("SHOW INDEXES")
				require.Empty(t, indexes.Errors)
				require.Len(t, indexes.Results, 1)
				require.Len(t, indexes.Results[0].Data, 2)
				for _, data := range indexes.Results[0].Data {
					for index, column := range indexes.Results[0].Columns {
						if column == "type" {
							require.Equal(t, "LOOKUP", data.Row[index])
						}
					}
				}
			}
		})
	}
}

func TestHTTPDifferentialResetTypedRows(t *testing.T) {
	for _, testCase := range []struct {
		name, constraints, indexes, wantError string
	}{
		{"quoted names and reordered columns", `{"results":[{"columns":["id","name"],"data":[{"row":[1,"constraint\u0060 name"]}]}]}`, `{"results":[{"columns":["name","id"],"data":[{"row":["index\u0060 name",2]}]}]}`, ""},
		{"missing result", `{}`, "", "returned 0 results"},
		{"missing column", `{"results":[{"columns":["id"]}]}`, "", "did not return column name"},
		{"short row", `{"results":[{"columns":["id","name"],"data":[{"row":[1]}]}]}`, "", "row without column name"},
		{"non-string name", `{"results":[{"columns":["name"],"data":[{"row":[1]}]}]}`, "", "invalid schema object name"},
		{"blank name", `{"results":[{"columns":["name"],"data":[{"row":[" "]}]}]}`, "", "invalid schema object name"},
		{"schema error", `{"errors":[{"code":"Neo.ClientError.Statement.SyntaxError","message":"probe"}]}`, "", "SHOW CONSTRAINTS"},
		{"index error", `{"results":[{"columns":["name"]}]}`, `{"errors":[{"code":"Neo.ClientError.Statement.SyntaxError","message":"probe"}]}`, "SHOW INDEXES"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var statements []string
			err := resetHTTPDifferentialBackend(func(statement string) TransactionResponse {
				statements = append(statements, statement)
				var result TransactionResponse
				switch statement {
				case "SHOW CONSTRAINTS":
					require.NoError(t, json.Unmarshal([]byte(testCase.constraints), &result))
				case "SHOW INDEXES":
					require.NoError(t, json.Unmarshal([]byte(testCase.indexes), &result))
				}
				return result
			})
			if testCase.wantError != "" {
				require.ErrorContains(t, err, testCase.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, []string{
				"MATCH (n) DETACH DELETE n", "SHOW CONSTRAINTS", "DROP CONSTRAINT `constraint`` name` IF EXISTS",
				"SHOW INDEXES", "DROP INDEX `index`` name` IF EXISTS",
				"CREATE LOOKUP INDEX differential_node_labels IF NOT EXISTS FOR (n) ON EACH labels(n)",
				"CREATE LOOKUP INDEX differential_relationship_types IF NOT EXISTS FOR ()-[r]-() ON EACH type(r)",
				"CALL db.clearQueryCaches()",
			}, statements)
		})
	}
}

func TestHTTPFixedDifferentialCorpusMatchesPinnedNeo4j(t *testing.T) {
	referenceURL := os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI")
	if referenceURL == "" {
		t.Skip("set NORNICDB_NEO4J_REFERENCE_HTTP_URI to run the pinned HTTP differential corpus")
	}
	content, err := os.ReadFile("../../testing/cypher/tck/testdata/differential/cases.json")
	require.NoError(t, err)
	type differentialHTTPCase struct {
		Name            string                 `json:"name"`
		Setup           []string               `json:"setup"`
		Query           string                 `json:"query"`
		Parameters      map[string]interface{} `json:"parameters"`
		ExpectedCode    string                 `json:"expected_code"`
		NoEffects       bool                   `json:"no_effects"`
		Ordered         bool                   `json:"ordered"`
		UnorderedLabels bool                   `json:"unordered_labels"`
	}
	var cases []differentialHTTPCase
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.UseNumber()
	require.NoError(t, decoder.Decode(&cases))
	cases = append(cases,
		differentialHTTPCase{Name: "HTTP map metadata follows literal order", Ordered: true, Query: "CREATE (n:H {a:1}) RETURN {node:n,k:1} AS m"},
		differentialHTTPCase{Name: "HTTP map metadata follows aliased order", Ordered: true, Query: "CREATE (n:H {a:1}) WITH {node:n,k:1} AS m RETURN m"},
		differentialHTTPCase{Name: "HTTP nested map metadata follows value order", Ordered: true, Query: "CREATE (n:H {a:1}) RETURN [{node:n,k:1}] AS m"},
		differentialHTTPCase{
			Name: "HTTP nested entities and ordinary maps", Ordered: true,
			Query: "CREATE p=(a:Person {name:'Alice'})-[r:KNOWS {since:2020}]->(b:Person {name:'Bob'}) RETURN a, r, [a, r], {entity:a}, {elementId:'user', labels:['custom'], properties:{value:1}}, null, p",
		},
		differentialHTTPCase{
			Name: "HTTP empty properties and metadata-like user fields", Ordered: true,
			Query: "CREATE (a:Empty)-[r:EMPTY]->(b:Empty) RETURN a, r, {id:1, labels:['L'], _nodeId:'user', _pathResult:'user'}, [], {}",
		},
	)
	client := &http.Client{Timeout: 15 * time.Second}
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit-transaction"
		}
		t.Run(mode, func(t *testing.T) {
			server, authenticator := setupTestServer(t)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			boltServer := bolt.NewWithDatabaseManager(&bolt.Config{ReadBufferSize: 8192, WriteBufferSize: 8192}, nil, server.dbManager)
			server.SetConnectionLister(boltServer.ConnectionListings)
			serveError := make(chan error, 1)
			go func() { serveError <- boltServer.Serve(listener) }()
			driver, err := neo4jdriver.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4jdriver.NoAuth())
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, driver.Close(context.Background()))
				require.NoError(t, boltServer.Close())
				require.NoError(t, <-serveError)
			})
			require.NoError(t, driver.VerifyConnectivity(context.Background()))
			local := httptest.NewServer(server.buildRouter())
			defer local.Close()
			token := "Bearer " + getAuthToken(t, authenticator, "admin")
			post := func(t *testing.T, endpoint, statement string, parameters ...map[string]interface{}) TransactionResponse {
				t.Helper()
				item := map[string]interface{}{"statement": statement}
				if len(parameters) > 0 {
					item["parameters"] = parameters[0]
				}
				statements := []map[string]any{item}
				if statement == "" {
					statements = []map[string]any{}
				}
				payload, err := json.Marshal(map[string]any{"statements": statements})
				require.NoError(t, err)
				request, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(payload))
				require.NoError(t, err)
				request.Header.Set("Content-Type", "application/json")
				if strings.HasPrefix(endpoint, local.URL+"/") {
					request.Header.Set("Authorization", token)
				}
				response, err := client.Do(request)
				require.NoError(t, err)
				defer response.Body.Close()
				require.Contains(t, []int{http.StatusOK, http.StatusCreated}, response.StatusCode)
				var result TransactionResponse
				require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
				return result
			}
			rows := func(response TransactionResponse) [][]interface{} {
				result := make([][]interface{}, 0)
				for _, data := range response.Results[0].Data {
					row := append([]interface{}{}, data.Row...)
					result = append(result, row)
				}
				return result
			}
			observeGraph := func(t *testing.T, endpoint string) ([][]interface{}, [][]interface{}) {
				t.Helper()
				observed := post(t, endpoint+"/commit", "MATCH (n) RETURN labels(n) AS labels, properties(n) AS properties")
				require.Empty(t, observed.Errors)
				nodes := rows(observed)
				for _, row := range nodes {
					labels := row[0].([]interface{})
					sort.Slice(labels, func(left, right int) bool { return labels[left].(string) < labels[right].(string) })
				}
				observed = post(t, endpoint+"/commit", "MATCH (a)-[r]->(b) RETURN properties(a) AS start, type(r) AS type, properties(r) AS properties, properties(b) AS end")
				require.Empty(t, observed.Errors)
				return nodes, rows(observed)
			}
			var metadataShape func(interface{}) interface{}
			metadataShape = func(value interface{}) interface{} {
				switch typed := value.(type) {
				case map[string]interface{}:
					require.Len(t, typed, 4)
					require.IsType(t, float64(0), typed["id"])
					require.NotEmpty(t, typed["elementId"])
					return map[string]interface{}{"type": typed["type"], "deleted": typed["deleted"]}
				case []interface{}:
					result := make([]interface{}, len(typed))
					for index, entry := range typed {
						result[index] = metadataShape(entry)
					}
					return result
				default:
					require.Nil(t, value)
					return nil
				}
			}
			for _, testCase := range cases {
				t.Run(testCase.Name, func(t *testing.T) {
					endpoints := []string{local.URL + "/db/nornic/tx", referenceURL + "/db/neo4j/tx"}
					var results [2]TransactionResponse
					var snapshots [2][][]interface{}
					var relationships [2][][]interface{}
					for backend, endpoint := range endpoints {
						require.NoError(t, resetHTTPDifferentialBackend(func(statement string) TransactionResponse {
							return post(t, endpoint+"/commit", statement)
						}))
						for _, setup := range testCase.Setup {
							require.Empty(t, post(t, endpoint+"/commit", setup).Errors)
						}
						var beforeNodes, beforeRelationships [][]interface{}
						if testCase.NoEffects {
							beforeNodes, beforeRelationships = observeGraph(t, endpoint)
						}
						queryEndpoint := endpoint + "/commit"
						if explicit {
							queryEndpoint = endpoint
						}
						results[backend] = post(t, queryEndpoint, testCase.Query, testCase.Parameters)
						if testCase.ExpectedCode != "" {
							require.Len(t, results[backend].Errors, 1)
							require.Equal(t, testCase.ExpectedCode, results[backend].Errors[0].Code)
						}
						if explicit && len(results[backend].Errors) == 0 {
							require.NotEmpty(t, results[backend].Commit)
							require.Empty(t, post(t, results[backend].Commit, "").Errors)
						}
						snapshots[backend], relationships[backend] = observeGraph(t, endpoint)
						if testCase.NoEffects {
							require.ElementsMatch(t, beforeNodes, snapshots[backend], "unexpected node effects")
							require.ElementsMatch(t, beforeRelationships, relationships[backend], "unexpected relationship effects")
						}
					}
					evidence, err := json.Marshal(map[string]interface{}{
						"case": testCase.Name, "route": "http/" + mode,
						"nornicdb": results[0].Results, "neo4j": results[1].Results,
						"nornicdb_errors": results[0].Errors, "neo4j_errors": results[1].Errors,
					})
					require.NoError(t, err)
					t.Logf("DIFFERENTIAL_RESULT %s", evidence)
					require.Equal(t, len(results[1].Errors), len(results[0].Errors), "NornicDB: %+v; Neo4j: %+v", results[0].Errors, results[1].Errors)
					if len(results[1].Errors) > 0 {
						require.Equal(t, results[1].Errors[0].Code, results[0].Errors[0].Code)
					} else {
						require.Equal(t, results[1].Results[0].Columns, results[0].Results[0].Columns)
						require.Equal(t, results[1].Results[0].Plan != nil, results[0].Results[0].Plan != nil, "plan field presence differs")
						require.Equal(t, results[1].Results[0].Profile != nil, results[0].Results[0].Profile != nil, "profile field presence differs")
						if testCase.UnorderedLabels {
							actual, expected := rows(results[0]), rows(results[1])
							require.Len(t, actual, 1)
							require.Len(t, expected, 1)
							require.ElementsMatch(t, expected[0][0], actual[0][0])
						} else if testCase.Ordered {
							require.Equal(t, rows(results[1]), rows(results[0]))
						} else {
							require.ElementsMatch(t, rows(results[1]), rows(results[0]))
						}
						if testCase.Ordered {
							for index, data := range results[1].Results[0].Data {
								require.Equal(t, metadataShape(data.Meta), metadataShape(results[0].Results[0].Data[index].Meta))
							}
						}
					}
					require.ElementsMatch(t, snapshots[1], snapshots[0], "committed graph differs")
					require.ElementsMatch(t, relationships[1], relationships[0], "committed relationships differ")
				})
			}
		})
	}
}

// Parameter numbers keep Neo4j's INTEGER / FLOAT distinction at any depth
// (#570).
func TestDecodeTransactionRequestKeepsIntegerParameters(t *testing.T) {
	body := `{"statements":[{"statement":"RETURN 1","parameters":{"big":9007199254740993,"f":1.5,"e":1e2,"neg":-3,"huge":1e400,"l":[1,2.5,[3]],"m":{"k":7,"f":0.5}}}]}`
	var req TransactionRequest
	require.NoError(t, decodeTransactionRequest(strings.NewReader(body), &req))
	params := req.Statements[0].Parameters
	require.Equal(t, int64(9007199254740993), params["big"])
	require.Equal(t, 1.5, params["f"])
	require.Equal(t, float64(100), params["e"])
	require.Equal(t, int64(-3), params["neg"])
	require.Equal(t, "1e400", params["huge"], "a number outside float64 range is kept as its text")
	require.Equal(t, []interface{}{int64(1), 2.5, []interface{}{int64(3)}}, params["l"])
	require.Equal(t, map[string]interface{}{"k": int64(7), "f": 0.5}, params["m"])
	require.Error(t, decodeTransactionRequest(strings.NewReader(`{"statements":[`), &req))
}

func TestHTTPCompoundMergeAndSubqueryRows(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	post := func(statement string) TransactionResponse {
		rec := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{
			"statements": []map[string]any{{"statement": statement}},
		}, token)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		var response TransactionResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		require.Empty(t, response.Errors, statement)
		return response
	}

	for _, testCase := range []struct {
		statement string
		want      [][]interface{}
	}{
		{"MERGE (a:HT514 {id: 1}) SET a.q = 1 WITH a MERGE (b:HX514 {k: a.id}) RETURN b.k AS k", [][]interface{}{{float64(1)}}},
		{"CREATE (a:HT640 {id: 1}) MERGE (n:HX640 {k: a.id}) SET n.extra = 2 RETURN n.k AS k, n.extra AS extra", [][]interface{}{{float64(1), float64(2)}}},
		{"UNWIND [1, 2, 3] AS i CALL (i) { WITH i AS j WHERE j > 1 RETURN j AS value } RETURN value ORDER BY value", [][]interface{}{{float64(2)}, {float64(3)}}},
		{"UNWIND [1, 2] AS i CALL () { CREATE (:HT648) } RETURN count(*) AS count", [][]interface{}{{float64(2)}}},
	} {
		t.Run(testCase.statement, func(t *testing.T) {
			response := post(testCase.statement)
			var rows [][]interface{}
			for _, data := range response.Results[0].Data {
				rows = append(rows, data.Row)
			}
			require.Equal(t, testCase.want, rows)
		})
	}

	stored := post("MATCH (b:HX514) RETURN b.k AS k")
	require.Equal(t, []interface{}{float64(1)}, stored.Results[0].Data[0].Row)
	post("CREATE (:HTForeach {id: 1})")
	foreach := post("MATCH (a:HTForeach) FOREACH (v IN [1, 2] | MERGE (:HXForeach {k: v})) RETURN a.id AS id")
	require.Equal(t, []interface{}{float64(1)}, foreach.Results[0].Data[0].Row)
	count := post("MATCH (n:HXForeach) RETURN count(n) AS count")
	require.Equal(t, []interface{}{float64(2)}, count.Results[0].Data[0].Row)
}

// /tx/commit reports the engine's error code (#575), the executor's counters
// in Neo4j's stats object (#576) and integer parameters (#570); explicit
// transactions report stats too, and the commit URL and Location header
// follow the request host (#578).
func TestHTTPTransactionAPIMatchesNeo4j(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")

	post := func(path string, body map[string]any) (*httptest.ResponseRecorder, TransactionResponse) {
		rec := makeRequest(t, server, http.MethodPost, path, body, token)
		var resp TransactionResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp), rec.Body.String())
		return rec, resp
	}

	for _, tc := range []struct{ stmt, code string }{
		{"RETURN 1 / 0 AS x", "Neo.ClientError.Statement.ArithmeticError"},
		{"RETURN toInteger([1]) AS x", "Neo.ClientError.Statement.TypeError"},
	} {
		_, resp := post("/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{{"statement": tc.stmt}}})
		require.Len(t, resp.Errors, 1, tc.stmt)
		require.Equal(t, tc.code, resp.Errors[0].Code, tc.stmt)
		require.NotContains(t, resp.Errors[0].Message, "Neo.", "the code is not repeated in the message")
		_, resp = post("/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{{"statement": tc.stmt}, {"statement": "RETURN 1"}}})
		require.Equal(t, tc.code, resp.Errors[0].Code, "multi-statement path")
	}

	_, resp := post("/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{
		{"statement": "UNWIND range(1, 5) AS i RETURN i, $a / 2 AS half SKIP $s LIMIT $l", "parameters": map[string]any{"a": 3, "s": 1, "l": 2}},
	}})
	require.Empty(t, resp.Errors)
	require.Len(t, resp.Results[0].Data, 2)
	require.EqualValues(t, 2, resp.Results[0].Data[0].Row[0])
	require.EqualValues(t, 1, resp.Results[0].Data[0].Row[1], "integer division")

	rec := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{
		{"statement": "CREATE (a:HS {v: 1})-[:R]->(b:HS) RETURN 1 AS ok", "includeStats": true},
		{"statement": "RETURN 1 AS x", "includeStats": true},
		{"statement": "RETURN 1 AS y"},
	}}, token)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	results := raw["results"].([]any)
	writeStats := results[0].(map[string]any)["stats"].(map[string]any)
	require.Len(t, writeStats, 14, "every Neo4j counter is present")
	require.Equal(t, true, writeStats["contains_updates"])
	require.EqualValues(t, 2, writeStats["nodes_created"])
	require.EqualValues(t, 1, writeStats["relationships_created"])
	require.Contains(t, writeStats, "relationship_deleted")
	readStats := results[1].(map[string]any)["stats"].(map[string]any)
	require.Equal(t, false, readStats["contains_updates"])
	require.EqualValues(t, 0, readStats["nodes_created"])
	require.NotContains(t, results[2].(map[string]any), "stats", "no stats without includeStats")

	openReq := httptest.NewRequest(http.MethodPost, "/db/nornic/tx", strings.NewReader(`{"statements":[{"statement":"CREATE (:HS2) RETURN 1 AS ok","includeStats":true}]}`))
	openReq.Host = "db.example:17474"
	openReq.Header.Set("Content-Type", "application/json")
	openReq.Header.Set("Authorization", token)
	openRec := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(openRec, openReq)
	require.Equal(t, http.StatusCreated, openRec.Code, openRec.Body.String())
	var openResp TransactionResponse
	require.NoError(t, json.Unmarshal(openRec.Body.Bytes(), &openResp))
	require.True(t, strings.HasPrefix(openResp.Commit, "http://db.example:17474/db/nornic/tx/"), openResp.Commit)
	require.Equal(t, strings.TrimSuffix(openResp.Commit, "/commit"), openRec.Header().Get("Location"))
	require.NotNil(t, openResp.Results[0].Stats)
	require.Equal(t, 1, openResp.Results[0].Stats.NodesCreated)
	require.True(t, strings.HasSuffix(openResp.Transaction.Expires, " GMT"), openResp.Transaction.Expires)
	rollback := makeRequest(t, server, http.MethodDelete, strings.TrimPrefix(openRec.Header().Get("Location"), "http://db.example:17474"), nil, token)
	require.Equal(t, http.StatusOK, rollback.Code, rollback.Body.String())
}

// A failing statement ends an explicit transaction, as in Neo4j's HTTP API:
// the statements after it in the request are not run, the transaction is
// rolled back (nothing it wrote is kept), and later requests to it get
// Neo.ClientError.Transaction.TransactionNotFound.
func TestHTTPExplicitTransactionEndsOnStatementError(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	post := func(path string, stmts ...string) (int, TransactionResponse) {
		list := make([]map[string]any, 0, len(stmts))
		for _, s := range stmts {
			list = append(list, map[string]any{"statement": s})
		}
		rec := makeRequest(t, server, http.MethodPost, path, map[string]any{"statements": list}, token)
		var resp TransactionResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp), rec.Body.String())
		return rec.Code, resp
	}
	stored := func() int64 {
		_, resp := post("/db/nornic/tx/commit", "MATCH (n:TxErr) RETURN count(n) AS c")
		require.Empty(t, resp.Errors)
		return int64(resp.Results[0].Data[0].Row[0].(float64))
	}
	for _, failing := range []struct {
		stmt, code string
		results    int // the CREATE before it, and its columns if it compiled (#668)
	}{
		{"RETURN 1 / 0 AS x", "Neo.ClientError.Statement.ArithmeticError", 2},
		{"RETRUN 1", "Neo.ClientError.Statement.SyntaxError", 1},
	} {
		t.Run(failing.stmt, func(t *testing.T) {
			_, _ = post("/db/nornic/tx/commit", "MATCH (n:TxErr) DETACH DELETE n")

			// Error in a request to an open transaction.
			code, resp := post("/db/nornic/tx", "CREATE (:TxErr {v: 'a'})")
			require.Equal(t, http.StatusCreated, code)
			require.Empty(t, resp.Errors)
			txPath := strings.TrimSuffix(resp.Commit, "/commit")
			code, resp = post(txPath, "CREATE (:TxErr {v: 'b'})", failing.stmt, "CREATE (:TxErr {v: 'c'})")
			require.Equal(t, http.StatusOK, code)
			require.Len(t, resp.Errors, 1)
			require.Equal(t, failing.code, resp.Errors[0].Code)
			require.Len(t, resp.Results, failing.results, "the statement after the failing one is not run")
			for _, path := range []string{txPath, txPath + "/commit"} {
				code, resp = post(path, "RETURN 1 AS one")
				require.Equal(t, http.StatusNotFound, code, path)
				require.Equal(t, "Neo.ClientError.Transaction.TransactionNotFound", resp.Errors[0].Code, path)
			}
			require.Zero(t, stored(), "nothing the transaction wrote is kept")

			// Error in the request that opens the transaction.
			code, resp = post("/db/nornic/tx", "CREATE (:TxErr {v: 'd'})", failing.stmt)
			require.Equal(t, http.StatusCreated, code)
			require.Equal(t, failing.code, resp.Errors[0].Code)
			code, _ = post(strings.TrimSuffix(resp.Commit, "/commit") + "/commit")
			require.Equal(t, http.StatusNotFound, code)
			require.Zero(t, stored())

			// Error in the commit request.
			_, resp = post("/db/nornic/tx", "CREATE (:TxErr {v: 'e'})")
			txPath = strings.TrimSuffix(resp.Commit, "/commit")
			_, resp = post(txPath+"/commit", failing.stmt, "CREATE (:TxErr {v: 'f'})")
			require.Equal(t, failing.code, resp.Errors[0].Code)
			code, _ = post(txPath + "/commit")
			require.Equal(t, http.StatusNotFound, code)
			require.Zero(t, stored())
		})
	}
}

// A statement that compiled and failed while running reports its columns with
// no rows next to the error; a statement failing at compile time, or one
// without columns, has no result (#668, as Neo4j 5.26).
func TestHTTPFailedStatementReportsItsColumns(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	post := func(path string, statements ...string) (*httptest.ResponseRecorder, TransactionResponse) {
		body := make([]map[string]any, 0, len(statements))
		for _, statement := range statements {
			body = append(body, map[string]any{"statement": statement})
		}
		rec := makeRequest(t, server, http.MethodPost, path, map[string]any{"statements": body}, token)
		var resp TransactionResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp), rec.Body.String())
		require.Len(t, resp.Errors, 1, rec.Body.String())
		return rec, resp
	}
	columnsOf := func(resp TransactionResponse) [][]string {
		columns := [][]string{}
		for _, result := range resp.Results {
			require.Empty(t, result.Data)
			columns = append(columns, result.Columns)
		}
		return columns
	}

	for statement, want := range map[string][][]string{
		"RETURN 1 AS a, 1 / 0 AS x":                                  {{"a", "x"}},
		"WITH 1 / 0 AS x RETURN x":                                   {{"x"}},
		"UNWIND [1] AS d CALL { WITH d RETURN 1 / 0 AS z } RETURN z": {{"z"}},
		"CREATE (n:F668) SET n.v = 1 / 0":                            {},
		"RETUR 1":                                                    {},
	} {
		_, resp := post("/db/nornic/tx/commit", statement)
		require.Equal(t, want, columnsOf(resp), statement)
	}

	_, resp := post("/db/nornic/tx/commit", "RETURN 1 AS a", "RETURN 1 / 0 AS x", "RETURN 2 AS b")
	require.Len(t, resp.Results, 2)
	require.Equal(t, []string{"a"}, resp.Results[0].Columns)
	require.Len(t, resp.Results[0].Data, 1)
	require.Equal(t, []string{"x"}, resp.Results[1].Columns)
	require.Empty(t, resp.Results[1].Data)

	rec := makeRequest(t, server, http.MethodPost, "/db/nornic/tx", map[string]any{"statements": []map[string]any{}}, token)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var opened TransactionResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &opened))
	_, resp = post(strings.TrimSuffix(opened.Commit, "/commit"), "RETURN 1 / 0 AS x")
	require.Equal(t, [][]string{{"x"}}, columnsOf(resp))
}

func TestGh668_HTTPFailurePreservesRowsAndRollsBack(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	for _, endpoint := range []string{"/db/nornic/tx/commit", "/db/nornic/tx"} {
		for _, testCase := range []struct {
			query   string
			code    string
			results int
			rows    []ResultRow
		}{
			{"RETURN 1 / 0 AS x", "Neo.ClientError.Statement.ArithmeticError", 3, []ResultRow{}},
			{"RETURN missingFunction() AS x", "Neo.ClientError.Statement.SyntaxError", 2, nil},
			{"UNWIND [1,0] AS d RETURN 1/d AS x", "Neo.ClientError.Statement.ArithmeticError", 3, []ResultRow{{Row: []interface{}{float64(1)}, Meta: []interface{}{nil}}}},
		} {
			t.Run(endpoint+testCase.query, func(t *testing.T) {
				recorder := makeRequest(t, server, http.MethodPost, endpoint, map[string]any{
					"statements": []map[string]any{{"statement": "CREATE (:FailureRollback)"}, {"statement": "RETURN 42 AS earlier"}, {"statement": testCase.query}, {"statement": "RETURN 99 AS skipped"}},
				}, token)
				var response TransactionResponse
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response), recorder.Body.String())
				require.Len(t, response.Errors, 1)
				require.Equal(t, testCase.code, response.Errors[0].Code)
				require.Len(t, response.Results, testCase.results)
				require.Equal(t, []interface{}{float64(42)}, response.Results[1].Data[0].Row)
				store, err := server.dbManager.GetStorage("nornic")
				require.NoError(t, err)
				nodes, err := store.GetNodesByLabel("FailureRollback")
				require.NoError(t, err)
				require.Empty(t, nodes)
				if testCase.results == 3 {
					require.Equal(t, []string{"x"}, response.Results[2].Columns)
					require.Equal(t, testCase.rows, response.Results[2].Data)
				}
			})
		}
	}
}

func TestGh668_HTTPReturnedPartialResult(t *testing.T) {
	server, authenticator := setupTestServer(t)
	claims, err := authenticator.ValidateToken(getAuthToken(t, authenticator, "admin"))
	require.NoError(t, err)
	response := &TransactionResponse{}
	queryError := server.runRequestStatement(context.Background(), claims, "nornic", StatementRequest{
		Statement: "UNWIND [1,0] AS d RETURN 1/d AS x",
	}, func(context.Context, string, string, map[string]interface{}) (*cypher.ExecuteResult, *cypher.StorageExecutor, error) {
		return &cypher.ExecuteResult{Columns: []string{"x"}, Rows: [][]interface{}{{int64(1)}}}, nil, errors.New("Neo.ClientError.Statement.ArithmeticError: / by zero")
	}, func(localization.Message) string { return "unused" }, response)
	require.NotNil(t, queryError)
	require.Equal(t, "Neo.ClientError.Statement.ArithmeticError", queryError.Code)
	require.Len(t, response.Results, 1)
	require.Equal(t, []string{"x"}, response.Results[0].Columns)
	require.Equal(t, []interface{}{int64(1)}, response.Results[0].Data[0].Row)
}

func TestGh668_HTTPTransactionalCallBoundaries(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	batch := "UNWIND [1,2] AS i CALL (i) { CREATE (:HTTPBatch {i:i}) } IN TRANSACTIONS OF 1 ROWS RETURN i"
	for _, precedingWrite := range []bool{false, true} {
		t.Run(fmt.Sprint(precedingWrite), func(t *testing.T) {
			statements := []map[string]any{}
			if precedingWrite {
				statements = append(statements, map[string]any{"statement": "CREATE (:BeforeHTTPBatch)"})
			}
			statements = append(statements, map[string]any{"statement": batch}, map[string]any{"statement": "RETURN 1/0 AS x"})
			recorder := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{"statements": statements}, token)
			var response TransactionResponse
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response), recorder.Body.String())
			require.Len(t, response.Errors, 1)
			store, err := server.dbManager.GetStorage("nornic")
			require.NoError(t, err)
			nodes, err := store.GetNodesByLabel("HTTPBatch")
			require.NoError(t, err)
			if precedingWrite {
				require.Contains(t, response.Errors[0].Message, "Expected transaction state to be empty")
				require.Len(t, nodes, 2)
				before, err := store.GetNodesByLabel("BeforeHTTPBatch")
				require.NoError(t, err)
				require.Empty(t, before)
			} else {
				require.Equal(t, "Neo.ClientError.Statement.ArithmeticError", response.Errors[0].Code)
				require.Len(t, nodes, 2)
				require.Len(t, response.Results, 2)
				require.Len(t, response.Results[0].Data, 2)
			}
		})
	}
}

// TestHTTPExplicitTransactionCommitFailureStatus verifies a COMMIT that fails
// (a UNIQUE value another transaction committed first) reports the failure's
// Neo4j status over HTTP, as Bolt does (#657).
func TestHTTPExplicitTransactionCommitFailureStatus(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	post := func(path string, stmts ...string) TransactionResponse {
		list := make([]map[string]any, 0, len(stmts))
		for _, s := range stmts {
			list = append(list, map[string]any{"statement": s})
		}
		rec := makeRequest(t, server, http.MethodPost, path, map[string]any{"statements": list}, token)
		var resp TransactionResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp), rec.Body.String())
		return resp
	}
	require.Empty(t, post("/db/nornic/tx/commit", "CREATE CONSTRAINT cf_k FOR (n:CF) REQUIRE n.k IS UNIQUE").Errors)
	open := post("/db/nornic/tx", "CREATE (:CF {k: 1})")
	require.Empty(t, open.Errors)
	require.Empty(t, post("/db/nornic/tx/commit", "CREATE (:CF {k: 1})").Errors)
	resp := post(open.Commit[strings.Index(open.Commit, "/db/"):])
	require.Len(t, resp.Errors, 1)
	require.Equal(t, "Neo.ClientError.Schema.ConstraintValidationFailed", resp.Errors[0].Code)
}
