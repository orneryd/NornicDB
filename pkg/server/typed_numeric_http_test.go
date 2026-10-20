package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/listinputs"
	"github.com/stretchr/testify/require"
)

func TestTypedNumericHTTPBoundaries(t *testing.T) {
	srv, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	peer := httptest.NewServer(srv.buildRouter())
	t.Cleanup(peer.Close)
	base := "/db/" + srv.dbManager.DefaultDatabaseName() + "/tx"
	store, err := srv.dbManager.GetStorage(srv.dbManager.DefaultDatabaseName())
	require.NoError(t, err)
	for _, input := range listinputs.NumericHTTPInputs() {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(input.Name), Labels: []string{"NumericWireProperty"}, Properties: map[string]interface{}{"name": input.Name, "xs": input.Property}})
		require.NoError(t, err)
	}
	request := func(t *testing.T, path, query string, params map[string]interface{}) TransactionResponse {
		t.Helper()
		statements := []map[string]interface{}{}
		if query != "" {
			// JSON has no native float/list widths. Keep float parameters as
			// floating tokens: encoding/json otherwise emits a whole float near
			// MaxInt64 as an integer token with a different exact integer value.
			if values, ok := params["xs"].([]float64); ok {
				copy := make(map[string]interface{}, len(params))
				for key, value := range params {
					copy[key] = value
				}
				numbers := make([]json.RawMessage, len(values))
				for i, value := range values {
					token := strconv.FormatFloat(value, 'g', -1, 64)
					if !strings.ContainsAny(token, ".eE") {
						token += ".0"
					}
					numbers[i] = json.RawMessage(token)
				}
				copy["xs"] = numbers
				params = copy
			}
			statements = append(statements, map[string]interface{}{"statement": query, "parameters": params})
		}
		body, err := json.Marshal(map[string]interface{}{"statements": statements})
		require.NoError(t, err)
		req, err := http.NewRequest(http.MethodPost, peer.URL+path, bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		response, err := peer.Client().Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		var payload TransactionResponse
		decoder := json.NewDecoder(response.Body)
		decoder.UseNumber()
		require.NoError(t, decoder.Decode(&payload))
		require.Empty(t, payload.Errors, query)
		return payload
	}
	// Compare exact numeric JSON tokens, including MaxInt64, instead of decoding
	// the independent expected value through a lossy float64 round trip.
	canonical := func(value interface{}) string {
		data, err := json.Marshal(value)
		require.NoError(t, err)
		return string(data)
	}
	for _, explicit := range []bool{false, true} {
		for _, input := range listinputs.NumericHTTPInputs() {
			t.Run(fmt.Sprintf("explicit=%t/%s", explicit, input.Name), func(t *testing.T) {
				path := base + "/commit"
				if explicit {
					opened := request(t, base, "", nil)
					commit, err := url.Parse(opened.Commit)
					require.NoError(t, err)
					path = strings.TrimSuffix(commit.Path, "/commit")
				}
				read := func(t *testing.T, query string, params map[string]interface{}) interface{} {
					t.Helper()
					payload := request(t, path, query, params)
					require.Len(t, payload.Results, 1, query)
					require.Len(t, payload.Results[0].Data, 1, query)
					return payload.Results[0].Data[0].Row[0]
				}
				params := map[string]interface{}{"name": input.Name}
				forms := []struct{ Name, Prefix, List string }{{"typed-property", "MATCH (n:NumericWireProperty {name:$name}) ", "n.xs"}}
				if listinputs.NumericWireParameter(input, true) {
					params["xs"] = input.List
					forms = append(forms, struct{ Name, Prefix, List string }{"parameter", "", "$xs"},
						struct{ Name, Prefix, List string }{"with", "WITH $xs AS xs ", "xs"},
						struct{ Name, Prefix, List string }{"nested", "WITH {items:[$xs]} AS box ", "head(box.items)"})
					read(t, "CREATE (n:NumericWireWritten {xs:$xs}) RETURN size(n.xs) AS v", params)
					forms = append(forms, struct{ Name, Prefix, List string }{"written-property", "MATCH (n:NumericWireWritten) ", "n.xs"})
				}
				for _, form := range forms {
					for _, check := range listinputs.NumericWireChecks(input) {
						t.Run(form.Name+"/"+check.Expression, func(t *testing.T) {
							query := form.Prefix + listinputs.Query(check.Expression, form.List)
							got := read(t, query, params)
							if want, floating := check.Want.(float64); floating {
								number, ok := got.(json.Number)
								require.True(t, ok, "%s: got %T", query, got)
								value, err := number.Float64()
								require.NoError(t, err, query)
								require.Equal(t, want, value, query)
							} else {
								require.Equal(t, canonical(check.Want), canonical(got), query)
							}
						})
					}
					t.Run(form.Name+"/context-string", func(t *testing.T) {
						want := read(t, form.Prefix+listinputs.Query("toString(head(%s))", form.List), params)
						got := read(t, form.Prefix+listinputs.Query("apoc.map.setKey({},'v',toString(coalesce(%s,[])[0]))", form.List), params)
						require.Equal(t, canonical(map[string]interface{}{"v": want}), canonical(got))
					})
				}
				request(t, path, "MATCH (n:NumericWireWritten) DETACH DELETE n", nil)
				if explicit {
					request(t, path+"/commit", "", nil)
				}
			})
		}
		t.Run(fmt.Sprintf("explicit=%t/computed-nonfinite", explicit), func(t *testing.T) {
			path := base + "/commit"
			if explicit {
				opened := request(t, base, "", nil)
				commit, err := url.Parse(opened.Commit)
				require.NoError(t, err)
				path = strings.TrimSuffix(commit.Path, "/commit")
			}
			for _, input := range []struct {
				text    string
				integer int64
			}{{"NaN", 0}, {"Infinity", math.MaxInt64}, {"-Infinity", math.MinInt64}} {
				for _, check := range []struct {
					expression string
					want       interface{}
				}{
					{"toInteger(head(%s))", input.integer},
					{"head(toIntegerList(%s))", input.integer},
					{"apoc.map.setKey({},'v',toInteger(coalesce(%s,[])[0]))", map[string]interface{}{"v": input.integer}},
					{"head(toIntegerList(coalesce(%s,[])[-1..]))", input.integer},
				} {
					t.Run(input.text+"/"+check.expression, func(t *testing.T) {
						query := listinputs.Query(check.expression, "[toFloat($number)]")
						payload := request(t, path, query, map[string]interface{}{"number": input.text})
						require.Len(t, payload.Results, 1)
						require.Len(t, payload.Results[0].Data, 1)
						require.Equal(t, canonical(check.want), canonical(payload.Results[0].Data[0].Row[0]), query)
					})
				}
			}
			if explicit {
				request(t, path+"/commit", "", nil)
			}
		})
	}
}
