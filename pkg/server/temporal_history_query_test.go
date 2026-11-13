// Public HTTP transport coverage for the approved historical read API:
//
//	CALL db.temporal.asOf(at, query, parameters = {}) YIELD row RETURN row
//
// setupTestServer opens the native database with MVCC retention enabled, so a
// previous property value is still resolvable at the caller's cutoff instead
// of having been overwritten in place.
package server

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type temporalHistoryHTTPFixture struct {
	server   *Server
	token    string
	endpoint string
}

func newTemporalHistoryHTTPFixture(t *testing.T) *temporalHistoryHTTPFixture {
	t.Helper()
	server, authenticator := setupTestServer(t)
	return &temporalHistoryHTTPFixture{
		server:   server,
		token:    "Bearer " + getAuthToken(t, authenticator, "admin"),
		endpoint: "/db/" + server.dbManager.DefaultDatabaseName() + "/tx/commit",
	}
}

func (f *temporalHistoryHTTPFixture) statement(t *testing.T, query string, params map[string]any) TransactionResponse {
	t.Helper()
	statement := map[string]any{"statement": query}
	if params != nil {
		statement["parameters"] = params
	}
	response := makeRequest(t, f.server, http.MethodPost, f.endpoint, map[string]any{"statements": []map[string]any{statement}}, f.token)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var decoded TransactionResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &decoded), response.Body.String())
	return decoded
}

func TestTemporalAsOfHTTPReturnsRetainedPropertyAndWholeRow(t *testing.T) {
	fixture := newTemporalHistoryHTTPFixture(t)

	created := fixture.statement(t, "CREATE (:Account {name:'before'})", nil)
	require.Empty(t, created.Errors)
	at := time.Now().UTC().Format(time.RFC3339Nano)
	updated := fixture.statement(t, "MATCH (n:Account) SET n.name='after'", nil)
	require.Empty(t, updated.Errors)

	retained := fixture.statement(t,
		`CALL db.temporal.asOf(datetime($at), 'MATCH (n:Account) RETURN n.name AS name') YIELD row RETURN row.name AS name`,
		map[string]any{"at": at})
	require.Empty(t, retained.Errors)
	require.Len(t, retained.Results, 1)
	require.Equal(t, []string{"name"}, retained.Results[0].Columns)
	require.Len(t, retained.Results[0].Data, 1)
	require.Equal(t, []any{"before"}, retained.Results[0].Data[0].Row, "the retained property value at the cutoff")

	whole := fixture.statement(t,
		`CALL db.temporal.asOf(datetime($at), 'RETURN 1 AS one, true AS two') YIELD row RETURN row`,
		map[string]any{"at": at})
	require.Empty(t, whole.Errors)
	require.Len(t, whole.Results, 1)
	require.Equal(t, []string{"row"}, whole.Results[0].Columns)
	require.Len(t, whole.Results[0].Data, 1)
	require.Equal(t, []any{map[string]any{"one": float64(1), "two": true}}, whole.Results[0].Data[0].Row, "the whole row is one public JSON object")
}

func TestTemporalAsOfHTTPRejectsMutatingNestedStatement(t *testing.T) {
	fixture := newTemporalHistoryHTTPFixture(t)

	refused := fixture.statement(t,
		`CALL db.temporal.asOf(datetime($at), 'CREATE (:Forbidden)') YIELD row RETURN row`,
		map[string]any{"at": time.Now().UTC().Format(time.RFC3339Nano)})
	require.Len(t, refused.Errors, 1)
	require.Equal(t, "Neo.ClientError.Statement.AccessMode", refused.Errors[0].Code)
	require.Contains(t, refused.Errors[0].Message, "read-only")

	count := fixture.statement(t, "MATCH (n:Forbidden) RETURN count(n) AS count", nil)
	require.Empty(t, count.Errors)
	require.Len(t, count.Results, 1)
	require.Len(t, count.Results[0].Data, 1)
	require.Equal(t, []any{float64(0)}, count.Results[0].Data[0].Row, "the refused nested write left no entity")
}

func TestTemporalAsOfHTTPKeepsDeprecatedIntervalSignature(t *testing.T) {
	fixture := newTemporalHistoryHTTPFixture(t)

	created := fixture.statement(t,
		`CREATE (:Interval {key:'a', valid_from:'2024-01-01T00:00:00Z', valid_to:'2024-02-01T00:00:00Z', name:'interval'})`, nil)
	require.Empty(t, created.Errors)

	interval := fixture.statement(t,
		`CALL db.temporal.asOf('Interval', 'key', 'a', 'valid_from', 'valid_to', '2024-01-15T00:00:00Z') YIELD node RETURN node.name AS name`, nil)
	require.Empty(t, interval.Errors)
	require.Len(t, interval.Results, 1)
	require.Equal(t, []string{"name"}, interval.Results[0].Columns)
	require.Len(t, interval.Results[0].Data, 1)
	require.Equal(t, []any{"interval"}, interval.Results[0].Data[0].Row)
}
