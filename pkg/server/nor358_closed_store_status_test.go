package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Closed-store status on the authenticated HTTP transaction endpoint (NOR-358;
// NOR-293 comment e2b952ca).
//
// A healthy statement that reaches a closed store is server-side
// unavailability, so the statement error carries the retryable
// Neo.TransientError.General.DatabaseUnavailable status with the store's own
// cause still visible. Both operations first prove their statement on the open
// store (a seeded row for the read, a persisted node for the write), so the
// closed-store failure is the close and not the statement. The failure is
// reported in the body of a 200 response, as Neo4j does.
const (
	// nor358ClosedStoreRead filters a stored property, so it must reach storage
	// instead of being answered from an index.
	nor358ClosedStoreRead = "MATCH (n:Nor358ClosedStore) WHERE n.tag = $tag RETURN n"
	// nor358ClosedStoreWrite creates and returns one node.
	nor358ClosedStoreWrite = "CREATE (n:Nor358ClosedStore {nor358: true}) RETURN n"
	// nor358ClosedStoreCount verifies the control write's persistence.
	nor358ClosedStoreCount = "MATCH (n:Nor358ClosedStore {nor358: true}) RETURN count(n) AS written"

	// nor358SeededTag is the parameter value the controls seed.
	nor358SeededTag = "nor358-closed-store-seeded-v1"
	// nor358TargetTag is the closed-store run's parameter value. The result
	// cache is keyed by statement text and parameter values, so the closed
	// run cannot replay the open store's answer.
	nor358TargetTag = "nor358-closed-store-target-v1"
)

// nor358PostStatement posts one statement to the implicit-transaction endpoint
// and returns its HTTP status with the decoded body.
func nor358PostStatement(t *testing.T, server *Server, token, statement string, params map[string]interface{}) (int, map[string]interface{}) {
	t.Helper()
	body := map[string]interface{}{"statement": statement}
	if params != nil {
		body["parameters"] = params
	}
	response := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{body},
	}, token)
	encoded := response.Body.Bytes()
	var payload map[string]interface{}
	require.NoError(t, json.Unmarshal(encoded, &payload), "status=%d body=%s", response.Code, encoded)
	return response.Code, payload
}

// nor358StatementError returns the code and message of the response's single
// statement error, failing on any other shape.
func nor358StatementError(t *testing.T, payload map[string]interface{}) (code, message string) {
	t.Helper()
	encoded, err := json.Marshal(payload["errors"])
	require.NoError(t, err)
	reported, ok := payload["errors"].([]interface{})
	require.True(t, ok, "the response must carry the errors list: %s", encoded)
	require.Len(t, reported, 1, "exactly one statement error is expected: %s", encoded)
	entry, ok := reported[0].(map[string]interface{})
	require.True(t, ok, "unexpected error entry: %s", encoded)
	code, _ = entry["code"].(string)
	message, _ = entry["message"].(string)
	return code, message
}

// nor358ResultRows returns every row the response's results carry.
func nor358ResultRows(t *testing.T, payload map[string]interface{}) [][]interface{} {
	t.Helper()
	encoded, err := json.Marshal(payload["results"])
	require.NoError(t, err)
	results, ok := payload["results"].([]interface{})
	require.True(t, ok, "the response must carry a results list: %s", encoded)
	var rows [][]interface{}
	for _, result := range results {
		entry, ok := result.(map[string]interface{})
		require.True(t, ok, "unexpected result entry: %s", encoded)
		data, ok := entry["data"].([]interface{})
		require.True(t, ok, "unexpected result data: %s", encoded)
		for _, item := range data {
			record, ok := item.(map[string]interface{})
			require.True(t, ok, "unexpected row entry: %s", encoded)
			values, ok := record["row"].([]interface{})
			require.True(t, ok, "unexpected row values: %s", encoded)
			rows = append(rows, values)
		}
	}
	return rows
}

func TestNOR358HTTPClosedStoreStatusClassification(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })

			t.Run("read", func(t *testing.T) {
				srv, authenticator := setupTestServer(t)
				token := "Bearer " + getAuthToken(t, authenticator, "admin")

				status, seeded := nor358PostStatement(t, srv, token, "CREATE (n:Nor358ClosedStore {tag: $tag}) RETURN n",
					map[string]interface{}{"tag": nor358SeededTag})
				require.Equal(t, http.StatusOK, status)
				require.Empty(t, seeded["errors"], "seeding the control row failed: %v", seeded["errors"])
				status, control := nor358PostStatement(t, srv, token, nor358ClosedStoreRead,
					map[string]interface{}{"tag": nor358SeededTag})
				require.Equal(t, http.StatusOK, status)
				require.Empty(t, control["errors"], "the transport must answer the read on the open store: %v", control["errors"])
				require.Len(t, nor358ResultRows(t, control), 1, "the control read must return the seeded node")

				require.NoError(t, srv.db.GetBaseStorageForManager().Close())
				status, payload := nor358PostStatement(t, srv, token, nor358ClosedStoreRead,
					map[string]interface{}{"tag": nor358TargetTag})
				require.Equal(t, http.StatusOK, status, "a statement failure is reported in the body, as Neo4j does")
				code, message := nor358StatementError(t, payload)
				encoded, err := json.Marshal(payload["errors"])
				require.NoError(t, err)
				t.Logf("HTTP closed-store read: errors=%s", encoded)
				require.Contains(t, message, storage.ErrStorageClosed.Error(), "the reported cause must stay visible: %s", encoded)
				require.Equal(t, nornicerrors.TransientDatabaseUnavailable, code,
					"HTTP closed-store read status must be the retryable unavailability class, got code=%q message=%q", code, message)
			})

			t.Run("write", func(t *testing.T) {
				srv, authenticator := setupTestServer(t)
				token := "Bearer " + getAuthToken(t, authenticator, "admin")

				status, control := nor358PostStatement(t, srv, token, nor358ClosedStoreWrite, nil)
				require.Equal(t, http.StatusOK, status)
				require.Empty(t, control["errors"], "the transport must run the write on the open store: %v", control["errors"])
				require.Len(t, nor358ResultRows(t, control), 1, "the control write must return its created node")
				status, verified := nor358PostStatement(t, srv, token, nor358ClosedStoreCount, nil)
				require.Equal(t, http.StatusOK, status)
				require.Empty(t, verified["errors"], "verifying the control write failed: %v", verified["errors"])
				rows := nor358ResultRows(t, verified)
				require.Len(t, rows, 1)
				require.Equal(t, []interface{}{float64(1)}, rows[0], "the control write must persist its node on the open store")

				require.NoError(t, srv.db.GetBaseStorageForManager().Close())
				status, payload := nor358PostStatement(t, srv, token, nor358ClosedStoreWrite, nil)
				require.Equal(t, http.StatusOK, status, "a statement failure is reported in the body, as Neo4j does")
				code, message := nor358StatementError(t, payload)
				encoded, err := json.Marshal(payload["errors"])
				require.NoError(t, err)
				t.Logf("HTTP closed-store write: errors=%s", encoded)
				require.Contains(t, message, storage.ErrStorageClosed.Error(), "the reported cause must stay visible: %s", encoded)
				require.Equal(t, nornicerrors.TransientDatabaseUnavailable, code,
					"HTTP closed-store write status must be the retryable unavailability class, got code=%q message=%q", code, message)
			})
		})
	}
}
