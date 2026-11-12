package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	cypherconfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// cdcWireEvents runs the change feed from over endpoint and returns each
// event's identifier and the name its created node was given, in feed order;
// an empty label reads the whole retained feed.
func cdcWireEvents(t *testing.T, server *Server, token, endpoint, from, label string) (ids, names []string) {
	t.Helper()
	selectors := []any{}
	if label != "" {
		selectors = append(selectors, map[string]any{"select": "n", "labels": []any{label}})
	}
	rows := httpRows(t, server, token, endpoint,
		"CALL db.cdc.query($from, $selectors) YIELD id, event RETURN id, event.state.after.properties.name",
		map[string]any{"from": from, "selectors": selectors})
	for _, row := range rows {
		ids = append(ids, row[0].(string))
		names = append(names, row[1].(string))
	}
	return ids, names
}

// cdcWireEarliest is db.cdc.earliest()'s identifier, read over endpoint.
func cdcWireEarliest(t *testing.T, server *Server, token, endpoint string) string {
	t.Helper()
	return httpRows(t, server, token, endpoint, "CALL db.cdc.earliest() YIELD id RETURN id", nil)[0][0].(string)
}

// cdcWireErrorCode runs statement with params over endpoint and returns its
// first error's code; the test fails when it succeeded.
func cdcWireErrorCode(t *testing.T, server *Server, token, endpoint, statement string, params map[string]any) string {
	t.Helper()
	response := makeRequest(t, server, http.MethodPost, endpoint,
		map[string]any{"statements": []any{map[string]any{"statement": statement, "parameters": params}}}, token)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var body struct {
		Errors []struct {
			Code string `json:"code"`
		} `json:"errors"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body), response.Body.String())
	require.Len(t, body.Errors, 1, statement)
	return body.Errors[0].Code
}

// Neo4j 5.26.30 Enterprise keeps the start-of-retained-feed marker
// db.cdc.earliest() returns usable after old events are pruned, while an
// ordinary event cursor of a pruned event expires. Over HTTP the marker is
// read from the wire, the storage engine's retention owner prunes the first
// write's events, and the unchanged marker still lists the second write's two
// events with their exact identifiers in commit order — through auto-commit,
// through a fresh explicit transaction, and again after NornicDB (with its
// database manager) is shut down and reopened on the same data directory.
func TestHTTPChangeFeedEarliestMarkerSurvivesPruningAndRestart(t *testing.T) {
	for _, parser := range []string{cypherconfig.ParserTypeNornic, cypherconfig.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := cypherconfig.GetParserType()
			cypherconfig.SetParserType(parser)
			t.Cleanup(func() { cypherconfig.SetParserType(previous) })

			dataDir := t.TempDir()
			before, authenticator, database := openPersistentDBConfigTestServer(t, dataDir)
			t.Cleanup(func() { _ = database.Close(); _ = before.dbManager.Close() })
			admin := getAuthToken(t, authenticator, "admin")
			token := "Bearer " + admin
			require.Empty(t, txErrorCode(t, before, "system", admin, "ALTER DATABASE nornic SET OPTION txLogEnrichment 'FULL'"))
			const autoCommit = "/db/nornic/tx/commit"
			earliest := cdcWireEarliest(t, before, token, autoCommit)

			// A commits two events; B commits two later ones. Every event
			// writes its own node name, so each event is uniquely identified.
			httpRows(t, before, token, autoCommit, "CREATE (:CdcPruned {name: 'a-1'}), (:CdcPruned {name: 'a-2'})", nil)
			prunedIDs, prunedNames := cdcWireEvents(t, before, token, autoCommit, earliest, "CdcPruned")
			require.Equal(t, []string{"a-1", "a-2"}, prunedNames)
			httpRows(t, before, token, autoCommit, "CREATE (:CdcKept {name: 'b-1'}), (:CdcKept {name: 'b-2'})", nil)
			keptIDs, keptNames := cdcWireEvents(t, before, token, autoCommit, earliest, "CdcKept")
			require.Equal(t, []string{"b-1", "b-2"}, keptNames)
			require.Len(t, keptIDs, 2)

			// The storage engine keeps a database's last transaction's events:
			// pruning with the retention owner's cutoff expires A's two and
			// leaves B's two.
			engine, ok := storage.UnwrapEngine(database.GetBaseStorageForManager()).(*storage.BadgerEngine)
			require.True(t, ok)
			pruned, err := engine.PruneChangeEvents(t.Context(), "nornic", time.Now().Add(time.Minute))
			require.NoError(t, err)
			require.Equal(t, 2, pruned)

			// The unchanged marker lists exactly B's events, in order, with
			// their exact identifiers, in an auto-commit statement and in
			// db.cdc.earliest() → db.cdc.query() chained in one statement.
			require.Equal(t, earliest, cdcWireEarliest(t, before, token, autoCommit))
			ids, names := cdcWireEvents(t, before, token, autoCommit, earliest, "")
			require.Equal(t, keptIDs, ids)
			require.Equal(t, keptNames, names)
			require.Equal(t, [][]any{{keptIDs[0]}, {keptIDs[1]}},
				httpRows(t, before, token, autoCommit, "CALL db.cdc.earliest() YIELD id CALL db.cdc.query(id) YIELD id AS change RETURN change", nil))

			// A's ordinary event cursor expired with its events.
			require.Equal(t, "Neo.ClientError.ChangeDataCapture.InvalidIdentifier",
				cdcWireErrorCode(t, before, token, autoCommit, "CALL db.cdc.query($from)", map[string]any{"from": prunedIDs[0]}))

			// A fresh explicit transaction reads the same retained feed from
			// the same marker.
			commit, errs := openHTTPTransaction(t, before, token)
			require.Empty(t, errs)
			ids, names = cdcWireEvents(t, before, token, strings.TrimSuffix(commit, "/commit"), earliest, "")
			require.Equal(t, keptIDs, ids)
			require.Equal(t, keptNames, names)
			httpRows(t, before, token, commit, "RETURN 1", nil)

			// A real shutdown of NornicDB and its database manager, then a
			// reopen of the same directory: the identifier the first session
			// returned still reads the retained feed, so it is bound to the
			// database's persisted identity, not to a rebuilt executor.
			require.NoError(t, before.Stop(t.Context()))
			require.NoError(t, database.Close())
			require.NoError(t, before.dbManager.Close())
			after, restarted, reopened := openPersistentDBConfigTestServer(t, dataDir)
			t.Cleanup(func() {
				_ = after.Stop(context.Background())
				_ = reopened.Close()
				_ = after.dbManager.Close()
			})
			token = "Bearer " + getAuthToken(t, restarted, "admin")
			require.Equal(t, earliest, cdcWireEarliest(t, after, token, autoCommit))
			ids, names = cdcWireEvents(t, after, token, autoCommit, earliest, "")
			require.Equal(t, keptIDs, ids)
			require.Equal(t, keptNames, names)
			require.Equal(t, [][]any{{keptIDs[0]}, {keptIDs[1]}},
				httpRows(t, after, token, autoCommit, "CALL db.cdc.earliest() YIELD id CALL db.cdc.query(id) YIELD id AS change RETURN change", nil))
			require.Equal(t, "Neo.ClientError.ChangeDataCapture.InvalidIdentifier",
				cdcWireErrorCode(t, after, token, autoCommit, "CALL db.cdc.query($from)", map[string]any{"from": prunedIDs[0]}))

			commit, errs = openHTTPTransaction(t, after, token)
			require.Empty(t, errs)
			ids, names = cdcWireEvents(t, after, token, strings.TrimSuffix(commit, "/commit"), earliest, "")
			require.Equal(t, keptIDs, ids)
			require.Equal(t, keptNames, names)
			httpRows(t, after, token, commit, "RETURN 1", nil)
		})
	}
}
