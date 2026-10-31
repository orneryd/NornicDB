package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// TestHTTPHiddenTargetAnswersAsAbsentWhereverItIsSelected compares the whole
// answer, through the real route, executor and allowlist, for a graph the
// principal may not access and one that does not exist, selected where
// execution may not reach it or may fail first.
func TestHTTPHiddenTargetAnswersAsAbsentWhereverItIsSelected(t *testing.T) {
	server, authenticator := setupTestServer(t)
	ctx := t.Context()
	dbName := server.dbManager.DefaultDatabaseName()
	require.NoError(t, server.dbManager.CreateDatabase(ctx, multidb.DatabaseCreateRequest{Name: "private"}))
	require.NoError(t, server.dbManager.CreateCompositeDatabase(ctx, "vis", []multidb.ConstituentRef{
		{Alias: "pub", DatabaseName: dbName, Type: "local", AccessMode: "read_write"},
		{Alias: "sec", DatabaseName: "private", Type: "local", AccessMode: "read_write"},
	}))
	require.NoError(t, server.dbManager.CreateCompositeDatabase(ctx, "hiddencomp", []multidb.ConstituentRef{
		{Alias: "ro", DatabaseName: "private", Type: "local", AccessMode: "read"},
	}))
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(context.Background(), "viewer", []string{dbName, "vis"}))
	require.NoError(t, server.privilegesStore.SavePrivilege(context.Background(), "viewer", dbName, true, false))
	token := getAuthToken(t, authenticator, "reader")

	answer := func(session, statement string, names ...string) (int, string) {
		recorder := makeRequest(t, server, http.MethodPost, "/db/"+session+"/tx/commit", map[string]any{
			"statements": []map[string]any{{"statement": statement}},
		}, "Bearer "+token)
		shape := map[string]any{}
		require.NoError(t, json.NewDecoder(recorder.Body).Decode(&shape))
		delete(shape, "lastBookmarks")
		headers := recorder.Header().Clone()
		headers.Del("Date")
		headers.Del("Content-Length")
		encoded, err := json.Marshal(map[string]any{"body": shape, "headers": headers})
		require.NoError(t, err)
		text := string(encoded)
		for _, name := range names {
			text = strings.ReplaceAll(text, name, "NAME")
		}
		return recorder.Code, text
	}
	for _, scenario := range []struct{ session, hidden, missing string }{
		{"vis", "vis.sec", "vis.abs"},
		{dbName, "hiddencomp", "absentcomp"},
		{dbName, "hiddencomp.ro", "absentcomp.ro"},
		{dbName, "private", "missing"},
	} {
		for _, form := range []string{
			"USE {name} UNWIND [] AS u RETURN u",
			"UNWIND [] AS u CALL { USE {name} RETURN 1 AS x } RETURN x",
			"UNWIND [1/0] AS u CALL { USE {name} RETURN 1 AS x } RETURN x",
			"MATCH (n:NoSuchLabel) CALL { USE {name} RETURN 1 AS x } RETURN x",
			"UNWIND [] AS u OPTIONAL CALL { USE {name} RETURN 1 AS x } RETURN x",
			"UNWIND [] AS u CALL { USE {name} CREATE (n) } RETURN u",
			"EXPLAIN UNWIND [] AS u CALL { USE {name} RETURN 1 AS x } RETURN x",
			"UNWIND [] AS u CALL { USE graph.byName('{name}') RETURN 1 AS x } RETURN x",
			"UNWIND [1] AS u CALL { USE graph.byName('{name}') RETURN 1 AS x } RETURN x",
		} {
			names := []string{scenario.hidden, scenario.missing}
			absentCode, absentAnswer := answer(scenario.session, strings.ReplaceAll(form, "{name}", scenario.missing), names...)
			hiddenCode, hiddenAnswer := answer(scenario.session, strings.ReplaceAll(form, "{name}", scenario.hidden), names...)
			require.Equal(t, absentCode, hiddenCode, "%s on %s", form, scenario.hidden)
			require.Equal(t, absentAnswer, hiddenAnswer, "%s on %s", form, scenario.hidden)
		}
	}
}
