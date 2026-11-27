package server

// Regression for N152-262 / report C57: one statement deletes a relationship
// and MERGEs the same pattern again. MERGE's relationship identity is
// deterministic, so the re-created relationship takes the identity the DELETE
// recorded for the statement. NornicDB then treated that live relationship as
// deleted: SET counted its properties but stored none, property reads failed
// with EntityNotFound, and a later DELETE in the statement replaced the row's
// live relationship with the deleted view.
//
// Every case runs over the public HTTP transaction API in autocommit (each
// statement its own POST /tx/commit) and in one explicit transaction (POST
// /tx, POST /tx/{id} per statement, POST /tx/{id}/commit). The expected
// results are Neo4j 5.26.30's for these statements: after DELETE the MERGE
// creates a new live relationship, so its SET persists, its properties read,
// `RETURN r` returns it, and a later DELETE of another entity leaves it alone.
// The controls pin the neighbouring routes that already behave: a MERGE with a
// different identity, CREATE, a relationship an earlier CREATE made, separate
// statements, SET in a later statement, and a node deleted and MERGE-d again.

import (
	"encoding/json"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// pdmClean removes every entity these cases create.
const pdmClean = "MATCH (n) WHERE n:PdmX OR n:PdmY OR n:PdmZ DETACH DELETE n"

const (
	pdmTwoNodes = "CREATE (:PdmX {id:'a'}), (:PdmX {id:'b'})"
	pdmRelN1    = "MATCH (a:PdmX {id:'a'}) MATCH (b:PdmX {id:'b'}) MERGE (a)-[r:PDM_R]->(b) SET r.n = 1"
	pdmRelK1N1  = "MATCH (a:PdmX {id:'a'}) MATCH (b:PdmX {id:'b'}) MERGE (a)-[r:PDM_R {k: 1}]->(b) SET r.n = 1"
	pdmDelete   = "CALL { MATCH (:PdmX)-[old:PDM_R]->(:PdmX) DELETE old RETURN count(*) AS removed }"
	pdmCheck    = "MATCH (:PdmX)-[r:PDM_R]->(:PdmX) RETURN count(r) AS c, collect(properties(r)) AS props"
)

// pdmSet compares a collection without order: Neo4j does not promise the order
// of labels(n).
type pdmSet []any

// pdmCounters is the result summary Neo4j 5.26.30 reports for a step, limited
// to the counters this regression is about.
type pdmCounters struct {
	propertiesSet        int
	relationshipsCreated int
	relationshipsDeleted int
}

// pdmStep is one statement of a case and the result Neo4j returns for it.
// A nil rows ignores the result rows (a write-only statement); a nil counters
// ignores the statement's summary.
type pdmStep struct {
	statement string
	rows      [][]any
	counters  *pdmCounters
}

// pdmVariant is one statement shape under test: its setup, the statements
// under test, and the post-commit read that shows what the statement left.
type pdmVariant struct {
	name  string
	group string
	setup []string
	steps []pdmStep
	check pdmStep
}

var pdmVariants = []pdmVariant{
	{
		name:  "call-set",
		group: "reported",
		setup: []string{pdmTwoNodes, pdmRelN1},
		steps: []pdmStep{{
			statement: pdmDelete + " CALL { MATCH (a:PdmX {id:'a'}) MATCH (b:PdmX {id:'b'}) MERGE (a)-[r:PDM_R]->(b) SET r.n = 2 RETURN count(*) AS written } RETURN removed, written",
			rows:      [][]any{{1, 1}},
			counters:  &pdmCounters{propertiesSet: 1, relationshipsCreated: 1, relationshipsDeleted: 1},
		}},
		check: pdmStep{statement: pdmCheck, rows: [][]any{{1, []any{map[string]any{"n": 2}}}}},
	},
	{
		name:  "call-set-plus",
		group: "reported",
		setup: []string{pdmTwoNodes, pdmRelN1},
		steps: []pdmStep{{
			statement: pdmDelete + " CALL { MATCH (a:PdmX {id:'a'}) MATCH (b:PdmX {id:'b'}) MERGE (a)-[r:PDM_R]->(b) SET r += {n: 2} RETURN count(*) AS written } RETURN removed, written",
			rows:      [][]any{{1, 1}},
			counters:  &pdmCounters{propertiesSet: 1, relationshipsCreated: 1, relationshipsDeleted: 1},
		}},
		check: pdmStep{statement: pdmCheck, rows: [][]any{{1, []any{map[string]any{"n": 2}}}}},
	},
	{
		name:  "call-set-replace",
		group: "reported",
		setup: []string{pdmTwoNodes, pdmRelN1},
		steps: []pdmStep{{
			statement: pdmDelete + " CALL { MATCH (a:PdmX {id:'a'}) MATCH (b:PdmX {id:'b'}) MERGE (a)-[r:PDM_R]->(b) SET r = {n: 2} RETURN count(*) AS written } RETURN removed, written",
			rows:      [][]any{{1, 1}},
		}},
		check: pdmStep{statement: pdmCheck, rows: [][]any{{1, []any{map[string]any{"n": 2}}}}},
	},
	{
		name:  "call-on-create",
		group: "reported",
		setup: []string{pdmTwoNodes, pdmRelN1},
		steps: []pdmStep{{
			statement: pdmDelete + " CALL { MATCH (a:PdmX {id:'a'}) MATCH (b:PdmX {id:'b'}) MERGE (a)-[r:PDM_R]->(b) ON CREATE SET r.n = 2 RETURN count(*) AS written } RETURN removed, written",
			rows:      [][]any{{1, 1}},
			counters:  &pdmCounters{propertiesSet: 1, relationshipsCreated: 1, relationshipsDeleted: 1},
		}},
		check: pdmStep{statement: pdmCheck, rows: [][]any{{1, []any{map[string]any{"n": 2}}}}},
	},
	{
		name:  "with-set",
		group: "reported",
		setup: []string{pdmTwoNodes, pdmRelN1},
		steps: []pdmStep{{
			statement: pdmDelete + " WITH removed MATCH (a:PdmX {id:'a'}) MATCH (b:PdmX {id:'b'}) MERGE (a)-[r:PDM_R]->(b) SET r.n = 2 RETURN removed, count(*) AS written",
			rows:      [][]any{{1, 1}},
			counters:  &pdmCounters{propertiesSet: 1, relationshipsCreated: 1, relationshipsDeleted: 1},
		}},
		check: pdmStep{statement: pdmCheck, rows: [][]any{{1, []any{map[string]any{"n": 2}}}}},
	},
	{
		name:  "chain-set",
		group: "reported",
		setup: []string{pdmTwoNodes, pdmRelN1},
		steps: []pdmStep{{
			statement: "MATCH (a:PdmX {id:'a'})-[old:PDM_R]->(b:PdmX {id:'b'}) DELETE old WITH a, b MERGE (a)-[r:PDM_R]->(b) SET r.n = 2 RETURN count(*) AS written",
			rows:      [][]any{{1}},
			counters:  &pdmCounters{propertiesSet: 1, relationshipsCreated: 1, relationshipsDeleted: 1},
		}},
		check: pdmStep{statement: pdmCheck, rows: [][]any{{1, []any{map[string]any{"n": 2}}}}},
	},
	{
		name:  "chain-inline-same-identity-set",
		group: "extra",
		setup: []string{pdmTwoNodes, pdmRelK1N1},
		steps: []pdmStep{{
			statement: "MATCH (a:PdmX {id:'a'})-[old:PDM_R]->(b:PdmX {id:'b'}) DELETE old WITH a, b MERGE (a)-[r:PDM_R {k: 1}]->(b) SET r.n = 2 RETURN count(*) AS written",
			rows:      [][]any{{1}},
			counters:  &pdmCounters{propertiesSet: 2, relationshipsCreated: 1, relationshipsDeleted: 1},
		}},
		check: pdmStep{statement: pdmCheck, rows: [][]any{{1, []any{map[string]any{"k": 1, "n": 2}}}}},
	},
	{
		name:  "ctl-inline-property",
		group: "control",
		setup: []string{pdmTwoNodes, pdmRelN1},
		steps: []pdmStep{{
			statement: pdmDelete + " CALL { MATCH (a:PdmX {id:'a'}) MATCH (b:PdmX {id:'b'}) MERGE (a)-[r:PDM_R {n: 2}]->(b) RETURN count(*) AS written } RETURN removed, written",
			rows:      [][]any{{1, 1}},
		}},
		check: pdmStep{statement: pdmCheck, rows: [][]any{{1, []any{map[string]any{"n": 2}}}}},
	},
	{
		name:  "ctl-create",
		group: "control",
		setup: []string{pdmTwoNodes, pdmRelN1},
		steps: []pdmStep{{
			statement: pdmDelete + " CALL { MATCH (a:PdmX {id:'a'}) MATCH (b:PdmX {id:'b'}) CREATE (a)-[r:PDM_R]->(b) SET r.n = 2 RETURN count(*) AS written } RETURN removed, written",
			rows:      [][]any{{1, 1}},
		}},
		check: pdmStep{statement: pdmCheck, rows: [][]any{{1, []any{map[string]any{"n": 2}}}}},
	},
	{
		name:  "ctl-old-made-by-create",
		group: "control",
		setup: []string{pdmTwoNodes, "MATCH (a:PdmX {id:'a'}) MATCH (b:PdmX {id:'b'}) CREATE (a)-[r:PDM_R {n: 1}]->(b)"},
		steps: []pdmStep{{
			statement: pdmDelete + " CALL { MATCH (a:PdmX {id:'a'}) MATCH (b:PdmX {id:'b'}) MERGE (a)-[r:PDM_R]->(b) SET r.n = 2 RETURN count(*) AS written } RETURN removed, written",
			rows:      [][]any{{1, 1}},
		}},
		check: pdmStep{statement: pdmCheck, rows: [][]any{{1, []any{map[string]any{"n": 2}}}}},
	},
	{
		name:  "ctl-two-statements",
		group: "control",
		setup: []string{pdmTwoNodes, pdmRelN1},
		steps: []pdmStep{
			{statement: "MATCH (:PdmX)-[old:PDM_R]->(:PdmX) DELETE old"},
			{statement: "MATCH (a:PdmX {id:'a'}) MATCH (b:PdmX {id:'b'}) MERGE (a)-[r:PDM_R]->(b) SET r.n = 2"},
		},
		check: pdmStep{statement: pdmCheck, rows: [][]any{{1, []any{map[string]any{"n": 2}}}}},
	},
	{
		name:  "ctl-set-next-statement",
		group: "control",
		setup: []string{pdmTwoNodes, pdmRelN1},
		steps: []pdmStep{
			{statement: pdmDelete + " CALL { MATCH (a:PdmX {id:'a'}) MATCH (b:PdmX {id:'b'}) MERGE (a)-[r:PDM_R]->(b) RETURN count(*) AS written } RETURN removed, written", rows: [][]any{{1, 1}}},
			{statement: "MATCH (:PdmX)-[r:PDM_R]->(:PdmX) SET r.n = 2"},
		},
		check: pdmStep{statement: pdmCheck, rows: [][]any{{1, []any{map[string]any{"n": 2}}}}},
	},
	{
		name:  "ctl-node-delete-remerge",
		group: "control",
		setup: []string{"CREATE (:PdmY {id:'y', v: 1})"},
		steps: []pdmStep{{
			statement: "MATCH (y:PdmY {id:'y'}) DELETE y WITH count(*) AS removed MERGE (n:PdmY {id:'y'}) SET n.v = 2, n:PdmZ RETURN removed, n.v AS v, labels(n) AS l",
			rows:      [][]any{{1, 2, pdmSet{"PdmY", "PdmZ"}}},
		}},
		check: pdmStep{
			statement: "MATCH (n:PdmY) RETURN count(n) AS c, collect(properties(n)) AS props, collect(labels(n)) AS labels",
			rows:      [][]any{{1, []any{map[string]any{"id": "y", "v": 2}}, []any{pdmSet{"PdmY", "PdmZ"}}}},
		},
	},
	{
		name:  "other-remove",
		group: "other",
		setup: []string{pdmTwoNodes, pdmRelK1N1},
		steps: []pdmStep{{
			statement: "MATCH (a:PdmX {id:'a'})-[old:PDM_R]->(b:PdmX {id:'b'}) DELETE old WITH a, b MERGE (a)-[r:PDM_R {k: 1}]->(b) REMOVE r.k RETURN count(*) AS written",
			rows:      [][]any{{1}},
		}},
		check: pdmStep{statement: pdmCheck, rows: [][]any{{1, []any{map[string]any{}}}}},
	},
	{
		name:  "other-on-match-set",
		group: "other",
		setup: []string{pdmTwoNodes, pdmRelN1},
		steps: []pdmStep{{
			statement: "MATCH (a:PdmX {id:'a'})-[old:PDM_R]->(b:PdmX {id:'b'}) DELETE old WITH a, b MERGE (a)-[r:PDM_R]->(b) WITH a, b MERGE (a)-[r2:PDM_R]->(b) ON MATCH SET r2.m = 5 RETURN count(*) AS written",
			rows:      [][]any{{1}},
		}},
		check: pdmStep{statement: pdmCheck, rows: [][]any{{1, []any{map[string]any{"m": 5}}}}},
	},
	{
		name:  "other-read-inline",
		group: "other",
		setup: []string{pdmTwoNodes, pdmRelK1N1},
		steps: []pdmStep{{
			statement: "MATCH (a:PdmX {id:'a'})-[old:PDM_R]->(b:PdmX {id:'b'}) DELETE old WITH a, b MERGE (a)-[r:PDM_R {k: 1}]->(b) RETURN r.k AS k, properties(r) AS p, keys(r) AS ks, type(r) AS t, r {.*} AS m",
			rows:      [][]any{{1, map[string]any{"k": 1}, []any{"k"}, "PDM_R", map[string]any{"k": 1}}},
		}},
		check: pdmStep{statement: pdmCheck, rows: [][]any{{1, []any{map[string]any{"k": 1}}}}},
	},
	{
		name:  "other-read-after-set",
		group: "other",
		setup: []string{pdmTwoNodes, pdmRelN1},
		steps: []pdmStep{{
			statement: "MATCH (a:PdmX {id:'a'})-[old:PDM_R]->(b:PdmX {id:'b'}) DELETE old WITH a, b MERGE (a)-[r:PDM_R]->(b) SET r.n = 2 RETURN r.n AS n, properties(r) AS p",
			rows:      [][]any{{2, map[string]any{"n": 2}}},
		}},
		check: pdmStep{statement: pdmCheck, rows: [][]any{{1, []any{map[string]any{"n": 2}}}}},
	},
	{
		name:  "other-later-delete-view",
		group: "other",
		setup: []string{pdmTwoNodes, pdmRelK1N1, "CREATE (:PdmZ {id:'z'})"},
		steps: []pdmStep{{
			statement: "MATCH (a:PdmX {id:'a'})-[old:PDM_R]->(b:PdmX {id:'b'}) DELETE old WITH a, b MERGE (a)-[r:PDM_R {k: 1}]->(b) WITH r MATCH (z:PdmZ {id:'z'}) DELETE z RETURN properties(r) AS p, r IS NULL AS gone",
			rows:      [][]any{{map[string]any{"k": 1}, false}},
		}},
		check: pdmStep{statement: pdmCheck, rows: [][]any{{1, []any{map[string]any{"k": 1}}}}},
	},
	{
		name:  "other-where-filter",
		group: "other",
		setup: []string{pdmTwoNodes, pdmRelK1N1},
		steps: []pdmStep{{
			statement: "MATCH (a:PdmX {id:'a'})-[old:PDM_R]->(b:PdmX {id:'b'}) DELETE old WITH a, b MERGE (a)-[r:PDM_R {k: 1}]->(b) WITH r WHERE r.k = 1 RETURN count(*) AS kept",
			rows:      [][]any{{1}},
		}},
		check: pdmStep{statement: pdmCheck, rows: [][]any{{1, []any{map[string]any{"k": 1}}}}},
	},
	{
		name:  "other-return-r",
		group: "other",
		setup: []string{pdmTwoNodes, pdmRelK1N1},
		steps: []pdmStep{{
			statement: "MATCH (a:PdmX {id:'a'})-[old:PDM_R]->(b:PdmX {id:'b'}) DELETE old WITH a, b MERGE (a)-[r:PDM_R {k: 1}]->(b) RETURN r, type(r) AS t",
			rows:      [][]any{{map[string]any{"k": 1}, "PDM_R"}},
		}},
		check: pdmStep{statement: pdmCheck, rows: [][]any{{1, []any{map[string]any{"k": 1}}}}},
	},
	{
		name:  "other-later-delete-return-r",
		group: "other",
		setup: []string{pdmTwoNodes, pdmRelK1N1, "CREATE (:PdmZ {id:'z'})"},
		steps: []pdmStep{{
			statement: "MATCH (a:PdmX {id:'a'})-[old:PDM_R]->(b:PdmX {id:'b'}) DELETE old WITH a, b MERGE (a)-[r:PDM_R {k: 1}]->(b) WITH r MATCH (z:PdmZ {id:'z'}) DELETE z RETURN r, type(r) AS t",
			rows:      [][]any{{map[string]any{"k": 1}, "PDM_R"}},
		}},
		check: pdmStep{statement: pdmCheck, rows: [][]any{{1, []any{map[string]any{"k": 1}}}}},
	},
	{
		name:  "other-later-delete-set",
		group: "other",
		setup: []string{pdmTwoNodes, pdmRelN1, "CREATE (:PdmZ {id:'z'})"},
		steps: []pdmStep{{
			statement: "MATCH (a:PdmX {id:'a'})-[old:PDM_R]->(b:PdmX {id:'b'}) DELETE old WITH a, b MERGE (a)-[r:PDM_R]->(b) WITH r MATCH (z:PdmZ {id:'z'}) DELETE z WITH r SET r.n = 2 RETURN count(*) AS written",
			rows:      [][]any{{1}},
		}},
		check: pdmStep{statement: pdmCheck, rows: [][]any{{1, []any{map[string]any{"n": 2}}}}},
	},
}

// pdmSame compares a Neo4j result value (want, written as Go literals) with a
// decoded HTTP row value (got): numbers compare by value, pdmSet ignores order.
func pdmSame(want, got any) bool {
	switch expected := want.(type) {
	case nil:
		return got == nil
	case pdmSet:
		actual, ok := got.([]any)
		if !ok || len(actual) != len(expected) {
			return false
		}
		remaining := append([]any(nil), actual...)
		for _, item := range expected {
			matched := -1
			for index, other := range remaining {
				if pdmSame(item, other) {
					matched = index
					break
				}
			}
			if matched < 0 {
				return false
			}
			remaining = append(remaining[:matched], remaining[matched+1:]...)
		}
		return true
	case []any:
		actual, ok := got.([]any)
		if !ok || len(actual) != len(expected) {
			return false
		}
		for index := range expected {
			if !pdmSame(expected[index], actual[index]) {
				return false
			}
		}
		return true
	case map[string]any:
		actual, ok := got.(map[string]any)
		if !ok || len(actual) != len(expected) {
			return false
		}
		for key, value := range expected {
			other, present := actual[key]
			if !present || !pdmSame(value, other) {
				return false
			}
		}
		return true
	case int:
		return pdmNumber(float64(expected), got)
	case int64:
		return pdmNumber(float64(expected), got)
	case float64:
		return pdmNumber(expected, got)
	}
	return reflect.DeepEqual(want, got)
}

func pdmNumber(want float64, got any) bool {
	switch actual := got.(type) {
	case float64:
		return actual == want
	case int:
		return float64(actual) == want
	case int64:
		return float64(actual) == want
	}
	return false
}

// pdmPost sends one HTTP transaction request and returns the decoded response
// with its status code. Errors are left to the caller's assertions.
func pdmPost(t *testing.T, server *Server, token, path string, statements []string, includeStats bool) (TransactionResponse, int) {
	t.Helper()
	list := make([]map[string]any, 0, len(statements))
	for _, statement := range statements {
		entry := map[string]any{"statement": statement}
		if includeStats {
			entry["includeStats"] = true
		}
		list = append(list, entry)
	}
	recorder := makeRequest(t, server, http.MethodPost, path, map[string]any{"statements": list}, token)
	var response TransactionResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response),
		"POST %s: status %d: %.600s", path, recorder.Code, recorder.Body.String())
	return response, recorder.Code
}

// pdmRequireNoErrors fails with the response's errors and the statement text.
func pdmRequireNoErrors(t *testing.T, response TransactionResponse, statement string) {
	t.Helper()
	if len(response.Errors) == 0 {
		return
	}
	raw, _ := json.Marshal(response.Errors)
	t.Fatalf("statement %q failed: %s", statement, raw)
}

// pdmRequireStep asserts the result Neo4j returns for one statement.
func pdmRequireStep(t *testing.T, response TransactionResponse, step pdmStep) {
	t.Helper()
	pdmRequireNoErrors(t, response, step.statement)
	require.Len(t, response.Results, 1, "one result per statement")
	result := response.Results[0]
	if step.rows != nil {
		require.Len(t, result.Data, len(step.rows), "rows of %q", step.statement)
		for index, expected := range step.rows {
			actual := result.Data[index].Row
			require.True(t, pdmSame(expected, actual),
				"row %d of %q:\nwant: %#v\n got: %#v", index, step.statement, expected, actual)
		}
	}
	if step.counters != nil {
		require.NotNil(t, result.Stats, "summary of %q", step.statement)
		require.Equal(t, step.counters.propertiesSet, result.Stats.PropertiesSet, "properties_set of %q", step.statement)
		require.Equal(t, step.counters.relationshipsCreated, result.Stats.RelationshipsCreated, "relationships_created of %q", step.statement)
		require.Equal(t, step.counters.relationshipsDeleted, result.Stats.RelationshipsDeleted, "relationship_deleted of %q", step.statement)
	}
}

// TestPDMergeDeletedRelationship runs every statement shape in both
// transaction modes over the HTTP API.
func TestPDMergeDeletedRelationship(t *testing.T) {
	server, authenticator := setupTestServerWithDatabaseConfig(t, nil, nil)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	database := server.dbManager.DefaultDatabaseName()
	autocommitPath := "/db/" + database + "/tx/commit"
	txRoot := "/db/" + database + "/tx"

	autocommit := func(t *testing.T, statement string, includeStats bool) TransactionResponse {
		t.Helper()
		response, status := pdmPost(t, server, token, autocommitPath, []string{statement}, includeStats)
		require.Equal(t, http.StatusOK, status, "autocommit status of %q", statement)
		return response
	}
	reset := func(t *testing.T) {
		t.Helper()
		pdmRequireNoErrors(t, autocommit(t, pdmClean, false), pdmClean)
	}

	for _, variant := range pdmVariants {
		for _, mode := range []string{"autocommit", "explicit"} {
			t.Run(variant.name+"/"+mode, func(t *testing.T) {
				reset(t)
				for _, setup := range variant.setup {
					pdmRequireNoErrors(t, autocommit(t, setup, false), setup)
				}
				switch mode {
				case "autocommit":
					for _, step := range variant.steps {
						pdmRequireStep(t, autocommit(t, step.statement, step.counters != nil), step)
					}
				case "explicit":
					opened, status := pdmPost(t, server, token, txRoot, nil, false)
					require.Equal(t, http.StatusCreated, status)
					pdmRequireNoErrors(t, opened, "open transaction")
					require.NotEmpty(t, opened.Commit, "open transaction commit URL")
					commit, err := url.Parse(opened.Commit)
					require.NoError(t, err)
					txPath := strings.TrimSuffix(commit.Path, "/commit")
					require.NotEqual(t, commit.Path, txPath, "commit URL %q names a transaction", opened.Commit)
					for _, step := range variant.steps {
						response, status := pdmPost(t, server, token, txPath, []string{step.statement}, step.counters != nil)
						require.Equal(t, http.StatusOK, status, "transaction status of %q", step.statement)
						pdmRequireStep(t, response, step)
					}
					committed, status := pdmPost(t, server, token, commit.Path, nil, false)
					require.Equal(t, http.StatusOK, status, "commit status")
					pdmRequireNoErrors(t, committed, "commit")
				default:
					t.Fatalf("unknown transaction mode %q", mode)
				}
				pdmRequireStep(t, autocommit(t, variant.check.statement, false), variant.check)
			})
		}
	}
}
