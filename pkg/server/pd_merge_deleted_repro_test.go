package server

// Reproduction for a relationship that one statement deletes and MERGEs again
// (repro only, not a product test). For every variant it runs the setup, the
// statements under test and a read after commit, over HTTP in autocommit
// (each statement its own POST /tx/commit) and in one explicit transaction
// (POST /tx, POST /tx/{id} per statement, POST /tx/{id}/commit). It prints
// what each request returned; it asserts nothing.

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"testing"
)

type pdmVariant struct {
	name   string
	group  string
	setup  []string
	stmts  []string
	check  string
}

type pdmResult struct {
	Columns []string `json:"columns"`
	Data    []struct {
		Row []interface{} `json:"row"`
	} `json:"data"`
}

type pdmResponse struct {
	Commit  string            `json:"commit"`
	Results []pdmResult       `json:"results"`
	Errors  []json.RawMessage `json:"errors"`
}

func pdmRows(r pdmResponse) string {
	var parts []string
	for _, res := range r.Results {
		var rows []string
		for _, d := range res.Data {
			b, _ := json.Marshal(d.Row)
			rows = append(rows, string(b))
		}
		parts = append(parts, fmt.Sprintf("%v=%s", res.Columns, strings.Join(rows, ";")))
	}
	errs := ""
	for _, e := range r.Errors {
		errs += string(e)
	}
	return fmt.Sprintf("rows=%s errors=%s", strings.Join(parts, "|"), errs)
}

func TestPDMergeDeletedRelationship(t *testing.T) {
	server, authenticator := setupTestServerWithDatabaseConfig(t, nil, nil)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	db := server.dbManager.DefaultDatabaseName()
	postWithParams := func(path string, list []map[string]interface{}) pdmResponse {
		body := map[string]interface{}{"statements": list}
		if list == nil {
			body["statements"] = []interface{}{}
		}
		rec := makeRequest(t, server, "POST", path, body, token)
		var resp pdmResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("%s: status %d: %v: %.400s", path, rec.Code, err, rec.Body.String())
		}
		return resp
	}
	post := func(path string, stmts ...string) pdmResponse {
		var list []map[string]interface{}
		for _, s := range stmts {
			list = append(list, map[string]interface{}{"statement": s})
		}
		return postWithParams(path, list)
	}
	auto := func(stmt string) pdmResponse { return post("/db/"+db+"/tx/commit", stmt) }
	for _, v := range pdmVariants {
		for _, mode := range []string{"autocommit", "explicit"} {
			if r := auto(pdmClean); len(r.Errors) > 0 {
				t.Fatalf("clean: %s", pdmRows(r))
			}
			for _, s := range v.setup {
				if r := auto(s); len(r.Errors) > 0 {
					t.Fatalf("%s setup %q: %s", v.name, s, pdmRows(r))
				}
			}
			switch mode {
			case "autocommit":
				for i, s := range v.stmts {
					t.Logf("PDM_STMT variant=%s group=%s mode=%s step=%d %s", v.name, v.group, mode, i, pdmRows(auto(s)))
				}
			case "explicit":
				open := post("/db/" + db + "/tx")
				u, err := url.Parse(open.Commit)
				if err != nil || open.Commit == "" {
					t.Fatalf("%s: open tx: %q %s", v.name, open.Commit, pdmRows(open))
				}
				txPath := strings.TrimSuffix(u.Path, "/commit")
				for i, s := range v.stmts {
					t.Logf("PDM_STMT variant=%s group=%s mode=%s step=%d %s", v.name, v.group, mode, i, pdmRows(post(txPath, s)))
				}
				t.Logf("PDM_COMMIT variant=%s mode=%s %s", v.name, mode, pdmRows(post(u.Path)))
			}
			t.Logf("PDM_CHECK variant=%s group=%s mode=%s %s", v.name, v.group, mode, pdmRows(auto(v.check)))
		}
	}
	pdmRunProductComparisons(t, db, postWithParams)
}

func pdmRunProductComparisons(t *testing.T, db string, post func(string, []map[string]interface{}) pdmResponse) {
	t.Helper()
	txRoot := "/db/" + db + "/tx"
	statement := func(query string, params map[string]interface{}) map[string]interface{} {
		return map[string]interface{}{"statement": query, "parameters": params}
	}
	check := func(label string, result pdmResponse) pdmResponse {
		t.Helper()
		if len(result.Errors) > 0 {
			t.Fatalf("%s: %s", label, pdmRows(result))
		}
		return result
	}
	auto := func(query string, params map[string]interface{}) pdmResponse {
		return check(query, post(txRoot+"/commit", []map[string]interface{}{statement(query, params)}))
	}
	const remove = "MATCH (a:PDRecordSet {collection_id: $collection})-[old:PD_RECORDS_COMPARED]->(b:PDRecordSet) " +
		"WHERE a.id IN $changed OR b.id IN $changed DELETE old RETURN count(*) AS removed"
	const write = "UNWIND $comparisons AS x MATCH (a:PDRecordSet {id: x.a}) MATCH (b:PDRecordSet {id: x.b}) " +
		"MERGE (a)-[r:PD_RECORDS_COMPARED]->(b) SET r += x.properties RETURN count(*) AS written"
	const combined = "CALL { " + remove + " } CALL { " + write + " } RETURN removed, written"
	auto("CREATE CONSTRAINT pd_w161_record_set IF NOT EXISTS FOR (n:PDRecordSet) REQUIRE n.id IS UNIQUE", nil)

	for _, size := range []int{1, 200} {
		for _, form := range []string{"one", "two"} {
			ids := make([]string, size+1)
			for i := range ids {
				ids[i] = fmt.Sprintf("w161-set-%d", i)
			}
			comparisons := func(score int) []map[string]interface{} {
				rows := make([]map[string]interface{}, size)
				for i := range rows {
					rows[i] = map[string]interface{}{
						"a": ids[0],
						"b": ids[i+1],
						"properties": map[string]interface{}{
							"score": score,
							"basis": fmt.Sprintf("b%d-%d", score, i+1),
						},
					}
				}
				return rows
			}
			auto("MATCH (n:PDRecordSet {collection_id:'wa-tip'}) DETACH DELETE n", nil)
			auto("UNWIND $ids AS id CREATE (:PDRecordSet {id:id, collection_id:'wa-tip', kind:'group'})",
				map[string]interface{}{"ids": ids})
			auto(write, map[string]interface{}{"comparisons": comparisons(1)})

			begin := check("open product transaction", post(txRoot, nil))
			commit, err := url.Parse(begin.Commit)
			if err != nil || begin.Commit == "" {
				t.Fatalf("open product transaction: %q %v", begin.Commit, err)
			}
			txPath := strings.TrimSuffix(commit.Path, "/commit")
			params := map[string]interface{}{"collection": "wa-tip", "changed": []string{ids[0]}, "comparisons": comparisons(2)}
			steps := []map[string]interface{}{statement(combined, params)}
			if form == "two" {
				steps = []map[string]interface{}{statement(remove, params), statement(write, params)}
			}
			for i, step := range steps {
				result := check("product step", post(txPath, []map[string]interface{}{step}))
				t.Logf("PDW161_PRODUCT_STMT size=%d form=%s step=%d %s", size, form, i, pdmRows(result))
			}
			check("commit product transaction", post(commit.Path, nil))

			result := auto("MATCH (a:PDRecordSet {collection_id:'wa-tip'})-[r:PD_RECORDS_COMPARED]->(b:PDRecordSet) "+
				"RETURN count(r) AS links, count(r.score) AS with_score, "+
				"sum(CASE WHEN r.score = 2 THEN 1 ELSE 0 END) AS new_score, "+
				"sum(CASE WHEN r.basis STARTS WITH 'b2-' THEN 1 ELSE 0 END) AS new_basis", nil)
			t.Logf("PDW161_PRODUCT_CHECK size=%d form=%s %s", size, form, pdmRows(result))
		}
	}
}
