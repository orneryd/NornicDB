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
	post := func(path string, stmts ...string) pdmResponse {
		var list []map[string]interface{}
		for _, s := range stmts {
			list = append(list, map[string]interface{}{"statement": s})
		}
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
}
