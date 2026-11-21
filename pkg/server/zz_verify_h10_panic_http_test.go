package server

// Verifier of hunt lead L-H10-004 (test-side only): what an HTTP client sees
// for a statement with one extra ')' before '{' on POST /db/nornic/tx/commit,
// and whether the same server then still runs a write and a read.

import (
	"encoding/json"
	"testing"
)

func TestVerifyH10ExtraParenHTTP(t *testing.T) {
	server, users := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, users, "admin")
	post := func(stmt string) (int, string) {
		resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{{"statement": stmt}}}, token)
		body := resp.Body.String()
		if len(body) > 1500 {
			body = body[:1500]
		}
		return resp.Code, body
	}
	emit := func(step, role, stmt string, code int, body string) {
		data, _ := json.Marshal(map[string]any{"lead": "L-H10-004", "route": "http", "step": step, "role": role, "statement": stmt, "status": code, "body": body})
		t.Logf("VERIFY_H10_PANIC_HTTP %s", data)
	}
	for _, c := range []struct{ step, role, stmt string }{
		{"smallest", "fail", ") {}"},
		{"call-subquery-extra-paren", "fail", "MATCH (n) CALL (n)) { RETURN 1 AS y } RETURN y"},
		{"control-extra-paren-no-brace", "control", "MATCH (n)) RETURN n"},
		{"after-write", "control", "CREATE (:VerifyH10 {k: 1})"},
		{"after-read", "control", "MATCH (n:VerifyH10) RETURN count(n) AS c"},
	} {
		code, body := post(c.stmt)
		emit(c.step, c.role, c.stmt, code, body)
		var parsed struct {
			Errors []struct{ Code, Message string } `json:"errors"`
		}
		_ = json.Unmarshal([]byte(body), &parsed)
		syntaxError := code == 200 && len(parsed.Errors) > 0 && parsed.Errors[0].Code == "Neo.ClientError.Statement.SyntaxError"
		switch c.role {
		case "fail":
			if !syntaxError {
				t.Errorf("DISAGREE %s: expected a Neo.ClientError.Statement.SyntaxError, got status %d body %s", c.step, code, body)
			}
		case "control":
			if c.step == "control-extra-paren-no-brace" && !syntaxError {
				t.Errorf("control %s: expected a syntax error, got %d %s", c.step, code, body)
			}
			if c.step != "control-extra-paren-no-brace" && (code != 200 || len(parsed.Errors) > 0) {
				t.Errorf("control %s failed: %d %s", c.step, code, body)
			}
		}
	}
}
