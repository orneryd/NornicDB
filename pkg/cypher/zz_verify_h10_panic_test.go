package cypher_test

// Verifier of hunt lead L-H10-004 (v1.5.2 verification campaign, test-side
// only): StorageExecutor.Execute on statements with one extra ')' before '{'.
// Each case runs on its own empty store; a panic is recovered and reported.
// "fail" cases must return an error, not panic; "control" cases must not
// panic (one is a syntax error of the same shape without '{', one is the valid
// CALL subquery).

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

var verifyH10PanicCases = []struct{ name, role, q string }{
	{"smallest", "fail", ") {}"},
	{"call-subquery-extra-paren", "fail", "MATCH (n) CALL (n)) { RETURN 1 AS y } RETURN y"},
	{"create-map-extra-paren", "fail", "CREATE (n:Person)) {name: 'a'}"},
	{"return-extra-paren", "fail", "RETURN 1) {}"},
	{"where-extra-paren", "fail", "MATCH (n) WHERE n.x = 1) {}"},
	{"call-proc-extra-paren", "fail", "CALL db.labels()) {}"},
	{"return-paren-extra-paren", "fail", "RETURN (1)) {}"},
	{"match-extra-paren", "fail", "MATCH (n)) {}"},
	{"control-extra-paren-no-brace", "control", "MATCH (n)) RETURN n"},
	{"control-valid-call-subquery", "control", "MATCH (n) CALL (n) { RETURN 1 AS y } RETURN y"},
}

func verifyH10RunRecovered(q string) (panicText, stack string, err error) {
	engine := storage.NewMemoryEngine()
	defer engine.Close()
	exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer func() {
		if r := recover(); r != nil {
			panicText = fmt.Sprint(r)
			s := string(debug.Stack())
			if i := strings.Index(s, "panic("); i >= 0 {
				s = s[i:]
			}
			if len(s) > 2500 {
				s = s[:2500]
			}
			stack = s
		}
	}()
	_, err = exec.Execute(ctx, q, nil)
	return
}

func TestVerifyH10ExtraParenPanic(t *testing.T) {
	for _, tc := range verifyH10PanicCases {
		tc := tc
		t.Run("L-H10-004/"+tc.name, func(t *testing.T) {
			p, stack, err := verifyH10RunRecovered(tc.q)
			rec := map[string]any{"lead": "L-H10-004", "case": tc.name, "role": tc.role, "statement": tc.q,
				"parser_config": config.GetParserType(), "panicked": p != ""}
			if p != "" {
				rec["panic"] = p
			}
			if err != nil {
				rec["error"] = err.Error()
			}
			data, _ := json.Marshal(rec)
			t.Logf("VERIFY_H10_PANIC %s", data)
			if p != "" {
				t.Logf("VERIFY_H10_PANIC_STACK %s:\n%s", tc.name, stack)
				t.Errorf("DISAGREE Execute panicked: %s", p)
			}
			if tc.name == "control-valid-call-subquery" && err != nil {
				t.Errorf("control: valid statement failed: %v", err)
			}
			if tc.name == "control-extra-paren-no-brace" && err == nil {
				t.Errorf("control: malformed statement accepted")
			}
		})
	}
}
