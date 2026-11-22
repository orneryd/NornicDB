package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Opaque user statements enter through executeInternal, while CALL and FOREACH
// bodies are visible to public compilation. Both must retain the literal-only
// coercion prohibition when request parameter facts become available.
func TestNOR79BooleanAdmissionPhases(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, test := range []struct {
			name, projection string
			accepts          bool
		}{
			{"parameter", "NOT $p AS a", true},
			{"literal", "NOT [1] AS a", false},
			{"mixed", "NOT $p AS a, NOT [1] AS b", false},
		} {
			body := "UNWIND [] AS r RETURN " + test.projection
			properties := "a: NOT $p"
			if test.name == "literal" {
				properties = "a: NOT [1]"
			}
			if test.name == "mixed" {
				properties += ", b: NOT [1]"
			}
			for _, wrapper := range []struct{ name, query string }{
				{"opaque", "CALL apoc.cypher.run($body, {p: $p}) YIELD value RETURN value"},
				{"call", "CALL { " + body + " } RETURN a"},
				{"foreach", "UNWIND [] AS r FOREACH (x IN [] | CREATE (:NOR79Phase {" + properties + "})) RETURN 1 AS a"},
			} {
				for _, mode := range norStaticTransactionModes() {
					t.Run(wrapper.name+"/"+test.name+"/"+mode, func(t *testing.T) {
						exec := norStaticFreshExecutor(t)
						if mode == "tx" {
							_, err := exec.Execute(t.Context(), "BEGIN", nil)
							require.NoError(t, err)
						}
						result, err := exec.Execute(t.Context(), wrapper.query, map[string]interface{}{"p": []interface{}{int64(1)}, "body": body})
						norStaticRollback(t, t.Context(), exec, mode)
						messageClass := "Coercion of list to boolean"
						if wrapper.name == "opaque" {
							// APOC preserves the underlying status but names its
							// existing procedure boundary in the message.
							messageClass = "apoc.cypher.run inner query failed: " + messageClass
						}
						mismatch, detail := norClassifyVerdict("RUN", result, err, test.accepts, messageClass)
						require.Equal(t, norVerdictMatches, mismatch, "%s: %s", wrapper.query, detail)
					})
				}
			}
		}
	})
}
