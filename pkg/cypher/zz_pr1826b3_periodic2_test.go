package cypher

// Author: pr1826-base3 test lane (Claude Opus 5.5), 23 Nov 2026.
// Follow-up probes for apoc.periodic.iterate with a SET/REMOVE action:
//  1. the same actions with a leading WITH (the form the default parser's
//     start-clause list admits), run through apoc.periodic.iterate;
//  2. the node image the iterate statement returns (deciding value for the
//     property loss seen when the action stores it), with a non-keyword
//     variable and a plain node as controls.
// The tests assert nothing: every outcome is in its PR1826B3W / PR1826B3IMG line.

import (
	"fmt"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func pr1826b3Keys(value interface{}) string {
	var props map[string]interface{}
	switch v := value.(type) {
	case *storage.Node:
		if v == nil {
			return "nil-node"
		}
		props = v.Properties
	case storage.Node:
		props = v.Properties
	case map[string]interface{}:
		if inner, ok := v["properties"].(map[string]interface{}); ok {
			props = inner
		} else {
			props = v
		}
	case nil:
		return "nil"
	default:
		return fmt.Sprintf("type=%T", value)
	}
	keys := make([]string, 0, len(props))
	for key := range props {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return fmt.Sprintf("type=%T keys=%v", value, keys)
}

const (
	pr1826b3Keyword    = `MATCH (end:Doc {key:'one'}) RETURN coalesce(end, null) AS x`
	pr1826b3Path       = `MATCH p=(n:Doc {key:'one'}) RETURN nodes(p)[0] AS x`
	pr1826b3Plain      = `MATCH (x:Doc {key:'one'}) RETURN x`
	pr1826b3NonKeyword = `MATCH (e:Doc {key:'one'}) RETURN coalesce(e, null) AS x`
	pr1826b3PathWhole  = `MATCH p=(n:Doc {key:'one'}) RETURN n AS x`
)

func TestPR1826B3PeriodicWithAction(t *testing.T) {
	for _, test := range []struct{ name, iterate, action string }{
		// The five forms of the reproduction with the action prefixed by WITH.
		{"with-r1-keyword", pr1826b3Keyword, "WITH $x AS x SET x.flag = 1"},
		{"with-r1-path", pr1826b3Path, "WITH $x AS x SET x.flag = 1"},
		{"with-plain-node", pr1826b3Plain, "WITH $x AS x SET x.flag = 1"},
		{"with-manual-label", `MATCH (p:Doc) WHERE p.key = 'one' RETURN p`, "WITH $p AS p SET p:Actor"},
		{"with-remove", pr1826b3Plain, "WITH $x AS x REMOVE x.body"},
		// The binding spelled without a parameter sign.
		{"withbare-plain-node", pr1826b3Plain, "WITH x AS x SET x.flag = 1"},
		{"withbare-r1-path", pr1826b3Path, "WITH x AS x SET x.flag = 1"},
		// Re-reading the node by id inside the action.
		{"rematch-r1-path", pr1826b3Path, "MATCH (y) WHERE id(y) = id($x) SET y.flag = 1"},
		// Controls for the property loss: a non-keyword variable, and the path's node returned directly.
		{"bare-nonkeyword", pr1826b3NonKeyword, "SET x.flag = 1"},
		{"bare-path-whole", pr1826b3PathWhole, "SET x.flag = 1"},
		{"with-nonkeyword", pr1826b3NonKeyword, "WITH $x AS x SET x.flag = 1"},
		{"with-path-whole", pr1826b3PathWhole, "WITH $x AS x SET x.flag = 1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			exec := pr1826b3Executor(t)
			query := fmt.Sprintf("CALL apoc.periodic.iterate(%q, %q, {batchSize: 10})", test.iterate, test.action)
			result, err := exec.Execute(t.Context(), query, nil)
			got := pr1826b3One(t, exec)
			if err != nil || result == nil || len(result.Rows) != 1 {
				t.Logf("PR1826B3W case=%s parser=%s error=%v stored=%s", test.name, config.GetParserType(), err, got)
				return
			}
			row := result.Rows[0]
			t.Logf("PR1826B3W case=%s parser=%s committed=%v failed=%v errorMessages=%v stored=%s",
				test.name, config.GetParserType(), row[3], row[4], row[7], got)
		})
	}
}

func TestPR1826B3IterateImage(t *testing.T) {
	for _, test := range []struct{ name, query string }{
		{"r1-keyword", pr1826b3Keyword},
		{"r1-path", pr1826b3Path},
		{"plain-node", pr1826b3Plain},
		{"nonkeyword", pr1826b3NonKeyword},
		{"path-whole", pr1826b3PathWhole},
	} {
		t.Run(test.name, func(t *testing.T) {
			exec := pr1826b3Executor(t)
			result, err := exec.Execute(t.Context(), test.query, nil)
			if err != nil || result == nil || len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
				t.Logf("PR1826B3IMG case=%s parser=%s error=%v rows=%v", test.name, config.GetParserType(), err, result)
				return
			}
			t.Logf("PR1826B3IMG case=%s parser=%s rows=%d x=%s", test.name, config.GetParserType(), len(result.Rows), pr1826b3Keys(result.Rows[0][0]))
		})
	}
}
