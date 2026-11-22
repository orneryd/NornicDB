package cypher

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Review lane R1 of PR #1826. Test-only file; compiles on the PR head and on
// its base. Every case is answered twice: by the ordinary executor, and by the
// same executor over a store that hides every optional read capability, so it
// can only read whole nodes. The two answers must be the same.

// r1FullReads hides every optional read capability of the wrapped database.
type r1FullReads struct {
	storage.Engine
	namespace string
}

func (f r1FullReads) Namespace() string { return f.namespace }

// r1Spy records the projections a statement asked its point reads and label
// scans for, as evidence next to a wrong row.
type r1Spy struct {
	*storage.NamespacedEngine
	log []string
}

func (s *r1Spy) GetNodeProjected(ctx context.Context, id storage.NodeID, properties []string) (*storage.Node, error) {
	s.log = append(s.log, fmt.Sprintf("GetNodeProjected%q", properties))
	return s.NamespacedEngine.GetNodeProjected(ctx, id, properties)
}

func (s *r1Spy) GetQueryNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	s.log = append(s.log, "GetQueryNode")
	return s.NamespacedEngine.GetQueryNode(ctx, id)
}

func (s *r1Spy) StreamNodesByLabelProjected(ctx context.Context, label string, properties []string, visit func(*storage.Node) error) error {
	s.log = append(s.log, fmt.Sprintf("StreamNodesByLabelProjected%q", properties))
	return s.NamespacedEngine.StreamNodesByLabelProjected(ctx, label, properties, visit)
}

func (s *r1Spy) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	s.log = append(s.log, fmt.Sprintf("StreamNodesWithOptions%q", opts.Projection))
	return s.NamespacedEngine.StreamNodesWithOptions(ctx, opts, visit)
}

func r1Map(m map[string]any) string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+":"+r1Value(m[key]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func r1Value(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case *storage.Node:
		if x == nil {
			return "null"
		}
		return "node" + r1Map(x.Properties)
	case *storage.Edge:
		if x == nil {
			return "null"
		}
		return "edge" + r1Map(x.Properties)
	case map[string]any:
		if _, path := x["_pathResult"]; path {
			nodes, relationships, _, _ := pathValueParts(x)
			return "path" + r1Value(nodes) + fmt.Sprint(len(relationships))
		}
		return r1Map(x)
	case string:
		return "'" + x + "'"
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		parts := make([]string, rv.Len())
		for i := range parts {
			parts[i] = r1Value(rv.Index(i).Interface())
		}
		return "[" + strings.Join(parts, ",") + "]"
	}
	return fmt.Sprintf("%v", v)
}

func r1Rows(result *ExecuteResult, err error) []string {
	if err != nil {
		return []string{"ERROR " + err.Error()}
	}
	out := make([]string, len(result.Rows))
	for i, row := range result.Rows {
		parts := make([]string, len(row))
		for j, value := range row {
			parts[j] = r1Value(value)
		}
		out[i] = strings.Join(parts, " | ")
	}
	sort.Strings(out)
	return out
}

func r1Fixture(t testing.TB, indexed bool) *storage.NamespacedEngine {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "r1")
	seed := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	var statements []string
	if indexed {
		statements = append(statements,
			"CREATE CONSTRAINT r1_doc_key IF NOT EXISTS FOR (n:Doc) REQUIRE n.key IS UNIQUE",
			"CREATE INDEX r1_doc_extra IF NOT EXISTS FOR (n:Doc) ON (n.extra)")
	}
	statements = append(statements,
		"CREATE (:Doc {key:'one', path:'guide', status:'current', extra:7, list:[1,2,3], body:'body one'})"+
			"-[:REL {w:1}]->(:Doc {key:'two', path:'second', status:'old', extra:8, list:[4], body:'body two'})",
		"CREATE (:Other {key:'x', ref:'one'})")
	for _, statement := range statements {
		_, err := seed.Execute(t.Context(), statement, nil)
		require.NoError(t, err, statement)
	}
	return store
}

type r1Case struct{ name, query string }

func r1ProofCases() []r1Case {
	return []r1Case{
		// Control group: the forms the PR's own tests cover.
		{"control/point", "MATCH (n:Doc {key:$k}) RETURN n.key, n.path, n.status"},
		{"control/residual", "MATCH (n:Doc) WHERE n.key=$k AND n.extra=7 RETURN n.path"},
		{"control/repeated-in-one-match", "UNWIND [$k] AS kk MATCH (n:Doc {key:kk}), (n:Doc {extra:7}) RETURN n.key"},
		{"control/three-templates", "MATCH (n:Doc {key:$k}), (m:Other), (n:Doc {extra:7}) RETURN n.key, m.key"},
		{"control/later-unlabelled-pattern", "MATCH (n:Doc {key:$k}) MATCH (n {extra:7}) RETURN n.key"},
		{"control/later-where", "MATCH (n:Doc {key:$k}) MATCH (n:Doc) WHERE n.extra = 7 RETURN n.key"},

		// A. A later read clause names the variable with a label.
		{"A/match-inline", "MATCH (n:Doc {key:$k}) MATCH (n:Doc {extra:7}) RETURN n.key"},
		{"A/with-match-inline", "MATCH (n:Doc {key:$k}) WITH n MATCH (n:Doc {extra:7}) RETURN n.key"},
		{"A/renamed-match-inline", "MATCH (n:Doc {key:$k}) WITH n AS m MATCH (m:Doc {extra:7}) RETURN m.key"},
		{"A/unwind-match-inline", "UNWIND [$k] AS kk MATCH (n:Doc {key:kk}) MATCH (n:Doc {extra:7}) RETURN n.key"},
		{"A/match-traversal-inline", "MATCH (n:Doc {key:$k}) MATCH (n:Doc {extra:7})-[:REL]->(m) RETURN n.key, m.key"},
		{"A/match-traversal-end-inline", "MATCH (n:Doc {key:'two'}) MATCH (m)-[:REL]->(n:Doc {extra:8}) RETURN n.key, m.key"},
		{"A/optional-match-inline", "MATCH (n:Doc {key:$k}) OPTIONAL MATCH (n:Doc {extra:7})-[:REL]->(m) RETURN n.key, m.key"},
		{"A/optional-match-node-inline", "MATCH (n:Doc {key:$k}) OPTIONAL MATCH (n:Doc {extra:7}) RETURN n.key"},
		{"A/where-exists-inline", "MATCH (n:Doc {key:$k}) WHERE EXISTS { (n:Doc {extra:7})-[:REL]->() } RETURN n.key"},
		{"A/where-exists-match-inline", "MATCH (n:Doc {key:$k}) WHERE EXISTS { MATCH (n:Doc {extra:7}) } RETURN n.key"},
		{"A/where-pattern-inline", "MATCH (n:Doc {key:$k}) WHERE (n:Doc {extra:7})-[:REL]->() RETURN n.key"},
		{"A/return-exists-inline", "MATCH (n:Doc {key:$k}) RETURN n.key, EXISTS { (n:Doc {extra:7})-[:REL]->() } AS e"},
		{"A/return-count-inline", "MATCH (n:Doc {key:$k}) RETURN n.key, COUNT { (n:Doc {extra:7})-[:REL]->() } AS c"},
		{"A/return-collect-inline", "MATCH (n:Doc {key:$k}) RETURN n.key, COLLECT { MATCH (n:Doc {extra:7}) RETURN 1 } AS c"},
		{"A/return-comprehension-inline", "MATCH (n:Doc {key:$k}) RETURN n.key, [(n:Doc {extra:7})-[:REL]->(m) | m.key] AS l"},
		{"A/with-where-exists-inline", "MATCH (n:Doc {key:$k}) WITH n WHERE EXISTS { (n:Doc {extra:7})-[:REL]->() } RETURN n.key"},
		{"A/not-exists-inline", "MATCH (n:Doc {key:$k}) WHERE NOT EXISTS { (n:Doc {extra:7})-[:REL]->() } RETURN n.key"},
		{"A/parameter-map", "MATCH (n:Doc {key:$k}) MATCH (n:Doc $extra) RETURN n.key"},

		// B. A path value carries the projected node.
		{"B/zero-length-path", "MATCH p=(n:Doc {key:$k}) RETURN p"},
		{"B/zero-length-nodes", "MATCH p=(n:Doc {key:$k}) RETURN [x IN nodes(p) | x.extra] AS l"},
		{"B/zero-length-nodes-index", "MATCH p=(n:Doc {key:$k}) RETURN n.key, nodes(p)[0].extra AS e"},
		{"B/zero-length-where-length", "MATCH p=(n:Doc {key:$k}) WHERE length(p)=0 AND n.extra=7 RETURN n.key"},
		{"B/zero-length-where-nodes", "MATCH p=(n:Doc {key:$k}) WHERE any(x IN nodes(p) WHERE x.extra=7) RETURN n.key"},
		{"B/zero-length-with", "MATCH p=(n:Doc {key:$k}) WITH p RETURN nodes(p)[0].extra AS e"},
		{"B/zero-length-properties", "MATCH p=(n:Doc {key:$k}) RETURN properties(nodes(p)[0]) AS e"},
		{"B/zero-length-label-scan", "MATCH p=(n:Doc) WHERE n.status='current' RETURN [x IN nodes(p) | x.extra] AS l"},
		{"B/later-path", "MATCH (n:Doc {key:$k}) MATCH p=(n:Doc)-[:REL]->(m) RETURN n.key, [x IN nodes(p) | x.extra] AS l"},
		{"B/later-path-value", "MATCH (n:Doc {key:$k}) MATCH p=(n:Doc)-[:REL]->(m) RETURN p"},
		{"B/later-optional-path", "MATCH (n:Doc {key:$k}) OPTIONAL MATCH p=(n:Doc)-[:REL]->(m) RETURN [x IN nodes(p) | x.extra] AS l"},
		{"B/later-path-comprehension", "MATCH (n:Doc {key:$k}) RETURN n.key, [p=(n:Doc)-[:REL]->(m) | [x IN nodes(p) | x.extra]] AS l"},
		{"B/later-varlength-zero", "MATCH (n:Doc {key:$k}) MATCH (n:Doc)-[:REL*0..1]->(m) RETURN n.key, m.extra"},
		{"B/later-varlength-zero-properties", "MATCH (n:Doc {key:$k}) MATCH (n:Doc)-[:REL*0..1]->(m) RETURN properties(m) AS p"},
		{"B/later-shortest-path", "MATCH (n:Doc {key:$k}) MATCH p=shortestPath((n:Doc)-[:REL*..3]->(m:Doc {key:'two'})) RETURN [x IN nodes(p) | x.extra] AS l"},
		{"B/start-node-function", "MATCH (n:Doc {key:$k}) MATCH (n:Doc)-[r:REL]->(m) RETURN startNode(r).extra AS e, m.key"},
		{"B/same-clause-path", "MATCH p=(n:Doc {key:$k})-[:REL]->(m) RETURN n.key, [x IN nodes(p) | x.extra] AS l"},
		{"B/quantified-path", "MATCH (n:Doc {key:$k}) MATCH p=(n:Doc)-[:REL]->{0,1}(m) RETURN [x IN nodes(p) | x.extra] AS l"},

		// C. The variable is spelled like a keyword.
		{"C/end-properties", "MATCH (end:Doc {key:$k}) RETURN properties(end) AS p"},
		{"C/end-keys", "MATCH (end:Doc {key:$k}) RETURN size(keys(end)) AS p"},
		{"C/end-map-projection", "MATCH (end:Doc {key:$k}) RETURN end {.*} AS p"},
		{"C/end-map-literal", "MATCH (end:Doc {key:$k}) RETURN {node: end} AS m"},
		{"C/end-coalesce", "MATCH (end:Doc {key:$k}) RETURN coalesce(end, null) AS m"},
		{"C/end-case", "MATCH (end:Doc {key:$k}) RETURN CASE WHEN true THEN end END AS m"},
		{"C/end-list-concat", "MATCH (end:Doc {key:$k}) RETURN [end] + [] AS m"},
		{"C/end-comprehension", "MATCH (end:Doc {key:$k}) RETURN [x IN [end] | x.extra] AS m"},
		{"C/end-later-match", "MATCH (end:Doc {key:$k}) MATCH (end)-[:REL]->(m) RETURN end.key, m.key"},
		{"C/end-later-match-inline", "MATCH (end:Doc {key:$k}) MATCH (end {extra:7}) RETURN end.key"},
		{"C/end-later-path", "MATCH (end:Doc {key:$k}) MATCH p=(end)-[:REL]->(m) RETURN [x IN nodes(p) | x.extra] AS l"},
		{"C/end-unwind", "MATCH (end:Doc {key:$k}) UNWIND [end] AS q RETURN q.extra"},
		{"C/desc-properties", "MATCH (desc:Doc {key:$k}) RETURN properties(desc) AS p"},
		{"C/asc-properties", "MATCH (asc:Doc {key:$k}) RETURN properties(asc) AS p"},
		{"C/contains-properties", "MATCH (contains:Doc {key:$k}) RETURN properties(contains) AS p"},
		{"C/starts-properties", "MATCH (starts:Doc {key:$k}) RETURN properties(starts) AS p"},
		{"C/ends-properties", "MATCH (ends:Doc {key:$k}) RETURN properties(ends) AS p"},
		{"C/start-end-pair", "MATCH (start:Doc {key:$k}), (end:Doc {key:'two'}) RETURN properties(start) AS s, properties(end) AS e"},
		{"C/end-collect", "MATCH (end:Doc {key:$k}) RETURN collect(end) AS m"},
		{"C/end-returned", "MATCH (end:Doc {key:$k}) RETURN end"},
		{"C/end-equality", "MATCH (end:Doc {key:$k}) RETURN end = end AS m, end.extra"},

		// D. The inline property map is a parameter.
		{"D/parameter-map", "MATCH (n:Doc $props) RETURN n.path"},
		{"D/parameter-map-unwind", "UNWIND [$props] AS q MATCH (n:Doc {key:q.key}) MATCH (n:Doc $extra) RETURN n.path"},
	}
}

func r1Params() map[string]any {
	return map[string]any{
		"k": "one", "prop": "extra",
		"props": map[string]any{"key": "one", "extra": int64(7)},
		"extra": map[string]any{"extra": int64(7)},
	}
}

func TestR1PR1826ProofAnswersWholeNodeReads(t *testing.T) {
	for _, indexed := range []bool{true, false} {
		for _, test := range r1ProofCases() {
			t.Run(fmt.Sprintf("indexed=%t/%s", indexed, test.name), func(t *testing.T) {
				ctx := t.Context()
				controlStore := r1Fixture(t, indexed)
				control := NewStorageExecutor(r1FullReads{controlStore, "r1"}, QueryCachePolicy{MaxEntries: 0})
				want := r1Rows(control.Execute(ctx, test.query, r1Params()))

				store := r1Fixture(t, indexed)
				spy := &r1Spy{NamespacedEngine: store}
				exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
				evidence := func() []string {
					spy.log = nil
					_, _ = NewStorageExecutor(spy, QueryCachePolicy{MaxEntries: 0}).Execute(ctx, test.query, r1Params())
					return spy.log
				}
				check := func(stage string, got []string) {
					if !reflect.DeepEqual(want, got) {
						t.Errorf("%s: %s\n  whole-node answer: %q\n  answer:            %q\n  reads: %v", stage, test.query, want, got, evidence())
					}
				}
				check("cold", r1Rows(exec.Execute(ctx, test.query, r1Params())))
				check("repeated", r1Rows(exec.Execute(ctx, test.query, r1Params())))
				_, err := exec.Execute(ctx, "MATCH (n:Doc) RETURN n", nil)
				require.NoError(t, err)
				check("after a whole-node read", r1Rows(exec.Execute(ctx, test.query, r1Params())))
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				check("explicit transaction", r1Rows(exec.Execute(ctx, test.query, r1Params())))
				_, err = exec.Execute(ctx, "ROLLBACK", nil)
				require.NoError(t, err)
			})
		}
	}
}
