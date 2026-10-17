package cypher

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The reference closure's tests use a library-like graph: Item nodes are
// selected with their HAS relationships, nodes and relationships name other
// nodes by id in reference properties (parent_id, owner_id, author_id,
// editor_id, cites, related_ids) in the domains Item, Note, Person and Topic,
// and LINK relationships are manual (source 'manual') or generated ones; a
// generated LINK with a reason is selected as referenced.

var referenceClosureNames = []string{"parent_id", "owner_id", "author_id", "editor_id", "cites", "related_ids"}

var referenceClosureDomains = []string{"Item", "Note", "Person", "Topic"}

// referenceClosureCall is the closure as one call: Items and HAS selected with
// the manual links, generated links with a reason as referenced, and from every
// reached node its generated links, plus every generated link between found
// nodes. Each seed list is its own COLLECT subquery: collecting one after
// another in WITH would group each later row by the earlier lists.
const referenceClosureCall = `
WITH COLLECT { MATCH (n:Item) RETURN n } AS items,
     COLLECT { MATCH ()-[h:HAS]->() RETURN h } AS has,
     COLLECT { MATCH ()-[u:LINK {source: 'manual'}]->() RETURN u } AS manual,
     COLLECT { MATCH ()-[x:LINK {source: 'generated'}]->() WHERE x.reason IS NOT NULL RETURN x } AS reasoned
CALL nornicdb.graph.expandReferences(
  [{category: 'selected', nodes: items, relationships: has + manual}, {category: 'referenced', relationships: reasoned}],
  {references: $names, domains: $domains,
   expand: [{relationshipFilter: 'LINK>', where: "relationship.source = 'generated'", fromSeeds: false}],
   between: [{relationshipFilter: 'LINK', where: "relationship.source = 'generated'"}],
   category: 'referenced'})
YIELD node, relationship, category
RETURN coalesce(elementId(node), elementId(relationship)) AS element, category`

func referenceClosureParams() map[string]interface{} {
	names := make([]interface{}, len(referenceClosureNames))
	for index, name := range referenceClosureNames {
		names[index] = name
	}
	domains := make([]interface{}, len(referenceClosureDomains))
	for index, domain := range referenceClosureDomains {
		domains[index] = domain
	}
	return map[string]interface{}{"names": names, "domains": domains}
}

// runReferenceClosure runs query and returns each element's category, in
// row order; an element twice fails.
func runReferenceClosure(t testing.TB, exec *StorageExecutor, ctx context.Context, query string, params map[string]interface{}) ([]string, map[string]string) {
	t.Helper()
	result, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	order := make([]string, 0, len(result.Rows))
	categories := make(map[string]string, len(result.Rows))
	for _, row := range result.Rows {
		element, category := row[0].(string), row[1].(string)
		_, twice := categories[element]
		require.False(t, twice, "element %s returned twice", element)
		categories[element] = category
		order = append(order, element)
	}
	return order, categories
}

// referenceClosureNamesOf maps elementId to a readable name: Label:id for a
// node, the name property for a relationship.
func referenceClosureNamesOf(t testing.TB, exec *StorageExecutor, ctx context.Context) map[string]string {
	t.Helper()
	result, err := exec.Execute(ctx, `MATCH (n) RETURN elementId(n) AS element, labels(n)[0] + ':' + coalesce(toString(n.id), '<null>') AS name
UNION ALL MATCH ()-[r]->() RETURN elementId(r) AS element, r.name AS name`, nil)
	require.NoError(t, err)
	names := make(map[string]string, len(result.Rows))
	for _, row := range result.Rows {
		names[row[0].(string)] = row[1].(string)
	}
	return names
}

// referenceClosureFixture covers each rule once; see the expectations in
// TestExpandReferencesFixture.
const referenceClosureFixture = `
CREATE (i1:Item {id: 'i1', parent_id: 's1'}), (i2:Item {id: 'i2', cites: ['n1', 'x9']}),
       (s1:Topic {id: 's1'}), (n1:Note {id: 'n1', editor_id: 'p1'}),
       (p1:Person {id: 'p1'}), (p1b:Note {id: 'p1'}),
       (v1:Note {id: 'v1'}), (e1:Person {id: 'e1'}), (n2:Note {id: 'n2', owner_id: 7}),
       (s7:Topic {id: 7}), (e2:Person {id: 'e2'}), (e3:Person {id: 'e3'}), (e4:Person {id: 'e4'}),
       (q1:Person {id: 'q1'}), (e5:Person {id: 'e5'}), (z1:Note {id: 'z1'}), (lone:Person {id: 'lone'})
CREATE (i1)-[:HAS {name: 'h1'}]->(v1),
       (n1)-[:LINK {name: 'l1', source: 'generated', author_id: 'n2'}]->(e1),
       (i1)-[:LINK {name: 'l2', source: 'generated'}]->(e2),
       (i2)-[:LINK {name: 'l3', source: 'generated'}]->(s1),
       (e3)-[:LINK {name: 'l4', source: 'generated'}]->(n1),
       (n1)-[:LINK {name: 'l6', source: 'other'}]->(e4),
       (i2)-[:LINK {name: 'u1', source: 'manual'}]->(q1),
       (q1)-[:LINK {name: 'l5', source: 'generated'}]->(e5),
       (z1)-[:LINK {name: 'a1', source: 'generated', reason: 'why'}]->(i1)`

func newReferenceClosureFixture(t testing.TB) (*StorageExecutor, context.Context) {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, referenceClosureFixture, nil)
	require.NoError(t, err)
	return exec, ctx
}

// TestExpandReferencesFixture pins the closure of the fixture: references
// in every domain (one id finds a Person and a Note), list and integer
// references, references held by a relationship, endpoints (a seeded link's
// start), branches only from reached nodes (i1's own generated link isn't
// followed; i2's is found once its end is), outgoing only for expand (e3's
// link to n1 isn't), and the where predicate (n1's link of another source isn't).
func TestExpandReferencesFixture(t *testing.T) {
	exec, ctx := newReferenceClosureFixture(t)
	names := referenceClosureNamesOf(t, exec, ctx)
	order, categories := runReferenceClosure(t, exec, ctx, referenceClosureCall, referenceClosureParams())
	got := map[string]string{}
	for element, category := range categories {
		got[names[element]] = category
	}
	require.Equal(t, map[string]string{
		"Item:i1": "selected", "Item:i2": "selected", "h1": "selected", "u1": "selected", "a1": "referenced",
		"Note:v1": "referenced", "Person:q1": "referenced", "Note:z1": "referenced",
		"Topic:s1": "referenced", "Note:n1": "referenced", "Person:p1": "referenced", "Note:p1": "referenced",
		"l1": "referenced", "Person:e1": "referenced", "Note:n2": "referenced", "Topic:7": "referenced",
		"l3": "referenced", "l5": "referenced", "Person:e5": "referenced",
	}, got)
	// Seeds first: the nodes, then the relationships, each after its ends.
	readable := make([]string, 0, 8)
	for _, element := range order[:8] {
		readable = append(readable, names[element])
	}
	require.ElementsMatch(t, []string{"Item:i1", "Item:i2"}, readable[:2])
	require.Equal(t, []string{"Note:v1", "h1", "Person:q1", "u1", "Note:z1", "a1"}, readable[2:])
}

// TestExpandReferencesMatchesRoundLoop is the acceptance test: the closure
// equals, element for element and category for category, the client's round
// loop (referenceClosureRounds) it replaces, on the fixture and on seeded
// random graphs with shared ids across domains, an index on some domains
// only, references on nodes and relationships, cycles and missing names.
func TestExpandReferencesMatchesRoundLoop(t *testing.T) {
	t.Run("fixture", func(t *testing.T) {
		exec, ctx := newReferenceClosureFixture(t)
		// The round loop follows STRING references only; the fixture's one
		// INTEGER reference becomes a STRING that names nothing.
		_, err := exec.Execute(ctx, "MATCH (n:Note {id: 'n2'}) SET n.owner_id = '7'", nil)
		require.NoError(t, err)
		_, got := runReferenceClosure(t, exec, ctx, referenceClosureCall, referenceClosureParams())
		require.Equal(t, referenceClosureRounds(t, exec, ctx), got)
	})
	for seed := int64(1); seed <= 12; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			ctx := context.Background()
			buildRandomReferenceGraph(t, exec, ctx, seed, 120, 220)
			_, got := runReferenceClosure(t, exec, ctx, referenceClosureCall, referenceClosureParams())
			want := referenceClosureRounds(t, exec, ctx)
			require.Equal(t, want, got)
			require.Greater(t, len(got), 0)
		})
	}
}

// buildRandomReferenceGraph creates nodes nodes over the four domains with
// ids from a pool smaller than the graph (so ids repeat within and across
// domains), reference properties naming pool ids and some missing ones, and
// links relationships (HAS, and manual, generated and reasoned LINKs, some
// naming ids themselves). Item and Person ids are indexed.
func buildRandomReferenceGraph(t testing.TB, exec *StorageExecutor, ctx context.Context, seed int64, nodes, links int) {
	t.Helper()
	random := rand.New(rand.NewSource(seed))
	pool := nodes * 3 / 4
	id := func() string {
		if random.Intn(10) == 0 {
			return fmt.Sprintf("missing%d", random.Intn(5))
		}
		return fmt.Sprintf("k%d", random.Intn(pool))
	}
	references := func(props map[string]interface{}, chance int) {
		for _, name := range referenceClosureNames {
			if random.Intn(chance) != 0 {
				continue
			}
			if strings.HasSuffix(name, "s") {
				list := make([]interface{}, random.Intn(3))
				for index := range list {
					list[index] = id()
				}
				props[name] = list
			} else {
				props[name] = id()
			}
		}
	}
	rows := make([]interface{}, nodes)
	for index := range rows {
		props := map[string]interface{}{"id": fmt.Sprintf("k%d", random.Intn(pool))}
		references(props, 9)
		rows[index] = map[string]interface{}{"key": index, "label": referenceClosureDomains[random.Intn(len(referenceClosureDomains))], "props": props}
	}
	for _, domain := range referenceClosureDomains {
		_, err := exec.Execute(ctx, "UNWIND $rows AS row WITH row WHERE row.label = $label CREATE (n:"+domain+") SET n = row.props, n.key = row.key", map[string]interface{}{"rows": rows, "label": domain})
		require.NoError(t, err)
	}
	for _, index := range []string{"CREATE INDEX FOR (n:Item) ON (n.id)", "CREATE INDEX FOR (n:Person) ON (n.id)"} {
		_, err := exec.Execute(ctx, index, nil)
		require.NoError(t, err)
	}
	edges := make([]interface{}, links)
	for index := range edges {
		props := map[string]interface{}{"name": fmt.Sprintf("r%d", index)}
		kind := "LINK"
		switch random.Intn(6) {
		case 0:
			kind = "HAS"
		case 1:
			props["source"] = "manual"
		case 2:
			props["source"] = "generated"
			props["reason"] = "why"
		default:
			props["source"] = "generated"
		}
		references(props, 6)
		edges[index] = map[string]interface{}{"a": random.Intn(nodes), "b": random.Intn(nodes), "kind": kind, "props": props}
	}
	for _, kind := range []string{"HAS", "LINK"} {
		_, err := exec.Execute(ctx, "UNWIND $edges AS edge WITH edge WHERE edge.kind = $kind MATCH (a {key: edge.a}), (b {key: edge.b}) CREATE (a)-[r:"+kind+"]->(b) SET r = edge.props", map[string]interface{}{"edges": edges, "kind": kind})
		require.NoError(t, err)
	}
}

// referenceClosureRounds is the client loop the procedure replaces, kept
// as the oracle: the seeds read by three statements, then one round of
// lookups per step (names in each domain, link starts not yet found, the
// generated links of new unseeded nodes, and the generated links to newly reached
// nodes whose start is found) until a round adds nothing. It returns each
// element's part, selected or referenced.
func referenceClosureRounds(t testing.TB, exec *StorageExecutor, ctx context.Context) map[string]string {
	t.Helper()
	parts := map[string]string{}
	asked := map[string]bool{}
	var names, elements, unseeded, reached map[string]bool
	reset := func() {
		names, elements, unseeded, reached = map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}
	}
	reset()
	follow := func(props map[string]interface{}) {
		for _, name := range referenceClosureNames {
			values, isList := cypherListValue(props[name])
			if !isList {
				values = []interface{}{props[name]}
			}
			for _, value := range values {
				if text, ok := value.(string); ok && !asked[text] {
					names[text] = true
				}
			}
		}
	}
	node := func(element string, props map[string]interface{}, part string) {
		if _, found := parts[element]; found {
			return
		}
		parts[element] = part
		follow(props)
		reached[element] = true
		delete(elements, element)
		if part == "referenced" {
			unseeded[element] = true
		}
	}
	link := func(row []interface{}, part string) {
		if _, found := parts[row[0].(string)]; found {
			return
		}
		node(row[4].(string), row[5].(map[string]interface{}), "referenced")
		if _, found := parts[row[2].(string)]; !found {
			elements[row[2].(string)] = true
		}
		parts[row[0].(string)] = part
		follow(row[1].(map[string]interface{}))
	}
	const linkRow = "elementId(r), properties(r), elementId(a), properties(a), elementId(b), properties(b)"
	run := func(query string, params map[string]interface{}) [][]interface{} {
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		return result.Rows
	}
	for _, row := range run("MATCH (n:Item) RETURN elementId(n), properties(n)", nil) {
		node(row[0].(string), row[1].(map[string]interface{}), "selected")
	}
	for _, row := range run("MATCH (a)-[r:HAS]->(b) RETURN "+linkRow, nil) {
		link(row, "selected")
	}
	for _, row := range run("MATCH (a)-[r:LINK]->(b) WHERE r.source = 'manual' OR r.reason IS NOT NULL RETURN "+linkRow+", r.source <> 'manual'", nil) {
		part := "selected"
		if row[6].(bool) {
			part = "referenced"
		}
		link(row, part)
	}
	sorted := func(set map[string]bool) []interface{} {
		keys := make([]string, 0, len(set))
		for key := range set {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out := make([]interface{}, len(keys))
		for index, key := range keys {
			out[index] = key
		}
		return out
	}
	for len(names) > 0 || len(elements) > 0 || len(reached) > 0 {
		round := map[string]interface{}{"names": sorted(names), "elements": sorted(elements), "unseeded": sorted(unseeded), "reached": sorted(reached)}
		for name := range names {
			asked[name] = true
		}
		reset()
		for _, domain := range referenceClosureDomains {
			for _, row := range run("UNWIND $names AS name MATCH (b:"+domain+" {id: name}) RETURN elementId(b), properties(b)", round) {
				node(row[0].(string), row[1].(map[string]interface{}), "referenced")
			}
		}
		for _, row := range run("UNWIND $elements AS element MATCH (b) WHERE elementId(b) = element RETURN elementId(b), properties(b)", round) {
			node(row[0].(string), row[1].(map[string]interface{}), "referenced")
		}
		for _, row := range run("UNWIND $unseeded AS start MATCH (a) WHERE elementId(a) = start MATCH (a)-[r:LINK]->(b) WHERE r.source = 'generated' RETURN "+linkRow, round) {
			link(row, "referenced")
		}
		for _, row := range run("UNWIND $reached AS end MATCH (b) WHERE elementId(b) = end MATCH (a)-[r:LINK]->(b) WHERE r.source = 'generated' RETURN "+linkRow, round) {
			if _, found := parts[row[2].(string)]; found {
				link(row, "referenced")
			}
		}
	}
	return parts
}

// TestExpandReferencesRules pins the settings one at a time on small graphs.
func TestExpandReferencesRules(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, `CREATE (a:A {id: 'a', ref: 'b', key: 'ka'}), (b:B {id: 'b', ref: 'c', more: [3]}), (c:C {id: 'c'}), (d:C {id: 3}), (e:C {id: 3.0}),
(f:A {id: 'f'}), (g:B {code: 'b'})
CREATE (a)-[:R {name: 'ar', w: 1}]->(f), (f)-[:R {name: 'fg', w: 2}]->(g), (c)-[:S {name: 'cf', w: 3}]->(f)`, nil)
	require.NoError(t, err)
	names := referenceClosureNamesOf(t, exec, ctx)
	closure := func(seeds, config string) map[string]string {
		_, categories := runReferenceClosure(t, exec, ctx, "MATCH (a {id: 'a'}), (c {id: 'c'}) OPTIONAL MATCH ()-[ar {name: 'ar'}]->() CALL nornicdb.graph.expandReferences("+seeds+", "+config+") YIELD node, relationship, category RETURN coalesce(elementId(node), elementId(relationship)), category", nil)
		got := map[string]string{}
		for element, category := range categories {
			got[names[element]] = category
		}
		return got
	}
	// Without domains, references are not followed.
	require.Equal(t, map[string]string{"A:a": "seed"}, closure("a", "{references: ['ref']}"))
	// An integer name finds an integer and an integral float id.
	require.Equal(t, map[string]string{"A:a": "seed", "B:b": "reached", "C:c": "reached", "C:3": "reached", "C:3.0": "reached"},
		closure("[a]", "{references: ['ref', 'more'], domains: ['B', 'C']}"))
	// idProperty: g's code is 'b'.
	require.Equal(t, map[string]string{"A:a": "seed", "B:<null>": "reached"},
		closure("[a]", "{references: ['ref'], domains: ['B'], idProperty: 'code'}"))
	// expand with no filter follows every relationship both ways;
	// relationshipFilter and where narrow it.
	require.Equal(t, map[string]string{"A:a": "seed", "ar": "reached", "A:f": "reached", "fg": "reached", "B:<null>": "reached", "cf": "reached", "C:c": "reached"},
		closure("[a]", "{expand: [{}]}"))
	require.Equal(t, map[string]string{"A:a": "seed", "ar": "reached", "A:f": "reached", "fg": "reached", "B:<null>": "reached"},
		closure("[a]", "{expand: [{relationshipFilter: 'R>'}]}"))
	require.Equal(t, map[string]string{"A:a": "seed", "ar": "reached", "A:f": "reached"},
		closure("[a]", "{expand: [{relationshipFilter: 'R>', where: 'relationship.w < 2 AND start.id = \"a\" AND end.id = \"f\"'}]}"))
	// fromSeeds false: nothing reached, nothing followed.
	require.Equal(t, map[string]string{"A:a": "seed"}, closure("[a]", "{expand: [{fromSeeds: false}]}"))
	// A seeded relationship brings its ends; between adds only relationships
	// between found nodes.
	require.Equal(t, map[string]string{"ar": "seed", "A:a": "reached", "A:f": "reached"},
		closure("[ar]", "{between: [{relationshipFilter: 'R'}]}"))
	require.Equal(t, map[string]string{"A:a": "seed", "C:c": "reached", "B:b": "reached", "C:3": "reached", "C:3.0": "reached", "A:f": "reached", "cf": "reached", "ar": "reached"},
		closure("[a, null]", "{references: ['ref', 'more'], domains: ['B', 'C'], between: [{}], expand: [{relationshipFilter: 'S>'}]}"))
	// between follows a relationship whatever its filter's direction once both
	// ends are found, with start and end bound for where; one whose end is
	// found later (f, by c's expand, after a was followed) waits for it.
	require.Equal(t, map[string]string{"A:a": "seed", "C:c": "seed", "cf": "reached", "A:f": "reached", "ar": "reached"},
		closure("[a, c]", "{expand: [{relationshipFilter: 'S>'}], between: [{relationshipFilter: '<R'}]}"))
	require.Equal(t, map[string]string{"A:a": "seed", "C:c": "seed", "cf": "reached", "A:f": "reached"},
		closure("[a, c]", "{expand: [{relationshipFilter: 'S>'}], between: [{relationshipFilter: 'R', where: 'start.id = \"f\"'}]}"))
	// The first category wins, seed nodes before relationships; groups and
	// bare elements mix.
	require.Equal(t, map[string]string{"A:a": "two", "ar": "one", "A:f": "x"},
		closure("[{category: 'one', relationships: [ar]}, {nodes: [a], category: 'two'}, ar, a, {category: 'three', nodes: [a]}]", "{category: 'x'}"))
	require.Equal(t, map[string]string{"A:a": "seed", "ar": "seed", "A:f": "reached"}, closure("[ar, a]", "{}"))
	// Null seeds: no rows.
	require.Empty(t, closure("null", "{}"))
}

// TestExpandReferencesSeesTransactionWrites: the closure reads the caller's
// transaction, its uncommitted writes included.
func TestExpandReferencesSeesTransactionWrites(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:A {id: 'a', ref: 'b'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:B {id: 'b', ref: 'c'}), (:B {id: 'c'})", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (a:A) CALL nornicdb.graph.expandReferences([a], {references: ['ref'], domains: ['B']}) YIELD node RETURN collect(node.id) AS ids", nil)
	require.NoError(t, err)
	require.Equal(t, []interface{}{"a", "b", "c"}, result.Rows[0][0])
	_, err = exec.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)
	result, err = exec.Execute(ctx, "MATCH (a:A) CALL nornicdb.graph.expandReferences([a], {references: ['ref'], domains: ['B']}) YIELD node RETURN collect(node.id) AS ids", nil)
	require.NoError(t, err)
	require.Equal(t, []interface{}{"a"}, result.Rows[0][0])
}

// TestExpandReferencesArgumentErrors: GDS's configuration messages for the
// configuration and its branches, a TypeError for a seed of another type.
func TestExpandReferencesArgumentErrors(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:A {id: 'a'})-[:R]->(:A {id: 'b'})", nil)
	require.NoError(t, err)
	for _, test := range []struct{ call, message string }{
		{"[a], {depth: 2}", "Unexpected configuration key: depth"},
		{"[a], {references: 3}", "The value of `references` must be of type `List`"},
		{"[a], {domains: [1]}", "The value of `domains` must be of type `List`"},
		{"[a], {idProperty: 1}", "The value of `idProperty` must be of type `String`"},
		{"[a], {category: true}", "The value of `category` must be of type `String`"},
		{"[a], {expand: {}}", "The value of `expand` must be of type `List`"},
		{"[a], {expand: [1]}", "The value of `expand` must be of type `List of Map`"},
		{"[a], {expand: [{where: 1}]}", "The value of `where` must be of type `String`"},
		{"[a], {expand: [{fromSeeds: 'no'}]}", "The value of `fromSeeds` must be of type `Boolean`"},
		{"[a], {between: [{fromSeeds: false}]}", "Unexpected configuration key: fromSeeds"},
		{"[{nodes: [r]}], {}", "The value of `nodes` must be of type `List of Node`"},
		{"[{relationships: [a]}], {}", "The value of `relationships` must be of type `List of Relationship`"},
		{"[{nodes: a}], {}", "The value of `nodes` must be of type `List`"},
		{"[{kind: 'x'}], {}", "Unexpected configuration key: kind"},
		{"['a'], {}", "LIST<NODE | RELATIONSHIP | MAP>"},
		{"[a], 'x'", "MAP"},
	} {
		_, err := exec.Execute(ctx, "MATCH (a {id: 'a'})-[r]->() CALL nornicdb.graph.expandReferences("+test.call+") YIELD node RETURN node", nil)
		require.Error(t, err, test.call)
		require.Contains(t, err.Error(), test.message, test.call)
	}
}
