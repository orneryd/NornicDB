// Package dynamiclabels defines the public dynamic-label regression corpus shared
// by embedded execution and Bolt, including the Neo4j 5.26 reference controls.
package dynamiclabels

import "strings"

// Fixture is reset before each case so writes cannot affect later controls.
const Fixture = "CREATE (a:Anchor {id: 0}), (n:A {id: 1}), (m:A:B {id: 2}), (b:B {id: 3}), (:C {id: 4}), (a)-[:R {p: 'x'}]->(n), (a)-[:R {p: 'x'}]->(m), (a)-[:R {p: 'x'}]->(b)"

// Case specifies rows or a Neo4j status code for a public statement.
type Case struct {
	Name string
	Query string
	Params map[string]any
	Rows [][]any
	Code string
	Cypher25 bool
	ReferenceCode string
	ReferenceMessage string
}

// Cases covers each dynamic-label spelling, value source, enclosing query form
// and mutation. Cypher 25 expression predicates are NornicDB controls; Neo4j
// 5.26 rejects those forms, which are included separately as error controls.
func Cases() []Case {
	one := [][]any{{int64(2)}}
	a := [][]any{{int64(1)}, {int64(2)}}
	anyAB := [][]any{{int64(1)}, {int64(2)}, {int64(3)}}
	var cases []Case
	for _, form := range []struct { name, text string; rows [][]any }{
		{"string", "$('A')", a},
		{"list", "$(['A','B'])", one},
		{"all", "$all(['A','B'])", one},
		{"any", "$any(['A','B'])", anyAB},
		{"upper-any", "$ANY(['A','B'])", anyAB},
		{"parameter-string", "$($label)", a},
		{"parameter-list", "$($labels)", one},
		{"parameter-all", "$all($labels)", one},
		{"parameter-any", "$any($labels)", anyAB},
		{"row-list", "$(ls)", one},
		{"row-all", "$all(ls)", one},
		{"row-any", "$any(ls)", anyAB},
		{"graph-expression", "$(CASE WHEN EXISTS { MATCH (:Anchor) } THEN 'A' ELSE 'Missing' END)", a},
		{"graph-all", "$all(CASE WHEN EXISTS { MATCH (:Anchor) } THEN ['A','B'] ELSE ['Missing'] END)", one},
		{"graph-any", "$any(CASE WHEN EXISTS { MATCH (:Anchor) } THEN ['A','B'] ELSE ['Missing'] END)", anyAB},
		{"edge-graph", "$(CASE WHEN EXISTS { MATCH ()-[r:R]->() WHERE r.p='x' } THEN 'A' ELSE 'Missing' END)", a},
		{"edge-graph-all", "$all(CASE WHEN EXISTS { MATCH ()-[r:R]->() WHERE r.p='x' } THEN ['A','B'] ELSE ['Missing'] END)", one},
		{"edge-graph-any", "$any(CASE WHEN EXISTS { MATCH ()-[r:R]->() WHERE r.p='x' } THEN ['A','B'] ELSE ['Missing'] END)", anyAB},
		{"edge-graph-upper-any", "$ANY(CASE WHEN EXISTS { MATCH ()-[r:R]->() WHERE r.p='x' } THEN ['A','B'] ELSE ['Missing'] END)", anyAB},
		{"empty-all", "$all([])", [][]any{{int64(0)}, {int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}}},
		{"empty-any", "$any([])", [][]any{}},
	} {
		prefix := ""
		if form.name == "row-list" || form.name == "row-all" || form.name == "row-any" { prefix = "WITH ['A','B'] AS ls " }
		params := map[string]any{"label":"A", "labels": []any{"A","B"}}
		cases = append(cases, Case{Name:"match/"+form.name, Query:prefix+"MATCH (n:"+form.text+") RETURN n.id AS id ORDER BY id", Params:params, Rows:form.rows})
		if form.name == "edge-graph" || form.name == "edge-graph-all" || form.name == "edge-graph-any" || form.name == "edge-graph-upper-any" {
			cases = append(cases, Case{Name:"profile/"+form.name, Query:"PROFILE MATCH (n:"+form.text+") RETURN n.id AS id ORDER BY id", Params:params, Rows:form.rows})
		}
		if prefix == "" {
			cases = append(cases, Case{Name:"call/"+form.name, Query:"CALL { MATCH (n:"+form.text+") RETURN n.id AS id } RETURN id ORDER BY id", Params:params, Rows:form.rows})
		}
	}
	for _, form := range []struct { name, text string; rows [][]any }{
		{"list", "$(['A','B'])", one}, {"all", "$all(['A','B'])", one}, {"any", "$any(['A','B'])", anyAB},
		{"row", "$any(ls)", anyAB},
		{"nested", "$any(CASE WHEN EXISTS { MATCH (:Anchor) } THEN ['A','B'] ELSE ['Missing'] END)", anyAB},
	} {
		prefix := "WITH ['A','B'] AS ls "
		cases = append(cases,
			Case{Name:"optional/"+form.name, Query:prefix+"MATCH (a:Anchor) OPTIONAL MATCH (a)-->(n:"+form.text+") RETURN n.id AS id ORDER BY id", Rows:form.rows},
			Case{Name:"comprehension/"+form.name, Query:prefix+"MATCH (a:Anchor) UNWIND [(a)-->(n:"+form.text+") | n.id] AS id RETURN id ORDER BY id", Rows:form.rows},
			Case{Name:"exists/"+form.name, Query:prefix+"MATCH (a:Anchor) WHERE EXISTS { MATCH (a)-->(n:"+form.text+") } RETURN a.id AS id", Rows:[][]any{{int64(0)}}},
			Case{Name:"pattern/"+form.name, Query:prefix+"MATCH (a:Anchor) WHERE (a)-->(:"+form.text+") RETURN a.id AS id", Rows:[][]any{{int64(0)}}},
		)
	}
	for _, clause := range []string{"CREATE", "SET", "REMOVE"} {
		for _, value := range []struct { name, text string }{{"string", "'D'"}, {"list", "['D','E']"}, {"parameter", "$labels"}, {"row", "ls"}} {
			query := "WITH ['D','E'] AS ls "
			want := int64(2)
			if value.name == "string" { want = 1 }
			switch clause {
			case "CREATE": query += "CREATE (n:$("+value.text+")) RETURN size(labels(n)) AS c"
			case "SET": query += "CREATE (n) SET n:$("+value.text+") RETURN size(labels(n)) AS c"
			case "REMOVE":
				query += "CREATE (n:D:E) REMOVE n:$("+value.text+") RETURN size(labels(n)) AS c"
				want = 2-want
			}
			cases = append(cases, Case{Name:clause+"/"+value.name, Query:query, Params:map[string]any{"labels":[]any{"D","E"}}, Rows:[][]any{{want}}})
			cases = append(cases, Case{Name:"call/"+clause+"/"+value.name, Query:"CALL { "+query+" } RETURN c", Params:map[string]any{"labels":[]any{"D","E"}}, Rows:[][]any{{want}}})
		}
	}
	for _, form := range []string{"$('A')", "$all(['A','B'])", "$any($labels)"} {
		query := "MATCH (n) WHERE n:"+form+" RETURN n.id AS id ORDER BY id"
		cases = append(cases, Case{Name:"where-5/"+form, Query:query, Params:map[string]any{"labels":[]any{"A","B"}}, Code:"Neo.ClientError.Statement.SyntaxError"})
		rows := a
		if form == "$all(['A','B'])" { rows = one }
		if form == "$any($labels)" { rows = anyAB }
		cases = append(cases, Case{Name:"where-25/"+form, Query:"CYPHER 25 "+query, Params:map[string]any{"labels":[]any{"A","B"}}, Rows:rows, Cypher25:true})
	}
	cases = append(cases,
		Case{Name:"CREATE/all", Query:"CREATE (n:$all($labels)) RETURN size(labels(n)) AS c", Params:map[string]any{"labels":[]any{"D","E"}}, Rows:[][]any{{int64(2)}}},
		Case{Name:"MERGE/row", Query:"WITH $labels AS ls MERGE (n:$(ls) {id: 2}) RETURN n.id AS id", Params:map[string]any{"labels":[]any{"A","B"}}, Rows:one},
		Case{Name:"optional/missing", Query:"OPTIONAL MATCH (n:$any(['Missing'])) RETURN n.id AS id", Rows:[][]any{{nil}}},
		Case{Name:"profile/runtime", Query:"PROFILE WITH ['A','B'] AS ls MATCH (n:$any(ls)) RETURN n.id AS id ORDER BY id", Rows:anyAB},
	)
	// Neo4j 5.26's planner throws a class-cast error when a dynamic label's
	// operand contains typed patterns. Preserve that reference observation and
	// also compare the equivalent supported, untyped pattern against its rows.
	for _, c := range cases {
		untyped := strings.ReplaceAll(c.Query, "MATCH (:Anchor)", "MATCH (anchor) WHERE anchor.id = 0")
		untyped = strings.ReplaceAll(untyped, "-[r:R]->", "-[r]->")
		if untyped != c.Query {
			c.Name += "/untyped"
			c.Query = untyped
			cases = append(cases, c)
		}
	}
	for i := range cases {
		if strings.Contains(cases[i].Query, "EXISTS { MATCH (:Anchor)") || strings.Contains(cases[i].Query, "-[r:R]->") {
			cases[i].ReferenceCode = "Neo.DatabaseError.General.UnknownError"
			cases[i].ReferenceMessage = "cannot be cast to class org.neo4j.cypher.internal.label_expressions.LabelExpression"
		}
	}
	return cases
}
