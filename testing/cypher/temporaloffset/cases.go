// Package temporaloffset supplies the mutation matrix shared by the Bolt and
// HTTP TemporalBinding regressions. HTTP constructs typed values in Cypher because JSON
// parameters cannot carry a driver datetime.
package temporaloffset

import "fmt"

// Case writes one entity and then reads its stored temporal properties.
type Case struct {
	Name  string
	Setup string
	Query string
	Read  string
}

// Projection compares the instant and text of both a property and a list member.
const Projection = " RETURN n.t.epochSeconds AS e, toString(n.t) AS s, n.ts[0].epochSeconds AS le, toString(n.ts[0]) AS ls"

// Cases covers CREATE/SET, MERGE actions and WITH predicates for nodes and relationships.
func Cases(text string) []Case {
	var cases []Case
	for _, relationship := range []bool{false, true} {
		for _, source := range []string{"$m", "{t:$t, ts:[$t]}", "{t:datetime('" + text + "'), ts:[datetime('" + text + "')]}"} {
			for _, route := range []string{"create", "property", "replace", "create-add", "merge", "field-merge", "keys-merge", "on-create", "on-match"} {
				id := fmt.Sprintf("temporal-binding-%d", len(cases))
				pattern := "(n:TemporalBinding {id:'" + id + "'})"
				read := "MATCH " + pattern
				prefix := ""
				if relationship {
					prefix = "MERGE (a:TemporalBindingA) MERGE (b:TemporalBindingB) "
					pattern = "(a)-[n:TemporalBinding {id:'" + id + "'}]->(b)"
					read = "MATCH (:TemporalBindingA)-[n:TemporalBinding {id:'" + id + "'}]->(:TemporalBindingB)"
				}
				setup, query := "", ""
				switch route {
				case "create":
					query = "CREATE (n:TemporalBinding {id:'" + id + "', t:$t, ts:[$t]})"
					if relationship {
						query = "CREATE (a)-[n:TemporalBinding {id:'" + id + "', t:$t, ts:[$t]}]->(b)"
					}
				case "property":
					query = "CREATE " + pattern + " SET n.t=$t, n.ts=[$t]"
				case "replace":
					query = "CREATE " + pattern + " SET n=" + source + ", n.id='" + id + "'"
				case "create-add":
					query = "CREATE " + pattern + " SET n += " + source
				case "merge":
					query = "MERGE " + pattern + " SET n += " + source
				case "field-merge":
					prefix = "WITH 1 AS x WHERE $m.t IS NOT NULL " + prefix
					query = "MERGE " + pattern + " SET n += " + source
				case "keys-merge":
					prefix = "WITH 1 AS x WHERE none(k IN keys($m) WHERE k='zz') " + prefix
					query = "MERGE " + pattern + " SET n += " + source
				case "on-create":
					query = "MERGE " + pattern + " ON CREATE SET n += " + source
				case "on-match":
					setup = prefix + "CREATE " + pattern
					query = "MERGE " + pattern + " ON MATCH SET n += " + source
				}
				cases = append(cases, Case{Name: fmt.Sprintf("rel=%t/%s/%s", relationship, source, route), Setup: setup, Query: prefix + query + Projection, Read: read + Projection})
			}
		}
	}
	return cases
}
