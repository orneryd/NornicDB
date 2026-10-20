package cypher

import (
	"fmt"
	"strings"
)

// The population crosses expression consumers with token spellings, predicate
// sources and statement boundaries. It is independent of the program compiler.
func relationshipAccessGeneratedCases() []struct{ name, q string } {
	var cases []struct{ name, q string }
	add := func(name, query string) { cases = append(cases, struct{ name, q string }{name, query}) }
	for _, token := range []string{"", "all", "ALL"} {
		for _, source := range []string{"k", "$x"} {
			graph := "MATCH ()-[r:T]->() WHERE r.p=" + source + " "
			prefix := "WITH $x AS k "
			label := "$" + token + "(head(COLLECT { " + graph + "RETURN r.p }))"
			name := fmt.Sprintf("generated_token_%s_%s", token, source)
			add(name+"_exists", prefix+"CREATE (n:$"+token+"(CASE WHEN EXISTS { "+graph+"} THEN 'Made' ELSE 'Missing' END)) RETURN labels(n)")
			add(name+"_collect", prefix+"CREATE (n:"+label+") RETURN labels(n)")
			add(name+"_type", prefix+"CREATE ()-[s:"+label+"]->() RETURN type(s)")
			for _, write := range []string{"CREATE", "MERGE"} {
				add(name+"_"+write+"_bound_token", prefix+"WITH head(COLLECT { "+graph+"RETURN r.p }) AS token "+write+" (n:$"+token+"(token) {id:7}) RETURN labels(n)")
			}
		}
		for _, write := range []string{"CREATE", "MERGE"} {
			prefix := "WITH $x AS k WHERE EXISTS { MATCH ()-[r:T]->() WHERE r.p=k } "
			add("generated_parameter_"+token+"_"+write+"_label", prefix+write+" (n:$"+token+"($x)) RETURN labels(n)")
			add("generated_parameter_"+token+"_"+write+"_type", prefix+write+" ()-[s:$"+token+"($x)]->() RETURN type(s)")
		}
	}
	for _, source := range []string{"'x'", "$x", "k"} {
		for _, inline := range []bool{false, true} {
			body := "MATCH (a)-[r:T]->() WHERE r.p=" + source
			if inline {
				body = "(a)-[r:T {p:" + source + "}]->()"
			}
			for _, kind := range []string{"COUNT", "EXISTS", "COLLECT", "CALL"} {
				readBody := body
				if inline && (kind == "CALL" || kind == "COLLECT") {
					readBody = "MATCH " + body
				}
				name := fmt.Sprintf("generated_anchor_%s_%s_%t", kind, source, inline)
				prefix := "MATCH (a:Key) WITH a,$x AS k "
				if kind == "CALL" {
					add(name, prefix+"CALL (a,k) { "+readBody+" RETURN r.p AS value } RETURN value")
				} else if kind == "COLLECT" {
					add(name, prefix+"RETURN COLLECT { "+readBody+" RETURN r.p }")
				} else {
					add(name, prefix+"RETURN "+kind+" { "+body+" }")
				}
			}
		}
	}
	for _, imports := range []string{"(outer,k)", "(*)", ""} {
		body := "MATCH ()-[r:T]->() WHERE r.p=outer.p RETURN r.p AS value"
		if imports == "" {
			body = "WITH outer,k " + body
		}
		call := "CALL " + imports + " { " + body + " }"
		prefix := "MATCH ()-[outer:T]->() WITH outer,$x AS k "
		add("generated_anchor_CALL_edge_"+imports, prefix+call+" RETURN value")
		for _, kind := range []string{"COUNT", "EXISTS", "COLLECT"} {
			add("generated_anchor_"+kind+"_edge_"+imports, prefix+"RETURN "+kind+" { "+call+" RETURN value }")
		}
	}
	return cases
}

func relationshipAccessGeneratedRows(name string) [][]interface{} {
	if strings.HasPrefix(name, "generated_anchor_COUNT_") {
		return [][]interface{}{{int64(1)}}
	}
	if strings.HasPrefix(name, "generated_anchor_EXISTS_") {
		return [][]interface{}{{true}}
	}
	if strings.HasPrefix(name, "generated_anchor_CALL_") || strings.HasSuffix(name, "_type") {
		return [][]interface{}{{"x"}}
	}
	label := "x"
	if strings.HasSuffix(name, "_exists") {
		label = "Made"
	}
	if strings.HasPrefix(name, "generated_match_") {
		label = "Key"
	}
	return [][]interface{}{{[]interface{}{label}}}
}

type relationshipAccessStorageCase struct {
	name, query, operator, index string
	referenceErrorCode           string
	estimate                     int64
	params                       map[string]interface{}
	rows                         [][]interface{}
}

// Cross public access families with predicate/projection forms, independently
// of the program compiler. The fixture determines these expected choices.
func relationshipAccessGeneratedVectorCases() []relationshipAccessStorageCase {
	var cases []relationshipAccessStorageCase
	for _, source := range []string{"'r1'", "$id"} {
		for _, projection := range []bool{false, true} {
			query := "MATCH ()-[e:RELATES_TO]->() WHERE e.uuid=" + source + " "
			if projection {
				query += "WITH e, vector.similarity.cosine(e.fact_embedding,$q) AS score RETURN e.uuid, score"
			} else {
				query += "RETURN e.uuid, vector.similarity.cosine(e.fact_embedding,$q) AS score"
			}
			cases = append(cases, relationshipAccessStorageCase{
				name:     fmt.Sprintf("generated_vector_%s_projection_%t", source, projection),
				query:    query + " ORDER BY score DESC LIMIT 5",
				operator: "RelationshipVectorIndexSeek", index: "relationship_vector_owner", estimate: 10,
				params: map[string]interface{}{"id": "r1", "q": []float64{1, 0, 0}},
				rows:   [][]interface{}{{"r1", 1.0}},
			})
		}
	}
	return cases
}

func relationshipAccessGeneratedStorageCases(id interface{}) []relationshipAccessStorageCase {
	var cases []relationshipAccessStorageCase
	for _, function := range []string{"elementId", "id"} {
		operator := "RelationshipByIdSeek"
		if function == "elementId" {
			operator = "RelationshipByElementIdSeek"
		}
		for _, source := range []string{"$id", fmt.Sprintf("'%v'", id)} {
			for _, consumer := range []string{"MATCH", "EXISTS", "COUNT"} {
				for _, relType := range []string{":ROUTE", ""} {
					for _, direction := range []string{"outgoing", "incoming", "both"} {
						relationship := "-[r" + relType + "]->"
						anchor := "a"
						if direction == "incoming" {
							relationship = "<-[r" + relType + "]-"
							anchor = "b"
						} else if direction == "both" {
							relationship = "-[r" + relType + "]-"
						}
						query := "MATCH ()" + relationship + "() WHERE " + function + "(r)=" + source + " RETURN r.p"
						rows := [][]interface{}{{"x"}}
						if direction == "both" {
							rows = append(rows, rows...)
						}
						if consumer != "MATCH" {
							query = "MATCH (a:RouteNode {id:'" + anchor + "'}) RETURN " + consumer + " { MATCH (a)" + relationship + "() WHERE " + function + "(r)=" + source + " }"
							rows = [][]interface{}{{int64(1)}}
							if consumer == "EXISTS" {
								rows = [][]interface{}{{true}}
							}
						}
						name := fmt.Sprintf("generated_%s_%s_%s", function, source, consumer)
						if relType == "" || direction != "outgoing" {
							name += fmt.Sprintf("_%s_typed_%t", direction, relType != "")
						}
						cases = append(cases, relationshipAccessStorageCase{
							name: name, query: query,
							operator: operator, estimate: 1, params: map[string]interface{}{"id": id}, rows: rows,
						})
					}
				}
			}
		}
	}
	for _, length := range []string{"*", "*1..2"} {
		pattern := "(a:RouteNode {id:'a'})-[r:ROUTE" + length + "]->(c:RouteNode {id:'c'})"
		for _, function := range []string{"shortestPath", "allShortestPaths"} {
			cases = append(cases, relationshipAccessStorageCase{
				name: "generated_" + function + length, query: "MATCH p=" + function + "(" + pattern + ") RETURN length(p)",
				operator: "Expand", estimate: 100, rows: [][]interface{}{{int64(2)}},
			})
		}
		cases = append(cases, relationshipAccessStorageCase{
			name:     "generated_shortest_expression" + length,
			query:    "MATCH (a:RouteNode {id:'a'}), (c:RouteNode {id:'c'}) RETURN length(shortestPath((a)-[r:ROUTE" + length + "]->(c)))",
			operator: "Expand", estimate: 100, rows: [][]interface{}{{int64(2)}},
			// NornicDB permits this unused relationship binding in an expression;
			// Neo4j rejects it. Keep the existing accepted extension.
			referenceErrorCode: "Neo.ClientError.Statement.SyntaxError",
		})
		cases = append(cases, relationshipAccessStorageCase{
			name:     "generated_shortest_expression_anonymous" + length,
			query:    "MATCH (a:RouteNode {id:'a'}), (c:RouteNode {id:'c'}) RETURN length(shortestPath((a)-[:ROUTE" + length + "]->(c)))",
			operator: "Expand", estimate: 100, rows: [][]interface{}{{int64(2)}},
		})
	}
	for _, selector := range []string{"ANY SHORTEST", "SHORTEST 1 GROUP", "ANY 1"} {
		for _, fixed := range []bool{false, true} {
			pattern := "(a:RouteNode {id:'a'})-[r:ROUTE*1..2]->(c:RouteNode {id:'c'})"
			length := int64(2)
			if fixed {
				pattern = "(a:RouteNode {id:'a'})-[r:ROUTE {p:'x'}]->(b:RouteNode {id:'b'})"
				length = 1
			}
			cases = append(cases, relationshipAccessStorageCase{
				name:     fmt.Sprintf("generated_selected_%s_fixed_%t", selector, fixed),
				query:    "MATCH p = " + selector + " " + pattern + " RETURN length(p)",
				operator: "Expand", estimate: 100, rows: [][]interface{}{{length}},
			})
		}
	}
	for _, direction := range []string{"outgoing", "incoming", "both"} {
		for _, labelled := range []bool{false, true} {
			start := "()"
			if labelled {
				start = "(:RouteNode)"
			}
			pattern := start + "-[r:ROUTE]->()"
			rows := [][]interface{}{{"x"}, {"x"}}
			if direction == "incoming" {
				pattern = start + "<-[r:ROUTE]-()"
			}
			if direction == "both" {
				pattern = start + "-[r:ROUTE]-()"
				rows = append(rows, rows...)
			}
			cases = append(cases, relationshipAccessStorageCase{
				name:  fmt.Sprintf("generated_scan_%s_labelled_%t", direction, labelled),
				query: "MATCH " + pattern + " RETURN r.p", operator: "Expand", estimate: 100, rows: rows,
			})
		}
	}
	return cases
}
