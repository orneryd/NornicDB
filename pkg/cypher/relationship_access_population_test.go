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
	for _, token := range []string{"", "all", "any", "ANY"} {
		add("generated_match_"+token, "MATCH (n:$"+token+"(CASE WHEN EXISTS { MATCH ()-[r:T]->() WHERE r.p='x' } THEN 'Key' ELSE 'Missing' END)) RETURN labels(n)")
	}
	for _, source := range []string{"'x'", "$x", "k"} {
		for _, inline := range []bool{false, true} {
			body := "MATCH (a)-[r:T]->() WHERE r.p=" + source
			if inline {
				body = "(a)-[r:T {p:" + source + "}]->()"
			}
			for _, kind := range []string{"COUNT", "EXISTS", "COLLECT", "CALL"} {
				readBody := body
				if inline && (kind == "CALL" || kind == "COLLECT") { readBody = "MATCH " + body }
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
		if imports == "" { body = "WITH outer,k " + body }
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
