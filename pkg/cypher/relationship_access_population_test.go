package cypher

import "fmt"

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
	}
	for _, source := range []string{"'x'", "$x", "k"} {
		for _, inline := range []bool{false, true} {
			body := "MATCH (a)-[r:T]->() WHERE r.p=" + source
			if inline { body = "(a)-[r:T {p:" + source + "}]->()" }
			for _, kind := range []string{"COUNT", "EXISTS", "COLLECT", "CALL"} {
				name := fmt.Sprintf("generated_anchor_%s_%s_%t", kind, source, inline)
				prefix := "MATCH (a:Key) WITH a,$x AS k "
				if kind == "CALL" {
					if inline { body = "MATCH " + body }
					add(name, prefix+"CALL (a,k) { "+body+" RETURN r.p AS value } RETURN value")
				} else if kind == "COLLECT" {
					if inline { body = "MATCH " + body }
					add(name, prefix+"RETURN COLLECT { "+body+" RETURN r.p }")
				} else {
					add(name, prefix+"RETURN "+kind+" { "+body+" }")
				}
			}
		}
	}
	return cases
}
