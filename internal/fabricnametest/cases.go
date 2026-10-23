// Package fabricnametest supplies the same NOR-72 regression population to
// native execution and both public protocols. It is test support only.
package fabricnametest

import (
	"fmt"
	"strings"
	"unicode"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

type Case struct {
	Name, Query, Reference string
	Params                 map[string]interface{}
}

// Cases derives names from the grammar vocabulary, rather than maintaining a
// private list of the keywords a particular Fabric reader happens to recognize.
// The shared Neo4j Community reference has one database. Static USE clauses
// keep their syntax and select neo4j. A generated dynamic selector's expected
// graph is also mapped to that database; Nornic's seed distinguishes its two
// constituents when a parameter and variable have the same spelling.
func Cases() []Case {
	cypherantlr.CypherLexerInit()
	var cases []Case
	add := func(name, query string, params map[string]interface{}, selectors ...string) {
		reference := strings.NewReplacer("USE comp.one ", "USE neo4j ", "USE comp ", "USE neo4j ").Replace(query)
		for _, selector := range selectors {
			reference = strings.ReplaceAll(reference, selector, "USE neo4j ")
		}
		cases = append(cases, Case{name, query, reference, params})
	}
	for _, literal := range cypherantlr.CypherLexerLexerStaticData.LiteralNames {
		word := strings.Trim(literal, "'")
		if word == "" || strings.IndexFunc(word, func(r rune) bool { return !unicode.IsLetter(r) }) >= 0 {
			continue
		}
		word = strings.ToLower(word)
		for _, quoted := range []bool{false, true} {
			name := word
			form := "plain"
			if quoted {
				name, form = "`"+word+"`", "quoted"
			}
			// Unquoted true/false/null are literals in expressions, including
			// after declaration. Their quoted identities and parameters still
			// belong to this family; their literal behavior is tested elsewhere.
			if quoted || (word != "true" && word != "false" && word != "null") {
				for _, distinct := range []string{"", "DISTINCT "} {
					horizon := "plain"
					if distinct != "" {
						horizon = "distinct"
					}
					prefix := fmt.Sprintf("USE comp WITH 7 AS %s WITH %s%s ", name, distinct, name)
					add(word+"/"+form+"/"+horizon+"/legacy", prefix+fmt.Sprintf("CALL { WITH %s USE comp.one RETURN %s AS k } RETURN k", name, name), nil)
					add(word+"/"+form+"/"+horizon+"/use_first", prefix+fmt.Sprintf("CALL { USE comp.one WITH %s RETURN %s AS k } RETURN k", name, name), nil)
					add(word+"/"+form+"/"+horizon+"/scoped", prefix+fmt.Sprintf("CALL (%s) { USE comp.one RETURN %s AS k } RETURN k", name, name), nil)
					add(word+"/"+form+"/"+horizon+"/retained", prefix+fmt.Sprintf("CALL (%s) { USE comp.one WITH DISTINCT 1 AS ignored RETURN %s AS k } RETURN k", name, name), nil)
					add(word+"/"+form+"/"+horizon+"/nested", prefix+fmt.Sprintf("CALL (%s) { WITH DISTINCT 1 AS ignored CALL () { USE comp.one RETURN 2 AS marker } RETURN %s AS k } RETURN k", name, name), nil)
					add(word+"/"+form+"/"+horizon+"/lookup", prefix+fmt.Sprintf("CALL { WITH %s USE comp.one MATCH (n:KeywordNameSeed) WHERE n.v = %s RETURN n.v AS k } RETURN k", name, name), nil)
					filtered := fmt.Sprintf("USE comp UNWIND [7,8] AS %s WITH %s%s ", name, distinct, name)
					add(word+"/"+form+"/"+horizon+"/predicate", filtered+fmt.Sprintf("CALL (%s) { USE comp.one WITH %s WHERE %s = 7 RETURN %s AS k } RETURN k", name, name, name, name), nil)
				}
			}
			add(word+"/"+form+"/parameter", "USE comp CALL { USE comp.one RETURN $"+name+" AS k } RETURN k", map[string]interface{}{word: int64(7)})
			add(word+"/"+form+"/parameter_identity", fmt.Sprintf("USE comp WITH 9 AS %s CALL (%s) { USE comp.one RETURN $%s AS k } RETURN k", name, name, name), map[string]interface{}{word: int64(7)})
			selector := fmt.Sprintf("USE graph.byName(CASE WHEN $%s = 7 THEN 'comp.one' ELSE 'comp.two' END) ", name)
			selected := fmt.Sprintf("USE comp WITH 9 AS %s CALL (%s) { %sMATCH (n:KeywordNameSeed) RETURN n.v AS k } RETURN k", name, name, selector)
			add(word+"/"+form+"/selection_parameter", selected, map[string]interface{}{word: int64(7)}, selector)
			if quoted || (word != "true" && word != "false" && word != "null") {
				selector = fmt.Sprintf("USE graph.byName(CASE WHEN %s = 9 THEN 'comp.one' ELSE 'comp.two' END) ", name)
				selected = fmt.Sprintf("USE comp WITH 9 AS %s WITH DISTINCT %s CALL (%s) { %sMATCH (n:KeywordNameSeed) RETURN n.v AS k } RETURN k", name, name, name, selector)
				add(word+"/"+form+"/selection_variable", selected, map[string]interface{}{word: int64(7)}, selector)
			}
		}
	}
	for _, word := range []string{"ret`urn", "use name", "返す"} {
		name := "`" + strings.ReplaceAll(word, "`", "``") + "`"
		add(word+"/escaped/import", fmt.Sprintf("USE comp WITH 7 AS %s WITH DISTINCT %s CALL (%s) { USE comp.one RETURN %s AS k } RETURN k", name, name, name, name), nil)
		add(word+"/escaped/parameter", "USE comp CALL { USE comp.one RETURN $"+name+" AS k } RETURN k", map[string]interface{}{word: int64(7)})
	}
	return cases
}
