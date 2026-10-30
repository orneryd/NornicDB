package callscopetest

import (
	"fmt"
	"strings"
)

// RuleCases is generated from the import/declaration contract, independently
// of the production scope walker. Compatibility names an explicit #907
// control, never a different error or an omitted comparison.
func RuleCases() []Case {
	forms := []struct{ name, header, leading string }{
		{"implicit", "CALL", ""},
		{"named", "CALL (a)", ""},
		{"all", "CALL (*)", ""},
		{"empty", "CALL ()", ""},
		{"with", "CALL", "WITH a "},
	}
	declarations := []struct {
		name, prefix, expression string
		shadows, scopedRebind    bool
	}{
		{"read", "", "a", false, false},
		{"unwind", "UNWIND [2] AS a ", "a", true, true},
		{"with-alias", "WITH 2 AS a ", "a", true, true},
		{"pattern", "MATCH (a:CallScopeRuleTarget) ", "a.n", true, false},
		{"yield", "CALL db.labels() YIELD label AS a ", "count(a)>=0", true, false},
		{"nested-call", "CALL () { RETURN 2 AS a } ", "a", true, false},
		{"collect", "", "COLLECT { UNWIND [2] AS a RETURN a }", true, false},
		{"exists", "", "EXISTS { UNWIND [2] AS a RETURN a }", true, false},
		{"count", "", "COUNT { MATCH (a:CallScopeRuleTarget) }", true, false},
	}
	var cases []Case
	for _, form := range forms {
		for _, declaration := range declarations {
			for _, horizon := range []bool{false, true} {
				for _, union := range []string{"", "UNION", "UNION ALL"} {
					projection := "RETURN " + declaration.expression + " AS v"
					if horizon {
						projection = "WITH " + declaration.expression + " AS v RETURN *"
					}
					branch := declaration.prefix + projection
					query := func(header, leading string) string {
						body := leading + branch
						if union != "" {
							body += " " + union + " " + leading + branch
						}
						return "WITH 1 AS a " + header + " { " + body + " } RETURN a,v"
					}
					item := Case{Name: fmt.Sprintf("rules/%s/%s/horizon-%t/%s", form.name, declaration.name, horizon, union), Query: query(form.header, form.leading)}
					item.Reference = item.Query
					if form.name == "implicit" {
						// A first importing WITH with an alias is still restricted.
						if declaration.shadows {
							item.Compatibility = query("CALL ()", "")
						} else if !horizon {
							item.Compatibility = query("CALL (a)", "")
						}
					}
					if (form.name == "named" || form.name == "all") && declaration.scopedRebind {
						item.Compatibility = query("CALL ()", "")
					}
					// Existing UNWIND also permits rebinding an ordinary local
					// selected by an importing WITH (#907).
					if form.name == "with" && declaration.name == "unwind" {
						item.Compatibility = query("CALL ()", "")
					}
					if item.Compatibility != "" {
						item.Extension = "907-explicit-scope-control"
					}
					cases = append(cases, item)
				}
			}
		}
	}
	// The two branches differ only in whether imported a is projected. The
	// same projection contract must reject every ordering and UNION variant.
	for _, header := range []string{"CALL (a)", "CALL (*)"} {
		for _, union := range []string{"UNION", "UNION ALL"} {
			for _, reverse := range []bool{false, true} {
				left := "MATCH (t:CallScopeRuleTarget) WHERE t.n=a RETURN *"
				right := "MATCH (t:CallScopeRuleTarget) WHERE t.n<>a RETURN t"
				if reverse {
					left, right = right, left
				}
				query := "UNWIND [1,2] AS a " + header + " { " + left + " " + union + " " + right + " } RETURN a,t.n"
				cases = append(cases, Case{Name: fmt.Sprintf("rules/wildcard/%s/%s/reverse-%t", header, union, reverse), Query: query, Reference: query})
			}
		}
		for _, body := range []string{
			"MATCH (t:CallScopeRuleTarget) RETURN t.n AS v,*",
			"WITH 5 AS v RETURN * UNION RETURN 6 AS v",
			"WITH a AS v RETURN * UNION WITH a+1 AS v RETURN v",
		} {
			query := "WITH 1 AS a " + header + " { " + body + " } RETURN a,v"
			cases = append(cases, Case{Name: "rules/wildcard-neighbour/" + header + "/" + strings.ReplaceAll(body, " ", "-"), Query: query, Reference: query})
		}
	}
	for _, query := range []string{
		"WITH 7 AS a CALL { MATCH (n:CallScopeRuleTarget) WITH n AS a WHERE COUNT { MATCH (a) }=1 RETURN a.n AS v } RETURN a,v",
		"WITH 7 AS a CALL { MATCH (n:CallScopeRuleTarget) WITH n AS a RETURN COLLECT { MATCH (a) RETURN a.n } AS v } RETURN a,v",
		"WITH 1 AS a CALL { WITH 0 AS z WITH z,a AS prior CALL () { RETURN 2 AS a } RETURN a+prior AS v } RETURN a,v",
	} {
		item := Case{Name: "rules/provenance-phase/" + query, Query: query, Reference: query}
		if strings.Contains(query, "prior") {
			item.Extension = "907-explicit-scope-control"
			item.Compatibility = "WITH 1 AS a CALL (a) { WITH 0 AS z WITH z,a AS prior CALL () { RETURN 2 AS inner } RETURN inner+prior AS v } RETURN a,v"
		}
		cases = append(cases, item)
	}
	return cases
}
