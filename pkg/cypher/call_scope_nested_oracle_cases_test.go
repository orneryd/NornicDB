package cypher

import (
	"fmt"
	"strings"
)

// Exercise both scope-boundary orders: CALL inside a scalar expression, and
// a scalar expression inside nested CALL. The compatibility control changes
// only bare CALL's implicit import to an explicit named import. Its projections
// deliberately avoid returning that import unchanged, so no other waiver is
// needed. Wildcard branches use a WITH horizon to keep that normalization
// independent of the bare CALL's initial current scope.
func generatedNestedCallScopeOracleCases() []callScopeOracleCase {
	var cases []callScopeOracleCase
	forms := []string{"explicit", "all", "implicit", "with", "with-all"}
	for _, outer := range forms {
		for _, inner := range forms {
			for _, kind := range []string{"COLLECT", "COUNT", "EXISTS"} {
				for _, projection := range []string{"return-all", "with-all", "explicit"} {
					for _, seed := range []string{"empty", "match"} {
						for _, layout := range []string{"call-in-scalar", "scalar-in-call"} {
							for _, union := range []string{"none", "UNION", "UNION ALL"} {
								horizons := []string{"drop"}
								if projection == "explicit" {
									horizons = append(horizons, "keep")
								}
								for _, horizon := range horizons {
									build := func(normalized bool) string {
										leafPrefix := ""
										if seed == "match" {
											leafPrefix = "MATCH (b:CallScopeOracleTarget {oracleRun:$oracleRun}) "
										}
										leaf := leafPrefix + "RETURN *"
										if projection == "with-all" {
											leaf = leafPrefix + "WITH * RETURN *"
										}
										if projection == "explicit" {
											leaf = leafPrefix + "RETURN a AS innerValue"
										}
										horizonPrefix := ""
										if horizon == "drop" {
											horizonPrefix = "WITH 0 AS scopeMarker "
										}
										innerBranch := horizonPrefix + leaf
										if layout == "scalar-in-call" {
											innerBranch = horizonPrefix + "RETURN " + kind + " { " + leaf + " } AS innerValue"
										}
										branches := []string{innerBranch}
										if union != "none" {
											branches = append(branches, innerBranch)
										}
										innerCall := callScopeOracleNestedCall(inner, branches, union, normalized)
										outerBranch := innerCall + " RETURN innerValue AS value"
										if layout == "call-in-scalar" {
											tail := "RETURN *"
											if projection == "with-all" {
												tail = "WITH * RETURN *"
											}
											if projection == "explicit" {
												tail = "RETURN innerValue AS item"
											}
											outerBranch = "RETURN " + kind + " { " + innerCall + " " + tail + " } AS value"
										}
										return callScopeOracleNestedCall(outer, []string{outerBranch}, "none", normalized) + " RETURN a,value"
									}
									item := callScopeOracleCase{name: fmt.Sprintf("nested/%s/%s/%s/%s/%s/%s/%s/%s", outer, inner, kind, projection, seed, layout, union, horizon)}
									item.query = "WITH a UNWIND [a,a+1] AS scopeRow WITH scopeRow AS a " + build(false)
									item.reference = "WITH $outer AS a UNWIND [a,a+1] AS scopeRow WITH scopeRow AS a " + build(false)
									if outer == "implicit" || inner == "implicit" {
										item.extension = "907-unscoped-implicit-import"
										item.compatibility = "WITH $outer AS a UNWIND [a,a+1] AS scopeRow WITH scopeRow AS a " + build(true)
									}
									cases = append(cases, item)
								}
							}
						}
					}
				}
			}
		}
	}
	return cases
}

func callScopeOracleNestedCall(form string, branches []string, union string, normalized bool) string {
	if normalized && form == "implicit" {
		// The retained implicit import enters both the ordinary current
		// scope and the carried import scope. CALL(a) alone models only
		// the latter; WITH a supplies its ordinary local counterpart.
		parts := make([]string, len(branches))
		for index, branch := range branches {
			parts[index] = "WITH a " + branch
		}
		item := callScopeOracleCase{call: "explicit", union: union}
		return strings.TrimSpace(item.callBody(parts, false))
	}
	item := callScopeOracleCase{call: form, union: union}
	return strings.TrimSpace(item.callBody(branches, false))
}
