package cypher

import "strings"

// extractScopeVariablesFromRemoveAndReturn extracts every variable name
// referenced in a REMOVE clause (e.g. "r.prop", "n:Label") and a RETURN
// clause, mirroring extractScopeVariablesFromSetAndReturn (executor_mutations.go).
// executeRemove uses this to keep every variable the REMOVE/RETURN clauses
// actually need in its internal MATCH probe's projection.
//
// extractVariableNamesFromPattern only scans node groups outside "[...]", so
// a relationship variable bound solely inside a bracketed pattern (e.g. the
// "r" in "OPTIONAL MATCH (n)-[r:TYPE]->(m)") is invisible to it. Without this
// helper, "REMOVE r.prop" silently dropped r from the probe's RETURN list, so
// the mutation loop never received a *storage.Edge to remove the property
// from -- the relationship-agnostic parseRemoveItems call site never even
// learns r is the intended target, only that "prop" should be removed from
// whatever entity in scope has it (eshu #5147).
func extractScopeVariablesFromRemoveAndReturn(removePart, returnPart string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, 4)
	add := func(v string) {
		v = strings.TrimSpace(v)
		if v == "" || !isValidIdentifier(v) {
			return
		}
		if _, ok := seen[v]; ok {
			return
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}

	for _, raw := range strings.Split(removePart, ",") {
		item := strings.TrimSpace(raw)
		if item == "" {
			continue
		}
		var varName string
		switch {
		case strings.Contains(item, "."):
			varName = strings.TrimSpace(item[:strings.Index(item, ".")])
		case strings.Contains(item, ":"):
			varName = strings.TrimSpace(item[:strings.Index(item, ":")])
		default:
			varName = item
		}
		add(varName)
	}

	if strings.TrimSpace(returnPart) != "" {
		for _, raw := range splitTopLevelCommaKeepEmpty(returnPart) {
			expr := strings.TrimSpace(raw)
			if expr == "" {
				continue
			}
			if asIdx := findKeywordIndex(expr, "AS"); asIdx > 0 {
				expr = strings.TrimSpace(expr[:asIdx])
			}
			add(extractVariableNameFromReturnItem(expr))
		}
	}
	return out
}
