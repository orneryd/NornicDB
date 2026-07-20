package cypher

import "strings"

// extractScopeVariablesFromRemoveAndReturn extracts every variable name
// referenced in a REMOVE clause (e.g. "r.prop", "n:Label") and a RETURN
// clause, mirroring extractScopeVariablesFromSetAndReturn (executor_mutations.go).
// executeRemove uses this to keep every variable the REMOVE/RETURN clauses
// actually need in its internal MATCH probe's projection.
type removeTargetBindings struct {
	propsByVar  map[string][]string
	labelsByVar map[string][]string
}

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
		switch {
		case strings.Contains(item, "."):
			add(strings.TrimSpace(item[:strings.Index(item, ".")]))
		case strings.Contains(item, ":"):
			add(strings.TrimSpace(item[:strings.Index(item, ":")]))
		default:
			add(item)
		}
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

func parseRemoveTargetBindings(removePart string) removeTargetBindings {
	bindings := removeTargetBindings{
		propsByVar:  make(map[string][]string),
		labelsByVar: make(map[string][]string),
	}

	for _, raw := range strings.Split(removePart, ",") {
		item := strings.TrimSpace(raw)
		if item == "" {
			continue
		}
		if dotIdx := strings.Index(item, "."); dotIdx >= 0 {
			varName := strings.TrimSpace(item[:dotIdx])
			propName := strings.TrimSpace(item[dotIdx+1:])
			if isValidIdentifier(varName) && propName != "" {
				bindings.propsByVar[varName] = append(bindings.propsByVar[varName], propName)
			}
			continue
		}
		if colonIdx := strings.Index(item, ":"); colonIdx >= 0 {
			varName := strings.TrimSpace(item[:colonIdx])
			if !isValidIdentifier(varName) {
				continue
			}
			for _, label := range strings.Split(item[colonIdx+1:], ":") {
				label = strings.TrimSpace(label)
				if label != "" {
					bindings.labelsByVar[varName] = append(bindings.labelsByVar[varName], label)
				}
			}
		}
	}

	return bindings
}

func (b removeTargetBindings) propertyNames(varName string) []string {
	return b.propsByVar[varName]
}

func (b removeTargetBindings) labelNames(varName string) []string {
	return b.labelsByVar[varName]
}
