package cypher

import "strings"

func containsRelExistencePattern(s string) bool {
	if strings.Contains(s, "-[") && strings.Contains(s, "]-") {
		// Covers "]->", "]-(", and the undirected bracketed "-[r]-" form.
		return true
	}
	return strings.Contains(s, ")--(") || strings.Contains(s, ")-->(") || strings.Contains(s, ")<--(")
}

func bareRelDirection(pattern, variable string) (incoming, outgoing, ok bool) {
	if strings.Contains(pattern, "-[") {
		return false, false, false
	}

	groupStart := strings.Index(pattern, "("+variable+")")
	var groupEnd int
	if groupStart >= 0 {
		groupEnd = groupStart + len(variable) + 2
	} else {
		idx := strings.Index(pattern, "("+variable+":")
		if idx < 0 {
			return false, false, false
		}
		closeRel := strings.Index(pattern[idx:], ")")
		if closeRel < 0 {
			return false, false, false
		}
		groupStart = idx
		groupEnd = idx + closeRel + 1
	}

	before := pattern[:groupStart]
	after := pattern[groupEnd:]

	switch {
	case strings.HasPrefix(after, "-->"):
		return false, true, true
	case strings.HasPrefix(after, "<--"):
		return true, false, true
	case strings.HasPrefix(after, "--"):
		return true, true, true
	}

	switch {
	case strings.HasSuffix(before, "-->"):
		return true, false, true
	case strings.HasSuffix(before, "<--"):
		return false, true, true
	case strings.HasSuffix(before, "--"):
		return true, true, true
	}

	return false, false, false
}
