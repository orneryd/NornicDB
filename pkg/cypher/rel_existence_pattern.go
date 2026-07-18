package cypher

import "strings"

// containsRelExistencePattern reports whether a WHERE fragment contains a
// relationship-existence pattern such as (n)--(), (n)-->(), (n)<--(), or a
// bracketed relationship pattern like (n)-[r]-() or (n)-[:TYPE]->().
//
// The pre-fix gate only recognized bracketed patterns with an explicit
// arrow (`-[...]->` or `<-[...]-`), so bracket-less patterns
// ((n)--(), (n)-->(), (n)<--()) and even the bracketed *undirected* form
// ((n)-[r]-()) fell through to evaluateWhereAsBoolean, whose default branch
// treats an unrecognized expression as true. That is the root cause of
// "WHERE NOT (n)--() matches nothing" and "WHERE (n)--() always true".
//
// containsRelExistencePattern must not match ordinary arithmetic that
// happens to contain a hyphen, e.g. "n.a - n.b" or "n.arr[0]-1" -- neither
// contains "-[" followed by "]-", nor ")--(", ")-->(", or ")<--(".
func containsRelExistencePattern(s string) bool {
	if strings.Contains(s, "-[") && strings.Contains(s, "]-") {
		// Covers "]->", "]-(", and the undirected bracketed "-[r]-" form.
		return true
	}
	return strings.Contains(s, ")--(") || strings.Contains(s, ")-->(") || strings.Contains(s, ")<--(")
}

// bareRelDirection determines the traversal direction implied by a
// bracket-less relationship-existence pattern -- e.g. "(n)-->()",
// "()<--(n)", "(n)--()" -- relative to variable.
//
// It returns ok=false when:
//   - pattern contains a bracketed relationship ("-["), which is handled by
//     the bracketed-pattern direction detection instead;
//   - variable's node group (e.g. "(n)" or "(n:Label)") cannot be located
//     in pattern;
//   - neither side of the located group is bounded by a recognized
//     bracket-less arrow ("-->", "<--", or undirected "--").
//
// When ok is true, incoming/outgoing describe which adjacency lists the
// caller should search from variable's perspective: "(n)-->()" is outgoing
// from n, "(n)<--()" is incoming to n, "()-->(n)" is incoming to n (the
// arrow points at n), "()<--(n)" is outgoing from n, and the undirected
// "--" form sets both.
func bareRelDirection(pattern, variable string) (incoming, outgoing, ok bool) {
	if strings.Contains(pattern, "-[") {
		return false, false, false
	}

	groupStart := strings.Index(pattern, "("+variable+")")
	var groupEnd int
	if groupStart >= 0 {
		groupEnd = groupStart + len(variable) + 2 // len("("+variable+")")
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
