package search

// Keep retained continuation/cached match slices immutable. Only allocate when
// a known property match is actually hidden; unknown legacy chunk locations
// retain their existing representation.
func visibleSearchMatches(matches []SearchMatch, properties map[string]interface{}) []SearchMatch {
	for i, match := range matches {
		if match.Property == "" {
			continue
		}
		if _, visible := properties[match.Property]; visible {
			continue
		}
		out := make([]SearchMatch, 0, len(matches)-1)
		out = append(out, matches[:i]...)
		for _, remaining := range matches[i+1:] {
			if remaining.Property == "" {
				out = append(out, remaining)
			} else if _, visible := properties[remaining.Property]; visible {
				out = append(out, remaining)
			}
		}
		return out
	}
	return matches
}
