package cypher

func hasRelationshipPattern(patterns []string) bool {
	for _, pattern := range patterns {
		if containsOutsideStrings(pattern, "-[") || containsOutsideStrings(pattern, "]-") {
			return true
		}
	}
	return false
}
