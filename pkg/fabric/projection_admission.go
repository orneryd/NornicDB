package fabric

import "strings"

// EmbeddingProjectionCandidate is syntax eligibility, independently of the
// statement frame's binding decision. Both native and physical adapters use it.
func EmbeddingProjectionCandidate(previous, next string, projection *QueryProjection, predicate string) string {
	switch previous {
	case "CREATE", "MERGE", "SET", "REMOVE", "DELETE", "DETACH DELETE", "FOREACH":
	default:
		return ""
	}
	if next != "RETURN" || projection == nil || len(projection.Items) != 1 || projection.Star || projection.Distinct || predicate != "" || projection.Tail != "" {
		return ""
	}
	item := projection.Items[0]
	if !item.HasAlias && strings.EqualFold(item.Name, "embedding") && !strings.HasPrefix(strings.TrimSpace(item.Expression), "`") {
		return item.Name
	}
	return ""
}
