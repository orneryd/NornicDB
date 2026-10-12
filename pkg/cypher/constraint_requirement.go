package cypher

import (
	"regexp"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// constraintRequirementSpelling is a uniqueness or key requirement as Neo4j
// spells it: properties, IS, an optional entity (NODE, RELATIONSHIP, REL)
// and UNIQUE or KEY.
var constraintRequirementSpelling = regexp.MustCompile(`(?is)^(.*?)\s+IS\s+(?:(NODE|RELATIONSHIP|REL)\s+)?(UNIQUE|KEY)\s*$`)

// canonicalConstraintRequirement rewrites a CREATE CONSTRAINT … FOR … REQUIRE
// uniqueness or key requirement from any of Neo4j's spellings (IS [NODE |
// RELATIONSHIP | REL] UNIQUE | KEY, on one property or a parenthesized list)
// to the one the constraint shapes read: a node pattern's `… IS UNIQUE` and
// `(…) IS NODE KEY`, a relationship pattern's `… IS UNIQUE` and `… IS
// RELATIONSHIP KEY`. An entity that isn't the pattern's is Neo4j's
// SyntaxError ('IS NODE KEY' does not allow relationship patterns). Any
// other statement is returned as it is.
func (e *StorageExecutor) canonicalConstraintRequirement(cypher string) (string, error) {
	_, patternSpan, requireExpr, err := e.parseCreateConstraintForRequireHead(cypher)
	if err != nil {
		return cypher, nil
	}
	match := constraintRequirementSpelling.FindStringSubmatch(requireExpr)
	if match == nil {
		return cypher, nil
	}
	_, _, relationship, err := parseCreateIndexForPattern(patternSpan)
	if err != nil {
		return cypher, nil
	}
	properties, entity, kind := strings.TrimSpace(match[1]), strings.ToUpper(match[2]), strings.ToUpper(match[3])
	if entity == "REL" {
		entity = "RELATIONSHIP"
	}
	if entity != "" && (entity == "RELATIONSHIP") != relationship {
		pattern := "node"
		if relationship {
			pattern = "relationship"
		}
		return "", localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidConstraintPattern",
			localization.CypherSchemaConstraintEntityMismatch("IS "+entity+" "+kind, pattern))
	}
	canonical := properties + " IS UNIQUE"
	switch {
	case kind == "KEY" && relationship:
		canonical = properties + " IS RELATIONSHIP KEY"
	case kind == "KEY":
		if !strings.HasPrefix(properties, "(") {
			properties = "(" + properties + ")"
		}
		canonical = properties + " IS NODE KEY"
	}
	start := strings.Index(cypher, requireExpr)
	return cypher[:start] + canonical + cypher[start+len(requireExpr):], nil
}
