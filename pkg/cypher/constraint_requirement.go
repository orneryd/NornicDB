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
func (e *StorageExecutor) canonicalConstraintRequirement(view braceView) (braceView, error) {
	_, patternSpan, requireExpr, err := e.parseCreateConstraintForRequireHead(view)
	if err != nil {
		return view, nil
	}
	indices := constraintRequirementSpelling.FindStringSubmatchIndex(requireExpr.text())
	if indices == nil {
		return view, nil
	}
	_, _, relationship, err := parseCreateIndexForPattern(patternSpan)
	if err != nil {
		return view, nil
	}
	properties := requireExpr.window(indices[2], indices[3]).trim()
	entity := ""
	if indices[4] >= 0 {
		entity = strings.ToUpper(requireExpr.text()[indices[4]:indices[5]])
	}
	kind := strings.ToUpper(requireExpr.text()[indices[6]:indices[7]])
	if entity == "REL" {
		entity = "RELATIONSHIP"
	}
	if entity != "" && (entity == "RELATIONSHIP") != relationship {
		pattern := "node"
		if relationship {
			pattern = "relationship"
		}
		return braceView{}, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidConstraintPattern",
			localization.CypherSchemaConstraintEntityMismatch("IS "+entity+" "+kind, pattern))
	}
	canonical := properties.wrap("", " IS UNIQUE")
	switch {
	case kind == "KEY" && relationship:
		canonical = properties.wrap("", " IS RELATIONSHIP KEY")
	case kind == "KEY":
		if !strings.HasPrefix(properties.text(), "(") {
			properties = properties.wrap("(", ")")
		}
		canonical = properties.wrap("", " IS NODE KEY")
	}
	relations, relationErr := requireExpr.source.CopyRelationsFrom(view.source)
	if relationErr != nil {
		panic(relationErr)
	}
	if len(relations) != 1 || relations[0].Accepted.Start != 0 || relations[0].Accepted.End != len(requireExpr.text()) {
		panic("cypher: requirement grammar lost its input extent")
	}
	span := relations[0].Original
	return view.assembleCode([]braceView{view.window(0, span.Start), canonical, view.window(span.End, len(view.text()))}, ""), nil
}
