package cypher

import (
	"context"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// relationshipAccess is the access planner's immutable decision. Key expressions
// are resolved only by execution, against its actual row. Planning never builds
// a second incoming scope and never evaluates an expression (including writes,
// procedures or graph expressions). A conditional seek has an explicit scan
// fallback when the existing constant/value owner cannot supply an indexed key.
type relationshipAccess struct {
	variable     string
	alternatives []relationshipSeek
	expressions  [][]string
	inline       map[string]bool
	vector       *relationshipVectorAccess
}

type relationshipAccessKey struct {
	schema         *storage.SchemaManager
	pattern, where string
}
type relationshipAccessPlans struct {
	mu    sync.Mutex
	plans map[relationshipAccessKey]*relationshipAccess
}
type relationshipAccessPlansKey struct{}

func withRelationshipAccessPlans(ctx context.Context, query string) context.Context {
	if ctx.Value(relationshipAccessPlansKey{}) != nil || !strings.Contains(query, "-[") {
		return ctx
	}
	return context.WithValue(ctx, relationshipAccessPlansKey{}, &relationshipAccessPlans{plans: make(map[relationshipAccessKey]*relationshipAccess)})
}

// Both execution and EXPLAIN call this owner. The cache is statement-local,
// schema/namespace-specific, and contains expression text, never row values.
func (e *StorageExecutor) relationshipAccessFor(ctx context.Context, pattern, where string) *relationshipAccess {
	program, _ := ctx.Value(relationshipAccessProgramKey{}).(*relationshipAccessProgram)
	if program == nil {
		return &relationshipAccess{}
	}
	template := program.byID[relationshipAccessID(pattern, where)]
	if template == nil {
		recordUnregisteredRelationshipAccess(ctx, pattern, where)
		return &relationshipAccess{}
	}
	if template.scanOnly {
		return &relationshipAccess{variable: template.syntax.variable}
	}
	// MERGE identity compares stored properties. Preserve its existing scan
	// contract instead of using a visible-property RANGE lookup for writes.
	if e.storedPropertyIdentity {
		return &relationshipAccess{variable: template.syntax.variable}
	}
	// Availability belongs to the current read view, even when nested entries
	// share a schema and the statement's immutable expression templates.
	reader, available := e.getStorage(ctx).(storage.RelationshipPropertySeeker)
	viewport, _ := TemporalViewportFromContext(ctx)
	if !available || !reader.RelationshipPropertySeeksAvailable() || viewport.Enabled() {
		return &relationshipAccess{variable: template.syntax.variable}
	}
	plans, _ := ctx.Value(relationshipAccessPlansKey{}).(*relationshipAccessPlans)
	if plans == nil {
		return e.planRelationshipAccess(ctx, template)
	}
	key := relationshipAccessKey{e.getStorage(ctx).GetSchema(), template.pattern, template.where}
	plans.mu.Lock()
	defer plans.mu.Unlock()
	if access := plans.plans[key]; access != nil {
		return access
	}
	access := e.planRelationshipAccess(ctx, template)
	if len(plans.plans) < 1024 {
		plans.plans[key] = access
	}
	return access
}

// relationshipAccessSyntax contains only graph-independent facts. Its maps are
// immutable; schema alternatives and row key values never enter the program.
type relationshipAccessSyntax struct {
	variable, relType string
	expressions       map[string]string
	inline            map[string]bool
}

func compileRelationshipAccessSyntax(pattern, where string) relationshipAccessSyntax {
	var syntax relationshipAccessSyntax
	// The structural mask removes node/property operands before the existing
	// traversal reader runs. No bindings, values or storage are consulted.
	reader := &StorageExecutor{}
	match := reader.parseTraversalPattern(context.Background(), relationshipAccessPatternStructure(pattern))
	if match == nil {
		return syntax
	}
	syntax.variable = match.Relationship.Variable
	if match.IsChained || match.Relationship.VariableLength || match.Relationship.MinHops != 1 || match.Relationship.MaxHops != 1 || len(match.Relationship.Types) != 1 {
		return syntax
	}
	syntax.relType = match.Relationship.Types[0]
	expressions := make(map[string]string)
	if open, close := firstRelationshipBracket(pattern); open >= 0 {
		props := relationshipDeclarationOf(pattern[open+1 : close]).properties
		if strings.HasPrefix(props, "{") && strings.HasSuffix(props, "}") {
			for _, pair := range reader.splitPropertyPairs(props[1 : len(props)-1]) {
				if colon := findTopLevelMapKeyValueSeparator(pair); colon >= 0 {
					property := normalizePropertyKey(pair[:colon])
					expressions[property] = strings.TrimSpace(pair[colon+1:])
					if syntax.inline == nil {
						syntax.inline = make(map[string]bool)
					}
					syntax.inline[property] = true
				}
			}
		}
	}
	for _, conjunct := range splitTopLevelAndConjuncts(where) {
		property, expr, ok := relationshipSeekEquality(syntax.variable, conjunct)
		if ok && expressions[property] == "" {
			expressions[property] = expr
		}
	}
	// The candidate relationship cannot be an incoming scalar key. Record this
	// once, rather than re-reading free variables for every schema/row.
	for property, expression := range expressions {
		for _, name := range expressionFreeVariables(expression) {
			if name == syntax.variable && name != "" {
				delete(expressions, property)
				break
			}
		}
	}
	syntax.expressions = expressions
	return syntax
}

func (e *StorageExecutor) planRelationshipAccess(ctx context.Context, template *relationshipAccessTemplate) *relationshipAccess {
	syntax := &template.syntax
	access := &relationshipAccess{variable: syntax.variable, inline: syntax.inline}
	if len(syntax.expressions) == 0 {
		return access
	}
	for _, index := range e.getStorage(ctx).GetSchema().RelationshipEqualityIndexes(syntax.relType) {
		keys := make([]string, len(index.Properties))
		complete := true
		for i, property := range index.Properties {
			keys[i] = syntax.expressions[property]
			if keys[i] == "" {
				complete = false
				break
			}
		}
		if complete {
			access.alternatives = append(access.alternatives, relationshipSeek{name: index.Name, relType: syntax.relType, properties: index.Properties, estimatedRows: 10})
			access.expressions = append(access.expressions, keys)
		}
	}
	return access
}

// Exact identifier ownership and the key expression are parsed once, without
// consulting any incoming bindings. This also serves the release parser, whose
// general node-seek reader evaluates its constant while reading the predicate.
func relationshipSeekEquality(variable, predicate string) (string, string, bool) {
	if variable == "" {
		return "", "", false
	}
	clause := unwrapOuterParens(strings.TrimSpace(predicate))
	if hasTopLevelLogicalOperator(clause) {
		return "", "", false
	}
	for _, keyword := range []string{"NOT", " IN ", " IS ", "<>", "!=", ">=", "<=", ">", "<"} {
		if topLevelKeywordIndex(clause, keyword) >= 0 {
			return "", "", false
		}
	}
	equals := topLevelEqualsIndex(clause)
	if equals < 0 {
		return "", "", false
	}
	left, right := strings.TrimSpace(clause[:equals]), strings.TrimSpace(clause[equals+1:])
	for _, pair := range [][2]string{{left, right}, {right, left}} {
		name, rest, ok := parseIdentifierToken(pair[0])
		if !ok || symbolicNameValue(name) != variable {
			continue
		}
		rest = strings.TrimSpace(rest)
		if !strings.HasPrefix(rest, ".") {
			continue
		}
		name, tail, ok := parseIdentifierToken(strings.TrimSpace(rest[1:]))
		if ok && strings.TrimSpace(tail) == "" {
			return symbolicNameValue(name), pair[1], true
		}
	}
	return "", "", false
}

func (access *relationshipAccess) resolve(ctx context.Context, e *StorageExecutor, properties map[string]interface{}) (relationshipSeek, bool) {
	for i, alternative := range access.alternatives {
		values := make([]interface{}, len(access.expressions[i]))
		complete := true
		for j, expr := range access.expressions[i] {
			var ok bool
			if access.inline[alternative.properties[j]] {
				// Pattern parsing already evaluated this expression for the row.
				// Reuse its value: evaluating it again could repeat a graph
				// expression or change a non-deterministic key.
				values[j], ok = properties[alternative.properties[j]]
			} else {
				values[j], ok = e.indexSeekConstant(ctx, expr)
			}
			if !ok {
				complete = false
				break
			}
		}
		if complete && storage.RelationshipIndexValuesSupported(values) {
			alternative.values = values
			return alternative, true
		}
	}
	return relationshipSeek{}, false
}

// Property expressions must not run while EXPLAIN reads traversal structure.
func relationshipAccessPatternStructure(pattern string) string {
	masked := []byte(pattern)
	brackets := 0
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '\'', '"', '`':
			i = skipCypherQuotedText(pattern, i, pattern[i]) - 1
		case '[':
			brackets++
		case ']':
			brackets--
		case '(':
			if brackets == 0 {
				if end := findMatchingDelimiter(pattern, i, '(', ')'); end >= 0 {
					for j := i + 1; j < end; j++ {
						masked[j] = ' '
					}
					i = end
				}
			}
		case '{':
			end := findMatchingDelimiter(pattern, i, '{', '}')
			if end < 0 {
				return pattern
			}
			for j := i; j <= end; j++ {
				masked[j] = ' '
			}
			i = end
		}
	}
	return string(masked)
}
