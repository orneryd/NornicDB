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
	// The authenticated execution projects its source from this shared descriptor.
	program      *relationshipAccessProgram
	variable     string
	alternatives []relationshipSeek
	expressions  map[string]string
	inline       map[string]bool
	vector       *relationshipVectorAccess
	id           *relationshipIDAccess
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

func withRelationshipAccessPlans(ctx context.Context, source *relationshipAccessProgram, mode ExecutionMode) context.Context {
	if mode == ModeExplain || ctx.Value(relationshipAccessPlansKey{}) != nil {
		return ctx
	}
	// Cache only choices that depend on the live schema. Scan/ID syntax is
	// immutable source data, not a new plan for every incoming statement/row.
	for _, template := range source.byID {
		if template.kind == relationshipPatternAccess && len(template.syntax.expressions) != 0 {
			return context.WithValue(ctx, relationshipAccessPlansKey{}, &relationshipAccessPlans{plans: make(map[relationshipAccessKey]*relationshipAccess)})
		}
	}
	return ctx
}

// Both execution and EXPLAIN call this owner. The cache is statement-local,
// schema/namespace-specific, and contains expression text, never row values.
func (e *StorageExecutor) relationshipAccessFor(statement admittedRelationshipStatement, ctx context.Context, pattern, where string) *relationshipAccess {
	identity := relationshipAccessSourceIdentity(statement, ctx, relationshipAccessIdentityFor(pattern, where))
	return e.relationshipAccessForIdentity(statement, ctx, identity)
}

// Cached identity carries immutable syntax only. Authority always comes from
// the actual admitted program, including when a template has been removed.
func (e *StorageExecutor) relationshipAccessForIdentity(statement admittedRelationshipStatement, ctx context.Context, identity *relationshipAccessIdentity) *relationshipAccess {
	program := statement.program
	template := program.byID[identity.id]
	if template == nil {
		recordUnregisteredRelationshipAccess(ctx, identity.key.pattern, identity.key.where)
		return &relationshipAccess{program: statement.program}
	}
	return e.relationshipTemplateAccess(statement, ctx, template)
}

// A consumer that already has the program's template need not normalize and
// look up its identity again. Availability and schema choices have one owner.
func (e *StorageExecutor) relationshipTemplateAccess(statement admittedRelationshipStatement, ctx context.Context, template *relationshipAccessTemplate) *relationshipAccess {
	if template.kind == relationshipPathAccess {
		return &template.scan
	}
	// MERGE identity compares stored properties. Preserve its existing scan
	// contract instead of using a visible-property RANGE lookup for writes.
	if e.storedPropertyIdentity {
		return &template.scan
	}
	if len(template.syntax.expressions) == 0 {
		return &template.base
	}
	// Availability belongs to the current read view, even when nested entries
	// share a schema and the statement's immutable expression templates.
	reader, available := e.getStorage(ctx).(storage.RelationshipPropertySeeker)
	viewport, _ := TemporalViewportFromContext(ctx)
	if !available || !reader.RelationshipPropertySeeksAvailable() || viewport.Enabled() {
		return &template.base
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
	id                *relationshipIDAccess
	// Read-only structure for a stream's pre-row eligibility check. A pattern
	// with value operands has no such shape: its existing seeded owner must
	// evaluate it with the actual incoming row, never during source admission.
	staticTraversal *TraversalMatch
}

func compileRelationshipAccessSyntax(pattern, where string) relationshipAccessSyntax {
	var syntax relationshipAccessSyntax
	// The structural mask removes node/property operands before the existing
	// traversal reader runs. No bindings, values or storage are consulted.
	reader := relationshipSyntaxReader()
	match := reader.parseTraversalPattern(context.Background(), relationshipAccessPatternStructure(pattern))
	if match == nil {
		return syntax
	}
	if !relationshipPatternNeedsRow(pattern) {
		syntax.staticTraversal = reader.parseTraversalPattern(context.Background(), pattern)
	}
	syntax.variable = match.Relationship.Variable
	if match.IsChained || match.Relationship.VariableLength || match.Relationship.MinHops != 1 || match.Relationship.MaxHops != 1 {
		return syntax
	}
	if variable, expression, kind, ok := idEqualitySyntax(where); ok && variable == syntax.variable {
		independent := true
		for _, name := range expressionFreeVariables(expression) {
			if name == syntax.variable {
				independent = false
				break
			}
		}
		if independent {
			syntax.id = &relationshipIDAccess{kind: kind, expression: expression, estimatedRows: 1}
		}
	}
	if len(match.Relationship.Types) != 1 {
		return syntax
	}
	syntax.relType = match.Relationship.Types[0]
	expressions := make(map[string]string)
	if open, close := firstRelationshipBracket(pattern); open >= 0 {
		props := relationshipDeclarationOf(pattern[open+1 : close]).properties
		if strings.HasPrefix(props, "{") && strings.HasSuffix(props, "}") {
			for _, pair := range splitTopLevelComma(props[1 : len(props)-1]) {
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

// relationshipPatternNeedsRow uses the canonical lexical and dynamic-token
// readers. Admission records only whether evaluation must wait for a row; it
// never evaluates a map entry or a dynamic node/relationship token.
func relationshipPatternNeedsRow(pattern string) bool {
	for index := 0; index < len(pattern); index++ {
		if end, quoted := skipCypherLexicalToken(pattern, index); quoted {
			index = end - 1
			continue
		}
		if pattern[index] == '{' || dynamicLabelStartsAt(pattern, index) {
			return true
		}
	}
	return false
}

func relationshipStaticTraversal(statement admittedRelationshipStatement, pattern, where string) *TraversalMatch {
	template := statement.program.byID[relationshipAccessID(pattern, where)]
	if template == nil {
		return nil
	}
	return template.syntax.staticTraversal
}

func (e *StorageExecutor) planRelationshipAccess(ctx context.Context, template *relationshipAccessTemplate) *relationshipAccess {
	syntax := &template.syntax
	access := template.base
	if len(syntax.expressions) == 0 {
		return &template.base
	}
	indexes := e.getStorage(ctx).GetSchema().RelationshipEqualityIndexes(syntax.relType)
	for position := range indexes {
		index := &indexes[position]
		complete := true
		for _, property := range index.Properties {
			if syntax.expressions[property] == "" {
				complete = false
				break
			}
		}
		if complete {
			access.alternatives = append(access.alternatives, relationshipSeek{name: index.Name, relType: syntax.relType, properties: index.Properties, estimatedRows: 10})
		}
	}
	return &access
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

func (access *relationshipAccess) resolve(admission admittedRelationshipStatement, ctx context.Context, e *StorageExecutor, properties map[string]interface{}) (relationshipSeek, bool) {
	for _, alternative := range access.alternatives {
		values := make([]interface{}, len(alternative.properties))
		complete := true
		for j, property := range alternative.properties {
			var ok bool
			if access.inline[property] {
				// Pattern parsing already evaluated this expression for the row.
				// Reuse its value: evaluating it again could repeat a graph
				// expression or change a non-deterministic key.
				values[j], ok = properties[property]
			} else {
				values[j], ok = e.indexSeekConstant(admission, ctx, access.expressions[property])
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
		if isCypherQuoteByte(pattern[i]) {
			i = skipCypherQuotedText(pattern, i, pattern[i]) - 1
			continue
		}
		switch pattern[i] {
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
