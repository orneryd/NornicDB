package cypher

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// pipelineApplyMatchProduct matches the comma-separated parts of one MATCH,
// part by part, and keeps the combinations its WHERE accepts. A WHERE
// condition that reads one node part's variable only (id(a) = $x,
// a.name STARTS WITH 'x') is matched with that part, so its seeks and filters
// narrow the part before any combination is built, as Neo4j plans it below
// its CartesianProduct; only the other conditions are evaluated on the
// combinations (#940).
func (e *StorageExecutor) pipelineApplyMatchProduct(ctx context.Context, rows []pipelineRow, parts []string, where string) ([]pipelineRow, bool, error) {
	nodeVariables := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "(") && !containsRelExistencePattern(part) {
			if variable := e.parseNodePattern(ctx, part).variable; variable != "" {
				nodeVariables = append(nodeVariables, variable)
			}
		}
	}
	nodeWhere, rest := splitWhereByVariable(where, nodeVariables)
	if joined, handled, err := e.pipelineApplyNodeJoinProduct(ctx, rows, parts, where); handled || err != nil {
		return joined, handled, err
	}
	where = rest
	var hidden, paths []string
	usedNames := strings.Join(parts, " ") + " " + where
	newBinding := func() string {
		for index := 0; ; index++ {
			name := fmt.Sprintf("__nornic_match_product_%d", index)
			if referencesVariable(usedNames, name) {
				continue
			}
			bound := false
			for _, row := range rows {
				if _, exists := row[name]; exists {
					bound = true
					break
				}
			}
			if !bound {
				usedNames += " " + name
				hidden = append(hidden, name)
				return name
			}
		}
	}
	for _, part := range parts {
		if err := ctx.Err(); err != nil {
			return nil, true, err
		}
		part = strings.TrimSpace(part)
		if containsRelExistencePattern(part) {
			path := extractPathAssignmentVariable(part)
			if path == "" {
				path = newBinding()
				part = path + " = " + part
			}
			paths = append(paths, path)
		} else if strings.HasPrefix(part, "(") {
			if variable := e.parseNodePattern(ctx, part).variable; variable == "" {
				part = "(" + newBinding() + part[1:]
			} else if own := nodeWhere[variable]; own != "" {
				part += " WHERE " + own
			}
		}
		expanded, handled, err := e.pipelineApplyMatch(ctx, rows, "MATCH "+part)
		if !handled || err != nil {
			return expanded, handled, err
		}
		rows = expanded
		if len(paths) > 1 {
			filtered := rows[:0]
			for _, row := range rows {
				if repeatableElements(ctx) || pipelineProductPathsUnique(row, paths) {
					filtered = append(filtered, row)
				}
			}
			rows = filtered
		}
	}
	filtered := rows[:0]
	for _, row := range rows {
		if strings.TrimSpace(where) != "" && !e.evaluateRowPredicate(ctx, where, row) {
			continue
		}
		for _, name := range hidden {
			delete(row, name)
		}
		filtered = append(filtered, row)
	}
	if failure := getExpressionFailure(ctx); failure != nil {
		return nil, true, failure
	}
	return filtered, true, nil
}

// pipelineApplyNodeJoinProduct joins node parts on a WHERE property
// equality (a.k = b.k) instead of building every combination. Each part's
// candidates are narrowed by the conditions that read its variable only
// (splitWhereByVariable) first.
func (e *StorageExecutor) pipelineApplyNodeJoinProduct(ctx context.Context, rows []pipelineRow, parts []string, where string) ([]pipelineRow, bool, error) {
	if strings.TrimSpace(where) == "" {
		return nil, false, nil
	}
	templates := make([]*pipelineNodeMatchTemplate, len(parts))
	variables := make(map[string]struct{}, len(parts))
	for index, part := range parts {
		template := e.pipelineNodeMatchTemplateFor("MATCH " + strings.TrimSpace(part))
		if !template.usable || template.labelErr != nil {
			return nil, false, nil
		}
		if _, repeated := variables[template.variable]; repeated {
			return nil, false, nil
		}
		variables[template.variable] = struct{}{}
		templates[index] = template
	}
	for _, template := range templates {
		for _, property := range template.properties {
			for _, reference := range semanticExpressionReferences(property.expr) {
				variable := strings.SplitN(reference, ".", 2)[0]
				if _, dependent := variables[variable]; dependent {
					return nil, false, nil
				}
			}
		}
	}
	joinable := false
	for _, term := range splitTopLevelAndConjuncts(where) {
		left, _, right, _, _, equality := parseCartesianVarPropEqualityTerm(strings.TrimSpace(term))
		_, leftLocal := variables[left]
		_, rightLocal := variables[right]
		joinable = joinable || equality && leftLocal && rightLocal && left != right
	}
	if !joinable {
		return nil, false, nil
	}
	names := make([]string, 0, len(variables))
	for name := range variables {
		names = append(names, name)
	}
	nodeWhere, _ := splitWhereByVariable(where, names)
	var out []pipelineRow
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, true, err
		}
		matches := make([]struct {
			variable string
			nodes    []*storage.Node
		}, len(templates))
		for index, template := range templates {
			pattern, resolved := template.node(ctx, e, row)
			if !resolved {
				return nil, true, newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "Cannot evaluate MATCH properties")
			}
			var nodes []*storage.Node
			if value, bound := row[pattern.variable]; bound {
				if node, typed := value.(*storage.Node); typed && node != nil && pipelineNodeMatchesPattern(node, pattern) {
					nodes = []*storage.Node{node}
				}
			} else {
				own := nodeWhere[pattern.variable]
				rowCtx := withValueBindings(ctx, row)
				var whereApplied bool
				var err error
				nodes, whereApplied, err = e.collectPipelineInitialNodeCandidates(rowCtx, pattern, own, pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1})
				if err != nil {
					return nil, true, err
				}
				if own != "" && !whereApplied {
					nodes = e.filterNodes(rowCtx, nodes, pattern.variable, own)
				}
			}
			matches[index].variable, matches[index].nodes = pattern.variable, nodes
		}
		combinations, _ := e.buildCombinationsUsingWhereJoin(matches, where)
		for _, combination := range combinations {
			joined := make(pipelineRow, len(row)+len(combination))
			for name, value := range row {
				joined[name] = value
			}
			for name, node := range combination {
				joined[name] = node
			}
			if e.evaluateRowPredicate(ctx, where, joined) {
				out = append(out, joined)
			}
		}
	}
	if failure := getExpressionFailure(ctx); failure != nil {
		return nil, true, failure
	}
	return out, true, nil
}

func pipelineProductPathsUnique(row pipelineRow, variables []string) bool {
	seen := make(map[storage.EdgeID]struct{})
	for _, variable := range variables {
		value, ok := row[variable].(map[string]interface{})
		if !ok {
			return false
		}
		_, relationships, _, ok := pathValueParts(value)
		if !ok {
			return false
		}
		for _, relationship := range relationships {
			edge, ok := relationship.(*storage.Edge)
			if !ok || edge == nil {
				return false
			}
			if _, reused := seen[edge.ID]; reused {
				return false
			}
			seen[edge.ID] = struct{}{}
		}
	}
	return true
}
