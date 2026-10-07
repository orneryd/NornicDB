package cypher

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) pipelineApplyMatchProduct(ctx context.Context, rows []pipelineRow, parts []string, where string) ([]pipelineRow, bool, error) {
	if joined, handled, err := e.pipelineApplyNodeJoinProduct(ctx, rows, parts, where); handled || err != nil {
		return joined, handled, err
	}
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
		} else if strings.HasPrefix(part, "(") && e.parseNodePattern(ctx, part).variable == "" {
			part = "(" + newBinding() + part[1:]
		}
		expanded, handled, err := e.pipelineApplyMatch(ctx, rows, "MATCH "+part)
		if !handled || err != nil {
			return expanded, handled, err
		}
		rows = expanded
		if len(paths) > 1 {
			filtered := rows[:0]
			for _, row := range rows {
				if pipelineProductPathsUnique(row, paths) {
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
				var err error
				nodes, _, err = e.collectPipelineInitialNodeCandidates(withValueBindings(ctx, row), pattern, "", pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1})
				if err != nil {
					return nil, true, err
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
