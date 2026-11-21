package cypher

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/orneryd/nornicdb/pkg/auth"
)

func isFineGrainedAuthRuleStatement(ctx context.Context, text string) bool {
	if auth.FineGrainedRequestFromContext(ctx) == nil || cypherVersionFromContext(ctx) != "25" {
		return false
	}
	_, handled, err := parseFineGrainedAuthRuleCommand(ctx, text)
	return handled || err != nil
}

func (e *StorageExecutor) executeFineGrainedAuthRuleStatement(ctx context.Context, request *auth.FineGrainedRequest, text string) (*ExecuteResult, error) {
	command, _, err := parseFineGrainedAuthRuleCommand(ctx, text)
	if err != nil {
		return nil, err
	}
	verb := command.Verb
	if verb == "show" {
		return e.executeShowWithTail(ctx, text, func(ctx context.Context, head string) (*ExecuteResult, error) {
			if !request.AllowsDBMS("dbms.auth_rule.show", "") || command.WithRoles && !request.AllowsDBMS("dbms.role.show", "") {
				return nil, auth.ErrFineGrainedForbidden
			}
			catalog, _, err := request.Runtime.ReadCatalog(ctx)
			if err != nil {
				return nil, err
			}
			if command.WithRoles {
				return showRolesWithAuthRules(ctx, head, request, catalog.AuthRules, command.AsCommands), nil
			}
			return showAuthRules(request, catalog.AuthRules, command.AsCommands), nil
		})
	}

	action := "dbms.auth_rule." + verb
	switch verb {
	case "grant":
		action = "dbms.role.assign"
	case "revoke":
		action = "dbms.role.remove"
	}
	if !request.AllowsDBMS(action, "") || command.Replace && !request.AllowsDBMS("dbms.auth_rule.drop", "") {
		return nil, auth.ErrFineGrainedForbidden
	}
	if err := request.Runtime.EditCatalog(ctx, func(catalog *auth.FineGrainedCatalog) error {
		return applyFineGrainedAuthRuleCommand(catalog, command, verb)
	}); err != nil {
		return nil, err
	}
	return &ExecuteResult{}, nil
}

func applyFineGrainedAuthRuleCommand(catalog *auth.FineGrainedCatalog, command fineGrainedAuthRuleCommand, verb string) error {
	names := command.Names
	if len(names) == 0 {
		names = []string{command.Name}
	}
	if verb == "grant" || verb == "revoke" {
		for _, name := range names {
			index := slices.IndexFunc(catalog.AuthRules, func(rule auth.FineGrainedAuthRule) bool { return rule.Name == name })
			if index < 0 {
				return missingFineGrainedAuthRule(name)
			}
			rule := &catalog.AuthRules[index]
			for _, role := range command.Roles {
				role = auth.NormalizeRoleName(role)
				if verb == "revoke" {
					rule.Roles = slices.DeleteFunc(rule.Roles, func(value string) bool { return value == role })
				} else if !slices.Contains(rule.Roles, role) {
					rule.Roles = append(rule.Roles, role)
				}
			}
			slices.Sort(rule.Roles)
		}
		return nil
	}
	index := slices.IndexFunc(catalog.AuthRules, func(rule auth.FineGrainedAuthRule) bool { return rule.Name == command.Name })
	if verb == "create" {
		if index >= 0 && !command.Replace {
			if command.IfNotExists {
				return nil
			}
			return duplicateFineGrainedAuthRule(command.Name)
		}
		rule := auth.FineGrainedAuthRule{Name: command.Name, Condition: command.Condition, Enabled: true}
		if command.Enabled != nil {
			rule.Enabled = *command.Enabled
		}
		if index >= 0 {
			catalog.AuthRules[index] = rule
		} else {
			catalog.AuthRules = append(catalog.AuthRules, rule)
		}
		return nil
	}
	if index < 0 {
		if command.IfExists {
			return nil
		}
		return missingFineGrainedAuthRule(command.Name)
	}
	switch verb {
	case "alter":
		if command.Condition != "" {
			catalog.AuthRules[index].Condition = command.Condition
		}
		if command.Enabled != nil {
			catalog.AuthRules[index].Enabled = *command.Enabled
		}
	case "rename":
		if slices.ContainsFunc(catalog.AuthRules, func(rule auth.FineGrainedAuthRule) bool { return rule.Name == command.NewName }) {
			return duplicateFineGrainedAuthRule(command.NewName)
		}
		catalog.AuthRules[index].Name = command.NewName
	case "drop":
		catalog.AuthRules = slices.Delete(catalog.AuthRules, index, index+1)
	}
	return nil
}

func missingFineGrainedAuthRule(name string) error {
	return newSemanticError("Neo.ClientError.Statement.ArgumentError", "AuthRuleNotFound",
		fmt.Sprintf("Authorization rule %q does not exist", name))
}

func duplicateFineGrainedAuthRule(name string) error {
	return newSemanticError("Neo.ClientError.Statement.ArgumentError", "AuthRuleAlreadyExists",
		fmt.Sprintf("Authorization rule %q already exists", name))
}

// SHOW AUTH RULES reveals role assignments only to a caller with SHOW ROLE.
func showAuthRules(request *auth.FineGrainedRequest, rules []auth.FineGrainedAuthRule, commands bool) *ExecuteResult {
	result := &ExecuteResult{Columns: []string{"name", "condition", "enabled", "roles"}}
	if commands {
		result.Columns = []string{"command", "roles"}
		withShowDefaultColumns(result, []string{"command"})
	}
	canShowRoles := request.AllowsDBMS("dbms.role.show", "")
	slices.SortFunc(rules, func(a, b auth.FineGrainedAuthRule) int { return strings.Compare(a.Name, b.Name) })
	for _, rule := range rules {
		var roles any
		if canShowRoles {
			roles = append([]string{}, rule.Roles...)
		}
		if commands {
			command := fmt.Sprintf("CREATE AUTH RULE %s SET CONDITION %s SET ENABLED %t",
				fineGrainedDisplayName(rule.Name), rule.Condition, rule.Enabled)
			result.Rows = append(result.Rows, []any{command, roles})
		} else {
			result.Rows = append(result.Rows, []any{rule.Name, rule.Condition, rule.Enabled, roles})
		}
	}
	return result
}

func showRolesWithAuthRules(ctx context.Context, head string, request *auth.FineGrainedRequest, rules []auth.FineGrainedAuthRule, commands bool) *ExecuteResult {
	result := &ExecuteResult{Columns: []string{"role", "authRule"}}
	if commands {
		result.Columns = []string{"command", "role", "authRule", "immutable"}
		withShowDefaultColumns(result, []string{"command"})
	}
	byRole := make(map[string][]string)
	for _, rule := range rules {
		for _, role := range rule.Roles {
			byRole[auth.NormalizeRoleName(role)] = append(byRole[auth.NormalizeRoleName(role)], rule.Name)
		}
	}
	populated := startsWithKeywords(ctx, head, "SHOW", "POPULATED")
	if populated {
		for _, user := range request.Runtime.Users() {
			for _, role := range user.Roles {
				name := auth.NormalizeRoleName(string(role))
				if _, ok := byRole[name]; !ok {
					byRole[name] = nil
				}
			}
			byRole[auth.FineGrainedPublicRole] = nil
		}
	}
	for _, role := range request.Runtime.RoleNames() {
		names, exists := byRole[role]
		if len(names) == 0 && !commands && (!populated || exists) {
			result.Rows = append(result.Rows, []any{role, nil})
		}
		slices.Sort(names)
		for _, name := range names {
			if commands {
				command := fmt.Sprintf("GRANT ROLE %s TO AUTH RULE %s",
					fineGrainedDisplayName(role), fineGrainedDisplayName(name))
				result.Rows = append(result.Rows, []any{command, role, name, auth.IsBuiltinRole(role)})
			} else {
				result.Rows = append(result.Rows, []any{role, name})
			}
		}
	}
	return result
}
