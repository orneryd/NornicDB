package cypher

import (
	"cmp"
	"context"
	"slices"
	"strings"

	"github.com/orneryd/nornicdb/pkg/auth"
)

func (e *StorageExecutor) executeFineGrainedPrivilege(ctx context.Context, request *auth.FineGrainedRequest, command fineGrainedPrivilegeCommand) (*ExecuteResult, error) {
	action := "dbms.security.privilege.assign"
	if strings.HasPrefix(command.Verb, "revoke") {
		action = "dbms.security.privilege.remove"
	}
	if !request.AllowsDBMS(action, "") || (command.Immutable && !authenticationDisabled(ctx)) {
		return nil, auth.ErrFineGrainedForbidden
	}
	if command.Verb == fineGrainedDenyVerb && command.Privilege == "MERGE" {
		return nil, fineGrainedPrivilegeSyntaxError("MERGE cannot be denied")
	}

	var predicate *auth.FineGrainedPredicate
	if command.Predicate != "" {
		labels, bound, err := parseFineGrainedPredicate(ctx, command.Predicate)
		if err != nil {
			return nil, err
		}
		command.Types = labels
		predicate = bound
		if predicate.Operator == "NATIVE TAG" && (command.Verb == fineGrainedDenyVerb || command.Verb == fineGrainedRevokeDenyVerb) {
			return nil, fineGrainedPrivilegeSyntaxError("native tag property rules cannot be denied")
		}
	}
	parts := fineGrainedPrivilegeParts(command)
	if len(parts) == 0 {
		return nil, unsupportedAdministrationCommandError(ctx, command.Privilege)
	}
	if len(command.Graphs) == 0 {
		command.Graphs = []string{""}
	}
	roles := command.Roles
	labels := fineGrainedPrivilegeValues(command.Types)
	properties := fineGrainedPrivilegeValues(command.Properties)
	if len(command.Users) != 0 {
		properties = fineGrainedPrivilegeValues(command.Users)
	}
	groups := [][]fineGrainedActionPart{parts}
	if command.Kind != fineGrainedDatabaseKind && command.Kind != fineGrainedDBMSKind {
		var nodes, relationships []fineGrainedActionPart
		for _, part := range parts {
			switch part.kind {
			case auth.FineGrainedNode:
				nodes = append(nodes, part)
			case auth.FineGrainedRelationship:
				relationships = append(relationships, part)
			}
		}
		groups = [][]fineGrainedActionPart{nodes, relationships}
	}
	var privileges []auth.FineGrainedPrivilege
	for _, graph := range command.Graphs {
		databaseID := ""
		if graph != "" && graph != "*" {
			resolved := resolveGraphSelection(e.dbManager, graph)
			databaseID = request.DatabaseID(resolved.target)
			if databaseID == "" || resolved.remote || resolved.err != nil {
				return nil, newSemanticError("Neo.ClientError.Database.DatabaseNotFound", "DatabaseNotFound", "Unknown privilege database")
			}
			graph = resolved.target
		}
		for _, typ := range labels {
			for _, property := range properties {
				for _, group := range groups {
					if len(group) != 0 {
						privileges = appendFineGrainedPrivilege(privileges, command, roles, group, databaseID, graph, typ, property, predicate)
					}
				}
			}
		}
	}
	err := request.Runtime.EditCatalog(ctx, func(catalog *auth.FineGrainedCatalog) error {
		return auth.ApplyFineGrainedPrivileges(catalog, privileges, command.Verb, authenticationDisabled(ctx))
	})
	return &ExecuteResult{}, err
}

func fineGrainedPrivilegeValues(values []string) []string {
	if len(values) == 0 || len(values) == 1 && values[0] == "*" {
		return []string{""}
	}
	return values
}

func appendFineGrainedPrivilege(dst []auth.FineGrainedPrivilege, command fineGrainedPrivilegeCommand, roles []string, parts []fineGrainedActionPart, databaseID, graph, typ, property string, predicate *auth.FineGrainedPredicate) []auth.FineGrainedPrivilege {
	kind := parts[0].kind
	meta := renderFineGrainedPrivilege(command, kind, graph, typ, property, predicate)
	meta.Key = auth.FineGrainedPrivilegeKey(databaseID, kind, meta.Clause)
	meta.Deny = command.Verb == fineGrainedDenyVerb
	meta.Immutable = command.Immutable
	for _, part := range parts {
		scope := auth.FineGrainedScope{
			Kind: part.kind, DatabaseID: databaseID, Predicate: predicate,
			HomeDatabase: graph == "" && part.kind != auth.FineGrainedDBMS,
		}
		if typ != "" && (part.kind == auth.FineGrainedNode || part.kind == auth.FineGrainedRelationship) {
			scope.Types = []string{typ}
		}
		if property != "" && part.property {
			if strings.HasPrefix(command.Privilege, "EXECUTE ") {
				scope.PropertyPatterns = []string{property}
			} else {
				scope.Properties = []string{property}
			}
		}
		meta.Entitlements = append(meta.Entitlements, auth.FineGrainedEntitlement{Action: part.id, Scope: scope, Deny: meta.Deny})
	}
	for _, role := range roles {
		meta.Role = auth.NormalizeRoleName(role)
		dst = append(dst, meta)
	}
	return dst
}

func renderFineGrainedPrivilege(command fineGrainedPrivilegeCommand, kind auth.FineGrainedResourceKind, graph, typ, property string, predicate *auth.FineGrainedPredicate) auth.FineGrainedPrivilege {
	record := auth.FineGrainedPrivilege{
		Name: lowerASCII(strings.ReplaceAll(command.Privilege, " ", "_")), Graph: graph, Resource: "database", Segment: "database",
	}
	action := command.Privilege
	typeName, propertyName := fineGrainedDisplayName(typ), fineGrainedDisplayName(property)
	if fineGrainedTargetsUsers(action) {
		if property != "" {
			action += " (" + propertyName + ")"
		}
		record.Segment = "USER(" + rawFineGrainedName(property) + ")"
	}
	if kind == auth.FineGrainedDBMS {
		record.Graph = "*"
		if strings.HasPrefix(action, "EXECUTE ") {
			action += " " + propertyName
			family := "PROCEDURE"
			if strings.HasSuffix(command.Privilege, " FUNCTION") {
				family = "FUNCTION"
			}
			record.Segment = family + "(" + rawFineGrainedName(property) + ")"
		}
		record.Clause = action + " ON DBMS"
		return record
	}
	target := "DATABASE"
	if kind != auth.FineGrainedDatabase {
		target = "GRAPH"
	}
	on := " ON " + target + " " + fineGrainedDisplayName(graph)
	if graph == "*" {
		on = " ON " + target + " *"
	}
	if graph == "" {
		on = " ON HOME " + target
		record.Graph = "HOME"
	}
	segment := ""
	element := "NODE"
	if kind == auth.FineGrainedRelationship {
		element = "RELATIONSHIP"
	}
	switch command.Privilege {
	case "READ", "MATCH", "MERGE", "SET PROPERTY":
		action += " {" + propertyName + "}"
		record.Resource = "property(" + rawFineGrainedName(property) + ")"
	case "SET LABEL", "REMOVE LABEL":
		action += " " + typeName
		record.Resource = "label"
	}
	if target == "GRAPH" {
		record.Segment = element + "(" + rawFineGrainedName(typ) + ")"
		if record.Resource == "database" {
			record.Resource = "graph"
		}
		switch {
		case predicate != nil:
			segment = " " + renderFineGrainedPredicate(predicate, kind, typ)
		case command.Privilege != "SET LABEL" && command.Privilege != "REMOVE LABEL" &&
			command.Privilege != "WRITE" && command.Privilege != "ALL GRAPH PRIVILEGES":
			segment = " " + element + "S " + typeName
		}
	}
	record.Clause = action + on + segment
	return record
}

func fineGrainedTargetsUsers(action string) bool {
	switch action {
	case "IMPERSONATE", "SHOW TRANSACTION", "TERMINATE TRANSACTION", "TRANSACTION MANAGEMENT":
		return true
	}
	return false
}

func rawFineGrainedName(name string) string {
	if name == "" {
		return "*"
	}
	return name
}

func fineGrainedDisplayName(name string) string {
	if name == "" {
		return "*"
	}
	return quoteKnowledgePolicyName(name)
}

func isShowUserPrivileges(ctx context.Context, query string) bool {
	if auth.FineGrainedRequestFromContext(ctx) == nil ||
		!(startsWithKeywords(ctx, query, "SHOW", "USER") || startsWithKeywords(ctx, query, "SHOW", "USERS")) {
		return false
	}
	head := showCommandHead(ctx, query)
	return findKeywordIndex(ctx, head, "PRIVILEGES") >= 0 || findKeywordIndex(ctx, head, "PRIVILEGE") >= 0
}

func (e *StorageExecutor) executeShowFineGrainedPrivileges(ctx context.Context, request *auth.FineGrainedRequest, head string) (*ExecuteResult, error) {
	showUsers := isShowUserPrivileges(ctx, head)
	var includedUsers map[string][]string
	if showUsers {
		names, err := fineGrainedPrivilegeUsers(ctx, head, request.AllowsDBMS("dbms.security.privilege.show", ""))
		if err != nil {
			return nil, err
		}
		includedUsers = make(map[string][]string, len(names))
		for _, user := range request.Runtime.Users() {
			if slices.Contains(names, user.Username) {
				roles := []string{auth.FineGrainedPublicRole}
				for _, role := range user.Roles {
					name := auth.NormalizeRoleName(string(role))
					if !slices.Contains(roles, name) {
						roles = append(roles, name)
					}
				}
				includedUsers[user.Username] = roles
			}
		}
	} else if !request.AllowsDBMS("dbms.security.privilege.show", "") {
		return nil, auth.ErrFineGrainedForbidden
	}

	catalog, _, err := request.Runtime.ReadCatalog(ctx)
	if err != nil {
		return nil, err
	}
	roles := showPrivilegeRoles(ctx, head)
	commands := findMultiWordKeywordIndex(ctx, head, "AS", "COMMANDS") >= 0
	revokes := findMultiWordKeywordIndex(ctx, head, "AS", "REVOKE COMMANDS") >= 0
	result := &ExecuteResult{Columns: slices.Clone(showPrivilegeColumns)}
	if commands || revokes {
		result.Columns = []string{"command"}
	} else if showUsers {
		result.Columns = append(result.Columns, "user")
	}
	slices.SortFunc(catalog.Privileges, func(left, right auth.FineGrainedPrivilege) int {
		return cmp.Or(cmp.Compare(left.Role, right.Role), cmp.Compare(left.Clause, right.Clause), cmp.Compare(left.Key, right.Key))
	})
	seenCommands := make(map[string]bool)
	for _, privilege := range catalog.Privileges {
		if len(roles) != 0 && !slices.Contains(roles, privilege.Role) {
			continue
		}
		mode, verb := "GRANTED", "GRANT"
		if privilege.Deny {
			mode, verb = "DENIED", "DENY"
		}
		var users []string
		if showUsers {
			for name, userRoles := range includedUsers {
				if slices.Contains(userRoles, privilege.Role) {
					users = append(users, name)
				}
			}
			if len(users) == 0 {
				continue
			}
			slices.Sort(users)
		}
		if !commands && !revokes {
			row := []any{mode, privilege.Name, privilege.Resource, privilege.Graph, privilege.Segment, privilege.Role, privilege.Immutable}
			if showUsers {
				for _, user := range users {
					result.Rows = append(result.Rows, append(slices.Clone(row), user))
				}
			} else {
				result.Rows = append(result.Rows, row)
			}
			continue
		}
		relation := " TO "
		if revokes {
			verb = "REVOKE " + verb
			relation = " FROM "
		}
		if privilege.Immutable {
			verb += " IMMUTABLE"
		}
		command := verb + " " + privilege.Clause + relation + fineGrainedDisplayName(privilege.Role)
		if !seenCommands[command] {
			result.Rows = append(result.Rows, []any{command})
			seenCommands[command] = true
		}
	}
	return result, nil
}

func fineGrainedPrivilegeUsers(ctx context.Context, head string, canInspectOthers bool) ([]string, error) {
	cursor := fineGrainedPrivilegeCursor{ctx: ctx, text: head}
	cursor.skipGap()
	cursor.takeKeyword("SHOW")
	if !cursor.takeKeyword("USERS") {
		cursor.takeKeyword("USER")
	}
	current, signedIn := authenticatedUserFromContext(ctx)
	if cursor.takeKeyword("PRIVILEGES") || cursor.takeKeyword("PRIVILEGE") {
		if !signedIn {
			return nil, auth.ErrFineGrainedForbidden
		}
		return []string{current.Name}, nil
	}
	names, err := cursor.readNameList(false, "expected a username", "")
	if err != nil {
		return nil, err
	}
	if !cursor.takeKeyword("PRIVILEGES") && !cursor.takeKeyword("PRIVILEGE") {
		return nil, fineGrainedPrivilegeSyntaxError("expected PRIVILEGES after usernames")
	}
	if !canInspectOthers && (!signedIn || len(names) != 1 || names[0] != current.Name) {
		return nil, auth.ErrFineGrainedForbidden
	}
	return names, nil
}
