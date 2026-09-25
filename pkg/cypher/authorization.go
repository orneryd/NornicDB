package cypher

import (
	"context"
	"strings"
)

// PermissionRequirements describes the entitlements required to execute a query.
// Procedure requirements come from the registered procedure contract, while query
// requirements preserve the established top-level Cypher classification.
type PermissionRequirements struct {
	Read   bool
	Write  bool
	Schema bool
	Admin  bool
}

// QueryPermissionRequirements returns the permissions required by a query and
// a registered procedure invoked by that query, when present.
func QueryPermissionRequirements(query string) PermissionRequirements {
	keywords := queryKeywords(query)
	requirements := PermissionRequirements{
		Read:   true,
		Schema: isSchemaPermissionQuery(query),
		Admin:  isAdminPermissionQuery(query),
	}
	if !requirements.Schema && !requirements.Admin {
		requirements.Write = keywords["CREATE"] || keywords["DELETE"] || keywords["SET"] ||
			keywords["MERGE"] || keywords["REMOVE"]
	}

	if procedure, found := RegisteredProcedureForCall(query); found {
		switch procedure.Mode {
		case ProcedureModeWrite:
			requirements.Write = true
		case ProcedureModeSchema:
			requirements.Schema = true
		case ProcedureModeDBMS, ProcedureModeAdmin:
			requirements.Admin = true
		}
	}
	return requirements
}

func isSchemaPermissionQuery(query string) bool {
	commandOffset := firstExecutableCypherOffset(query)
	for _, command := range [][2]string{
		{"CREATE", "INDEX"},
		{"CREATE", "RANGE INDEX"},
		{"CREATE", "FULLTEXT INDEX"},
		{"CREATE", "VECTOR INDEX"},
		{"DROP", "INDEX"},
		{"CREATE", "CONSTRAINT"},
		{"DROP", "CONSTRAINT"},
	} {
		if findMultiWordKeywordIndex(query, command[0], command[1]) == commandOffset {
			return true
		}
	}
	return false
}

func isAdminPermissionQuery(query string) bool {
	commandOffset := firstExecutableCypherOffset(query)
	for _, command := range [][2]string{
		{"CREATE", "DATABASE"},
		{"DROP", "DATABASE"},
		{"ALTER", "DATABASE"},
		{"CREATE", "COMPOSITE DATABASE"},
		{"DROP", "COMPOSITE DATABASE"},
		{"ALTER", "COMPOSITE DATABASE"},
		{"CREATE", "ALIAS"},
		{"DROP", "ALIAS"},
		{"CREATE", "DECAY PROFILE"},
		{"ALTER", "DECAY PROFILE"},
		{"DROP", "DECAY PROFILE"},
		{"CREATE", "PROMOTION PROFILE"},
		{"ALTER", "PROMOTION PROFILE"},
		{"DROP", "PROMOTION PROFILE"},
		{"CREATE", "PROMOTION POLICY"},
		{"ALTER", "PROMOTION POLICY"},
		{"DROP", "PROMOTION POLICY"},
	} {
		if findMultiWordKeywordIndex(query, command[0], command[1]) == commandOffset {
			return true
		}
	}
	return isCreateProcedureCommand(query) || isDropProcedureCommand(query)
}

func firstExecutableCypherOffset(query string) int {
	index := 0
	if strings.HasPrefix(query, "\xef\xbb\xbf") {
		index = 3
	}
	for {
		for index < len(query) && isASCIISpace(query[index]) {
			index++
		}
		switch {
		case index+1 < len(query) && query[index] == '/' && query[index+1] == '/':
			index += 2
			for index < len(query) && query[index] != '\n' {
				index++
			}
		case index+1 < len(query) && query[index] == '/' && query[index+1] == '*':
			index += 2
			for index+1 < len(query) && (query[index] != '*' || query[index+1] != '/') {
				index++
			}
			if index+1 >= len(query) {
				return len(query)
			}
			index += 2
		default:
			return index
		}
	}
}

func queryKeywords(query string) map[string]bool {
	keywords := make(map[string]bool)
	for index := 0; index < len(query); {
		switch query[index] {
		case '/', '\'', '"', '`':
			if query[index] == '/' && index+1 < len(query) && query[index+1] == '/' {
				index += 2
				for index < len(query) && query[index] != '\n' {
					index++
				}
				continue
			}
			if query[index] == '/' && index+1 < len(query) && query[index+1] == '*' {
				index += 2
				for index+1 < len(query) && (query[index] != '*' || query[index+1] != '/') {
					index++
				}
				if index+1 < len(query) {
					index += 2
				}
				continue
			}
			if query[index] != '/' {
				quote := query[index]
				index++
				for index < len(query) {
					if query[index] == '\\' && quote != '`' && index+1 < len(query) {
						index += 2
						continue
					}
					if query[index] == quote {
						if index+1 < len(query) && query[index+1] == quote {
							index += 2
							continue
						}
						index++
						break
					}
					index++
				}
				continue
			}
		}

		if !isCypherIdentifierStart(query[index]) {
			index++
			continue
		}
		start := index
		index++
		for index < len(query) && isCypherIdentifierPart(query[index]) {
			index++
		}
		if isQualifiedOrMapKey(query, start, index) {
			continue
		}
		keywords[strings.ToUpper(query[start:index])] = true
	}
	return keywords
}

func isQualifiedOrMapKey(query string, start, end int) bool {
	left := start - 1
	for left >= 0 && isASCIISpace(query[left]) {
		left--
	}
	if left >= 0 && (query[left] == '.' || query[left] == ':' || query[left] == '$') {
		return true
	}
	right := end
	for right < len(query) && isASCIISpace(query[right]) {
		right++
	}
	return right < len(query) && query[right] == ':'
}

func isCypherIdentifierStart(value byte) bool {
	return value == '_' || value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
}

func isCypherIdentifierPart(value byte) bool {
	return isCypherIdentifierStart(value) || value >= '0' && value <= '9'
}

// PermissionChecker answers whether the caller holds an entitlement.
type PermissionChecker func(permission string) bool

type permissionCheckerKey struct{}

// DatabasePermissionResolver answers whether the caller holds an entitlement
// for the database where a statement is executing.
type DatabasePermissionResolver func(database, permission string) bool

type databaseAuthorization struct {
	database string
	resolver DatabasePermissionResolver
}

type databaseAuthorizationKey struct{}

// WithPermissionChecker attaches a caller's effective entitlements to an
// execution context. Nested dynamic statements inherit the same checker.
func WithPermissionChecker(ctx context.Context, checker PermissionChecker) context.Context {
	if checker == nil {
		return ctx
	}
	return context.WithValue(ctx, permissionCheckerKey{}, checker)
}

// WithDatabasePermissionResolver attaches database-scoped entitlements to an
// execution context. USE routing updates the selected database automatically.
func WithDatabasePermissionResolver(ctx context.Context, database string, resolver DatabasePermissionResolver) context.Context {
	if resolver == nil {
		return ctx
	}
	return context.WithValue(ctx, databaseAuthorizationKey{}, databaseAuthorization{
		database: database,
		resolver: resolver,
	})
}

func withExecutionDatabase(ctx context.Context, database string) context.Context {
	ctx = context.WithValue(ctx, ctxKeyUseDatabase, database)
	authorization, ok := ctx.Value(databaseAuthorizationKey{}).(databaseAuthorization)
	if !ok {
		return ctx
	}
	authorization.database = database
	return context.WithValue(ctx, databaseAuthorizationKey{}, authorization)
}

func authorizeDatabaseSelection(ctx context.Context, database string) error {
	if _, ok := ctx.Value(databaseAuthorizationKey{}).(databaseAuthorization); !ok {
		return nil
	}
	return AuthorizeQuery(withExecutionDatabase(ctx, database), "RETURN 1")
}

// PermissionDeniedError identifies the entitlement needed for a query.
type PermissionDeniedError struct {
	Permission string
}

// BoltErrorCode is the Neo4j status of a permission failure, on Bolt and
// HTTP alike (nornicerrors.Neo4jStatus).
func (e *PermissionDeniedError) BoltErrorCode() string {
	return "Neo.ClientError.Security.Forbidden"
}

// StatusMessage is the message clients see for a permission failure: the
// missing permission only, whatever statement or procedure it was found in.
func (e *PermissionDeniedError) StatusMessage() string {
	return e.Error()
}

func (e *PermissionDeniedError) Error() string {
	switch e.Permission {
	case "schema":
		return "Schema operations require schema permission"
	case "admin":
		return "Admin operations require admin permission"
	case "write":
		return "Write operations require write permission"
	default:
		return "Read operations require read permission"
	}
}

// AuthorizeQuery enforces requirements when an entitlement checker is present.
// Top-level and nested execution boundaries both call it so dynamically
// supplied procedure statements cannot bypass transport authorization.
func AuthorizeQuery(ctx context.Context, query string) error {
	checker, _ := ctx.Value(permissionCheckerKey{}).(PermissionChecker)
	databaseAuth, hasDatabaseAuth := ctx.Value(databaseAuthorizationKey{}).(databaseAuthorization)
	if checker == nil && !hasDatabaseAuth {
		return nil
	}
	hasPermission := checker
	if hasDatabaseAuth {
		hasPermission = func(permission string) bool {
			return databaseAuth.resolver(databaseAuth.database, permission)
		}
	}
	requirements := QueryPermissionRequirements(query)
	for _, permission := range []struct {
		name     string
		required bool
	}{
		{name: "schema", required: requirements.Schema},
		{name: "admin", required: requirements.Admin},
		{name: "write", required: requirements.Write},
		{name: "read", required: requirements.Read},
	} {
		if permission.required && !hasPermission(permission.name) {
			return &PermissionDeniedError{Permission: permission.name}
		}
	}
	return nil
}

// RegisteredProcedureForCall resolves a CALL statement through the canonical
// procedure registry without exposing handlers to callers.
func RegisteredProcedureForCall(query string) (ProcedureSpec, bool) {
	ensureBuiltInProceduresRegistered()
	procedure, found := globalProcedureRegistry.Get(extractProcedureName(query))
	if !found {
		return ProcedureSpec{}, false
	}
	return procedure.Spec, true
}
