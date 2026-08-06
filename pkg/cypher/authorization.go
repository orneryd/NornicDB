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
	upper := strings.ToUpper(query)
	requirements := PermissionRequirements{
		Read: true,
		Write: strings.Contains(upper, "CREATE") ||
			strings.Contains(upper, "DELETE") ||
			strings.Contains(upper, "SET ") ||
			strings.Contains(upper, "MERGE") ||
			strings.Contains(upper, "REMOVE "),
		Schema: strings.Contains(upper, "INDEX") || strings.Contains(upper, "CONSTRAINT"),
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

// PermissionChecker answers whether the caller holds an entitlement.
type PermissionChecker func(permission string) bool

type permissionCheckerKey struct{}

// WithPermissionChecker attaches a caller's effective entitlements to an
// execution context. Nested dynamic statements inherit the same checker.
func WithPermissionChecker(ctx context.Context, checker PermissionChecker) context.Context {
	if checker == nil {
		return ctx
	}
	return context.WithValue(ctx, permissionCheckerKey{}, checker)
}

// PermissionDeniedError identifies the entitlement needed for a query.
type PermissionDeniedError struct {
	Permission string
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
// It is used at nested execution boundaries, where Bolt cannot inspect a
// statement supplied dynamically by a procedure argument.
func AuthorizeQuery(ctx context.Context, query string) error {
	checker, _ := ctx.Value(permissionCheckerKey{}).(PermissionChecker)
	if checker == nil {
		return nil
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
		if permission.required && !checker(permission.name) {
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
