package cypher

import "context"

// admittedProcedureHandler is the internal invocation contract. Registration
// stores the function; the executing statement supplies its admission later.
type admittedProcedureHandler func(admittedRelationshipStatement, context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error)

func (procedure registeredProcedure) invoke(statement admittedRelationshipStatement, ctx context.Context, exec *StorageExecutor, query string, args []interface{}) (*ExecuteResult, error) {
	if procedure.admittedHandler != nil {
		return procedure.admittedHandler(statement, ctx, exec, query, args)
	}
	return procedure.Handler(ctx, exec, query, args)
}

func (r *ProcedureRegistry) RegisterBuiltIn(spec ProcedureSpec, handler ProcedureHandler) error {
	return r.registerBuiltInEntry(spec, handler, nil)
}

func (r *ProcedureRegistry) registerAdmittedBuiltIn(spec ProcedureSpec, handler admittedProcedureHandler) error {
	if handler == nil {
		return r.registerBuiltInEntry(spec, nil, nil)
	}
	// The public callback is an entry boundary for callers outside native
	// dispatch. Native calls use invoke and carry their original source.
	public := func(ctx context.Context, exec *StorageExecutor, query string, args []interface{}) (*ExecuteResult, error) {
		ctx, statement := compileRelationshipProcedureEntry(ctx, query)
		return handler(statement, ctx, exec, query, args)
	}
	return r.registerBuiltInEntry(spec, public, handler)
}

// Public callback entry belongs to the caller's statement when one exists.
// Its shortened CALL text must not replace that source or restore removed
// sites. A direct public invocation with no statement admits its supplied text.
// Context interpretation remains at admission; kernels accept only its result.
func compileRelationshipProcedureEntry(ctx context.Context, query string) (context.Context, admittedRelationshipStatement) {
	if current, _ := ctx.Value(relationshipStatementKey{}).(*relationshipStatementBinding); current != nil {
		return compileRelationshipStatement(ctx, current.query, current.program)
	}
	return compileRelationshipStatement(ctx, query, nil)
}
