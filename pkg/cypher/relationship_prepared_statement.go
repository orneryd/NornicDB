package cypher

import "context"

// Prepared metadata is immutable and retains no execution context, parameter
// values, schema decisions, row values or storage views. The exact-text plan
// owns this admission even after the syntax interning entry is evicted.
type preparedRelationshipStatement struct {
	admission       admittedRelationshipStatement
	binding         *relationshipStatementBinding
	mode            ExecutionMode
	hasSites        bool
	liveSchemaPlans bool
}

func prepareRelationshipStatement(query string, mode ExecutionMode) *preparedRelationshipStatement {
	// The existing compiler remains the sole admission constructor. Its
	// temporary context is deliberately discarded rather than cached.
	_, admission := compileRelationshipStatement(context.Background(), query, nil)
	program := admission.program
	prepared := &preparedRelationshipStatement{admission: admission, binding: &program.self, mode: mode, hasSites: len(program.byID) != 0}
	for _, template := range program.byID {
		if !template.scanOnly && len(template.syntax.expressions) != 0 {
			prepared.liveSchemaPlans = true
			break
		}
	}
	return prepared
}

func (prepared *preparedRelationshipStatement) bind(ctx context.Context, params map[string]interface{}) (context.Context, admittedRelationshipStatement) {
	// Explicit source authority is meaningful only for a statement with sites.
	// Preserve the public source-removal contract, without probing ambient
	// contexts for statements that cannot enter a relationship site.
	if prepared.hasSites {
		if source, _ := ctx.Value(relationshipAccessProgramKey{}).(*relationshipAccessProgram); source != nil && source.query == prepared.binding.query && source != prepared.admission.program {
			return compileRelationshipStatement(ctx, prepared.binding.query, source, params)
		}
	}
	if prepared.liveSchemaPlans {
		ctx = withRelationshipAccessPlans(ctx, prepared.admission.program, prepared.mode)
	}
	return &queryParamsContext{Context: ctx, params: params, statement: prepared.binding}, prepared.admission
}
