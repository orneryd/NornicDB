package storage

// Tests evaluate a bare expression the way a revision does: the one plan
// compiler compiles it, then the plan evaluator runs it.

func evaluateNodeConstraintContractExpression(view constraintGraphView, node *Node, expr string) (bool, error) {
	plan, _ := compileConstraintContractEntryPlan(ConstraintContractEntry{Kind: ConstraintContractKindBooleanNode, Expression: expr})
	return evaluateNodeConstraintContractPlan(view, node, &plan)
}

func evaluateRelationshipConstraintContractExpression(view constraintGraphView, edge *Edge, expr string) (bool, error) {
	plan, _ := compileConstraintContractEntryPlan(ConstraintContractEntry{Kind: ConstraintContractKindBooleanRelationship, Expression: expr})
	return evaluateRelationshipConstraintContractPlan(view, edge, &plan)
}

func evaluateNodeConstraintContractExpressionEngine(engine Engine, node *Node, expr string) (bool, error) {
	return evaluateNodeConstraintContractExpression(engineConstraintView{engine}, node, expr)
}

func evaluateRelationshipConstraintContractExpressionEngine(engine Engine, edge *Edge, expr string) (bool, error) {
	return evaluateRelationshipConstraintContractExpression(engineConstraintView{engine}, edge, expr)
}

func (tx *BadgerTransaction) evaluateNodeConstraintContractExpressionLocked(node *Node, expr string) (bool, error) {
	return evaluateNodeConstraintContractExpression(txConstraintView{tx}, node, expr)
}

func (tx *BadgerTransaction) evaluateRelationshipConstraintContractExpressionLocked(edge *Edge, expr string) (bool, error) {
	return evaluateRelationshipConstraintContractExpression(txConstraintView{tx}, edge, expr)
}
