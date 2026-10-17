package cypher

// cancellationOwnerAllowlist: findings of TestCancellationOwnerStaticInventory
// that are correct as they are, each with its reason. Keys are
// "<rule> <function> …" exactly as the test prints them.
var cancellationOwnerAllowlist = map[string]string{
	// A: context-free entry points. Each forwards to its context form; rule B
	// routes every caller that has a statement context or probe to that form.
	"A StorageExecutor.callDbSchemaVisualization context.Background (context-free wrapper of callDbSchemaVisualizationWithContext)":                   "context-free entry; statement callers use the context form (rule B)",
	"A StorageExecutor.callDbmsListConfig context.Background (context-free wrapper of callDbmsListConfigArguments)":                                    "context-free entry; statement callers use the context form (rule B)",
	"A StorageExecutor.callDbmsListConnections context.Background (context-free wrapper of callDbmsListConnectionsWithContext)":                        "context-free entry; statement callers use the context form (rule B)",
	"A StorageExecutor.evaluateExpressionFromValues context.Background (context-free wrapper of evaluateExpressionFromValuesContext)":                  "context-free entry; statement callers use the context form (rule B)",
	"A StorageExecutor.evaluatePipelineAggregate context.Background (context-free wrapper of evaluatePipelineAggregateWithContext)":                     "context-free entry; statement callers use the context form (rule B)",
	"A StorageExecutor.evaluatePipelineAggregateExpression context.Background (context-free wrapper of evaluatePipelineAggregateExpressionWithContext)": "context-free entry; statement callers use the context form (rule B)",
	"A StorageExecutor.parseSelectedPathMatch context.Background (context-free wrapper of parseSelectedPathMatchContext)":                              "context-free entry; statement callers use the context form (rule B)",
	"A StorageExecutor.pathToMap context.Background (context-free wrapper of pathToMapContext)":                                                        "context-free entry; statement callers use the context form (rule B)",
	"A buildSelectedPathPlan context.Background (context-free wrapper of buildSelectedPathPlanContext)":                                                "context-free entry; statement callers use the context form (rule B)",
	"A callPluginHandler context.Background (context-free wrapper of callPluginHandlerContext)":                                                        "context-free entry; statement callers use the context form (rule B)",
	"A compileSelectedPathPlan context.Background (context-free wrapper of compileSelectedPathPlanContext)":                                            "context-free entry; statement callers use the context form (rule B)",
	"A evaluateComparisonChain context.Background (context-free wrapper of evaluateComparisonChainInContext)":                                          "context-free entry; statement callers use the context form (rule B)",
	"A evaluateLogicalExpression context.Background (context-free wrapper of evaluateLogicalExpressionInContext)":                                      "context-free entry; statement callers use the context form (rule B)",
	"A incidentEdgeLists context.Background (context-free wrapper of incidentEdgeListsContext)":                                                        "context-free entry; statement callers use the context form (rule B)",
	"A parallelFilterNodes context.Background (context-free wrapper of parallelFilterNodesWithCancellation)":                                           "context-free entry; statement callers use the context form (rule B)",
	"A parsePathPatternItems context.Background (context-free wrapper of parsePathPatternItemsContext)":                                                "context-free entry; statement callers use the context form (rule B)",
	"A pathSearchAutomaton.walkPath context.Background (context-free wrapper of walkPathContext)":                                                      "context-free entry; statement callers use the context form (rule B)",
	"A reverseSlice context.Background (context-free wrapper of reverseSliceContext)":                                                                  "context-free entry; statement callers use the context form (rule B)",
	"A undirectedIncidentEdges context.Background (context-free wrapper of undirectedIncidentEdgesContext)":                                            "context-free entry; statement callers use the context form (rule B)",
	"A logicalPlan.evaluate context.Background":                                                                                                        "context-free entry of evaluateInContext (method name shared, so not auto-tagged); statement callers call evaluateInContext",

	// A: no statement deadline to keep at that point.
	"A StorageExecutor.applyDeleteReturnProjection context.Background": "default only when info.ctx is nil; statement callers pass the statement context in info.ctx",
	"A buildPathSearchAutomatonContext context.Background":             "default only for a nil context argument",
	"A withParams context.Background":                                  "default only for a nil context argument",
	"A rowStatementContextOrBackground context.Background":             "a row without a statement context has no deadline to keep",
	"A probeStatementContext context.Background":                       "no probe: no statement deadline to keep",
	"A gdsStatementContext context.Background":                         "an internal GDS call without a statement context has no deadline to keep",
	"A StorageExecutor.applyOrderByToResult context.Background":        "no statement caller (test-only helper)",
	"A StorageExecutor.evaluateRelationshipPatternInWhere context.Background": "no statement caller (test-only helper)",
	"A StorageExecutor.evaluateConditionFromValues context.Background": "no statement caller (test-only helper)",
	"A StorageExecutor.evaluateCaseExpressionFromValues context.Background": "context-free entry; its statement caller attaches the probe (withStatementProbe)",

	// A: a fresh context on purpose, with the statement probe attached.
	"A StorageExecutor.evaluateRowFallbackProbed context.Background":            "fresh failure slot keeps the shared evaluator's errors local; the statement probe is attached next",
	"A StorageExecutor.evaluateExpressionFromValuesContext context.Background":  "fresh context keeps CASE errors local as before; withStatementProbe attaches the statement probe",
	"A rowLogicalNode.eval context.Background":                                  "operands evaluate under the node's probe; the plan's context only carries the row's statement context when a fold needs it",

	// A: outside the statement's work.
	"A StorageExecutor.dropIndexByName context.Background": "schema DDL commit hook; must not be torn by a statement deadline once the change is committed",
	"A StorageExecutor.dropIndexOfKind context.Background": "schema DDL mutation; must not be torn by a statement deadline once it starts",
	"A StorageExecutor.emitRejectionReport context.Background": "logging after the statement returned",
	"A StorageExecutor.logEvent context.Background":            "logging, not statement work",
	"A StorageExecutor.observeQuery context.Background":        "metrics after the statement returned",

	// B: an ambiguous method name, resolved by hand: the receiver is not a no-probe wrapper.
	"B StorageExecutor.Execute -> restore (ambiguous method)":                               "receiver is *queryRewrite (restore is not a wrapper)",
	"B StorageExecutor.callDbSchemaTypeProperties -> add (ambiguous method)":                "receiver is *schemaTypeCategory (add is not a wrapper)",
	"B StorageExecutor.conditionValue -> evaluate (ambiguous method)":                       "receiver is nullEvaluationHandler (evaluate is not a wrapper)",
	"B StorageExecutor.evaluateExpressionWithContextFullOperators -> evaluate (ambiguous method)": "receiver is nullEvaluationHandler (evaluate is not a wrapper)",
	"B StorageExecutor.evaluateRowPredicateTextFromSubqueries -> evaluate (ambiguous method)": "receiver is nullEvaluationHandler (evaluate is not a wrapper)",
	"B StorageExecutor.evaluateRowValueTextWithCancellation -> evaluate (ambiguous method)":   "receiver is nullEvaluationHandler (evaluate is not a wrapper)",
	"B StorageExecutor.rowPredicatePartTruth -> evaluate (ambiguous method)":                  "receiver is nullEvaluationHandler (evaluate is not a wrapper)",
	"B rowNullTestNode.eval -> evaluate (ambiguous method)":                                   "receiver is nullEvaluationHandler (evaluate is not a wrapper)",
	"B rowPredicateNullNode.holds -> evaluate (ambiguous method)":                             "receiver is nullEvaluationHandler (evaluate is not a wrapper)",
	"B StorageExecutor.rowPredicatePartText -> evaluate (ambiguous method)":                   "receiver is operandIsolation, which takes the statement context",
	"B StorageExecutor.rowTextFoldOperand -> evaluate (ambiguous method)":                     "receiver is operandIsolation, which takes the statement context",
	"B rowDeferredOperandNode.eval -> evaluate (ambiguous method)":                            "receiver is operandIsolation, which takes the row's statement context",
	"B rowPredicateOperand.foldOperand -> evaluate (ambiguous method)":                        "receiver is operandIsolation, which takes the statement context",
	"B StorageExecutor.validateSemanticScopes -> add (ambiguous method)":                      "receiver is *semanticValidationCache (add is not a wrapper)",
	"B cypherFabricExecutor.executeFragmentQuery -> add (ambiguous method)":                   "receiver is *fabricStatsAccumulator (add is not a wrapper)",
	"B pipelineAggregateState.addWithCancellation -> add (ambiguous method)":                  "receiver is *cypherValueIndex; its probe field is set from the caller's probe",

	// D: constant-time leaves with no loop and no nested evaluation.
	"D rowConstNode.eval":      "constant-time leaf: returns a stored value",
	"D rowEmptyListNode.eval":  "constant-time leaf: returns an empty list",
	"D rowLiteralNode.eval":    "constant-time leaf: returns a parsed literal",
	"D rowUnresolvedNode.eval": "constant-time leaf: reports the expression as unresolved",
}
