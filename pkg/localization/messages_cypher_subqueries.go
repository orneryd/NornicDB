package localization

import (
	"fmt"
	"strconv"
)

const (
	MessageCypherSubqueriesCallNotFound                      MessageID = "cyphersubqueries.call_not_found"
	MessageCypherSubqueriesMatchBeforeCallNotFound           MessageID = "cyphersubqueries.match_before_call_not_found"
	MessageCypherSubqueriesNodePatternInvalid                MessageID = "cyphersubqueries.node_pattern_invalid"
	MessageCypherSubqueriesOuterMatchBindingsFailed          MessageID = "cyphersubqueries.outer_match_bindings_failed"
	MessageCypherSubqueriesNodeLoadFailed                    MessageID = "cyphersubqueries.node_load_failed"
	MessageCypherSubqueriesCallForNodeFailed                 MessageID = "cyphersubqueries.call_for_node_failed"
	MessageCypherSubqueriesOuterMatchSeedsFailed             MessageID = "cyphersubqueries.outer_match_seeds_failed"
	MessageCypherSubqueriesCallBodyEmpty                     MessageID = "cyphersubqueries.call_body_empty"
	MessageCypherSubqueriesUseClauseFailed                   MessageID = "cyphersubqueries.use_clause_failed"
	MessageCypherSubqueriesUseDatabaseFailed                 MessageID = "cyphersubqueries.use_database_failed"
	MessageCypherSubqueriesCorrelatedUnionParseFailed        MessageID = "cyphersubqueries.correlated_union_parse_failed"
	MessageCypherSubqueriesUnionWithImportsFailed            MessageID = "cyphersubqueries.union_with_imports_failed"
	MessageCypherSubqueriesStaticUnionBranchFailed           MessageID = "cyphersubqueries.static_union_branch_failed"
	MessageCypherSubqueriesCorrelatedUnionBranchFailed       MessageID = "cyphersubqueries.correlated_union_branch_failed"
	MessageCypherSubqueriesUnionColumnCountMismatch          MessageID = "cyphersubqueries.union_column_count_mismatch"
	MessageCypherSubqueriesCorrelatedImportFailed            MessageID = "cyphersubqueries.correlated_import_failed"
	MessageCypherSubqueriesOptionalImportFallbackFailed      MessageID = "cyphersubqueries.optional_import_fallback_failed"
	MessageCypherSubqueriesWithImportUnknownVariable         MessageID = "cyphersubqueries.with_import_unknown_variable"
	MessageCypherSubqueriesCorrelatedVariableEmpty           MessageID = "cyphersubqueries.correlated_variable_empty"
	MessageCypherSubqueriesOuterMatchProjectionMissing       MessageID = "cyphersubqueries.outer_match_projection_missing"
	MessageCypherSubqueriesCallBodyExpected                  MessageID = "cyphersubqueries.call_body_expected"
	MessageCypherSubqueriesCallError                         MessageID = "cyphersubqueries.call_error"
	MessageCypherSubqueriesTransactionImportShapeUnsupported MessageID = "cyphersubqueries.transaction_import_shape_unsupported"
	MessageCypherSubqueriesTransactionBodyEmpty              MessageID = "cyphersubqueries.transaction_body_empty"
	MessageCypherSubqueriesTransactionBatchFailed            MessageID = "cyphersubqueries.transaction_batch_failed"
	MessageCypherSubqueriesExecutionFailed                   MessageID = "cyphersubqueries.execution_failed"
	MessageCypherSubqueriesFirstBatchFailed                  MessageID = "cyphersubqueries.first_batch_failed"
	MessageCypherSubqueriesBatchFailed                       MessageID = "cyphersubqueries.batch_failed"
	MessageCypherSubqueriesBatchProgressFailed               MessageID = "cyphersubqueries.batch_progress_failed"
	MessageCypherSubqueriesAfterCallClauseUnsupported        MessageID = "cyphersubqueries.after_call_clause_unsupported"
	MessageCypherSubqueriesChainedTransactionsUnsupported    MessageID = "cyphersubqueries.chained_transactions_unsupported"
	MessageCypherSubqueriesWithQueryClauseRequired           MessageID = "cyphersubqueries.with_query_clause_required"
	MessageCypherSubqueriesWithBodyEmpty                     MessageID = "cyphersubqueries.with_body_empty"
	MessageCypherSubqueriesWithImportExpressionInvalid       MessageID = "cyphersubqueries.with_import_expression_invalid"
	MessageCypherSubqueriesWithImportsRequired               MessageID = "cyphersubqueries.with_imports_required"
	MessageCypherSubqueriesSeedRowMissingVariable            MessageID = "cyphersubqueries.seed_row_missing_variable"
	MessageCypherSubqueriesBatchedLookupFailed               MessageID = "cyphersubqueries.batched_lookup_failed"
	MessageCypherSubqueriesBatchedLookupColumnsUnexpected    MessageID = "cyphersubqueries.batched_lookup_columns_unexpected"
	MessageCypherSubqueriesRAGCandidatesRequired             MessageID = "cyphersubqueries.rag_candidates_required"
	MessageCypherSubqueriesInferenceManagerUnavailable       MessageID = "cyphersubqueries.inference_manager_unavailable"
	MessageCypherSubqueriesInferMessagesEmpty                MessageID = "cyphersubqueries.infer_messages_empty"
	MessageCypherSubqueriesInferPromptOrMessagesRequired     MessageID = "cyphersubqueries.infer_prompt_or_messages_required"
	MessageCypherSubqueriesQueryRequired                     MessageID = "cyphersubqueries.query_required"
	MessageCypherSubqueriesRAGSyntaxInvalid                  MessageID = "cyphersubqueries.rag_syntax_invalid"
	MessageCypherSubqueriesRAGRequestArgumentRequired        MessageID = "cyphersubqueries.rag_request_argument_required"
	MessageCypherSubqueriesRAGParenthesisUnmatched           MessageID = "cyphersubqueries.rag_parenthesis_unmatched"
	MessageCypherSubqueriesRAGParameterMustBeMap             MessageID = "cyphersubqueries.rag_parameter_must_be_map"
	MessageCypherSubqueriesRAGRequestMustBeMapLiteral        MessageID = "cyphersubqueries.rag_request_must_be_map_literal"
	MessageCypherSubqueriesRAGCandidateIDRequired            MessageID = "cyphersubqueries.rag_candidate_id_required"
	MessageCypherSubqueriesRAGFailClosedInvalid              MessageID = "cyphersubqueries.rag_fail_closed_invalid"
	MessageCypherSubqueriesRAGFailClosedEmbeddingUnavailable MessageID = "cyphersubqueries.rag_fail_closed_embedding_unavailable"
)

func cypherSubqueriesMessage(id MessageID, fallback string, data map[string]any) Message {
	return Message{ID: id, Fallback: fallback, Data: data}
}

func cypherSubqueriesCauseMessage(id MessageID, prefix string, cause error) Message {
	return cypherSubqueriesMessage(id, prefix+cause.Error(), map[string]any{"Cause": cause.Error()})
}

func CypherSubqueriesCallNotFound() Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesCallNotFound, "CALL not found in query", nil)
}

func CypherSubqueriesMatchBeforeCallNotFound() Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesMatchBeforeCallNotFound, "MATCH not found before CALL", nil)
}

func CypherSubqueriesNodePatternInvalid(pattern string) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesNodePatternInvalid, "could not parse node pattern: "+pattern, map[string]any{"Pattern": pattern})
}

func CypherSubqueriesOuterMatchBindingsFailed(cause error) Message {
	return cypherSubqueriesCauseMessage(MessageCypherSubqueriesOuterMatchBindingsFailed, "failed to evaluate outer MATCH bindings: ", cause)
}

func CypherSubqueriesNodeLoadFailed(cause error) Message {
	return cypherSubqueriesCauseMessage(MessageCypherSubqueriesNodeLoadFailed, "failed to get nodes: ", cause)
}

func CypherSubqueriesCallForNodeFailed(node string, cause error) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesCallForNodeFailed, "failed to execute CALL for node "+node+": "+cause.Error(), map[string]any{"Node": node, "Cause": cause.Error()})
}

func CypherSubqueriesOuterMatchSeedsFailed(cause error) Message {
	return cypherSubqueriesCauseMessage(MessageCypherSubqueriesOuterMatchSeedsFailed, "failed to evaluate outer MATCH seeds: ", cause)
}

func CypherSubqueriesCallBodyEmpty() Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesCallBodyEmpty, "invalid CALL {} subquery: empty body", nil)
}

func CypherSubqueriesUseClauseFailed(cause error) Message {
	return cypherSubqueriesCauseMessage(MessageCypherSubqueriesUseClauseFailed, "CALL subquery USE clause error: ", cause)
}

func CypherSubqueriesUseDatabaseFailed(database string, cause error) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesUseDatabaseFailed, "CALL subquery USE "+database+" failed: "+cause.Error(), map[string]any{"Database": database, "Cause": cause.Error()})
}

func CypherSubqueriesCorrelatedUnionParseFailed() Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesCorrelatedUnionParseFailed, "failed to parse UNION branches in correlated CALL subquery", nil)
}

func CypherSubqueriesUnionWithImportsFailed(branch int, cause error) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesUnionWithImportsFailed, fmt.Sprintf("failed to parse UNION branch %d WITH imports: %s", branch, cause), map[string]any{"Branch": branch, "Cause": cause.Error()})
}

func CypherSubqueriesStaticUnionBranchFailed(branch int, cause error) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesStaticUnionBranchFailed, fmt.Sprintf("failed static UNION subquery branch %d: %s", branch, cause), map[string]any{"Branch": branch, "Cause": cause.Error()})
}

func CypherSubqueriesCorrelatedUnionBranchFailed(branch int, seed string, cause error) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesCorrelatedUnionBranchFailed, fmt.Sprintf("failed correlated UNION subquery branch %d for seed %s: %s", branch, seed, cause), map[string]any{"Branch": branch, "Seed": seed, "Cause": cause.Error()})
}

func CypherSubqueriesUnionColumnCountMismatch(first, second int) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesUnionColumnCountMismatch, fmt.Sprintf("UNION queries must return the same number of columns (got %d and %d)", first, second), map[string]any{"First": first, "Second": second})
}

func CypherSubqueriesCorrelatedImportFailed(importVariable string, cause error) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesCorrelatedImportFailed, "failed to resolve correlated import "+importVariable+": "+cause.Error(), map[string]any{"Import": importVariable, "Cause": cause.Error()})
}

func CypherSubqueriesOptionalImportFallbackFailed(importVariable string, cause error) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesOptionalImportFallbackFailed, "failed optional import fallback for "+importVariable+": "+cause.Error(), map[string]any{"Import": importVariable, "Cause": cause.Error()})
}

func CypherSubqueriesWithImportUnknownVariable(variable string) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesWithImportUnknownVariable, "CALL subquery WITH imports unknown variable: "+variable, map[string]any{"Variable": variable})
}

func CypherSubqueriesCorrelatedVariableEmpty() Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesCorrelatedVariableEmpty, "empty correlated variable", nil)
}

func CypherSubqueriesOuterMatchProjectionMissing(variable string) Message {
	quoted := strconv.Quote(variable)
	return cypherSubqueriesMessage(MessageCypherSubqueriesOuterMatchProjectionMissing, "outer MATCH did not project correlated variable "+quoted, map[string]any{"Variable": quoted})
}

func CypherSubqueriesCallBodyExpected() Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesCallBodyExpected, "invalid CALL {} subquery: empty body (expected CALL { <query> })", nil)
}

func CypherSubqueriesCallError(cause error) Message {
	return cypherSubqueriesCauseMessage(MessageCypherSubqueriesCallError, "CALL subquery error: ", cause)
}

func CypherSubqueriesTransactionImportShapeUnsupported(variable string) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesTransactionImportShapeUnsupported, "unsupported CALL ("+variable+") IN TRANSACTIONS import shape", map[string]any{"Variable": variable})
}

func CypherSubqueriesTransactionBodyEmpty(variable string) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesTransactionBodyEmpty, "invalid CALL ("+variable+") IN TRANSACTIONS: empty body", map[string]any{"Variable": variable})
}

func CypherSubqueriesTransactionBatchFailed(variable string, batch int, cause error) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesTransactionBatchFailed, fmt.Sprintf("CALL (%s) IN TRANSACTIONS batch %d failed: %s", variable, batch, cause), map[string]any{"Variable": variable, "Batch": batch, "Cause": cause.Error()})
}

func CypherSubqueriesExecutionFailed(cause error) Message {
	return cypherSubqueriesCauseMessage(MessageCypherSubqueriesExecutionFailed, "subquery execution failed: ", cause)
}

func CypherSubqueriesFirstBatchFailed(cause error) Message {
	return cypherSubqueriesCauseMessage(MessageCypherSubqueriesFirstBatchFailed, "batch 1 failed: ", cause)
}

func CypherSubqueriesBatchFailed(batch int, cause error) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesBatchFailed, fmt.Sprintf("batch %d failed: %s", batch, cause), map[string]any{"Batch": batch, "Cause": cause.Error()})
}

func CypherSubqueriesBatchProgressFailed(batch, total int, cause error) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesBatchProgressFailed, fmt.Sprintf("batch %d/%d failed: %s", batch, total, cause), map[string]any{"Batch": batch, "Total": total, "Cause": cause.Error()})
}

func CypherSubqueriesAfterCallClauseUnsupported(clause string) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesAfterCallClauseUnsupported, "unsupported clause after CALL {}: "+clause+" (supported: RETURN, ORDER BY, SKIP, LIMIT)", map[string]any{"Clause": clause})
}

func CypherSubqueriesChainedTransactionsUnsupported(batchSize int) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesChainedTransactionsUnsupported, fmt.Sprintf("CALL {} IN TRANSACTIONS is not supported in chained CALL subqueries (batchSize=%d)", batchSize), map[string]any{"BatchSize": batchSize})
}

func CypherSubqueriesWithQueryClauseRequired() Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesWithQueryClauseRequired, "invalid CALL {} subquery: WITH must be followed by a query clause", nil)
}

func CypherSubqueriesWithBodyEmpty() Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesWithBodyEmpty, "invalid CALL {} subquery: empty query body after WITH", nil)
}

func CypherSubqueriesWithImportExpressionInvalid(expression string) Message {
	quoted := strconv.Quote(expression)
	return cypherSubqueriesMessage(MessageCypherSubqueriesWithImportExpressionInvalid, "invalid WITH import expression: "+quoted, map[string]any{"Expression": quoted})
}

func CypherSubqueriesWithImportsRequired() Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesWithImportsRequired, "invalid CALL {} subquery: WITH clause does not import variables", nil)
}

func CypherSubqueriesSeedRowMissingVariable(variable string) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesSeedRowMissingVariable, "CALL subquery seed row missing variable: "+variable, map[string]any{"Variable": variable})
}

func CypherSubqueriesBatchedLookupFailed(cause error) Message {
	return cypherSubqueriesCauseMessage(MessageCypherSubqueriesBatchedLookupFailed, "batched correlated CALL lookup failed: ", cause)
}

func CypherSubqueriesBatchedLookupColumnsUnexpected(columns string) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesBatchedLookupColumnsUnexpected, "batched correlated CALL lookup produced unexpected columns: "+columns, map[string]any{"Columns": columns})
}

func CypherSubqueriesRAGCandidatesRequired() Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesRAGCandidatesRequired, "db.rerank requires non-empty candidates", nil)
}

func CypherSubqueriesInferenceManagerUnavailable() Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesInferenceManagerUnavailable, "inference manager is not configured", nil)
}

func CypherSubqueriesInferMessagesEmpty() Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesInferMessagesEmpty, "db.infer messages cannot be empty", nil)
}

func CypherSubqueriesInferPromptOrMessagesRequired() Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesInferPromptOrMessagesRequired, "db.infer requires prompt or messages", nil)
}

func CypherSubqueriesQueryRequired() Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesQueryRequired, "query is required", nil)
}

func CypherSubqueriesRAGSyntaxInvalid(procedure string) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesRAGSyntaxInvalid, "invalid "+procedure+" syntax", map[string]any{"Procedure": procedure})
}

func CypherSubqueriesRAGRequestArgumentRequired(procedure string) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesRAGRequestArgumentRequired, procedure+" requires a request argument", map[string]any{"Procedure": procedure})
}

func CypherSubqueriesRAGParenthesisUnmatched(procedure string) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesRAGParenthesisUnmatched, "unmatched parenthesis in "+procedure, map[string]any{"Procedure": procedure})
}

func CypherSubqueriesRAGParameterMustBeMap(procedure, parameter string) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesRAGParameterMustBeMap, procedure+" parameter "+parameter+" must be a map", map[string]any{"Procedure": procedure, "Parameter": parameter})
}

func CypherSubqueriesRAGRequestMustBeMapLiteral(procedure string) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesRAGRequestMustBeMapLiteral, procedure+" request must be a map literal", map[string]any{"Procedure": procedure})
}

func CypherSubqueriesRAGCandidateIDRequired() Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesRAGCandidateIDRequired, "db.rerank candidate id is required", nil)
}

func CypherSubqueriesRAGFailClosedInvalid(field string) Message {
	return cypherSubqueriesMessage(MessageCypherSubqueriesRAGFailClosedInvalid, "db.retrieve failClosed has an invalid "+field+" value", map[string]any{"Field": field})
}

func CypherSubqueriesRAGFailClosedEmbeddingUnavailable(cause error) Message {
	return cypherSubqueriesCauseMessage(MessageCypherSubqueriesRAGFailClosedEmbeddingUnavailable, "db.retrieve failClosed requires a usable query embedding: ", cause)
}
