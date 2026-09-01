package localization

import (
	"fmt"
	"strconv"
)

const (
	MessageCypherTransactionsAlreadyActive                  MessageID = "cyphertransactions.already_active"
	MessageCypherTransactionsEngineUnsupported              MessageID = "cyphertransactions.engine_unsupported"
	MessageCypherTransactionsPrimeNamespaceFailed           MessageID = "cyphertransactions.prime_namespace_failed"
	MessageCypherTransactionsStartFailed                    MessageID = "cyphertransactions.start_failed"
	MessageCypherTransactionsPinNamespaceFailed             MessageID = "cyphertransactions.pin_namespace_failed"
	MessageCypherTransactionsConfigureFailed                MessageID = "cyphertransactions.configure_failed"
	MessageCypherTransactionsWALBeginFailed                 MessageID = "cyphertransactions.wal_begin_failed"
	MessageCypherTransactionsNoActive                       MessageID = "cyphertransactions.no_active"
	MessageCypherTransactionsUnknownType                    MessageID = "cyphertransactions.unknown_type"
	MessageCypherTransactionsCommitFailed                   MessageID = "cyphertransactions.commit_failed"
	MessageCypherTransactionsRollbackFailed                 MessageID = "cyphertransactions.rollback_failed"
	MessageCypherTransactionsShowInTransactionUnsupported   MessageID = "cyphertransactions.show_in_transaction_unsupported"
	MessageCypherTransactionsQueryInTransactionUnsupported  MessageID = "cyphertransactions.query_in_transaction_unsupported"
	MessageCypherTransactionsInvalidScriptAction            MessageID = "cyphertransactions.invalid_script_action"
	MessageCypherTransactionsCaseBlockMissing               MessageID = "cyphertransactions.case_block_missing"
	MessageCypherTransactionsCaseSyntaxInvalid              MessageID = "cyphertransactions.case_syntax_invalid"
	MessageCypherTransactionsConditionNotBoolean            MessageID = "cyphertransactions.condition_not_boolean"
	MessageCypherTransactionsQueryTypeUnsupported           MessageID = "cyphertransactions.query_type_unsupported"
	MessageCypherTransactionsReturnClauseNotFound           MessageID = "cyphertransactions.return_clause_not_found"
	MessageCypherTransactionsSyntaxStartInvalid             MessageID = "cyphertransactions.syntax_start_invalid"
	MessageCypherTransactionsSyntaxUnbalancedAt             MessageID = "cyphertransactions.syntax_unbalanced_at"
	MessageCypherTransactionsSyntaxUnbalancedParentheses    MessageID = "cyphertransactions.syntax_unbalanced_parentheses"
	MessageCypherTransactionsSyntaxUnbalancedSquareBrackets MessageID = "cyphertransactions.syntax_unbalanced_square_brackets"
	MessageCypherTransactionsSyntaxUnbalancedCurlyBraces    MessageID = "cyphertransactions.syntax_unbalanced_curly_braces"
	MessageCypherTransactionsSyntaxUnclosedQuote            MessageID = "cyphertransactions.syntax_unclosed_quote"
	MessageCypherTransactionsDeleteResidualRelationships    MessageID = "cyphertransactions.delete_residual_relationships"
	MessageCypherTransactionsMatchWithUnwindClausesRequired MessageID = "cyphertransactions.match_with_unwind_clauses_required"
	MessageCypherTransactionsStorageFailed                  MessageID = "cyphertransactions.storage_failed"
	MessageCypherTransactionsUnwindASRequired               MessageID = "cyphertransactions.unwind_as_required"
	MessageCypherTransactionsOrderByParseFailed             MessageID = "cyphertransactions.order_by_parse_failed"
	MessageCypherTransactionsMultiMatchReturnRequired       MessageID = "cyphertransactions.multi_match_return_required"
	MessageCypherTransactionsMultipleMatchExpected          MessageID = "cyphertransactions.multiple_match_expected"
)

const (
	cypherTransactionSupportedQueryTypes = "MATCH, CREATE, MERGE, DELETE, SET, REMOVE, RETURN, WITH, UNWIND, CALL, FOREACH, LOAD CSV, SHOW, DROP, ALTER"
	cypherTransactionValidStartClauses   = "MATCH, CREATE, MERGE, DELETE, CALL, SHOW, EXPLAIN, PROFILE, ALTER, USE, BEGIN, COMMIT, ROLLBACK, etc."
)

func cypherTransactionsMessage(id MessageID, fallback string, data map[string]any) Message {
	return Message{ID: id, Fallback: fallback, Data: data}
}

func cypherTransactionsCauseMessage(id MessageID, prefix string, cause error) Message {
	return cypherTransactionsMessage(id, prefix+cause.Error(), map[string]any{"Cause": cause.Error()})
}

func CypherTransactionsAlreadyActive() Message {
	return cypherTransactionsMessage(MessageCypherTransactionsAlreadyActive, "transaction already active", nil)
}

func CypherTransactionsEngineUnsupported() Message {
	return cypherTransactionsMessage(MessageCypherTransactionsEngineUnsupported, "engine does not support transactions", nil)
}

func CypherTransactionsPrimeNamespaceFailed(cause error) Message {
	return cypherTransactionsCauseMessage(MessageCypherTransactionsPrimeNamespaceFailed, "failed to prime transaction namespace: ", cause)
}

func CypherTransactionsStartFailed(cause error) Message {
	return cypherTransactionsCauseMessage(MessageCypherTransactionsStartFailed, "failed to start transaction: ", cause)
}

func CypherTransactionsPinNamespaceFailed(cause error) Message {
	return cypherTransactionsCauseMessage(MessageCypherTransactionsPinNamespaceFailed, "failed to pin transaction namespace: ", cause)
}

func CypherTransactionsConfigureFailed(cause error) Message {
	return cypherTransactionsCauseMessage(MessageCypherTransactionsConfigureFailed, "failed to configure transaction: ", cause)
}

func CypherTransactionsWALBeginFailed(cause error) Message {
	return cypherTransactionsCauseMessage(MessageCypherTransactionsWALBeginFailed, "failed to write WAL tx begin: ", cause)
}

func CypherTransactionsNoActive() Message {
	return cypherTransactionsMessage(MessageCypherTransactionsNoActive, "no active transaction", nil)
}

func CypherTransactionsUnknownType() Message {
	return cypherTransactionsMessage(MessageCypherTransactionsUnknownType, "unknown transaction type", nil)
}

func CypherTransactionsCommitFailed(cause error) Message {
	return cypherTransactionsCauseMessage(MessageCypherTransactionsCommitFailed, "commit failed: ", cause)
}

func CypherTransactionsRollbackFailed(cause error) Message {
	return cypherTransactionsCauseMessage(MessageCypherTransactionsRollbackFailed, "rollback failed: ", cause)
}

func CypherTransactionsShowInTransactionUnsupported(query string) Message {
	return cypherTransactionsMessage(MessageCypherTransactionsShowInTransactionUnsupported, "unsupported SHOW command in transaction: "+query, map[string]any{"Query": query})
}

func CypherTransactionsQueryInTransactionUnsupported(query string) Message {
	return cypherTransactionsMessage(MessageCypherTransactionsQueryInTransactionUnsupported, "unsupported query type in transaction: "+query, map[string]any{"Query": query})
}

func CypherTransactionsInvalidScriptAction(action string) Message {
	return cypherTransactionsMessage(MessageCypherTransactionsInvalidScriptAction, "invalid transaction script action: "+action, map[string]any{"Action": action})
}

func CypherTransactionsCaseBlockMissing() Message {
	return cypherTransactionsMessage(MessageCypherTransactionsCaseBlockMissing, "invalid transaction CASE script: missing CASE block", nil)
}

func CypherTransactionsCaseSyntaxInvalid() Message {
	return cypherTransactionsMessage(MessageCypherTransactionsCaseSyntaxInvalid, "invalid transaction CASE syntax: expected CASE WHEN ... THEN ROLLBACK ELSE RETURN ... COMMIT", nil)
}

func CypherTransactionsConditionNotBoolean(value any) Message {
	formatted := fmt.Sprint(value)
	return cypherTransactionsMessage(MessageCypherTransactionsConditionNotBoolean, "condition expression did not evaluate to boolean: "+formatted, map[string]any{"Value": formatted})
}

func CypherTransactionsQueryTypeUnsupported(queryType string) Message {
	fallback := "unsupported query type: " + queryType + " (supported: " + cypherTransactionSupportedQueryTypes + ")"
	return cypherTransactionsMessage(MessageCypherTransactionsQueryTypeUnsupported, fallback, map[string]any{"QueryType": queryType, "Supported": cypherTransactionSupportedQueryTypes})
}

func CypherTransactionsReturnClauseNotFound(query string) Message {
	quoted := strconv.Quote(query)
	return cypherTransactionsMessage(MessageCypherTransactionsReturnClauseNotFound, "RETURN clause not found in query: "+quoted, map[string]any{"Query": quoted})
}

func CypherTransactionsSyntaxStartInvalid() Message {
	fallback := "syntax error: query must start with a valid clause (" + cypherTransactionValidStartClauses + ")"
	return cypherTransactionsMessage(MessageCypherTransactionsSyntaxStartInvalid, fallback, map[string]any{"ValidClauses": cypherTransactionValidStartClauses})
}

func CypherTransactionsSyntaxUnbalancedAt(position int) Message {
	return cypherTransactionsMessage(MessageCypherTransactionsSyntaxUnbalancedAt, fmt.Sprintf("syntax error: unbalanced brackets at position %d", position), map[string]any{"Position": position})
}

func CypherTransactionsSyntaxUnbalancedParentheses() Message {
	return cypherTransactionsMessage(MessageCypherTransactionsSyntaxUnbalancedParentheses, "syntax error: unbalanced parentheses", nil)
}

func CypherTransactionsSyntaxUnbalancedSquareBrackets() Message {
	return cypherTransactionsMessage(MessageCypherTransactionsSyntaxUnbalancedSquareBrackets, "syntax error: unbalanced square brackets", nil)
}

func CypherTransactionsSyntaxUnbalancedCurlyBraces() Message {
	return cypherTransactionsMessage(MessageCypherTransactionsSyntaxUnbalancedCurlyBraces, "syntax error: unbalanced curly braces", nil)
}

func CypherTransactionsSyntaxUnclosedQuote() Message {
	return cypherTransactionsMessage(MessageCypherTransactionsSyntaxUnclosedQuote, "syntax error: unclosed quote", nil)
}

func CypherTransactionsDeleteResidualRelationships(nodeID string) Message {
	fallback := "Cannot delete node " + nodeID + ", because it still has relationships. To delete this node, you must first delete its relationships (or use DETACH DELETE)"
	return cypherTransactionsMessage(MessageCypherTransactionsDeleteResidualRelationships, fallback, map[string]any{"NodeID": nodeID})
}

func CypherTransactionsMatchWithUnwindClausesRequired() Message {
	return cypherTransactionsMessage(MessageCypherTransactionsMatchWithUnwindClausesRequired, "MATCH, WITH, UNWIND, and RETURN clauses required (e.g., MATCH (n) WITH n UNWIND n.items AS item RETURN item)", nil)
}

func CypherTransactionsStorageFailed(cause error) Message {
	return cypherTransactionsCauseMessage(MessageCypherTransactionsStorageFailed, "storage error: ", cause)
}

func CypherTransactionsUnwindASRequired() Message {
	return cypherTransactionsMessage(MessageCypherTransactionsUnwindASRequired, "UNWIND requires AS clause (e.g., UNWIND [1,2,3] AS x)", nil)
}

func CypherTransactionsOrderByParseFailed() Message {
	return cypherTransactionsMessage(MessageCypherTransactionsOrderByParseFailed, "failed to parse ORDER BY clause", nil)
}

func CypherTransactionsMultiMatchReturnRequired() Message {
	return cypherTransactionsMessage(MessageCypherTransactionsMultiMatchReturnRequired, "multi-MATCH query requires RETURN clause", nil)
}

func CypherTransactionsMultipleMatchExpected() Message {
	return cypherTransactionsMessage(MessageCypherTransactionsMultipleMatchExpected, "expected multiple MATCH clauses", nil)
}
