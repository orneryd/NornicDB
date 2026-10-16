package localization

import "strconv"

const (
	MessageCypherAdminDatabaseManagerUnavailable        MessageID = "cypheradmin.database_manager_unavailable"
	MessageCypherAdminInvalidSyntax                     MessageID = "cypheradmin.invalid_syntax"
	MessageCypherAdminDatabaseNameExpected              MessageID = "cypheradmin.database_name_expected"
	MessageCypherAdminDatabaseNameEmpty                 MessageID = "cypheradmin.database_name_empty"
	MessageCypherAdminInvalidDatabaseName               MessageID = "cypheradmin.invalid_database_name"
	MessageCypherAdminDatabaseAlreadyExists             MessageID = "cypheradmin.database_already_exists"
	MessageCypherAdminCreateDatabaseFailed              MessageID = "cypheradmin.create_database_failed"
	MessageCypherAdminInvalidIdentifier                 MessageID = "cypheradmin.invalid_identifier"
	MessageCypherAdminDatabaseDoesNotExist              MessageID = "cypheradmin.database_does_not_exist"
	MessageCypherAdminDropDatabaseFailed                MessageID = "cypheradmin.drop_database_failed"
	MessageCypherAdminAliasNameExpected                 MessageID = "cypheradmin.alias_name_expected"
	MessageCypherAdminTermExpected                      MessageID = "cypheradmin.term_expected"
	MessageCypherAdminClauseExpected                    MessageID = "cypheradmin.clause_expected"
	MessageCypherAdminAliasNameEmpty                    MessageID = "cypheradmin.alias_name_empty"
	MessageCypherAdminInvalidAliasName                  MessageID = "cypheradmin.invalid_alias_name"
	MessageCypherAdminCreateAliasFailed                 MessageID = "cypheradmin.create_alias_failed"
	MessageCypherAdminAliasDoesNotExist                 MessageID = "cypheradmin.alias_does_not_exist"
	MessageCypherAdminDropAliasFailed                   MessageID = "cypheradmin.drop_alias_failed"
	MessageCypherAdminKeywordExpected                   MessageID = "cypheradmin.keyword_expected"
	MessageCypherAdminDatabaseNotFound                  MessageID = "cypheradmin.database_not_found"
	MessageCypherAdminInvalidLimitsType                 MessageID = "cypheradmin.invalid_limits_type"
	MessageCypherAdminLimitAssignmentExpected           MessageID = "cypheradmin.limit_assignment_expected"
	MessageCypherAdminInvalidLimitAssignment            MessageID = "cypheradmin.invalid_limit_assignment"
	MessageCypherAdminInvalidLimitValue                 MessageID = "cypheradmin.invalid_limit_value"
	MessageCypherAdminInvalidDurationLimitValue         MessageID = "cypheradmin.invalid_duration_limit_value"
	MessageCypherAdminUnknownLimitName                  MessageID = "cypheradmin.unknown_limit_name"
	MessageCypherAdminSetDatabaseLimitsFailed           MessageID = "cypheradmin.set_database_limits_failed"
	MessageCypherAdminTransactionIDsType                MessageID = "cypheradmin.transaction_ids_type"
	MessageCypherAdminTransactionIDType                 MessageID = "cypheradmin.transaction_id_type"
	MessageCypherAdminVariableNotDefined                MessageID = "cypheradmin.variable_not_defined"
	MessageCypherAdminShowTransactionsInvalidInput      MessageID = "cypheradmin.show_transactions_invalid_input"
	MessageCypherAdminTerminateTransactionsInvalidInput MessageID = "cypheradmin.terminate_transactions_invalid_input"
	MessageCypherAdminShowClauseInvalid                 MessageID = "cypheradmin.show_clause_invalid"
	MessageCypherAdminUnsupportedAdministrationCommand  MessageID = "cypheradmin.unsupported_administration_command"
	MessageCypherAdminTransactionTerminated             MessageID = "cypheradmin.transaction_terminated"
	MessageCypherAdminTerminateTransactionsIDRequired   MessageID = "cypheradmin.terminate_transactions_id_required"
	MessageCypherAdminTransactionIDUnparseable          MessageID = "cypheradmin.transaction_id_unparseable"
	MessageCypherAdminTransactionIDDatabaseNameLength   MessageID = "cypheradmin.transaction_id_database_name_length"
	MessageCypherAdminDatabaseOptionUnknown             MessageID = "cypheradmin.database_option_unknown"
	MessageCypherAdminDatabaseOptionUnsupported         MessageID = "cypheradmin.database_option_unsupported"
	MessageCypherAdminDatabaseOptionValue               MessageID = "cypheradmin.database_option_value"
	MessageCypherAdminDatabaseOptionsUnavailable        MessageID = "cypheradmin.database_options_unavailable"
)

// CypherAdminDatabaseOptionUnknown identifies a database option name Neo4j doesn't define.
func CypherAdminDatabaseOptionUnknown(option string) Message {
	return Message{ID: MessageCypherAdminDatabaseOptionUnknown, Fallback: "Could not create database with specified options: unknown option '" + option + "'", Data: map[string]any{"Option": option}}
}

// CypherAdminDatabaseOptionUnsupported identifies a Neo4j database option NornicDB doesn't implement.
func CypherAdminDatabaseOptionUnsupported(option string) Message {
	return Message{ID: MessageCypherAdminDatabaseOptionUnsupported, Fallback: "the database option '" + option + "' is not supported by NornicDB", Data: map[string]any{"Option": option}}
}

// CypherAdminDatabaseOptionValue identifies a database option value outside the option's values.
func CypherAdminDatabaseOptionValue(option, value, allowed string) Message {
	return Message{ID: MessageCypherAdminDatabaseOptionValue, Fallback: "invalid value '" + value + "' for database option '" + option + "': expected one of " + allowed, Data: map[string]any{"Option": option, "Value": value, "Allowed": allowed}}
}

// CypherAdminDatabaseOptionsUnavailable identifies a database manager without database options.
func CypherAdminDatabaseOptionsUnavailable(command string) Message {
	return Message{ID: MessageCypherAdminDatabaseOptionsUnavailable, Fallback: "database options are not available - " + command + " requires a database manager with options", Data: map[string]any{"Command": command}}
}

// CypherAdminDatabaseManagerUnavailable identifies an admin command that requires multi-database support.
func CypherAdminDatabaseManagerUnavailable(command string) Message {
	return Message{ID: MessageCypherAdminDatabaseManagerUnavailable, Fallback: "database manager not available - " + command + " requires multi-database support", Data: map[string]any{"Command": command}}
}

// CypherAdminInvalidSyntax identifies malformed admin command syntax.
func CypherAdminInvalidSyntax(command string) Message {
	return Message{ID: MessageCypherAdminInvalidSyntax, Fallback: "invalid " + command + " syntax", Data: map[string]any{"Command": command}}
}

// CypherAdminDatabaseNameExpected identifies a missing database name.
func CypherAdminDatabaseNameExpected(command string) Message {
	return Message{ID: MessageCypherAdminDatabaseNameExpected, Fallback: "invalid " + command + " syntax: database name expected", Data: map[string]any{"Command": command}}
}

// CypherAdminDatabaseNameEmpty identifies an empty database name.
func CypherAdminDatabaseNameEmpty(command string) Message {
	return Message{ID: MessageCypherAdminDatabaseNameEmpty, Fallback: "invalid " + command + " syntax: database name cannot be empty", Data: map[string]any{"Command": command}}
}

// CypherAdminInvalidDatabaseName identifies whitespace in a database name.
func CypherAdminInvalidDatabaseName(database string) Message {
	return Message{ID: MessageCypherAdminInvalidDatabaseName, Fallback: "invalid database name: '" + database + "' (cannot contain whitespace)", Data: map[string]any{"Database": database}}
}

// CypherAdminDatabaseAlreadyExists identifies a conflicting database creation.
func CypherAdminDatabaseAlreadyExists(database string) Message {
	return Message{ID: MessageCypherAdminDatabaseAlreadyExists, Fallback: "database '" + database + "' already exists", Data: map[string]any{"Database": database}}
}

// CypherAdminCreateDatabaseFailed identifies a wrapped database creation failure.
func CypherAdminCreateDatabaseFailed(database string, cause error) Message {
	return Message{ID: MessageCypherAdminCreateDatabaseFailed, Fallback: "failed to create database '" + database + "': " + cause.Error(), Data: map[string]any{"Database": database, "Cause": cause.Error()}}
}

// CypherAdminInvalidIdentifier identifies unsupported nested backticks in an identifier.
func CypherAdminInvalidIdentifier(identifier string) Message {
	quotedIdentifier := strconv.Quote(identifier)
	return Message{ID: MessageCypherAdminInvalidIdentifier, Fallback: "invalid identifier " + quotedIdentifier + ": nested backticks are not supported", Data: map[string]any{"Identifier": quotedIdentifier}}
}

// CypherAdminDatabaseDoesNotExist identifies a missing database during DROP DATABASE.
func CypherAdminDatabaseDoesNotExist(database string) Message {
	return Message{ID: MessageCypherAdminDatabaseDoesNotExist, Fallback: "database '" + database + "' does not exist", Data: map[string]any{"Database": database}}
}

// CypherAdminDropDatabaseFailed identifies a wrapped database deletion failure.
func CypherAdminDropDatabaseFailed(database string, cause error) Message {
	return Message{ID: MessageCypherAdminDropDatabaseFailed, Fallback: "failed to drop database '" + database + "': " + cause.Error(), Data: map[string]any{"Database": database, "Cause": cause.Error()}}
}

// CypherAdminAliasNameExpected identifies a missing alias name.
func CypherAdminAliasNameExpected(command string) Message {
	return Message{ID: MessageCypherAdminAliasNameExpected, Fallback: "invalid " + command + " syntax: alias name expected", Data: map[string]any{"Command": command}}
}

// CypherAdminTermExpected identifies a required machine-language command term.
func CypherAdminTermExpected(command, term string) Message {
	return Message{ID: MessageCypherAdminTermExpected, Fallback: "invalid " + command + " syntax: " + term + " expected", Data: map[string]any{"Command": command, "Term": term}}
}

// CypherAdminClauseExpected identifies a required machine-language command clause.
func CypherAdminClauseExpected(command, clause string) Message {
	return Message{ID: MessageCypherAdminClauseExpected, Fallback: "invalid " + command + " syntax: " + clause + " clause expected", Data: map[string]any{"Command": command, "Clause": clause}}
}

// CypherAdminAliasNameEmpty identifies an empty alias name.
func CypherAdminAliasNameEmpty(command string) Message {
	return Message{ID: MessageCypherAdminAliasNameEmpty, Fallback: "invalid " + command + " syntax: alias name cannot be empty", Data: map[string]any{"Command": command}}
}

// CypherAdminInvalidAliasName identifies whitespace in an alias name.
func CypherAdminInvalidAliasName(alias string) Message {
	return Message{ID: MessageCypherAdminInvalidAliasName, Fallback: "invalid alias name: '" + alias + "' (cannot contain whitespace)", Data: map[string]any{"Alias": alias}}
}

// CypherAdminCreateAliasFailed identifies a wrapped alias creation failure.
func CypherAdminCreateAliasFailed(alias, database string, cause error) Message {
	return Message{ID: MessageCypherAdminCreateAliasFailed, Fallback: "failed to create alias '" + alias + "' for database '" + database + "': " + cause.Error(), Data: map[string]any{"Alias": alias, "Database": database, "Cause": cause.Error()}}
}

// CypherAdminAliasDoesNotExist identifies a missing alias during DROP ALIAS.
func CypherAdminAliasDoesNotExist(alias string) Message {
	return Message{ID: MessageCypherAdminAliasDoesNotExist, Fallback: "alias '" + alias + "' does not exist", Data: map[string]any{"Alias": alias}}
}

// CypherAdminDropAliasFailed identifies a wrapped alias deletion failure.
func CypherAdminDropAliasFailed(alias string, cause error) Message {
	return Message{ID: MessageCypherAdminDropAliasFailed, Fallback: "failed to drop alias '" + alias + "': " + cause.Error(), Data: map[string]any{"Alias": alias, "Cause": cause.Error()}}
}

// CypherAdminKeywordExpected identifies a required machine-language keyword.
func CypherAdminKeywordExpected(command, keyword string) Message {
	return Message{ID: MessageCypherAdminKeywordExpected, Fallback: "invalid " + command + " syntax: " + keyword + " keyword expected", Data: map[string]any{"Command": command, "Keyword": keyword}}
}

// CypherAdminDatabaseNotFound identifies a wrapped database limit lookup failure.
func CypherAdminDatabaseNotFound(database string, cause error) Message {
	return Message{ID: MessageCypherAdminDatabaseNotFound, Fallback: "database '" + database + "' not found: " + cause.Error(), Data: map[string]any{"Database": database, "Cause": cause.Error()}}
}

// CypherAdminInvalidLimitsType identifies an unexpected database manager response type.
func CypherAdminInvalidLimitsType() Message {
	return Message{ID: MessageCypherAdminInvalidLimitsType, Fallback: "invalid limits type returned from database manager"}
}

// CypherAdminLimitAssignmentExpected identifies a missing ALTER DATABASE limit assignment.
func CypherAdminLimitAssignmentExpected() Message {
	return Message{ID: MessageCypherAdminLimitAssignmentExpected, Fallback: "invalid ALTER DATABASE syntax: limit assignment expected"}
}

// CypherAdminInvalidLimitAssignment identifies a malformed limit assignment.
func CypherAdminInvalidLimitAssignment(assignment string) Message {
	return Message{ID: MessageCypherAdminInvalidLimitAssignment, Fallback: "invalid limit assignment syntax: expected 'limit_name = value', got '" + assignment + "'", Data: map[string]any{"Assignment": assignment}}
}

// CypherAdminInvalidLimitValue identifies a malformed numeric limit value.
func CypherAdminInvalidLimitValue(limit string, cause error) Message {
	return Message{ID: MessageCypherAdminInvalidLimitValue, Fallback: "invalid " + limit + " value: " + cause.Error(), Data: map[string]any{"Limit": limit, "Cause": cause.Error()}}
}

// CypherAdminInvalidDurationLimitValue identifies a malformed duration limit value.
func CypherAdminInvalidDurationLimitValue(limit string, cause error) Message {
	return Message{ID: MessageCypherAdminInvalidDurationLimitValue, Fallback: "invalid " + limit + " value (expected duration like '60s' or '5m'): " + cause.Error(), Data: map[string]any{"Limit": limit, "Cause": cause.Error()}}
}

// CypherAdminUnknownLimitName identifies an unsupported limit key.
func CypherAdminUnknownLimitName(limit string) Message {
	const supported = "max_nodes, max_edges, max_bytes, max_query_time, max_results, max_concurrent_queries, max_connections, max_queries_per_second, max_writes_per_second"
	return Message{ID: MessageCypherAdminUnknownLimitName, Fallback: "unknown limit name: '" + limit + "' (supported: " + supported + ")", Data: map[string]any{"Limit": limit, "Supported": supported}}
}

// CypherAdminSetDatabaseLimitsFailed identifies a wrapped database limit update failure.
func CypherAdminSetDatabaseLimitsFailed(database string, cause error) Message {
	return Message{ID: MessageCypherAdminSetDatabaseLimitsFailed, Fallback: "failed to set limits for database '" + database + "': " + cause.Error(), Data: map[string]any{"Database": database, "Cause": cause.Error()}}
}

func CypherAdminTransactionIDsType(value string) Message {
	return Message{ID: MessageCypherAdminTransactionIDsType, Fallback: "Expected a string or a list of strings, but got: " + value, Data: map[string]any{"Value": value}}
}

func CypherAdminTransactionIDType(value string) Message {
	return Message{ID: MessageCypherAdminTransactionIDType, Fallback: "Expected a string, but got: " + value, Data: map[string]any{"Value": value}}
}

func CypherAdminVariableNotDefined(variable string) Message {
	return Message{ID: MessageCypherAdminVariableNotDefined, Fallback: "Variable `" + variable + "` not defined", Data: map[string]any{"Variable": variable}}
}

func CypherAdminShowTransactionsInvalidInput(input string) Message {
	return Message{ID: MessageCypherAdminShowTransactionsInvalidInput, Fallback: "Invalid input '" + input + "': expected a string, an expression, 'SHOW', 'TERMINATE', 'WHERE' or 'YIELD'", Data: map[string]any{"Input": input}}
}

func CypherAdminTerminateTransactionsInvalidInput(input string) Message {
	return Message{ID: MessageCypherAdminTerminateTransactionsInvalidInput, Fallback: "Invalid input '" + input + "': expected a string or an expression", Data: map[string]any{"Input": input}}
}

func CypherAdminShowClauseInvalid() Message {
	return Message{ID: MessageCypherAdminShowClauseInvalid, Fallback: "invalid SHOW command: expected WHERE, or YIELD [ORDER BY] [SKIP] [LIMIT] [WHERE] [RETURN]"}
}

func CypherAdminUnsupportedAdministrationCommand(command string) Message {
	return Message{ID: MessageCypherAdminUnsupportedAdministrationCommand, Fallback: "Unsupported administration command: " + command, Data: map[string]any{"Command": command}}
}

func CypherAdminTransactionTerminated() Message {
	return Message{ID: MessageCypherAdminTransactionTerminated, Fallback: "The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result."}
}

func CypherAdminTerminateTransactionsIDRequired() Message {
	return Message{ID: MessageCypherAdminTerminateTransactionsIDRequired, Fallback: "TERMINATE TRANSACTIONS requires a transaction id or a list of transaction ids"}
}

func CypherAdminTransactionIDUnparseable() Message {
	return Message{ID: MessageCypherAdminTransactionIDUnparseable, Fallback: "Could not parse id (expected format: <databasename>-transaction-<id>)"}
}

func CypherAdminTransactionIDDatabaseNameLength() Message {
	return Message{ID: MessageCypherAdminTransactionIDDatabaseNameLength, Fallback: "The provided database name must have a length between 3 and 63 characters."}
}

// Procedure privileges: a GRANT to a role that does not exist, and immutable
// privileges (GRANT / REVOKE IMMUTABLE …), which Neo4j lets anyone
// administer only while authentication is disabled.
const (
	MessageCypherAdminImmutablePrivilegeGrantForbidden  MessageID = "cypheradmin.immutable_privilege_grant_forbidden"
	MessageCypherAdminImmutablePrivilegeRevokeForbidden MessageID = "cypheradmin.immutable_privilege_revoke_forbidden"
	MessageCypherAdminPrivilegeRoleNotFound             MessageID = "cypheradmin.privilege_role_not_found"
)

// CypherAdminImmutablePrivilegeGrantForbidden identifies a GRANT IMMUTABLE
// while authentication is enabled.
func CypherAdminImmutablePrivilegeGrantForbidden() Message {
	return Message{ID: MessageCypherAdminImmutablePrivilegeGrantForbidden, Fallback: "Permission cannot be granted for ASSIGN IMMUTABLE PRIVILEGE."}
}

// CypherAdminImmutablePrivilegeRevokeForbidden identifies a REVOKE of an
// immutable privilege while authentication is enabled.
func CypherAdminImmutablePrivilegeRevokeForbidden() Message {
	return Message{ID: MessageCypherAdminImmutablePrivilegeRevokeForbidden, Fallback: "Immutable privileges cannot be revoked."}
}

// CypherAdminPrivilegeRoleNotFound identifies a GRANT to a role that does
// not exist; action is the privilege's action (execute, execute_boosted).
func CypherAdminPrivilegeRoleNotFound(action, role string) Message {
	return Message{ID: MessageCypherAdminPrivilegeRoleNotFound, Fallback: "Failed to grant " + action + " privilege to role '" + role + "': Role does not exist.", Data: map[string]any{"Action": action, "Role": role}}
}

const (
	MessageCypherAdminSystemGraphAccessUnsupported MessageID = "cypheradmin.system_graph_access_unsupported"
	MessageCypherAdminSystemProcedureUnsupported   MessageID = "cypheradmin.system_procedure_unsupported"
	MessageCypherAdminTransactionStatementType     MessageID = "cypheradmin.transaction_statement_type"
)

// CypherAdminSystemGraphAccessUnsupported reports the system statement policy.
func CypherAdminSystemGraphAccessUnsupported() Message {
	return Message{ID: MessageCypherAdminSystemGraphAccessUnsupported, Fallback: "Graph access is not supported on the system database"}
}

// CypherAdminSystemProcedureUnsupported reports a call of a procedure that
// doesn't run on the system database, in Neo4j's words; query is the
// statement.
func CypherAdminSystemProcedureUnsupported(query string) Message {
	return Message{ID: MessageCypherAdminSystemProcedureUnsupported, Fallback: "Not a recognised system command or procedure. This Cypher command can only be executed in a user database: " + query, Data: map[string]any{"Query": query}}
}

// CypherAdminTransactionStatementType reports a statement whose type a
// transaction can't take, in Neo4j's words; previous and requested are
// Neo4j's statement type names ("Read query", "Write query", "Schema
// modification", "Administration command").
func CypherAdminTransactionStatementType(previous, requested string) Message {
	return Message{ID: MessageCypherAdminTransactionStatementType, Fallback: "Tried to execute " + requested + " after executing " + previous, Data: map[string]any{"Previous": previous, "Requested": requested}}
}

const (
	MessageCypherAdminShowClauseUnexpected            MessageID = "cypheradmin.show_clause_unexpected"
	MessageCypherAdminShowExpressionInvalid           MessageID = "cypheradmin.show_expression_invalid"
	MessageCypherAdminShowPatternComprehensionInvalid MessageID = "cypheradmin.show_pattern_comprehension_invalid"
	MessageCypherAdminShowPatternExpressionInvalid    MessageID = "cypheradmin.show_pattern_expression_invalid"
)

// CypherAdminShowClauseUnexpected reports a clause after a SHOW command's
// YIELD on the system database, in Neo4j's words; clause is the clause's
// first keyword.
func CypherAdminShowClauseUnexpected(clause string) Message {
	return Message{ID: MessageCypherAdminShowClauseUnexpected, Fallback: "Invalid input '" + clause + "': expected ',', 'AS', 'ORDER BY', 'LIMIT', 'OFFSET', 'RETURN', 'SHOW', 'SKIP', 'TERMINATE', 'WHERE' or <EOF>", Data: map[string]any{"Clause": clause}}
}

// CypherAdminShowExpressionInvalid reports a COUNT, EXISTS or COLLECT
// subquery in a SHOW command on the system database, in Neo4j's words;
// expression is COUNT, EXISTS or COLLECT.
func CypherAdminShowExpressionInvalid(expression string) Message {
	return Message{ID: MessageCypherAdminShowExpressionInvalid, Fallback: "The " + expression + " expression is not valid on SHOW commands.", Data: map[string]any{"Expression": expression}}
}

// CypherAdminShowPatternComprehensionInvalid reports a pattern
// comprehension in a SHOW command on the system database, in Neo4j's words.
func CypherAdminShowPatternComprehensionInvalid() Message {
	return Message{ID: MessageCypherAdminShowPatternComprehensionInvalid, Fallback: "Pattern comprehensions are not valid on SHOW commands."}
}

// CypherAdminShowPatternExpressionInvalid reports a pattern expression (a
// pattern predicate, EXISTS of a pattern, shortestPath) in a SHOW command
// on the system database, in Neo4j's words.
func CypherAdminShowPatternExpressionInvalid() Message {
	return Message{ID: MessageCypherAdminShowPatternExpressionInvalid, Fallback: "Pattern expressions are not valid on SHOW commands."}
}
