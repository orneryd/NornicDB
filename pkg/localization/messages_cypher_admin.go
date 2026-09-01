package localization

import "strconv"

const (
	MessageCypherAdminDatabaseManagerUnavailable MessageID = "cypheradmin.database_manager_unavailable"
	MessageCypherAdminInvalidSyntax              MessageID = "cypheradmin.invalid_syntax"
	MessageCypherAdminDatabaseNameExpected       MessageID = "cypheradmin.database_name_expected"
	MessageCypherAdminDatabaseNameEmpty          MessageID = "cypheradmin.database_name_empty"
	MessageCypherAdminInvalidDatabaseName        MessageID = "cypheradmin.invalid_database_name"
	MessageCypherAdminDatabaseAlreadyExists      MessageID = "cypheradmin.database_already_exists"
	MessageCypherAdminCreateDatabaseFailed       MessageID = "cypheradmin.create_database_failed"
	MessageCypherAdminInvalidIdentifier          MessageID = "cypheradmin.invalid_identifier"
	MessageCypherAdminDatabaseDoesNotExist       MessageID = "cypheradmin.database_does_not_exist"
	MessageCypherAdminDropDatabaseFailed         MessageID = "cypheradmin.drop_database_failed"
	MessageCypherAdminAliasNameExpected          MessageID = "cypheradmin.alias_name_expected"
	MessageCypherAdminTermExpected               MessageID = "cypheradmin.term_expected"
	MessageCypherAdminClauseExpected             MessageID = "cypheradmin.clause_expected"
	MessageCypherAdminAliasNameEmpty             MessageID = "cypheradmin.alias_name_empty"
	MessageCypherAdminInvalidAliasName           MessageID = "cypheradmin.invalid_alias_name"
	MessageCypherAdminCreateAliasFailed          MessageID = "cypheradmin.create_alias_failed"
	MessageCypherAdminAliasDoesNotExist          MessageID = "cypheradmin.alias_does_not_exist"
	MessageCypherAdminDropAliasFailed            MessageID = "cypheradmin.drop_alias_failed"
	MessageCypherAdminKeywordExpected            MessageID = "cypheradmin.keyword_expected"
	MessageCypherAdminDatabaseNotFound           MessageID = "cypheradmin.database_not_found"
	MessageCypherAdminInvalidLimitsType          MessageID = "cypheradmin.invalid_limits_type"
	MessageCypherAdminLimitAssignmentExpected    MessageID = "cypheradmin.limit_assignment_expected"
	MessageCypherAdminInvalidLimitAssignment     MessageID = "cypheradmin.invalid_limit_assignment"
	MessageCypherAdminInvalidLimitValue          MessageID = "cypheradmin.invalid_limit_value"
	MessageCypherAdminInvalidDurationLimitValue  MessageID = "cypheradmin.invalid_duration_limit_value"
	MessageCypherAdminUnknownLimitName           MessageID = "cypheradmin.unknown_limit_name"
	MessageCypherAdminSetDatabaseLimitsFailed    MessageID = "cypheradmin.set_database_limits_failed"
)

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
