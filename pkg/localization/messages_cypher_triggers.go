package localization

// APOC trigger messages (apoc.trigger.*, pkg/cypher/apoc_trigger*.go). The
// fallbacks of the procedures' own errors keep APOC 5's wording.
const (
	MessageCypherProceduresTriggerNotEnabled             MessageID = "cypherprocedures.trigger_not_enabled"
	MessageCypherProceduresTriggerSystemDatabaseRequired MessageID = "cypherprocedures.trigger_system_database_required"
	MessageCypherProceduresTriggerDatabaseNotFound       MessageID = "cypherprocedures.trigger_database_not_found"
	MessageCypherProceduresTriggerBadTarget              MessageID = "cypherprocedures.trigger_bad_target"
	MessageCypherProceduresTriggerStatementInvalid       MessageID = "cypherprocedures.trigger_statement_invalid"
	MessageCypherProceduresTriggerPhaseInvalid           MessageID = "cypherprocedures.trigger_phase_invalid"
	MessageCypherProceduresTriggerParamsInvalid          MessageID = "cypherprocedures.trigger_params_invalid"
	MessageCypherProceduresTriggerNotFound               MessageID = "cypherprocedures.trigger_not_found"
	MessageCypherProceduresTriggerStoreFailed            MessageID = "cypherprocedures.trigger_store_failed"
	MessageCypherProceduresTriggerFailed                 MessageID = "cypherprocedures.trigger_failed"
)

// CypherProceduresTriggerNotEnabled is a trigger procedure called while
// triggers are disabled (apoc.trigger.enabled, the default).
func CypherProceduresTriggerNotEnabled() Message {
	return cypherProceduresMessage(MessageCypherProceduresTriggerNotEnabled, "Triggers have not been enabled. Set 'apoc.trigger.enabled=true' (NORNICDB_APOC_TRIGGER_ENABLED=true) in the configuration.", nil)
}

// CypherProceduresTriggerSystemDatabaseRequired is a system trigger
// procedure (install, drop, dropAll, stop, start, show) called on a user
// database.
func CypherProceduresTriggerSystemDatabaseRequired() Message {
	return cypherProceduresMessage(MessageCypherProceduresTriggerSystemDatabaseRequired, "The procedure should be executed against a system database.", nil)
}

// CypherProceduresTriggerDatabaseNotFound is apoc.trigger.install for a
// database that doesn't exist.
func CypherProceduresTriggerDatabaseNotFound(database string) Message {
	return cypherProceduresMessage(MessageCypherProceduresTriggerDatabaseNotFound, "The user database with name '"+database+"' does not exist", map[string]any{"Database": database})
}

// CypherProceduresTriggerBadTarget is apoc.trigger.install for the system
// database.
func CypherProceduresTriggerBadTarget() Message {
	return cypherProceduresMessage(MessageCypherProceduresTriggerBadTarget, "Triggers can only be installed on user databases.", nil)
}

// CypherProceduresTriggerStatementInvalid is a trigger whose statement does
// not parse: refused when it is installed, not at every later commit.
func CypherProceduresTriggerStatementInvalid(name string, cause error) Message {
	return cypherProceduresMessage(MessageCypherProceduresTriggerStatementInvalid, "trigger "+name+": invalid statement: "+storageErrorText(cause), map[string]any{"Name": name, "Cause": storageErrorText(cause)})
}

// CypherProceduresTriggerPhaseInvalid is a trigger selector whose phase is
// none of before, after, rollback and afterAsync.
func CypherProceduresTriggerPhaseInvalid(phase string) Message {
	return cypherProceduresMessage(MessageCypherProceduresTriggerPhaseInvalid, "invalid trigger phase '"+phase+"': expected before, after, rollback or afterAsync", map[string]any{"Phase": phase})
}

// CypherProceduresTriggerParamsInvalid is a trigger's config.params that
// can't be stored.
func CypherProceduresTriggerParamsInvalid(cause error) Message {
	return cypherProceduresMessage(MessageCypherProceduresTriggerParamsInvalid, "invalid trigger params: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}

// CypherProceduresTriggerNotFound is a pause or resume of a trigger that
// isn't installed.
func CypherProceduresTriggerNotFound(name string) Message {
	return cypherProceduresMessage(MessageCypherProceduresTriggerNotFound, "trigger not found: "+name, map[string]any{"Name": name})
}

// CypherProceduresTriggerStoreFailed is a failure to read or write the
// triggers kept in the system database.
func CypherProceduresTriggerStoreFailed(cause error) Message {
	return cypherProceduresMessage(MessageCypherProceduresTriggerStoreFailed, "trigger store: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}

// CypherProceduresTriggerFailed is the error of a trigger run that failed:
// each failing trigger's name and error, in APOC's {name=error, …} form.
func CypherProceduresTriggerFailed(failures string) Message {
	return cypherProceduresMessage(MessageCypherProceduresTriggerFailed, "Error executing triggers "+failures, map[string]any{"Failures": failures})
}
