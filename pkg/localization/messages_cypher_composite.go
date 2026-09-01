package localization

const (
	MessageCypherCompositeConstituentAliasExpected          MessageID = "cyphercomposite.constituent_alias_expected"
	MessageCypherCompositeConstituentAliasNameEmpty         MessageID = "cyphercomposite.constituent_alias_name_empty"
	MessageCypherCompositeConstituentAliasNameInvalid       MessageID = "cyphercomposite.constituent_alias_name_invalid"
	MessageCypherCompositeConstituentForDatabaseExpected    MessageID = "cyphercomposite.constituent_for_database_expected"
	MessageCypherCompositeConstituentDatabaseNameEmpty      MessageID = "cyphercomposite.constituent_database_name_empty"
	MessageCypherCompositeConstituentDatabaseNameInvalid    MessageID = "cyphercomposite.constituent_database_name_invalid"
	MessageCypherCompositeConstituentRemoteURIExpected      MessageID = "cyphercomposite.constituent_remote_uri_expected"
	MessageCypherCompositeConstituentRemoteURIInvalid       MessageID = "cyphercomposite.constituent_remote_uri_invalid"
	MessageCypherCompositeConstituentRemoteURIEmpty         MessageID = "cyphercomposite.constituent_remote_uri_empty"
	MessageCypherCompositeConstituentUserEmpty              MessageID = "cyphercomposite.constituent_user_empty"
	MessageCypherCompositeConstituentUserInvalid            MessageID = "cyphercomposite.constituent_user_invalid"
	MessageCypherCompositeConstituentPasswordEmpty          MessageID = "cyphercomposite.constituent_password_empty"
	MessageCypherCompositeConstituentPasswordInvalid        MessageID = "cyphercomposite.constituent_password_invalid"
	MessageCypherCompositeConstituentOIDCCredentialExpected MessageID = "cyphercomposite.constituent_oidc_credential_forwarding_expected"
	MessageCypherCompositeConstituentSecretRefExpected      MessageID = "cyphercomposite.constituent_secret_ref_expected"
	MessageCypherCompositeConstituentSecretRefEmpty         MessageID = "cyphercomposite.constituent_secret_ref_empty"
	MessageCypherCompositeConstituentSecretRefInvalid       MessageID = "cyphercomposite.constituent_secret_ref_invalid"
	MessageCypherCompositeConstituentTypeEmpty              MessageID = "cyphercomposite.constituent_type_empty"
	MessageCypherCompositeConstituentTypeInvalid            MessageID = "cyphercomposite.constituent_type_invalid"
	MessageCypherCompositeConstituentTypeUnsupported        MessageID = "cyphercomposite.constituent_type_unsupported"
	MessageCypherCompositeConstituentTypeContradictsAT      MessageID = "cyphercomposite.constituent_type_contradicts_at"
	MessageCypherCompositeConstituentAccessModeEmpty        MessageID = "cyphercomposite.constituent_access_mode_empty"
	MessageCypherCompositeConstituentAccessModeInvalid      MessageID = "cyphercomposite.constituent_access_mode_invalid"
	MessageCypherCompositeConstituentAccessModeUnsupported  MessageID = "cyphercomposite.constituent_access_mode_unsupported"
	MessageCypherCompositeConstituentUnexpectedToken        MessageID = "cyphercomposite.constituent_unexpected_token"
	MessageCypherCompositeConstituentAuthModesConflict      MessageID = "cyphercomposite.constituent_auth_modes_conflict"
	MessageCypherCompositeConstituentUserPasswordRequired   MessageID = "cyphercomposite.constituent_user_password_required"
	MessageCypherCompositeConstituentRemoteRequired         MessageID = "cyphercomposite.constituent_remote_required"
	MessageCypherCompositeDatabaseManagerUnavailable        MessageID = "cyphercomposite.database_manager_unavailable"
	MessageCypherCompositeCreateInvalidSyntax               MessageID = "cyphercomposite.create_invalid_syntax"
	MessageCypherCompositeCreateDatabaseNameExpected        MessageID = "cyphercomposite.create_database_name_expected"
	MessageCypherCompositeCreateDatabaseNameEmpty           MessageID = "cyphercomposite.create_database_name_empty"
	MessageCypherCompositeCreateTokenizeFailed              MessageID = "cyphercomposite.create_tokenize_failed"
	MessageCypherCompositeCreateConstituentRequired         MessageID = "cyphercomposite.create_constituent_required"
	MessageCypherCompositeCreateDatabaseFailed              MessageID = "cyphercomposite.create_database_failed"
	MessageCypherCompositeDropInvalidSyntax                 MessageID = "cyphercomposite.drop_invalid_syntax"
	MessageCypherCompositeDropDatabaseNameExpected          MessageID = "cyphercomposite.drop_database_name_expected"
	MessageCypherCompositeDropDatabaseNameEmpty             MessageID = "cyphercomposite.drop_database_name_empty"
	MessageCypherCompositeDropDatabaseFailed                MessageID = "cyphercomposite.drop_database_failed"
	MessageCypherCompositeShowConstituentsInvalidSyntax     MessageID = "cyphercomposite.show_constituents_invalid_syntax"
	MessageCypherCompositeShowConstituentsNameExpected      MessageID = "cyphercomposite.show_constituents_name_expected"
	MessageCypherCompositeGetConstituentsFailed             MessageID = "cyphercomposite.get_constituents_failed"
	MessageCypherCompositeAlterInvalidSyntax                MessageID = "cyphercomposite.alter_invalid_syntax"
	MessageCypherCompositeAlterDatabaseKeywordExpected      MessageID = "cyphercomposite.alter_database_keyword_expected"
	MessageCypherCompositeAlterDatabaseNameExpected         MessageID = "cyphercomposite.alter_database_name_expected"
	MessageCypherCompositeAlterDatabaseNameEmpty            MessageID = "cyphercomposite.alter_database_name_empty"
	MessageCypherCompositeAlterTokenizeFailed               MessageID = "cyphercomposite.alter_tokenize_failed"
	MessageCypherCompositeAlterAddAliasExpected             MessageID = "cyphercomposite.alter_add_alias_expected"
	MessageCypherCompositeAddAliasUnexpectedToken           MessageID = "cyphercomposite.add_alias_unexpected_token"
	MessageCypherCompositeAddConstituentFailed              MessageID = "cyphercomposite.add_constituent_failed"
	MessageCypherCompositeAlterDropAliasExpected            MessageID = "cyphercomposite.alter_drop_alias_expected"
	MessageCypherCompositeDropAliasNameEmpty                MessageID = "cyphercomposite.drop_alias_name_empty"
	MessageCypherCompositeRemoveConstituentFailed           MessageID = "cyphercomposite.remove_constituent_failed"
	MessageCypherCompositeAlterActionExpected               MessageID = "cyphercomposite.alter_action_expected"
)

func cypherCompositeMessage(id MessageID, fallback string, data map[string]any) Message {
	return Message{ID: id, Fallback: fallback, Data: data}
}

func cypherCompositeCauseMessage(id MessageID, prefix string, cause error) Message {
	return cypherCompositeMessage(id, prefix+cause.Error(), map[string]any{"Cause": cause.Error()})
}

func CypherCompositeConstituentAliasExpected() Message {
	return cypherCompositeMessage(MessageCypherCompositeConstituentAliasExpected, "invalid constituent syntax: ALIAS expected", nil)
}

func CypherCompositeConstituentAliasNameEmpty() Message {
	return cypherCompositeMessage(MessageCypherCompositeConstituentAliasNameEmpty, "invalid constituent syntax: alias name cannot be empty", nil)
}

func CypherCompositeConstituentAliasNameInvalid(cause error) Message {
	return cypherCompositeCauseMessage(MessageCypherCompositeConstituentAliasNameInvalid, "invalid constituent syntax: alias name: ", cause)
}

func CypherCompositeConstituentForDatabaseExpected() Message {
	return cypherCompositeMessage(MessageCypherCompositeConstituentForDatabaseExpected, "invalid constituent syntax: FOR DATABASE expected", nil)
}

func CypherCompositeConstituentDatabaseNameEmpty() Message {
	return cypherCompositeMessage(MessageCypherCompositeConstituentDatabaseNameEmpty, "invalid constituent syntax: database name cannot be empty", nil)
}

func CypherCompositeConstituentDatabaseNameInvalid(cause error) Message {
	return cypherCompositeCauseMessage(MessageCypherCompositeConstituentDatabaseNameInvalid, "invalid constituent syntax: database name: ", cause)
}

func CypherCompositeConstituentRemoteURIExpected() Message {
	return cypherCompositeMessage(MessageCypherCompositeConstituentRemoteURIExpected, "invalid constituent syntax: remote URI expected after AT", nil)
}

func CypherCompositeConstituentRemoteURIInvalid(cause error) Message {
	return cypherCompositeCauseMessage(MessageCypherCompositeConstituentRemoteURIInvalid, "invalid constituent syntax: remote URI: ", cause)
}

func CypherCompositeConstituentRemoteURIEmpty() Message {
	return cypherCompositeMessage(MessageCypherCompositeConstituentRemoteURIEmpty, "invalid constituent syntax: remote URI cannot be empty", nil)
}

func CypherCompositeConstituentUserEmpty() Message {
	return cypherCompositeMessage(MessageCypherCompositeConstituentUserEmpty, "invalid constituent syntax: user cannot be empty", nil)
}

func CypherCompositeConstituentUserInvalid(cause error) Message {
	return cypherCompositeCauseMessage(MessageCypherCompositeConstituentUserInvalid, "invalid constituent syntax: user: ", cause)
}

func CypherCompositeConstituentPasswordEmpty() Message {
	return cypherCompositeMessage(MessageCypherCompositeConstituentPasswordEmpty, "invalid constituent syntax: password cannot be empty", nil)
}

func CypherCompositeConstituentPasswordInvalid(cause error) Message {
	return cypherCompositeCauseMessage(MessageCypherCompositeConstituentPasswordInvalid, "invalid constituent syntax: password: ", cause)
}

func CypherCompositeConstituentOIDCCredentialExpected() Message {
	return cypherCompositeMessage(MessageCypherCompositeConstituentOIDCCredentialExpected, "invalid constituent syntax: OIDC CREDENTIAL FORWARDING expected", nil)
}

func CypherCompositeConstituentSecretRefExpected() Message {
	return cypherCompositeMessage(MessageCypherCompositeConstituentSecretRefExpected, "invalid constituent syntax: SECRET REF expected", nil)
}

func CypherCompositeConstituentSecretRefEmpty() Message {
	return cypherCompositeMessage(MessageCypherCompositeConstituentSecretRefEmpty, "invalid constituent syntax: secret ref cannot be empty", nil)
}

func CypherCompositeConstituentSecretRefInvalid(cause error) Message {
	return cypherCompositeCauseMessage(MessageCypherCompositeConstituentSecretRefInvalid, "invalid constituent syntax: secret ref: ", cause)
}

func CypherCompositeConstituentTypeEmpty() Message {
	return cypherCompositeMessage(MessageCypherCompositeConstituentTypeEmpty, "invalid constituent syntax: type cannot be empty", nil)
}

func CypherCompositeConstituentTypeInvalid(cause error) Message {
	return cypherCompositeCauseMessage(MessageCypherCompositeConstituentTypeInvalid, "invalid constituent syntax: type: ", cause)
}

func CypherCompositeConstituentTypeUnsupported() Message {
	return cypherCompositeMessage(MessageCypherCompositeConstituentTypeUnsupported, "invalid constituent syntax: type must be local or remote", nil)
}

func CypherCompositeConstituentTypeContradictsAT() Message {
	return cypherCompositeMessage(MessageCypherCompositeConstituentTypeContradictsAT, "invalid constituent syntax: TYPE local contradicts AT (remote URI already specified)", nil)
}

func CypherCompositeConstituentAccessModeEmpty() Message {
	return cypherCompositeMessage(MessageCypherCompositeConstituentAccessModeEmpty, "invalid constituent syntax: access mode cannot be empty", nil)
}

func CypherCompositeConstituentAccessModeInvalid(cause error) Message {
	return cypherCompositeCauseMessage(MessageCypherCompositeConstituentAccessModeInvalid, "invalid constituent syntax: access mode: ", cause)
}

func CypherCompositeConstituentAccessModeUnsupported() Message {
	return cypherCompositeMessage(MessageCypherCompositeConstituentAccessModeUnsupported, "invalid constituent syntax: access mode must be read, write, or read_write", nil)
}

func CypherCompositeConstituentUnexpectedToken(token string) Message {
	return cypherCompositeMessage(MessageCypherCompositeConstituentUnexpectedToken, "invalid constituent syntax: unexpected token '"+token+"'", map[string]any{"Token": token})
}

func CypherCompositeConstituentAuthModesConflict() Message {
	return cypherCompositeMessage(MessageCypherCompositeConstituentAuthModesConflict, "invalid constituent syntax: cannot combine OIDC CREDENTIAL FORWARDING with USER/PASSWORD", nil)
}

func CypherCompositeConstituentUserPasswordRequired() Message {
	return cypherCompositeMessage(MessageCypherCompositeConstituentUserPasswordRequired, "invalid constituent syntax: USER and PASSWORD must both be provided", nil)
}

func CypherCompositeConstituentRemoteRequired() Message {
	return cypherCompositeMessage(MessageCypherCompositeConstituentRemoteRequired, "invalid constituent syntax: USER/PASSWORD and OIDC CREDENTIAL FORWARDING require a remote constituent (AT '<url>' or TYPE remote)", nil)
}

func CypherCompositeDatabaseManagerUnavailable(command string) Message {
	return cypherCompositeMessage(MessageCypherCompositeDatabaseManagerUnavailable, "database manager not available - "+command+" requires multi-database support", map[string]any{"Command": command})
}

func CypherCompositeCreateInvalidSyntax() Message {
	return cypherCompositeMessage(MessageCypherCompositeCreateInvalidSyntax, "invalid CREATE COMPOSITE DATABASE syntax", nil)
}

func CypherCompositeCreateDatabaseNameExpected() Message {
	return cypherCompositeMessage(MessageCypherCompositeCreateDatabaseNameExpected, "invalid CREATE COMPOSITE DATABASE syntax: database name expected", nil)
}

func CypherCompositeCreateDatabaseNameEmpty() Message {
	return cypherCompositeMessage(MessageCypherCompositeCreateDatabaseNameEmpty, "invalid CREATE COMPOSITE DATABASE syntax: database name cannot be empty", nil)
}

func CypherCompositeCreateTokenizeFailed(cause error) Message {
	return cypherCompositeCauseMessage(MessageCypherCompositeCreateTokenizeFailed, "invalid CREATE COMPOSITE DATABASE syntax: ", cause)
}

func CypherCompositeCreateConstituentRequired() Message {
	return cypherCompositeMessage(MessageCypherCompositeCreateConstituentRequired, "invalid CREATE COMPOSITE DATABASE syntax: at least one constituent required", nil)
}

func CypherCompositeCreateDatabaseFailed(database string, cause error) Message {
	return cypherCompositeMessage(MessageCypherCompositeCreateDatabaseFailed, "failed to create composite database '"+database+"': "+cause.Error(), map[string]any{"Database": database, "Cause": cause.Error()})
}

func CypherCompositeDropInvalidSyntax() Message {
	return cypherCompositeMessage(MessageCypherCompositeDropInvalidSyntax, "invalid DROP COMPOSITE DATABASE syntax", nil)
}

func CypherCompositeDropDatabaseNameExpected() Message {
	return cypherCompositeMessage(MessageCypherCompositeDropDatabaseNameExpected, "invalid DROP COMPOSITE DATABASE syntax: database name expected", nil)
}

func CypherCompositeDropDatabaseNameEmpty() Message {
	return cypherCompositeMessage(MessageCypherCompositeDropDatabaseNameEmpty, "invalid DROP COMPOSITE DATABASE syntax: database name cannot be empty", nil)
}

func CypherCompositeDropDatabaseFailed(database string, cause error) Message {
	return cypherCompositeMessage(MessageCypherCompositeDropDatabaseFailed, "failed to drop composite database '"+database+"': "+cause.Error(), map[string]any{"Database": database, "Cause": cause.Error()})
}

func CypherCompositeShowConstituentsInvalidSyntax() Message {
	return cypherCompositeMessage(MessageCypherCompositeShowConstituentsInvalidSyntax, "invalid SHOW CONSTITUENTS syntax", nil)
}

func CypherCompositeShowConstituentsNameExpected() Message {
	return cypherCompositeMessage(MessageCypherCompositeShowConstituentsNameExpected, "invalid SHOW CONSTITUENTS syntax: FOR COMPOSITE DATABASE name expected", nil)
}

func CypherCompositeGetConstituentsFailed(cause error) Message {
	return cypherCompositeCauseMessage(MessageCypherCompositeGetConstituentsFailed, "failed to get constituents: ", cause)
}

func CypherCompositeAlterInvalidSyntax() Message {
	return cypherCompositeMessage(MessageCypherCompositeAlterInvalidSyntax, "invalid ALTER COMPOSITE DATABASE syntax", nil)
}

func CypherCompositeAlterDatabaseKeywordExpected() Message {
	return cypherCompositeMessage(MessageCypherCompositeAlterDatabaseKeywordExpected, "invalid ALTER COMPOSITE DATABASE syntax: DATABASE expected after COMPOSITE", nil)
}

func CypherCompositeAlterDatabaseNameExpected() Message {
	return cypherCompositeMessage(MessageCypherCompositeAlterDatabaseNameExpected, "invalid ALTER COMPOSITE DATABASE syntax: database name expected", nil)
}

func CypherCompositeAlterDatabaseNameEmpty() Message {
	return cypherCompositeMessage(MessageCypherCompositeAlterDatabaseNameEmpty, "invalid ALTER COMPOSITE DATABASE syntax: database name cannot be empty", nil)
}

func CypherCompositeAlterTokenizeFailed(cause error) Message {
	return cypherCompositeCauseMessage(MessageCypherCompositeAlterTokenizeFailed, "invalid ALTER COMPOSITE DATABASE syntax: ", cause)
}

func CypherCompositeAlterAddAliasExpected() Message {
	return cypherCompositeMessage(MessageCypherCompositeAlterAddAliasExpected, "invalid ALTER COMPOSITE DATABASE syntax: ADD ALIAS expected", nil)
}

func CypherCompositeAddAliasUnexpectedToken(token string) Message {
	return cypherCompositeMessage(MessageCypherCompositeAddAliasUnexpectedToken, "invalid ADD ALIAS syntax: unexpected token '"+token+"'", map[string]any{"Token": token})
}

func CypherCompositeAddConstituentFailed(database string, cause error) Message {
	return cypherCompositeMessage(MessageCypherCompositeAddConstituentFailed, "failed to add constituent to composite database '"+database+"': "+cause.Error(), map[string]any{"Database": database, "Cause": cause.Error()})
}

func CypherCompositeAlterDropAliasExpected() Message {
	return cypherCompositeMessage(MessageCypherCompositeAlterDropAliasExpected, "invalid ALTER COMPOSITE DATABASE syntax: DROP ALIAS expected", nil)
}

func CypherCompositeDropAliasNameEmpty() Message {
	return cypherCompositeMessage(MessageCypherCompositeDropAliasNameEmpty, "invalid DROP ALIAS syntax: alias name cannot be empty", nil)
}

func CypherCompositeRemoveConstituentFailed(database string, cause error) Message {
	return cypherCompositeMessage(MessageCypherCompositeRemoveConstituentFailed, "failed to remove constituent from composite database '"+database+"': "+cause.Error(), map[string]any{"Database": database, "Cause": cause.Error()})
}

func CypherCompositeAlterActionExpected() Message {
	return cypherCompositeMessage(MessageCypherCompositeAlterActionExpected, "invalid ALTER COMPOSITE DATABASE syntax: ADD ALIAS or DROP ALIAS expected", nil)
}
