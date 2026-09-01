package localization

import (
	"strconv"
)

const (
	MessageMultidbConstituentAliasRequired     MessageID = "multidb.constituent_alias_required"
	MessageMultidbConstituentDatabaseRequired  MessageID = "multidb.constituent_database_required"
	MessageMultidbConstituentTypeInvalid       MessageID = "multidb.constituent_type_invalid"
	MessageMultidbAccessModeInvalid            MessageID = "multidb.access_mode_invalid"
	MessageMultidbRemoteURIRequired            MessageID = "multidb.remote_uri_required"
	MessageMultidbRemoteAuthModeInvalid        MessageID = "multidb.remote_auth_mode_invalid"
	MessageMultidbRemoteUserRequired           MessageID = "multidb.remote_user_required"
	MessageMultidbRemotePasswordRequired       MessageID = "multidb.remote_password_required"
	MessageMultidbOIDCCredentialsForbidden     MessageID = "multidb.oidc_credentials_forbidden"
	MessageMultidbMaxNodesReached              MessageID = "multidb.max_nodes_reached"
	MessageMultidbMaxEdgesReached              MessageID = "multidb.max_edges_reached"
	MessageMultidbMaxBytesExceeded             MessageID = "multidb.max_bytes_exceeded"
	MessageMultidbMaxConcurrentQueries         MessageID = "multidb.max_concurrent_queries_reached"
	MessageMultidbQueryRateExceeded            MessageID = "multidb.query_rate_exceeded"
	MessageMultidbWriteRateExceeded            MessageID = "multidb.write_rate_exceeded"
	MessageMultidbMaxConnections               MessageID = "multidb.max_connections_reached"
	MessageMultidbCredentialFormatInvalid      MessageID = "multidb.credential_format_invalid"
	MessageMultidbCredentialPayloadTruncated   MessageID = "multidb.credential_payload_truncated"
	MessageMultidbRemotePasswordEmpty          MessageID = "multidb.remote_password_empty"
	MessageMultidbCredentialKeyConfigRequired  MessageID = "multidb.credential_key_config_required"
	MessageMultidbStoredPasswordMissing        MessageID = "multidb.stored_password_missing"
	MessageMultidbCredentialDecryptKeyRequired MessageID = "multidb.credential_decrypt_key_required"
	MessageMultidbInvalidConstituent           MessageID = "multidb.invalid_constituent"
)

func MultidbConstituentAliasRequired() Message {
	return Message{ID: MessageMultidbConstituentAliasRequired, Fallback: "constituent alias cannot be empty"}
}
func MultidbConstituentDatabaseRequired() Message {
	return Message{ID: MessageMultidbConstituentDatabaseRequired, Fallback: "constituent database name cannot be empty"}
}
func MultidbConstituentTypeInvalid() Message {
	return Message{ID: MessageMultidbConstituentTypeInvalid, Fallback: "constituent type must be 'local' or 'remote'"}
}
func MultidbAccessModeInvalid() Message {
	return Message{ID: MessageMultidbAccessModeInvalid, Fallback: "access mode must be 'read', 'write', or 'read_write'"}
}
func MultidbRemoteURIRequired() Message {
	return Message{ID: MessageMultidbRemoteURIRequired, Fallback: "remote constituent URI cannot be empty"}
}
func MultidbRemoteAuthModeInvalid() Message {
	return Message{ID: MessageMultidbRemoteAuthModeInvalid, Fallback: "remote auth mode must be 'oidc_forwarding' or 'user_password'"}
}
func MultidbRemoteUserRequired() Message {
	return Message{ID: MessageMultidbRemoteUserRequired, Fallback: "remote constituent user cannot be empty when auth mode is user_password"}
}
func MultidbRemotePasswordRequired() Message {
	return Message{ID: MessageMultidbRemotePasswordRequired, Fallback: "remote constituent password cannot be empty when auth mode is user_password"}
}
func MultidbOIDCCredentialsForbidden() Message {
	return Message{ID: MessageMultidbOIDCCredentialsForbidden, Fallback: "remote constituent user/password cannot be set when auth mode is oidc_forwarding"}
}

func MultidbMaxNodesReached(name string, current, limit int64) Message {
	return Message{ID: MessageMultidbMaxNodesReached, Fallback: "storage limit exceeded: database '" + name + "' has reached max_nodes limit (" + strconv.FormatInt(current, 10) + "/" + strconv.FormatInt(limit, 10) + ")", Data: map[string]any{"Name": name, "Current": current, "Limit": limit}}
}
func MultidbMaxEdgesReached(name string, current, limit int64) Message {
	return Message{ID: MessageMultidbMaxEdgesReached, Fallback: "storage limit exceeded: database '" + name + "' has reached max_edges limit (" + strconv.FormatInt(current, 10) + "/" + strconv.FormatInt(limit, 10) + ")", Data: map[string]any{"Name": name, "Current": current, "Limit": limit}}
}
func MultidbMaxBytesExceeded(name string, current, limit, entitySize int64) Message {
	return Message{ID: MessageMultidbMaxBytesExceeded, Fallback: "storage limit exceeded: database '" + name + "' would exceed max_bytes limit (current: " + strconv.FormatInt(current, 10) + " bytes, limit: " + strconv.FormatInt(limit, 10) + " bytes, new entity: " + strconv.FormatInt(entitySize, 10) + " bytes)", Data: map[string]any{"Name": name, "Current": current, "Limit": limit, "EntitySize": entitySize}}
}
func MultidbMaxConcurrentQueriesReached(name string, current, limit int) Message {
	return Message{ID: MessageMultidbMaxConcurrentQueries, Fallback: "query limit exceeded: database '" + name + "' has reached max_concurrent_queries limit (" + strconv.Itoa(current) + "/" + strconv.Itoa(limit) + ")", Data: map[string]any{"Name": name, "Current": current, "Limit": limit}}
}
func MultidbQueryRateExceeded(name string, limit int) Message {
	return Message{ID: MessageMultidbQueryRateExceeded, Fallback: "rate limit exceeded: database '" + name + "' exceeded max_queries_per_second (" + strconv.Itoa(limit) + ")", Data: map[string]any{"Name": name, "Limit": limit}}
}
func MultidbWriteRateExceeded(name string, limit int) Message {
	return Message{ID: MessageMultidbWriteRateExceeded, Fallback: "rate limit exceeded: database '" + name + "' exceeded max_writes_per_second (" + strconv.Itoa(limit) + ")", Data: map[string]any{"Name": name, "Limit": limit}}
}
func MultidbMaxConnectionsReached(name string, current, limit int) Message {
	return Message{ID: MessageMultidbMaxConnections, Fallback: "connection limit exceeded: database '" + name + "' has reached max_connections limit (" + strconv.Itoa(current) + "/" + strconv.Itoa(limit) + ")", Data: map[string]any{"Name": name, "Current": current, "Limit": limit}}
}

func MultidbCredentialFormatInvalid() Message {
	return Message{ID: MessageMultidbCredentialFormatInvalid, Fallback: "remote credential is not encrypted with expected format"}
}
func MultidbCredentialPayloadTruncated() Message {
	return Message{ID: MessageMultidbCredentialPayloadTruncated, Fallback: "remote credential payload is truncated"}
}
func MultidbRemotePasswordEmpty() Message {
	return Message{ID: MessageMultidbRemotePasswordEmpty, Fallback: "remote constituent password cannot be empty"}
}
func MultidbCredentialKeyConfigurationRequired() Message {
	return Message{ID: MessageMultidbCredentialKeyConfigRequired, Fallback: "remote user/password auth requires remote credential encryption key configuration"}
}
func MultidbStoredPasswordMissing() Message {
	return Message{ID: MessageMultidbStoredPasswordMissing, Fallback: "remote constituent password is missing"}
}
func MultidbCredentialDecryptKeyRequired() Message {
	return Message{ID: MessageMultidbCredentialDecryptKeyRequired, Fallback: "remote credential encryption key is required to decrypt stored remote credentials"}
}

func MultidbInvalidConstituent(index int, cause error) Message {
	return Message{ID: MessageMultidbInvalidConstituent, Fallback: "invalid constituent at index " + strconv.Itoa(index) + ": " + cause.Error(), Data: map[string]any{"Index": index, "Cause": cause.Error()}}
}
