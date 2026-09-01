package localization

import "fmt"

const (
	MessageMultidbCompositeConstituentDatabaseNotFound MessageID = "multidb.composite.constituent_database_not_found"
	MessageMultidbCompositeDatabaseAsConstituent       MessageID = "multidb.composite.database_as_constituent"
	MessageMultidbCompositeDuplicateAlias              MessageID = "multidb.composite.duplicate_alias"
	MessageMultidbCompositeSecureCredentialsFailed     MessageID = "multidb.composite.secure_remote_credentials_failed"
	MessageMultidbCompositeDatabaseNotComposite        MessageID = "multidb.composite.database_not_composite"
	MessageMultidbCompositePersistAfterDropFailed      MessageID = "multidb.composite.persist_metadata_after_drop_failed"
	MessageMultidbCompositeAliasExists                 MessageID = "multidb.composite.alias_exists"
	MessageMultidbCompositeAliasNotFound               MessageID = "multidb.composite.alias_not_found"
	MessageMultidbManagerDeleteDatabaseDataFailed      MessageID = "multidb.manager.delete_database_data_failed"
	MessageMultidbManagerResolveRemoteCredentials      MessageID = "multidb.manager.resolve_remote_credentials_failed"
	MessageMultidbManagerGetRemoteStorageFailed        MessageID = "multidb.manager.get_remote_storage_failed"
	MessageMultidbManagerGetConstituentStorageFailed   MessageID = "multidb.manager.get_constituent_storage_failed"
	MessageMultidbManagerRemoteFactoryNotConfigured    MessageID = "multidb.manager.remote_engine_factory_not_configured"
	MessageMultidbManagerRemoteFactoryReturnedNil      MessageID = "multidb.manager.remote_engine_factory_returned_nil"
	MessageMultidbManagerInvalidStatus                 MessageID = "multidb.manager.invalid_status"
	MessageMultidbManagerAliasContainsWhitespace       MessageID = "multidb.manager.alias_contains_whitespace"
	MessageMultidbManagerAliasReserved                 MessageID = "multidb.manager.alias_reserved"
	MessageMultidbMetadataParseFailed                  MessageID = "multidb.metadata.parse_failed"
	MessageMultidbMetadataSerializeFailed              MessageID = "multidb.metadata.serialize_failed"
	MessageMultidbStorageGetNodeCountFailed            MessageID = "multidb.storage.get_node_count_failed"
	MessageMultidbStorageGetEdgeCountFailed            MessageID = "multidb.storage.get_edge_count_failed"
	MessageMultidbStorageGetSizeFailed                 MessageID = "multidb.storage.get_size_failed"
	MessageMultidbStorageCalculateNodeSizeFailed       MessageID = "multidb.storage.calculate_node_size_failed"
	MessageMultidbStorageCalculateEdgeSizeFailed       MessageID = "multidb.storage.calculate_edge_size_failed"
	MessageMultidbStorageGetAllNodesFailed             MessageID = "multidb.storage.get_all_nodes_failed"
	MessageMultidbStorageGetAllEdgesFailed             MessageID = "multidb.storage.get_all_edges_failed"
	MessageMultidbStorageEncodeNodeFailed              MessageID = "multidb.storage.encode_node_failed"
	MessageMultidbStorageEncodeEdgeFailed              MessageID = "multidb.storage.encode_edge_failed"
	MessageMultidbStorageGetAllNodesForSizeFailed      MessageID = "multidb.storage.get_all_nodes_for_size_calculation_failed"
	MessageMultidbStorageGetAllEdgesForSizeFailed      MessageID = "multidb.storage.get_all_edges_for_size_calculation_failed"
	MessageMultidbStorageGetOutgoingEdgesFailed        MessageID = "multidb.storage.get_outgoing_edges_failed"
	MessageMultidbStorageGetIncomingEdgesFailed        MessageID = "multidb.storage.get_incoming_edges_failed"
)

func multidbOperationCause(id MessageID, prefix string, cause error) Message {
	return Message{ID: id, Fallback: prefix + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

func MultidbCompositeConstituentDatabaseNotFound(database string, cause error) Message {
	return Message{ID: MessageMultidbCompositeConstituentDatabaseNotFound, Fallback: fmt.Sprintf("constituent database '%s' not found: %s", database, cause), Data: map[string]any{"Database": database, "Cause": cause.Error()}}
}

func MultidbCompositeDatabaseAsConstituent(database string) Message {
	return Message{ID: MessageMultidbCompositeDatabaseAsConstituent, Fallback: fmt.Sprintf("cannot use composite database '%s' as constituent", database), Data: map[string]any{"Database": database}}
}

func MultidbCompositeDuplicateAlias(alias string) Message {
	return Message{ID: MessageMultidbCompositeDuplicateAlias, Fallback: fmt.Sprintf("duplicate constituent alias: '%s'", alias), Data: map[string]any{"Alias": alias}}
}

func MultidbCompositeSecureRemoteCredentialsFailed(alias string, cause error) Message {
	return Message{ID: MessageMultidbCompositeSecureCredentialsFailed, Fallback: fmt.Sprintf("failed to secure remote credentials for alias '%s': %s", alias, cause), Data: map[string]any{"Alias": alias, "Cause": cause.Error()}}
}

func MultidbCompositeDatabaseNotComposite(database string) Message {
	return Message{ID: MessageMultidbCompositeDatabaseNotComposite, Fallback: fmt.Sprintf("database '%s' is not a composite database", database), Data: map[string]any{"Database": database}}
}

func MultidbCompositePersistMetadataAfterDropFailed(cause error) Message {
	return multidbOperationCause(MessageMultidbCompositePersistAfterDropFailed, "failed to persist metadata after drop: ", cause)
}

func MultidbCompositeAliasExists(alias string) Message {
	return Message{ID: MessageMultidbCompositeAliasExists, Fallback: fmt.Sprintf("constituent alias '%s' already exists", alias), Data: map[string]any{"Alias": alias}}
}

func MultidbCompositeAliasNotFound(alias string) Message {
	return Message{ID: MessageMultidbCompositeAliasNotFound, Fallback: fmt.Sprintf("constituent alias '%s' not found", alias), Data: map[string]any{"Alias": alias}}
}

func MultidbManagerDeleteDatabaseDataFailed(cause error) Message {
	return multidbOperationCause(MessageMultidbManagerDeleteDatabaseDataFailed, "failed to delete database data: ", cause)
}

func MultidbManagerResolveRemoteCredentialsFailed(alias string, cause error) Message {
	return Message{ID: MessageMultidbManagerResolveRemoteCredentials, Fallback: fmt.Sprintf("failed to resolve remote credentials for constituent '%s': %s", alias, cause), Data: map[string]any{"Alias": alias, "Cause": cause.Error()}}
}

func MultidbManagerGetRemoteStorageFailed(alias string, cause error) Message {
	return Message{ID: MessageMultidbManagerGetRemoteStorageFailed, Fallback: fmt.Sprintf("failed to get remote storage for constituent '%s': %s", alias, cause), Data: map[string]any{"Alias": alias, "Cause": cause.Error()}}
}

func MultidbManagerGetConstituentStorageFailed(database string, cause error) Message {
	return Message{ID: MessageMultidbManagerGetConstituentStorageFailed, Fallback: fmt.Sprintf("failed to get storage for constituent '%s': %s", database, cause), Data: map[string]any{"Database": database, "Cause": cause.Error()}}
}

func MultidbManagerRemoteEngineFactoryNotConfigured(alias string) Message {
	return Message{ID: MessageMultidbManagerRemoteFactoryNotConfigured, Fallback: fmt.Sprintf("remote constituent '%s' cannot be opened: remote engine factory is not configured", alias), Data: map[string]any{"Alias": alias}}
}

func MultidbManagerRemoteEngineFactoryReturnedNil(alias string) Message {
	return Message{ID: MessageMultidbManagerRemoteFactoryReturnedNil, Fallback: fmt.Sprintf("remote engine factory returned nil for constituent '%s'", alias), Data: map[string]any{"Alias": alias}}
}

func MultidbManagerInvalidStatus(status string) Message {
	return Message{ID: MessageMultidbManagerInvalidStatus, Fallback: fmt.Sprintf("invalid status: %s (must be 'online' or 'offline')", status), Data: map[string]any{"Status": status}}
}

func MultidbManagerAliasContainsWhitespace(alias string, cause error) Message {
	return Message{ID: MessageMultidbManagerAliasContainsWhitespace, Fallback: fmt.Sprintf("%s: '%s' (cannot contain whitespace)", cause, alias), Data: map[string]any{"Alias": alias}}
}

func MultidbManagerAliasReserved(alias string, cause error) Message {
	return Message{ID: MessageMultidbManagerAliasReserved, Fallback: fmt.Sprintf("%s: '%s' (reserved name)", cause, alias), Data: map[string]any{"Alias": alias}}
}

func MultidbMetadataParseFailed(cause error) Message {
	return multidbOperationCause(MessageMultidbMetadataParseFailed, "failed to parse database metadata: ", cause)
}

func MultidbMetadataSerializeFailed(cause error) Message {
	return multidbOperationCause(MessageMultidbMetadataSerializeFailed, "failed to serialize database metadata: ", cause)
}

func MultidbStorageGetNodeCountFailed(cause error) Message {
	return multidbOperationCause(MessageMultidbStorageGetNodeCountFailed, "failed to get node count: ", cause)
}

func MultidbStorageGetEdgeCountFailed(cause error) Message {
	return multidbOperationCause(MessageMultidbStorageGetEdgeCountFailed, "failed to get edge count: ", cause)
}

func MultidbStorageGetSizeFailed(cause error) Message {
	return multidbOperationCause(MessageMultidbStorageGetSizeFailed, "failed to get storage size: ", cause)
}

func MultidbStorageCalculateNodeSizeFailed(cause error) Message {
	return multidbOperationCause(MessageMultidbStorageCalculateNodeSizeFailed, "failed to calculate node size: ", cause)
}

func MultidbStorageCalculateEdgeSizeFailed(cause error) Message {
	return multidbOperationCause(MessageMultidbStorageCalculateEdgeSizeFailed, "failed to calculate edge size: ", cause)
}

func MultidbStorageGetAllNodesFailed(cause error) Message {
	return multidbOperationCause(MessageMultidbStorageGetAllNodesFailed, "failed to get all nodes: ", cause)
}

func MultidbStorageGetAllEdgesFailed(cause error) Message {
	return multidbOperationCause(MessageMultidbStorageGetAllEdgesFailed, "failed to get all edges: ", cause)
}

func MultidbStorageEncodeNodeFailed(cause error) Message {
	return multidbOperationCause(MessageMultidbStorageEncodeNodeFailed, "failed to encode node: ", cause)
}

func MultidbStorageEncodeEdgeFailed(cause error) Message {
	return multidbOperationCause(MessageMultidbStorageEncodeEdgeFailed, "failed to encode edge: ", cause)
}

func MultidbStorageGetAllNodesForSizeCalculationFailed(cause error) Message {
	return multidbOperationCause(MessageMultidbStorageGetAllNodesForSizeFailed, "failed to get all nodes for size calculation: ", cause)
}

func MultidbStorageGetAllEdgesForSizeCalculationFailed(cause error) Message {
	return multidbOperationCause(MessageMultidbStorageGetAllEdgesForSizeFailed, "failed to get all edges for size calculation: ", cause)
}

func MultidbStorageGetOutgoingEdgesFailed(cause error) Message {
	return multidbOperationCause(MessageMultidbStorageGetOutgoingEdgesFailed, "get outgoing edges: ", cause)
}

func MultidbStorageGetIncomingEdgesFailed(cause error) Message {
	return multidbOperationCause(MessageMultidbStorageGetIncomingEdgesFailed, "get incoming edges: ", cause)
}
