package localization

import "fmt"

const (
	MessageNornicDBCoreNotFound                              MessageID = "nornicdbcore.not_found"
	MessageNornicDBCoreInvalidID                             MessageID = "nornicdbcore.invalid_id"
	MessageNornicDBCoreDatabaseClosed                        MessageID = "nornicdbcore.database_closed"
	MessageNornicDBCoreInvalidInput                          MessageID = "nornicdbcore.invalid_input"
	MessageNornicDBCoreQueryEmbeddingDimensionMismatch       MessageID = "nornicdbcore.query_embedding_dimension_mismatch"
	MessageNornicDBCoreCypherRowDecodeFailed                 MessageID = "nornicdbcore.cypher_row_decode_failed"
	MessageNornicDBCoreDecodeDestinationRequired             MessageID = "nornicdbcore.decode_destination_required"
	MessageNornicDBCoreDecodeFieldFailed                     MessageID = "nornicdbcore.decode_field_failed"
	MessageNornicDBCoreDecodeAssignmentFailed                MessageID = "nornicdbcore.decode_assignment_failed"
	MessageNornicDBCoreNodeIDRequired                        MessageID = "nornicdbcore.node_id_required"
	MessageNornicDBCoreNodeAlreadyExists                     MessageID = "nornicdbcore.node_already_exists"
	MessageNornicDBCoreSourceNodeNotFound                    MessageID = "nornicdbcore.source_node_not_found"
	MessageNornicDBCoreTargetNodeNotFound                    MessageID = "nornicdbcore.target_node_not_found"
	MessageNornicDBCoreSearchServiceNotInitialized           MessageID = "nornicdbcore.search_service_not_initialized"
	MessageNornicDBCoreFindSimilarLimitInvalid               MessageID = "nornicdbcore.find_similar_limit_invalid"
	MessageNornicDBCoreNodeEmbeddingMissing                  MessageID = "nornicdbcore.node_embedding_missing"
	MessageNornicDBCoreSchemaManagerNotInitialized           MessageID = "nornicdbcore.schema_manager_not_initialized"
	MessageNornicDBCoreIndexTypeUnsupported                  MessageID = "nornicdbcore.index_type_unsupported"
	MessageNornicDBCoreBackupNodesReadFailed                 MessageID = "nornicdbcore.backup_nodes_read_failed"
	MessageNornicDBCoreBackupEdgesReadFailed                 MessageID = "nornicdbcore.backup_edges_read_failed"
	MessageNornicDBCoreBackupMarshalFailed                   MessageID = "nornicdbcore.backup_marshal_failed"
	MessageNornicDBCoreBackupWriteFailed                     MessageID = "nornicdbcore.backup_write_failed"
	MessageNornicDBCoreBackupReadFailed                      MessageID = "nornicdbcore.backup_read_failed"
	MessageNornicDBCoreBackupParseFailed                     MessageID = "nornicdbcore.backup_parse_failed"
	MessageNornicDBCoreRestoreNodeFailed                     MessageID = "nornicdbcore.restore_node_failed"
	MessageNornicDBCoreRestoreEdgeFailed                     MessageID = "nornicdbcore.restore_edge_failed"
	MessageNornicDBCoreRestoreTemporalIndexesFailed          MessageID = "nornicdbcore.restore_temporal_indexes_failed"
	MessageNornicDBCoreRestoreMVCCHeadsFailed                MessageID = "nornicdbcore.restore_mvcc_heads_failed"
	MessageNornicDBCoreEncryptionPasswordRequired            MessageID = "nornicdbcore.encryption_password_required"
	MessageNornicDBCoreEncryptionSaltGenerateFailed          MessageID = "nornicdbcore.encryption_salt_generate_failed"
	MessageNornicDBCoreDataDirectoryCreateFailed             MessageID = "nornicdbcore.data_directory_create_failed"
	MessageNornicDBCoreEncryptionSaltSaveFailed              MessageID = "nornicdbcore.encryption_salt_save_failed"
	MessageNornicDBCorePersistentStorageOpenFailed           MessageID = "nornicdbcore.persistent_storage_open_failed"
	MessageNornicDBCoreWALInitializeFailed                   MessageID = "nornicdbcore.wal_initialize_failed"
	MessageNornicDBCoreInferenceInitializeFailed             MessageID = "nornicdbcore.inference_initialize_failed"
	MessageNornicDBCoreReplicationStorageAdapterCreateFailed MessageID = "nornicdbcore.replication_storage_adapter_create_failed"
	MessageNornicDBCoreReplicationReplicatorCreateFailed     MessageID = "nornicdbcore.replication_replicator_create_failed"
	MessageNornicDBCoreReplicationStartFailed                MessageID = "nornicdbcore.replication_start_failed"
	MessageNornicDBCoreNilDatabase                           MessageID = "nornicdbcore.nil_database"
	MessageNornicDBCoreStorageEngineNotInitialized           MessageID = "nornicdbcore.storage_engine_not_initialized"
	MessageNornicDBCoreStorageProbeFailed                    MessageID = "nornicdbcore.storage_probe_failed"
	MessageNornicDBCoreCloseFailed                           MessageID = "nornicdbcore.close_failed"
	MessageNornicDBCoreAutoEmbedNotEnabled                   MessageID = "nornicdbcore.auto_embed_not_enabled"
	MessageNornicDBCoreClearEmbeddingsUnsupported            MessageID = "nornicdbcore.clear_embeddings_unsupported"
	MessageNornicDBCoreQueryDimensionMismatchForDatabase     MessageID = "nornicdbcore.query_embedding_dimension_mismatch_for_database"
	MessageNornicDBCoreConsentUserIDRequired                 MessageID = "nornicdbcore.consent_user_id_required"
	MessageNornicDBCoreConsentPurposeRequired                MessageID = "nornicdbcore.consent_purpose_required"
	MessageNornicDBCoreConsentExistingCheckFailed            MessageID = "nornicdbcore.consent_existing_check_failed"
	MessageNornicDBCoreConsentCheckFailed                    MessageID = "nornicdbcore.consent_check_failed"
	MessageNornicDBCoreConsentGetFailed                      MessageID = "nornicdbcore.consent_get_failed"
	MessageNornicDBCoreConsentStreamFailed                   MessageID = "nornicdbcore.consent_stream_failed"
	MessageNornicDBCoreSearchPersistenceDatabaseInvalid      MessageID = "nornicdbcore.search_persistence_database_invalid"
	MessageNornicDBCoreSearchSystemDatabaseUnsupported       MessageID = "nornicdbcore.search_system_database_unsupported"
	MessageNornicDBCoreSearchBaseStorageUnavailable          MessageID = "nornicdbcore.search_base_storage_unavailable"
	MessageNornicDBCoreSearchDatabaseNotInitialized          MessageID = "nornicdbcore.search_database_not_initialized"
	MessageNornicDBCoreInferenceBaseStorageUnavailable       MessageID = "nornicdbcore.inference_base_storage_unavailable"
	MessageNornicDBCoreEmbedBatchFailed                      MessageID = "nornicdbcore.embed_batch_failed"
	MessageNornicDBCoreEmbeddingCountMismatch                MessageID = "nornicdbcore.embedding_count_mismatch"
	MessageNornicDBCoreBootstrapKnowledgePolicyFailed        MessageID = "nornicdbcore.bootstrap_knowledge_policy_failed"
	MessageNornicDBCoreBootstrapDDLExpected                  MessageID = "nornicdbcore.bootstrap_ddl_expected"
	MessageNornicDBCoreBootstrapDDLCommandUnsupported        MessageID = "nornicdbcore.bootstrap_ddl_command_unsupported"
	MessageNornicDBCoreEncryptionMasterKeyInvalid            MessageID = "nornicdbcore.encryption_master_key_invalid"
	MessageNornicDBCoreEncryptionProviderInitializeFailed    MessageID = "nornicdbcore.encryption_provider_initialize_failed"
	MessageNornicDBCoreEncryptionDEKMetadataDecodeFailed     MessageID = "nornicdbcore.encryption_dek_metadata_decode_failed"
	MessageNornicDBCoreEncryptionDEKProviderMismatch         MessageID = "nornicdbcore.encryption_dek_provider_mismatch"
	MessageNornicDBCoreEncryptionDEKCiphertextDecodeFailed   MessageID = "nornicdbcore.encryption_dek_ciphertext_decode_failed"
	MessageNornicDBCoreEncryptionDEKUnwrapFailed             MessageID = "nornicdbcore.encryption_dek_unwrap_failed"
	MessageNornicDBCoreEncryptionDEKRotateFailed             MessageID = "nornicdbcore.encryption_dek_rotate_failed"
	MessageNornicDBCoreEncryptionDEKGenerateFailed           MessageID = "nornicdbcore.encryption_dek_generate_failed"
	MessageNornicDBCoreEncryptionDEKEmpty                    MessageID = "nornicdbcore.encryption_dek_empty"
	MessageNornicDBCoreEncryptionDEKDirectoryCreateFailed    MessageID = "nornicdbcore.encryption_dek_directory_create_failed"
	MessageNornicDBCoreEncryptionDEKMetadataEncodeFailed     MessageID = "nornicdbcore.encryption_dek_metadata_encode_failed"
	MessageNornicDBCoreEncryptionDEKMetadataPersistFailed    MessageID = "nornicdbcore.encryption_dek_metadata_persist_failed"
	MessageNornicDBCoreEncryptionMasterKeyRequired           MessageID = "nornicdbcore.encryption_master_key_required"
	MessageNornicDBCoreEncryptionMasterKeyLengthInvalid      MessageID = "nornicdbcore.encryption_master_key_length_invalid"
)

func nornicDBCoreCause(id MessageID, fallback string, cause error) Message {
	return Message{ID: id, Fallback: fallback, Data: map[string]any{"Cause": cause.Error()}}
}

// NornicDBCoreNotFound identifies a missing database entity.
func NornicDBCoreNotFound() Message {
	return Message{ID: MessageNornicDBCoreNotFound, Fallback: "not found"}
}

// NornicDBCoreInvalidID identifies an invalid database entity ID.
func NornicDBCoreInvalidID() Message {
	return Message{ID: MessageNornicDBCoreInvalidID, Fallback: "invalid ID"}
}

// NornicDBCoreDatabaseClosed identifies an operation on a closed database.
func NornicDBCoreDatabaseClosed() Message {
	return Message{ID: MessageNornicDBCoreDatabaseClosed, Fallback: "database is closed"}
}

// NornicDBCoreInvalidInput identifies invalid embedded API input.
func NornicDBCoreInvalidInput() Message {
	return Message{ID: MessageNornicDBCoreInvalidInput, Fallback: "invalid input"}
}

// NornicDBCoreQueryEmbeddingDimensionMismatch identifies incompatible query and index dimensions.
func NornicDBCoreQueryEmbeddingDimensionMismatch() Message {
	return Message{ID: MessageNornicDBCoreQueryEmbeddingDimensionMismatch, Fallback: "query embedding dimension mismatch"}
}

// NornicDBCoreCypherRowDecodeFailed identifies typed Cypher row decoding failure.
func NornicDBCoreCypherRowDecodeFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreCypherRowDecodeFailed, "failed to decode row: "+cause.Error(), cause)
}

// NornicDBCoreDecodeDestinationRequired identifies an invalid typed decode destination.
func NornicDBCoreDecodeDestinationRequired() Message {
	return Message{ID: MessageNornicDBCoreDecodeDestinationRequired, Fallback: "dest must be a non-nil pointer"}
}

// NornicDBCoreDecodeFieldFailed identifies a field-specific typed decode failure.
func NornicDBCoreDecodeFieldFailed(field string, cause error) Message {
	return Message{ID: MessageNornicDBCoreDecodeFieldFailed, Fallback: fmt.Sprintf("field %s: %s", field, cause), Data: map[string]any{"Field": field, "Cause": cause.Error()}}
}

// NornicDBCoreDecodeAssignmentFailed identifies an incompatible typed field assignment.
func NornicDBCoreDecodeAssignmentFailed(value any, target any) Message {
	return Message{ID: MessageNornicDBCoreDecodeAssignmentFailed, Fallback: fmt.Sprintf("cannot assign %T to %v", value, target), Data: map[string]any{"ValueType": fmt.Sprintf("%T", value), "TargetType": fmt.Sprint(target)}}
}

// NornicDBCoreNodeIDRequired identifies an empty caller-provided node ID.
func NornicDBCoreNodeIDRequired() Message {
	return Message{ID: MessageNornicDBCoreNodeIDRequired, Fallback: "node ID must not be empty"}
}

// NornicDBCoreNodeAlreadyExists identifies a duplicate caller-provided node ID.
func NornicDBCoreNodeAlreadyExists(nodeID string) Message {
	return Message{ID: MessageNornicDBCoreNodeAlreadyExists, Fallback: fmt.Sprintf("node %q already exists", nodeID), Data: map[string]any{"NodeID": nodeID}}
}

// NornicDBCoreSourceNodeNotFound identifies a missing relationship source node.
func NornicDBCoreSourceNodeNotFound() Message {
	return Message{ID: MessageNornicDBCoreSourceNodeNotFound, Fallback: "source node not found"}
}

// NornicDBCoreTargetNodeNotFound identifies a missing relationship target node.
func NornicDBCoreTargetNodeNotFound() Message {
	return Message{ID: MessageNornicDBCoreTargetNodeNotFound, Fallback: "target node not found"}
}

// NornicDBCoreSearchServiceNotInitialized identifies an unavailable default search service.
func NornicDBCoreSearchServiceNotInitialized() Message {
	return Message{ID: MessageNornicDBCoreSearchServiceNotInitialized, Fallback: "search service not initialized"}
}

// NornicDBCoreFindSimilarLimitInvalid identifies a non-positive similarity result limit.
func NornicDBCoreFindSimilarLimitInvalid() Message {
	return Message{ID: MessageNornicDBCoreFindSimilarLimitInvalid, Fallback: "limit must be greater than 0"}
}

// NornicDBCoreNodeEmbeddingMissing identifies a similarity source node without an embedding.
func NornicDBCoreNodeEmbeddingMissing() Message {
	return Message{ID: MessageNornicDBCoreNodeEmbeddingMissing, Fallback: "node has no embedding"}
}

// NornicDBCoreSchemaManagerNotInitialized identifies unavailable schema management.
func NornicDBCoreSchemaManagerNotInitialized() Message {
	return Message{ID: MessageNornicDBCoreSchemaManagerNotInitialized, Fallback: "schema manager not initialized"}
}

// NornicDBCoreIndexTypeUnsupported identifies an unsupported embedded API index type.
func NornicDBCoreIndexTypeUnsupported(indexType string) Message {
	return Message{ID: MessageNornicDBCoreIndexTypeUnsupported, Fallback: fmt.Sprintf("unsupported index type: %s (use: property, fulltext, vector, range)", indexType), Data: map[string]any{"IndexType": indexType}}
}

// NornicDBCoreBackupNodesReadFailed identifies a backup node-read failure.
func NornicDBCoreBackupNodesReadFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreBackupNodesReadFailed, "failed to get nodes: "+cause.Error(), cause)
}

// NornicDBCoreBackupEdgesReadFailed identifies a backup edge-read failure.
func NornicDBCoreBackupEdgesReadFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreBackupEdgesReadFailed, "failed to get edges: "+cause.Error(), cause)
}

// NornicDBCoreBackupMarshalFailed identifies backup serialization failure.
func NornicDBCoreBackupMarshalFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreBackupMarshalFailed, "failed to marshal backup: "+cause.Error(), cause)
}

// NornicDBCoreBackupWriteFailed identifies backup persistence failure.
func NornicDBCoreBackupWriteFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreBackupWriteFailed, "failed to write backup: "+cause.Error(), cause)
}

// NornicDBCoreBackupReadFailed identifies backup file read failure.
func NornicDBCoreBackupReadFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreBackupReadFailed, "failed to read backup: "+cause.Error(), cause)
}

// NornicDBCoreBackupParseFailed identifies backup deserialization failure.
func NornicDBCoreBackupParseFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreBackupParseFailed, "failed to parse backup: "+cause.Error(), cause)
}

// NornicDBCoreRestoreNodeFailed identifies a node restore failure.
func NornicDBCoreRestoreNodeFailed(nodeID string, cause error) Message {
	return Message{ID: MessageNornicDBCoreRestoreNodeFailed, Fallback: fmt.Sprintf("failed to restore node %s: %s", nodeID, cause), Data: map[string]any{"NodeID": nodeID, "Cause": cause.Error()}}
}

// NornicDBCoreRestoreEdgeFailed identifies an edge restore failure.
func NornicDBCoreRestoreEdgeFailed(edgeID string, cause error) Message {
	return Message{ID: MessageNornicDBCoreRestoreEdgeFailed, Fallback: fmt.Sprintf("failed to restore edge %s: %s", edgeID, cause), Data: map[string]any{"EdgeID": edgeID, "Cause": cause.Error()}}
}

// NornicDBCoreRestoreTemporalIndexesFailed identifies temporal-index rebuild failure after restore.
func NornicDBCoreRestoreTemporalIndexesFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreRestoreTemporalIndexesFailed, "failed to rebuild temporal indexes after restore: "+cause.Error(), cause)
}

// NornicDBCoreRestoreMVCCHeadsFailed identifies MVCC-head rebuild failure after restore.
func NornicDBCoreRestoreMVCCHeadsFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreRestoreMVCCHeadsFailed, "failed to rebuild mvcc heads after restore: "+cause.Error(), cause)
}

// NornicDBCoreEncryptionPasswordRequired identifies missing password-based encryption configuration.
func NornicDBCoreEncryptionPasswordRequired() Message {
	return Message{ID: MessageNornicDBCoreEncryptionPasswordRequired, Fallback: "encryption is enabled but no password was provided"}
}

// NornicDBCoreEncryptionSaltGenerateFailed identifies encryption salt generation failure.
func NornicDBCoreEncryptionSaltGenerateFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreEncryptionSaltGenerateFailed, "failed to generate encryption salt: "+cause.Error(), cause)
}

// NornicDBCoreDataDirectoryCreateFailed identifies database directory creation failure.
func NornicDBCoreDataDirectoryCreateFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreDataDirectoryCreateFailed, "failed to create data directory: "+cause.Error(), cause)
}

// NornicDBCoreEncryptionSaltSaveFailed identifies encryption salt persistence failure.
func NornicDBCoreEncryptionSaltSaveFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreEncryptionSaltSaveFailed, "failed to save encryption salt: "+cause.Error(), cause)
}

// NornicDBCorePersistentStorageOpenFailed identifies persistent storage open failure.
func NornicDBCorePersistentStorageOpenFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCorePersistentStorageOpenFailed, "failed to open persistent storage: "+cause.Error(), cause)
}

// NornicDBCoreWALInitializeFailed identifies write-ahead log initialization failure.
func NornicDBCoreWALInitializeFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreWALInitializeFailed, "failed to initialize WAL: "+cause.Error(), cause)
}

// NornicDBCoreInferenceInitializeFailed identifies inference initialization failure.
func NornicDBCoreInferenceInitializeFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreInferenceInitializeFailed, "init inference: "+cause.Error(), cause)
}

// NornicDBCoreReplicationStorageAdapterCreateFailed identifies replication adapter creation failure.
func NornicDBCoreReplicationStorageAdapterCreateFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreReplicationStorageAdapterCreateFailed, "replication: create storage adapter: "+cause.Error(), cause)
}

// NornicDBCoreReplicationReplicatorCreateFailed identifies replicator creation failure.
func NornicDBCoreReplicationReplicatorCreateFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreReplicationReplicatorCreateFailed, "replication: create replicator: "+cause.Error(), cause)
}

// NornicDBCoreReplicationStartFailed identifies replication startup failure.
func NornicDBCoreReplicationStartFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreReplicationStartFailed, "replication: start: "+cause.Error(), cause)
}

// NornicDBCoreNilDatabase identifies a nil health-check receiver.
func NornicDBCoreNilDatabase() Message {
	return Message{ID: MessageNornicDBCoreNilDatabase, Fallback: "nornicdb: nil DB"}
}

// NornicDBCoreStorageEngineNotInitialized identifies an unavailable health-check storage engine.
func NornicDBCoreStorageEngineNotInitialized() Message {
	return Message{ID: MessageNornicDBCoreStorageEngineNotInitialized, Fallback: "nornicdb: storage engine not initialized"}
}

// NornicDBCoreStorageProbeFailed identifies health-check storage probe failure.
func NornicDBCoreStorageProbeFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreStorageProbeFailed, "nornicdb: storage probe: "+cause.Error(), cause)
}

// NornicDBCoreCloseFailed identifies one or more database shutdown failures.
func NornicDBCoreCloseFailed(details string) Message {
	return Message{ID: MessageNornicDBCoreCloseFailed, Fallback: "close errors: " + details, Data: map[string]any{"Errors": details}}
}

// NornicDBCoreAutoEmbedNotEnabled identifies an unavailable automatic embedding worker.
func NornicDBCoreAutoEmbedNotEnabled() Message {
	return Message{ID: MessageNornicDBCoreAutoEmbedNotEnabled, Fallback: "auto-embed not enabled"}
}

// NornicDBCoreClearEmbeddingsUnsupported identifies an unsupported storage operation.
func NornicDBCoreClearEmbeddingsUnsupported() Message {
	return Message{ID: MessageNornicDBCoreClearEmbeddingsUnsupported, Fallback: "storage engine does not support ClearAllEmbeddings"}
}

// NornicDBCoreQueryDimensionMismatchForDatabase identifies a database-specific dimension mismatch.
func NornicDBCoreQueryDimensionMismatchForDatabase(database string, indexDimensions, queryDimensions int) Message {
	return Message{ID: MessageNornicDBCoreQueryDimensionMismatchForDatabase, Fallback: fmt.Sprintf("database %q: query embedding dimension mismatch (index dims %d, query dims %d)", database, indexDimensions, queryDimensions), Data: map[string]any{"Database": database, "IndexDimensions": indexDimensions, "QueryDimensions": queryDimensions}}
}

// NornicDBCoreConsentUserIDRequired identifies missing consent subject input.
func NornicDBCoreConsentUserIDRequired() Message {
	return Message{ID: MessageNornicDBCoreConsentUserIDRequired, Fallback: "user_id is required"}
}

// NornicDBCoreConsentPurposeRequired identifies missing consent purpose input.
func NornicDBCoreConsentPurposeRequired() Message {
	return Message{ID: MessageNornicDBCoreConsentPurposeRequired, Fallback: "purpose is required"}
}

// NornicDBCoreConsentExistingCheckFailed identifies failure checking an existing consent record.
func NornicDBCoreConsentExistingCheckFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreConsentExistingCheckFailed, "checking existing consent: "+cause.Error(), cause)
}

// NornicDBCoreConsentCheckFailed identifies consent lookup failure.
func NornicDBCoreConsentCheckFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreConsentCheckFailed, "checking consent: "+cause.Error(), cause)
}

// NornicDBCoreConsentGetFailed identifies consent retrieval failure.
func NornicDBCoreConsentGetFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreConsentGetFailed, "getting consent: "+cause.Error(), cause)
}

// NornicDBCoreConsentStreamFailed identifies consent stream failure.
func NornicDBCoreConsentStreamFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreConsentStreamFailed, "streaming consent nodes: "+cause.Error(), cause)
}

// NornicDBCoreSearchPersistenceDatabaseInvalid identifies an unsafe search-persistence database name.
func NornicDBCoreSearchPersistenceDatabaseInvalid(database string) Message {
	return Message{ID: MessageNornicDBCoreSearchPersistenceDatabaseInvalid, Fallback: fmt.Sprintf("invalid database name for search persistence: %q", database), Data: map[string]any{"Database": database}}
}

// NornicDBCoreSearchSystemDatabaseUnsupported identifies search requested for the system database.
func NornicDBCoreSearchSystemDatabaseUnsupported() Message {
	return Message{ID: MessageNornicDBCoreSearchSystemDatabaseUnsupported, Fallback: "search service not available for system database"}
}

// NornicDBCoreSearchBaseStorageUnavailable identifies search initialization without base storage.
func NornicDBCoreSearchBaseStorageUnavailable() Message {
	return Message{ID: MessageNornicDBCoreSearchBaseStorageUnavailable, Fallback: "search service unavailable: base storage is nil"}
}

// NornicDBCoreSearchDatabaseNotInitialized identifies missing per-database search state.
func NornicDBCoreSearchDatabaseNotInitialized(database string) Message {
	return Message{ID: MessageNornicDBCoreSearchDatabaseNotInitialized, Fallback: fmt.Sprintf("search service not initialized for database %q", database), Data: map[string]any{"Database": database}}
}

// NornicDBCoreInferenceBaseStorageUnavailable identifies inference initialization without base storage.
func NornicDBCoreInferenceBaseStorageUnavailable() Message {
	return Message{ID: MessageNornicDBCoreInferenceBaseStorageUnavailable, Fallback: "inference unavailable: base storage is nil"}
}

// NornicDBCoreEmbedBatchFailed identifies a failed embedding micro-batch.
func NornicDBCoreEmbedBatchFailed(start, end, total int, nodeID string, cause error) Message {
	return Message{ID: MessageNornicDBCoreEmbedBatchFailed, Fallback: fmt.Sprintf("batch %d-%d/%d failed for %s: %s", start, end, total, nodeID, cause), Data: map[string]any{"Start": start, "End": end, "Total": total, "NodeID": nodeID, "Cause": cause.Error()}}
}

// NornicDBCoreEmbeddingCountMismatch identifies an embedding provider batch-size mismatch.
func NornicDBCoreEmbeddingCountMismatch(nodeID string, actual, expected int) Message {
	return Message{ID: MessageNornicDBCoreEmbeddingCountMismatch, Fallback: fmt.Sprintf("embedding count mismatch for %s: got %d, expected %d", nodeID, actual, expected), Data: map[string]any{"NodeID": nodeID, "Actual": actual, "Expected": expected}}
}

// NornicDBCoreBootstrapKnowledgePolicyFailed identifies default policy bootstrap failure.
func NornicDBCoreBootstrapKnowledgePolicyFailed(namespace string, cause error) Message {
	return Message{ID: MessageNornicDBCoreBootstrapKnowledgePolicyFailed, Fallback: fmt.Sprintf("bootstrap default knowledge policy for namespace %q: %s", namespace, cause), Data: map[string]any{"Namespace": namespace, "Cause": cause.Error()}}
}

// NornicDBCoreBootstrapDDLExpected identifies a non-policy statement in the bootstrap set.
func NornicDBCoreBootstrapDDLExpected() Message {
	return Message{ID: MessageNornicDBCoreBootstrapDDLExpected, Fallback: "not a knowledge-policy DDL statement"}
}

// NornicDBCoreBootstrapDDLCommandUnsupported identifies an unsupported parsed bootstrap command.
func NornicDBCoreBootstrapDDLCommandUnsupported(command any) Message {
	commandType := fmt.Sprintf("%T", command)
	return Message{ID: MessageNornicDBCoreBootstrapDDLCommandUnsupported, Fallback: "unsupported bootstrap DDL command " + commandType, Data: map[string]any{"CommandType": commandType}}
}

// NornicDBCoreEncryptionMasterKeyInvalid identifies invalid provider master-key input.
func NornicDBCoreEncryptionMasterKeyInvalid(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreEncryptionMasterKeyInvalid, "invalid encryption master key: "+cause.Error(), cause)
}

// NornicDBCoreEncryptionProviderInitializeFailed identifies KMS provider initialization failure.
func NornicDBCoreEncryptionProviderInitializeFailed(provider string, cause error) Message {
	return Message{ID: MessageNornicDBCoreEncryptionProviderInitializeFailed, Fallback: fmt.Sprintf("failed to initialize encryption provider %q: %s", provider, cause), Data: map[string]any{"Provider": provider, "Cause": cause.Error()}}
}

// NornicDBCoreEncryptionDEKMetadataDecodeFailed identifies persisted DEK metadata decoding failure.
func NornicDBCoreEncryptionDEKMetadataDecodeFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreEncryptionDEKMetadataDecodeFailed, "failed to decode persisted DEK metadata: "+cause.Error(), cause)
}

// NornicDBCoreEncryptionDEKProviderMismatch identifies incompatible persisted and configured providers.
func NornicDBCoreEncryptionDEKProviderMismatch(persistedProvider, configuredProvider string) Message {
	return Message{ID: MessageNornicDBCoreEncryptionDEKProviderMismatch, Fallback: fmt.Sprintf("persisted DEK was created with provider %q, not %q", persistedProvider, configuredProvider), Data: map[string]any{"PersistedProvider": persistedProvider, "ConfiguredProvider": configuredProvider}}
}

// NornicDBCoreEncryptionDEKCiphertextDecodeFailed identifies persisted DEK ciphertext decoding failure.
func NornicDBCoreEncryptionDEKCiphertextDecodeFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreEncryptionDEKCiphertextDecodeFailed, "failed to decode persisted DEK ciphertext: "+cause.Error(), cause)
}

// NornicDBCoreEncryptionDEKUnwrapFailed identifies persisted DEK decryption failure.
func NornicDBCoreEncryptionDEKUnwrapFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreEncryptionDEKUnwrapFailed, "failed to unwrap persisted DEK: "+cause.Error(), cause)
}

// NornicDBCoreEncryptionDEKRotateFailed identifies wrapped DEK rotation failure.
func NornicDBCoreEncryptionDEKRotateFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreEncryptionDEKRotateFailed, "failed to rotate persisted wrapped DEK: "+cause.Error(), cause)
}

// NornicDBCoreEncryptionDEKGenerateFailed identifies provider-backed DEK generation failure.
func NornicDBCoreEncryptionDEKGenerateFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreEncryptionDEKGenerateFailed, "failed to generate provider-backed DEK: "+cause.Error(), cause)
}

// NornicDBCoreEncryptionDEKEmpty identifies a provider returning no plaintext DEK.
func NornicDBCoreEncryptionDEKEmpty() Message {
	return Message{ID: MessageNornicDBCoreEncryptionDEKEmpty, Fallback: "provider returned empty plaintext DEK"}
}

// NornicDBCoreEncryptionDEKDirectoryCreateFailed identifies DEK metadata directory creation failure.
func NornicDBCoreEncryptionDEKDirectoryCreateFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreEncryptionDEKDirectoryCreateFailed, "failed to create data directory for DEK metadata: "+cause.Error(), cause)
}

// NornicDBCoreEncryptionDEKMetadataEncodeFailed identifies DEK metadata encoding failure.
func NornicDBCoreEncryptionDEKMetadataEncodeFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreEncryptionDEKMetadataEncodeFailed, "failed to encode DEK metadata: "+cause.Error(), cause)
}

// NornicDBCoreEncryptionDEKMetadataPersistFailed identifies DEK metadata persistence failure.
func NornicDBCoreEncryptionDEKMetadataPersistFailed(cause error) Message {
	return nornicDBCoreCause(MessageNornicDBCoreEncryptionDEKMetadataPersistFailed, "failed to persist DEK metadata: "+cause.Error(), cause)
}

// NornicDBCoreEncryptionMasterKeyRequired identifies missing provider-backed encryption key input.
func NornicDBCoreEncryptionMasterKeyRequired() Message {
	return Message{ID: MessageNornicDBCoreEncryptionMasterKeyRequired, Fallback: "master key is required for provider-backed encryption"}
}

// NornicDBCoreEncryptionMasterKeyLengthInvalid identifies malformed provider master-key input.
func NornicDBCoreEncryptionMasterKeyLengthInvalid() Message {
	return Message{ID: MessageNornicDBCoreEncryptionMasterKeyLengthInvalid, Fallback: "expected 32-byte key as base64, hex, or raw string"}
}
