package localization

import "fmt"

const (
	MessageStorageClientAsyncFlushIncompleteDetailed       MessageID = "storage.client.async.flush_incomplete_detailed"
	MessageStorageClientAsyncFlushIncomplete               MessageID = "storage.client.async.flush_incomplete"
	MessageStorageClientAsyncCloseFailed                   MessageID = "storage.client.async.close_failed"
	MessageStorageClientAsyncCloseEngineFailed             MessageID = "storage.client.async.close_engine_failed"
	MessageStorageClientNodeIDNamespaceUnprefixed          MessageID = "storage.client.node.id_namespace_unprefixed"
	MessageStorageClientNodeIDNamespaceRequired            MessageID = "storage.client.node.id_namespace_required"
	MessageStorageClientNodeEncodeFailed                   MessageID = "storage.client.node.encode_failed"
	MessageStorageClientNodeWriteFailed                    MessageID = "storage.client.node.write_failed"
	MessageStorageClientNodeLabelIndexKeyBuildFailed       MessageID = "storage.client.node.label_index_key_build_failed"
	MessageStorageClientNodeLabelIndexWriteFailed          MessageID = "storage.client.node.label_index_write_failed"
	MessageStorageClientNodeIndexCatalogWriteFailed        MessageID = "storage.client.node.index_catalog_write_failed"
	MessageStorageClientNodePendingEmbedIndexWriteFailed   MessageID = "storage.client.node.pending_embed_index_write_failed"
	MessageStorageClientNodeEmbeddingChunkDeleteFailed     MessageID = "storage.client.node.embedding_chunk_delete_failed"
	MessageStorageClientNodeEmbeddingChunksDeleteFailed    MessageID = "storage.client.node.embedding_chunks_delete_failed"
	MessageStorageClientNodeEmbeddingChunkStoreFailed      MessageID = "storage.client.node.embedding_chunk_store_failed"
	MessageStorageClientNodeEmbeddingPayloadBudgetExceeded MessageID = "storage.client.node.embedding_payload_budget_exceeded"
	MessageStorageClientEdgeEncodeFailed                   MessageID = "storage.client.edge.encode_failed"
	MessageStorageClientReceiptTransactionIDRequired       MessageID = "storage.client.receipt.transaction_id_required"
	MessageStorageClientReceiptWALSequenceRequired         MessageID = "storage.client.receipt.wal_sequence_required"
	MessageStorageClientReceiptWALRangeInvalid             MessageID = "storage.client.receipt.wal_range_invalid"
	MessageStorageClientReceiptNilReceiver                 MessageID = "storage.client.receipt.nil_receiver"
	MessageStorageClientReceiptHashMarshalFailed           MessageID = "storage.client.receipt.hash_marshal_failed"
	MessageStorageClientStorageClosed                      MessageID = "storage.client.storage_closed"
	MessageStorageClientBackupFileCreateFailed             MessageID = "storage.client.backup.file_create_failed"
	MessageStorageClientBackupFailed                       MessageID = "storage.client.backup.failed"
	MessageStorageClientBackupFlushFailed                  MessageID = "storage.client.backup.flush_failed"
	MessageStorageClientBackupSyncFailed                   MessageID = "storage.client.backup.sync_failed"
	MessageStorageClientDeletePrefixRequired               MessageID = "storage.client.delete_prefix.required"
	MessageStorageClientDropPrefixFailed                   MessageID = "storage.client.delete_prefix.drop_failed"
	MessageStorageClientCleanLabelIndexFailed              MessageID = "storage.client.delete_prefix.clean_label_index_failed"
	MessageStorageClientCleanEdgeTypeIndexFailed           MessageID = "storage.client.delete_prefix.clean_edge_type_index_failed"
)

func storageClientMessage(id MessageID, fallback string, data map[string]any) Message {
	return Message{ID: id, Fallback: fallback, Data: data}
}

func StorageClientAsyncFlushIncompleteDetailed(nodesFailed, edgesFailed, deletesFailed int, details string) Message {
	return storageClientMessage(MessageStorageClientAsyncFlushIncompleteDetailed, fmt.Sprintf("flush incomplete: %d nodes failed, %d edges failed, %d deletes failed (%s)", nodesFailed, edgesFailed, deletesFailed, details), map[string]any{"NodesFailed": nodesFailed, "EdgesFailed": edgesFailed, "DeletesFailed": deletesFailed, "Details": details})
}

func StorageClientAsyncFlushIncomplete(nodesFailed, edgesFailed, deletesFailed int) Message {
	return storageClientMessage(MessageStorageClientAsyncFlushIncomplete, fmt.Sprintf("flush incomplete: %d nodes failed, %d edges failed, %d deletes failed", nodesFailed, edgesFailed, deletesFailed), map[string]any{"NodesFailed": nodesFailed, "EdgesFailed": edgesFailed, "DeletesFailed": deletesFailed})
}

func storageClientAsyncCloseData(nodesFailed, edgesFailed, deletesFailed, pendingNodes, pendingEdges, pendingNodeDeletes, pendingEdgeDeletes int) map[string]any {
	return map[string]any{
		"HasFlushErrors":     nodesFailed+edgesFailed+deletesFailed > 0,
		"HasUnflushed":       pendingNodes+pendingEdges+pendingNodeDeletes+pendingEdgeDeletes > 0,
		"NodesFailed":        nodesFailed,
		"EdgesFailed":        edgesFailed,
		"DeletesFailed":      deletesFailed,
		"PendingNodes":       pendingNodes,
		"PendingEdges":       pendingEdges,
		"PendingNodeDeletes": pendingNodeDeletes,
		"PendingEdgeDeletes": pendingEdgeDeletes,
	}
}

func storageClientAsyncCloseDetail(data map[string]any) string {
	detail := ""
	if data["HasFlushErrors"].(bool) {
		detail = fmt.Sprintf("flush errors: %d nodes failed, %d edges failed, %d deletes failed", data["NodesFailed"], data["EdgesFailed"], data["DeletesFailed"])
	}
	if data["HasUnflushed"].(bool) {
		if detail != "" {
			detail += "; "
		}
		detail += fmt.Sprintf("unflushed: %d nodes, %d edges, %d node deletes, %d edge deletes (POTENTIAL DATA LOSS)", data["PendingNodes"], data["PendingEdges"], data["PendingNodeDeletes"], data["PendingEdgeDeletes"])
	}
	return detail
}

func StorageClientAsyncCloseFailed(nodesFailed, edgesFailed, deletesFailed, pendingNodes, pendingEdges, pendingNodeDeletes, pendingEdgeDeletes int) Message {
	data := storageClientAsyncCloseData(nodesFailed, edgesFailed, deletesFailed, pendingNodes, pendingEdges, pendingNodeDeletes, pendingEdgeDeletes)
	return storageClientMessage(MessageStorageClientAsyncCloseFailed, "async engine close: "+storageClientAsyncCloseDetail(data), data)
}

func StorageClientAsyncCloseEngineFailed(nodesFailed, edgesFailed, deletesFailed, pendingNodes, pendingEdges, pendingNodeDeletes, pendingEdgeDeletes int, cause error) Message {
	data := storageClientAsyncCloseData(nodesFailed, edgesFailed, deletesFailed, pendingNodes, pendingEdges, pendingNodeDeletes, pendingEdgeDeletes)
	data["Cause"] = storageErrorText(cause)
	return storageClientMessage(MessageStorageClientAsyncCloseEngineFailed, storageClientAsyncCloseDetail(data)+"; engine close: "+storageErrorText(cause), data)
}

func StorageClientNodeIDNamespaceUnprefixed(nodeID string) Message {
	return storageClientMessage(MessageStorageClientNodeIDNamespaceUnprefixed, fmt.Sprintf("node ID must be prefixed with namespace (e.g., 'nornic:node-123'), got unprefixed ID: %s", nodeID), map[string]any{"NodeID": nodeID})
}

func StorageClientNodeIDNamespaceRequired(nodeID string) Message {
	return storageClientMessage(MessageStorageClientNodeIDNamespaceRequired, fmt.Sprintf("node ID must be prefixed with namespace (e.g., 'nornic:node-123'), got: %s", nodeID), map[string]any{"NodeID": nodeID})
}

func StorageClientNodeEncodeFailed(cause error) Message {
	return storageClientMessage(MessageStorageClientNodeEncodeFailed, "failed to encode node: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}

func StorageClientNodeWriteFailed(cause error) Message {
	return storageClientMessage(MessageStorageClientNodeWriteFailed, "failed to write node: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}

func StorageClientNodeLabelIndexKeyBuildFailed(cause error) Message {
	return storageClientMessage(MessageStorageClientNodeLabelIndexKeyBuildFailed, "failed to build label index key: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}

func StorageClientNodeLabelIndexWriteFailed(cause error) Message {
	return storageClientMessage(MessageStorageClientNodeLabelIndexWriteFailed, "failed to write label index: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}

func StorageClientNodeIndexCatalogWriteFailed(cause error) Message {
	return storageClientMessage(MessageStorageClientNodeIndexCatalogWriteFailed, "failed to write index catalog: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}

func StorageClientNodePendingEmbedIndexWriteFailed(cause error) Message {
	return storageClientMessage(MessageStorageClientNodePendingEmbedIndexWriteFailed, "failed to write pending embed index: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}

func StorageClientNodeEmbeddingChunkDeleteFailed(cause error) Message {
	return storageClientMessage(MessageStorageClientNodeEmbeddingChunkDeleteFailed, "failed to delete old embedding chunk: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}

func StorageClientNodeEmbeddingChunksDeleteFailed(cause error) Message {
	return storageClientMessage(MessageStorageClientNodeEmbeddingChunksDeleteFailed, "failed to delete old embedding chunks: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}

func StorageClientNodeEmbeddingChunkStoreFailed(chunkIndex int, cause error) Message {
	return storageClientMessage(MessageStorageClientNodeEmbeddingChunkStoreFailed, fmt.Sprintf("failed to store embedding chunk %d: %s", chunkIndex, storageErrorText(cause)), map[string]any{"ChunkIndex": chunkIndex, "Cause": storageErrorText(cause)})
}

func StorageClientNodeEmbeddingPayloadBudgetExceeded(chunkIndex int) Message {
	return storageClientMessage(MessageStorageClientNodeEmbeddingPayloadBudgetExceeded, fmt.Sprintf("failed to store embedding payload for chunk %d: entry exceeds per-txn write budget", chunkIndex), map[string]any{"ChunkIndex": chunkIndex})
}

func StorageClientEdgeEncodeFailed(cause error) Message {
	return storageClientMessage(MessageStorageClientEdgeEncodeFailed, "failed to encode edge: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}

func StorageClientReceiptTransactionIDRequired() Message {
	return storageClientMessage(MessageStorageClientReceiptTransactionIDRequired, "receipt: tx_id is required", nil)
}

func StorageClientReceiptWALSequenceRequired() Message {
	return storageClientMessage(MessageStorageClientReceiptWALSequenceRequired, "receipt: wal sequence must be non-zero", nil)
}

func StorageClientReceiptWALRangeInvalid(walSeqEnd, walSeqStart uint64) Message {
	return storageClientMessage(MessageStorageClientReceiptWALRangeInvalid, fmt.Sprintf("receipt: wal_seq_end (%d) < wal_seq_start (%d)", walSeqEnd, walSeqStart), map[string]any{"WALSeqEnd": walSeqEnd, "WALSeqStart": walSeqStart})
}

func StorageClientReceiptNilReceiver() Message {
	return storageClientMessage(MessageStorageClientReceiptNilReceiver, "receipt: nil receiver", nil)
}

func StorageClientReceiptHashMarshalFailed(cause error) Message {
	return storageClientMessage(MessageStorageClientReceiptHashMarshalFailed, "receipt: hash marshal failed: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}

func StorageClientStorageClosed() Message {
	return storageClientMessage(MessageStorageClientStorageClosed, "storage closed", nil)
}

func StorageClientBackupFileCreateFailed(path string, cause error) Message {
	return storageClientMessage(MessageStorageClientBackupFileCreateFailed, "failed to create backup file: "+storageErrorText(cause), map[string]any{"Path": path, "Cause": storageErrorText(cause)})
}

func StorageClientBackupFailed(cause error) Message {
	return storageClientMessage(MessageStorageClientBackupFailed, "backup failed: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}

func StorageClientBackupFlushFailed(cause error) Message {
	return storageClientMessage(MessageStorageClientBackupFlushFailed, "failed to flush backup: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}

func StorageClientBackupSyncFailed(cause error) Message {
	return storageClientMessage(MessageStorageClientBackupSyncFailed, "failed to sync backup: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}

func StorageClientDeletePrefixRequired() Message {
	return storageClientMessage(MessageStorageClientDeletePrefixRequired, "prefix cannot be empty", nil)
}

func StorageClientDropPrefixFailed(prefix byte, cause error) Message {
	return storageClientMessage(MessageStorageClientDropPrefixFailed, fmt.Sprintf("failed to drop prefix %x: %s", prefix, storageErrorText(cause)), map[string]any{"Prefix": prefix, "Cause": storageErrorText(cause)})
}

func StorageClientCleanLabelIndexFailed(cause error) Message {
	return storageClientMessage(MessageStorageClientCleanLabelIndexFailed, "failed to clean label index: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}

func StorageClientCleanEdgeTypeIndexFailed(cause error) Message {
	return storageClientMessage(MessageStorageClientCleanEdgeTypeIndexFailed, "failed to clean edge type index: "+storageErrorText(cause), map[string]any{"Cause": storageErrorText(cause)})
}
