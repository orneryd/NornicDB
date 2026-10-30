package localization

const (
	MessageStorageCompositeConstituentNotFound          MessageID = "storagecomposite.constituent_not_found"
	MessageStorageCompositeNoWritableConstituents       MessageID = "storagecomposite.no_writable_constituents"
	MessageStorageCompositeWriteTargetAmbiguous         MessageID = "storagecomposite.write_target_ambiguous"
	MessageStorageCompositeEdgeRequired                 MessageID = "storagecomposite.edge_required"
	MessageStorageCompositeReadOnlyStartNode            MessageID = "storagecomposite.read_only_start_node"
	MessageStorageCompositeStartNodeNotFound            MessageID = "storagecomposite.start_node_not_found"
	MessageStorageCompositeConstituentQueryFailed       MessageID = "storagecomposite.constituent_query_failed"
	MessageStorageCompositeBulkWriteTargetAmbiguous     MessageID = "storagecomposite.bulk_write_target_ambiguous"
	MessageStorageCompositeConstituentLookupFailed      MessageID = "storagecomposite.constituent_lookup_failed"
	MessageStorageCompositeNodeBulkCreateFailed         MessageID = "storagecomposite.node_bulk_create_failed"
	MessageStorageCompositeStartNodeCheckFailed         MessageID = "storagecomposite.start_node_check_failed"
	MessageStorageCompositeEdgeBulkCreateFailed         MessageID = "storagecomposite.edge_bulk_create_failed"
	MessageStorageCompositeDefaultLookupFailed          MessageID = "storagecomposite.default_constituent_lookup_failed"
	MessageStorageCompositeUnroutedEdgeBulkCreateFailed MessageID = "storagecomposite.unrouted_edge_bulk_create_failed"
	MessageStorageCompositeConstituentCloseFailed       MessageID = "storagecomposite.constituent_close_failed"
	MessageStorageCompositeNodeCountFailed              MessageID = "storagecomposite.node_count_failed"
	MessageStorageCompositeEdgeCountFailed              MessageID = "storagecomposite.edge_count_failed"
	MessageStorageCompositeDeleteByPrefixUnsupported    MessageID = "storagecomposite.delete_by_prefix_unsupported"
	MessageStorageCompositeConstituentStreamFailed      MessageID = "storagecomposite.constituent_stream_failed"
)

// StorageCompositeConstituentNotFound identifies an unknown constituent alias.
func StorageCompositeConstituentNotFound(alias string) Message {
	return Message{ID: MessageStorageCompositeConstituentNotFound, Fallback: "constituent '" + alias + "' not found", Data: map[string]any{"Alias": alias}}
}

// StorageCompositeNoWritableConstituents identifies a composite without a writable target.
func StorageCompositeNoWritableConstituents() Message {
	return Message{ID: MessageStorageCompositeNoWritableConstituents, Fallback: "no writable constituents available"}
}

// StorageCompositeWriteTargetAmbiguous identifies an unscoped composite write.
func StorageCompositeWriteTargetAmbiguous() Message {
	return Message{ID: MessageStorageCompositeWriteTargetAmbiguous, Fallback: "ambiguous composite write target: use USE <composite.constituent> or set properties.database_id to a writable constituent"}
}

// StorageCompositeEdgeRequired identifies a nil edge write.
func StorageCompositeEdgeRequired() Message {
	return Message{ID: MessageStorageCompositeEdgeRequired, Fallback: "edge cannot be nil"}
}

// StorageCompositeReadOnlyStartNode identifies an edge whose start node cannot be written.
func StorageCompositeReadOnlyStartNode(alias string) Message {
	return Message{ID: MessageStorageCompositeReadOnlyStartNode, Fallback: "start node found in read-only constituent '" + alias + "', cannot create edge", Data: map[string]any{"Alias": alias}}
}

// StorageCompositeStartNodeNotFound identifies an edge with no routable start node.
func StorageCompositeStartNodeNotFound() Message {
	return Message{ID: MessageStorageCompositeStartNodeNotFound, Fallback: "start node not found in any constituent"}
}

// StorageCompositeConstituentQueryFailed identifies a failed constituent query.
func StorageCompositeConstituentQueryFailed(alias string, cause error) Message {
	return Message{ID: MessageStorageCompositeConstituentQueryFailed, Fallback: "error querying constituent '" + alias + "': " + cause.Error(), Data: map[string]any{"Alias": alias, "Cause": cause.Error()}}
}

// StorageCompositeBulkWriteTargetAmbiguous identifies an unscoped bulk node write.
func StorageCompositeBulkWriteTargetAmbiguous() Message {
	return Message{ID: MessageStorageCompositeBulkWriteTargetAmbiguous, Fallback: "ambiguous composite write target in bulk create: use USE <composite.constituent> or set properties.database_id"}
}

// StorageCompositeConstituentLookupFailed identifies a constituent lookup failure during a bulk write.
func StorageCompositeConstituentLookupFailed(alias string, cause error) Message {
	return Message{ID: MessageStorageCompositeConstituentLookupFailed, Fallback: "failed to get constituent '" + alias + "': " + cause.Error(), Data: map[string]any{"Alias": alias, "Cause": cause.Error()}}
}

// StorageCompositeNodeBulkCreateFailed identifies a failed constituent node bulk write.
func StorageCompositeNodeBulkCreateFailed(alias string, cause error) Message {
	return Message{ID: MessageStorageCompositeNodeBulkCreateFailed, Fallback: "failed to create nodes in constituent '" + alias + "': " + cause.Error(), Data: map[string]any{"Alias": alias, "Cause": cause.Error()}}
}

// StorageCompositeStartNodeCheckFailed identifies a failed start-node routing lookup.
func StorageCompositeStartNodeCheckFailed(cause error) Message {
	return Message{ID: MessageStorageCompositeStartNodeCheckFailed, Fallback: "error checking start node: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// StorageCompositeEdgeBulkCreateFailed identifies a failed constituent edge bulk write.
func StorageCompositeEdgeBulkCreateFailed(alias string, cause error) Message {
	return Message{ID: MessageStorageCompositeEdgeBulkCreateFailed, Fallback: "failed to create edges in constituent '" + alias + "': " + cause.Error(), Data: map[string]any{"Alias": alias, "Cause": cause.Error()}}
}

// StorageCompositeDefaultConstituentLookupFailed identifies a failed default constituent lookup.
func StorageCompositeDefaultConstituentLookupFailed(cause error) Message {
	return Message{ID: MessageStorageCompositeDefaultLookupFailed, Fallback: "failed to get default constituent: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// StorageCompositeUnroutedEdgeBulkCreateFailed identifies a failed fallback edge bulk write.
func StorageCompositeUnroutedEdgeBulkCreateFailed(cause error) Message {
	return Message{ID: MessageStorageCompositeUnroutedEdgeBulkCreateFailed, Fallback: "failed to create unrouted edges: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// StorageCompositeConstituentCloseFailed identifies a failed constituent close.
func StorageCompositeConstituentCloseFailed(alias string, cause error) Message {
	return Message{ID: MessageStorageCompositeConstituentCloseFailed, Fallback: "error closing constituent '" + alias + "': " + cause.Error(), Data: map[string]any{"Alias": alias, "Cause": cause.Error()}}
}

// StorageCompositeNodeCountFailed identifies a failed constituent node count.
func StorageCompositeNodeCountFailed(alias string, cause error) Message {
	return Message{ID: MessageStorageCompositeNodeCountFailed, Fallback: "error counting nodes in constituent '" + alias + "': " + cause.Error(), Data: map[string]any{"Alias": alias, "Cause": cause.Error()}}
}

// StorageCompositeEdgeCountFailed identifies a failed constituent edge count.
func StorageCompositeEdgeCountFailed(alias string, cause error) Message {
	return Message{ID: MessageStorageCompositeEdgeCountFailed, Fallback: "error counting edges in constituent '" + alias + "': " + cause.Error(), Data: map[string]any{"Alias": alias, "Cause": cause.Error()}}
}

// StorageCompositeDeleteByPrefixUnsupported identifies an unsupported composite prefix deletion.
func StorageCompositeDeleteByPrefixUnsupported() Message {
	return Message{ID: MessageStorageCompositeDeleteByPrefixUnsupported, Fallback: "DeleteByPrefix not supported on composite databases - delete from constituent databases instead"}
}

// StorageCompositeConstituentStreamFailed identifies a failed constituent stream.
func StorageCompositeConstituentStreamFailed(alias string, cause error) Message {
	return Message{ID: MessageStorageCompositeConstituentStreamFailed, Fallback: "error streaming from constituent '" + alias + "': " + cause.Error(), Data: map[string]any{"Alias": alias, "Cause": cause.Error()}}
}
