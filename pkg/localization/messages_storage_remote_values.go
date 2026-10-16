package localization

// Remote transport validation messages preserve stable catalog identities.
const (
	MessageStorageRemoteRESTInvalidJSON           MessageID = "storage.remote.rest_invalid_json"
	MessageStorageRemotePathEntitySequenceMissing MessageID = "storage.remote.path_entity_sequence_missing"
	MessageStorageRemotePathNonEntity             MessageID = "storage.remote.path_non_entity"
	MessageStorageRemotePathEntitiesInconsistent  MessageID = "storage.remote.path_entities_inconsistent"
	MessageStorageRemoteRESTMapKeysMismatch       MessageID = "storage.remote.rest_map_keys_mismatch"
	MessageStorageRemoteRESTListLengthMismatch    MessageID = "storage.remote.rest_list_length_mismatch"
	MessageStorageRemoteRESTRowLengthMismatch     MessageID = "storage.remote.rest_row_length_mismatch"
)

// StorageRemoteRESTInvalidJSON reports malformed remote graph result metadata.
// For example, use this message when an HTTP peer supplies truncated JSON in its rest field.
func StorageRemoteRESTInvalidJSON() Message {
	return storageValidationMessage(MessageStorageRemoteRESTInvalidJSON, "invalid remote HTTP REST JSON", nil)
}

// StorageRemotePathEntitySequenceMissing reports malformed remote graph result metadata.
// For example, use this message when path metadata accompanies an object instead of an entity sequence.
func StorageRemotePathEntitySequenceMissing() Message {
	return storageValidationMessage(MessageStorageRemotePathEntitySequenceMissing, "remote path has no entity sequence", nil)
}

// StorageRemotePathNonEntity reports malformed remote graph result metadata.
// For example, use this message when a path sequence contains a scalar between its entities.
func StorageRemotePathNonEntity() Message {
	return storageValidationMessage(MessageStorageRemotePathNonEntity, "remote path contains a non-entity", nil)
}

// StorageRemotePathEntitiesInconsistent reports malformed remote graph result metadata.
// For example, use this message when a relationship does not join its adjacent path nodes.
func StorageRemotePathEntitiesInconsistent() Message {
	return storageValidationMessage(MessageStorageRemotePathEntitiesInconsistent, "remote HTTP path has inconsistent entities", nil)
}

// StorageRemoteRESTMapKeysMismatch reports malformed remote graph result metadata.
// For example, use this message when a nested REST map omits a key present in the row map.
func StorageRemoteRESTMapKeysMismatch() Message {
	return storageValidationMessage(MessageStorageRemoteRESTMapKeysMismatch, "remote HTTP row/REST map keys differ", nil)
}

// StorageRemoteRESTListLengthMismatch reports malformed remote graph result metadata.
// For example, use this message when a two-item row list has only one REST item.
func StorageRemoteRESTListLengthMismatch() Message {
	return storageValidationMessage(MessageStorageRemoteRESTListLengthMismatch, "remote HTTP row/REST list lengths differ", nil)
}

// StorageRemoteRESTRowLengthMismatch reports malformed remote graph result metadata.
// For example, use this message when a two-column row has only one REST column.
func StorageRemoteRESTRowLengthMismatch() Message {
	return storageValidationMessage(MessageStorageRemoteRESTRowLengthMismatch, "remote HTTP row/REST lengths differ", nil)
}
