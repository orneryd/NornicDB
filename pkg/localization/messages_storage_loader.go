package localization

import "fmt"

const (
	MessageStorageLoaderLoadingNodes            MessageID = "storage.loader.loading_nodes"
	MessageStorageLoaderLoadingRelationships    MessageID = "storage.loader.loading_relationships"
	MessageStorageLoaderOpeningFile             MessageID = "storage.loader.opening_file"
	MessageStorageLoaderDecodingJSON            MessageID = "storage.loader.decoding_json"
	MessageStorageLoaderCreatingNodes           MessageID = "storage.loader.creating_nodes"
	MessageStorageLoaderCreatingEdges           MessageID = "storage.loader.creating_edges"
	MessageStorageLoaderExportEngineUnsupported MessageID = "storage.loader.export_engine_unsupported"
	MessageStorageLoaderGettingAllNodes         MessageID = "storage.loader.getting_all_nodes"
	MessageStorageLoaderGettingAllEdges         MessageID = "storage.loader.getting_all_edges"
	MessageStorageLoaderCreatingFile            MessageID = "storage.loader.creating_file"
	MessageStorageLoaderEncodingJSON            MessageID = "storage.loader.encoding_json"
	MessageStorageLoaderParsingNodeJSON         MessageID = "storage.loader.parsing_node_json"
	MessageStorageLoaderConvertingNode          MessageID = "storage.loader.converting_node"
	MessageStorageLoaderScanningFile            MessageID = "storage.loader.scanning_file"
	MessageStorageLoaderParsingRelationshipJSON MessageID = "storage.loader.parsing_relationship_json"
	MessageStorageLoaderConvertingRelationship  MessageID = "storage.loader.converting_relationship"
	MessageStorageLoaderGettingNodes            MessageID = "storage.loader.getting_nodes"
	MessageStorageLoaderGettingEdges            MessageID = "storage.loader.getting_edges"
)

func storageLoaderMessage(id MessageID, fallback string, data map[string]any) Message {
	return Message{ID: id, Fallback: fallback, Data: data}
}

func storageLoaderCauseData(cause error) map[string]any {
	return map[string]any{"Cause": storageErrorText(cause)}
}

func storageLoaderPathCauseData(path string, cause error) map[string]any {
	return map[string]any{"Path": path, "Cause": storageErrorText(cause)}
}

func StorageLoaderLoadingNodes(path string, cause error) Message {
	return storageLoaderMessage(MessageStorageLoaderLoadingNodes, "loading nodes: "+storageErrorText(cause), storageLoaderPathCauseData(path, cause))
}

func StorageLoaderLoadingRelationships(path string, cause error) Message {
	return storageLoaderMessage(MessageStorageLoaderLoadingRelationships, "loading relationships: "+storageErrorText(cause), storageLoaderPathCauseData(path, cause))
}

func StorageLoaderOpeningFile(path string, cause error) Message {
	return storageLoaderMessage(MessageStorageLoaderOpeningFile, "opening file: "+storageErrorText(cause), storageLoaderPathCauseData(path, cause))
}

func StorageLoaderDecodingJSON(cause error) Message {
	return storageLoaderMessage(MessageStorageLoaderDecodingJSON, "decoding JSON: "+storageErrorText(cause), storageLoaderCauseData(cause))
}

func StorageLoaderCreatingNodes(cause error) Message {
	return storageLoaderMessage(MessageStorageLoaderCreatingNodes, "creating nodes: "+storageErrorText(cause), storageLoaderCauseData(cause))
}

func StorageLoaderCreatingEdges(cause error) Message {
	return storageLoaderMessage(MessageStorageLoaderCreatingEdges, "creating edges: "+storageErrorText(cause), storageLoaderCauseData(cause))
}

func StorageLoaderExportEngineUnsupported(engineType string) Message {
	return storageLoaderMessage(MessageStorageLoaderExportEngineUnsupported, fmt.Sprintf("SaveToNeo4jExport: engine type %s does not support full export", engineType), map[string]any{"EngineType": engineType})
}

func StorageLoaderGettingAllNodes(cause error) Message {
	return storageLoaderMessage(MessageStorageLoaderGettingAllNodes, "getting all nodes: "+storageErrorText(cause), storageLoaderCauseData(cause))
}

func StorageLoaderGettingAllEdges(cause error) Message {
	return storageLoaderMessage(MessageStorageLoaderGettingAllEdges, "getting all edges: "+storageErrorText(cause), storageLoaderCauseData(cause))
}

func StorageLoaderCreatingFile(path string, cause error) Message {
	return storageLoaderMessage(MessageStorageLoaderCreatingFile, "creating file: "+storageErrorText(cause), storageLoaderPathCauseData(path, cause))
}

func StorageLoaderEncodingJSON(cause error) Message {
	return storageLoaderMessage(MessageStorageLoaderEncodingJSON, "encoding JSON: "+storageErrorText(cause), storageLoaderCauseData(cause))
}

func StorageLoaderParsingNodeJSON(cause error) Message {
	return storageLoaderMessage(MessageStorageLoaderParsingNodeJSON, "parsing node JSON: "+storageErrorText(cause), storageLoaderCauseData(cause))
}

func StorageLoaderConvertingNode(cause error) Message {
	return storageLoaderMessage(MessageStorageLoaderConvertingNode, "converting node: "+storageErrorText(cause), storageLoaderCauseData(cause))
}

func StorageLoaderScanningFile(cause error) Message {
	return storageLoaderMessage(MessageStorageLoaderScanningFile, "scanning file: "+storageErrorText(cause), storageLoaderCauseData(cause))
}

func StorageLoaderParsingRelationshipJSON(cause error) Message {
	return storageLoaderMessage(MessageStorageLoaderParsingRelationshipJSON, "parsing relationship JSON: "+storageErrorText(cause), storageLoaderCauseData(cause))
}

func StorageLoaderConvertingRelationship(cause error) Message {
	return storageLoaderMessage(MessageStorageLoaderConvertingRelationship, "converting relationship: "+storageErrorText(cause), storageLoaderCauseData(cause))
}

func StorageLoaderGettingNodes(cause error) Message {
	return storageLoaderMessage(MessageStorageLoaderGettingNodes, "getting nodes: "+storageErrorText(cause), storageLoaderCauseData(cause))
}

func StorageLoaderGettingEdges(cause error) Message {
	return storageLoaderMessage(MessageStorageLoaderGettingEdges, "getting edges: "+storageErrorText(cause), storageLoaderCauseData(cause))
}
