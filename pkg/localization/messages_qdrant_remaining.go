package localization

import (
	"fmt"
	"strconv"
)

const (
	MessageQdrantInvalidVectorsConfig      MessageID = "qdrant.invalid_vectors_config"
	MessageQdrantVectorSizePositive        MessageID = "qdrant.vector_size_positive"
	MessageQdrantCreateCollectionFailed    MessageID = "qdrant.create_collection_failed"
	MessageQdrantListCollectionsFailed     MessageID = "qdrant.list_collections_failed"
	MessageQdrantDeleteCollectionFailed    MessageID = "qdrant.delete_collection_failed"
	MessageQdrantStoreNotConfigured        MessageID = "qdrant.collection_store_not_configured"
	MessageQdrantTooManyPoints             MessageID = "qdrant.too_many_points"
	MessageQdrantReadPointsFailed          MessageID = "qdrant.read_existing_points_failed"
	MessageQdrantCreatePointsFailed        MessageID = "qdrant.create_points_failed"
	MessageQdrantUpdatePointFailed         MessageID = "qdrant.update_point_failed"
	MessageQdrantMissingVector             MessageID = "qdrant.missing_vector_for_point"
	MessageQdrantIndexPointFailed          MessageID = "qdrant.index_point_failed"
	MessageQdrantOffsetTooLarge            MessageID = "qdrant.offset_too_large"
	MessageQdrantHNSWEfTooLarge            MessageID = "qdrant.hnsw_ef_too_large"
	MessageQdrantPointVectorRequired       MessageID = "qdrant.point_id_and_vectors_required"
	MessageQdrantQueryVariantMissing       MessageID = "qdrant.query_variant_missing"
	MessageQdrantQueryVariantUnsupported   MessageID = "qdrant.query_variant_unsupported"
	MessageQdrantPointNotFoundCause        MessageID = "qdrant.point_not_found_with_cause"
	MessageQdrantVectorNotFound            MessageID = "qdrant.vector_not_found_for_point"
	MessageQdrantInputVariantUnsupported   MessageID = "qdrant.vector_input_variant_unsupported"
	MessageQdrantPositiveRequired          MessageID = "qdrant.positive_example_required"
	MessageQdrantPositiveAverageFailed     MessageID = "qdrant.positive_average_failed"
	MessageQdrantNegativeAverageFailed     MessageID = "qdrant.negative_average_failed"
	MessageQdrantRecommendDimMismatch      MessageID = "qdrant.recommend_dimension_mismatch"
	MessageQdrantVectorKindUnsupported     MessageID = "qdrant.vector_kind_unsupported"
	MessageQdrantScanPointsFailed          MessageID = "qdrant.scan_points_failed"
	MessageQdrantSelectorInvalid           MessageID = "qdrant.points_selector_invalid"
	MessageQdrantVectorsTypeUnsupported    MessageID = "qdrant.vectors_type_unsupported"
	MessageQdrantGetCollectionPointsFailed MessageID = "qdrant.get_collection_points_failed"
	MessageQdrantFullSnapshotStorage       MessageID = "qdrant.full_snapshot_storage_required"
	MessageQdrantCreateBackupFailed        MessageID = "qdrant.create_backup_failed"
	MessageQdrantGetNodesFailed            MessageID = "qdrant.get_nodes_failed"
	MessageQdrantGetEdgesFailed            MessageID = "qdrant.get_edges_failed"
)

func QdrantInvalidVectorsConfig() Message {
	return Message{ID: MessageQdrantInvalidVectorsConfig, Fallback: "invalid vectors_config"}
}

func QdrantVectorSizeMustBePositive() Message {
	return Message{ID: MessageQdrantVectorSizePositive, Fallback: "vector size must be positive"}
}

func QdrantCreateCollectionFailed(cause error) Message {
	return qdrantCauseMessage(MessageQdrantCreateCollectionFailed, "failed to create collection: ", cause)
}

func QdrantListCollectionsFailed(cause error) Message {
	return qdrantCauseMessage(MessageQdrantListCollectionsFailed, "failed to list collections: ", cause)
}

func QdrantDeleteCollectionFailed(cause error) Message {
	return qdrantCauseMessage(MessageQdrantDeleteCollectionFailed, "failed to delete collection: ", cause)
}

func QdrantCollectionStoreNotConfigured() Message {
	return Message{ID: MessageQdrantStoreNotConfigured, Fallback: "collection store not configured"}
}

func QdrantTooManyPoints(count, maximum int) Message {
	return Message{ID: MessageQdrantTooManyPoints, Fallback: "too many points: " + strconv.Itoa(count) + " > " + strconv.Itoa(maximum), Data: map[string]any{"Count": count, "Maximum": maximum}}
}

func QdrantReadExistingPointsFailed(cause error) Message {
	return qdrantCauseMessage(MessageQdrantReadPointsFailed, "failed to read existing points: ", cause)
}

func QdrantCreatePointsFailed(cause error) Message {
	return qdrantCauseMessage(MessageQdrantCreatePointsFailed, "failed to create points: ", cause)
}

func QdrantUpdatePointFailed(pointID string, cause error) Message {
	return Message{ID: MessageQdrantUpdatePointFailed, Fallback: "failed to update point " + pointID + ": " + cause.Error(), Data: map[string]any{"PointID": pointID, "Cause": cause.Error()}}
}

func QdrantMissingVectorForPoint(name, pointID string) Message {
	return Message{ID: MessageQdrantMissingVector, Fallback: "missing vector " + strconv.Quote(name) + " for point " + pointID, Data: map[string]any{"Name": name, "PointID": pointID}}
}

func QdrantIndexPointFailed(pointID string, cause error) Message {
	return Message{ID: MessageQdrantIndexPointFailed, Fallback: "failed to index point " + pointID + ": " + cause.Error(), Data: map[string]any{"PointID": pointID, "Cause": cause.Error()}}
}

func QdrantOffsetTooLarge() Message {
	return Message{ID: MessageQdrantOffsetTooLarge, Fallback: "offset too large"}
}

func QdrantHNSWEfTooLarge() Message {
	return Message{ID: MessageQdrantHNSWEfTooLarge, Fallback: "hnsw_ef too large"}
}

func QdrantPointIDAndVectorsRequired() Message {
	return Message{ID: MessageQdrantPointVectorRequired, Fallback: "point id and vectors are required"}
}

func QdrantQueryVariantMissing() Message {
	return Message{ID: MessageQdrantQueryVariantMissing, Fallback: "query without Query.variant is not implemented"}
}

func QdrantQueryVariantUnsupported(variant any) Message {
	value := fmt.Sprintf("%T", variant)
	return Message{ID: MessageQdrantQueryVariantUnsupported, Fallback: "query variant " + value + " is not implemented", Data: map[string]any{"Variant": value}}
}

func QdrantPointNotFoundWithCause(cause error) Message {
	return qdrantCauseMessage(MessageQdrantPointNotFoundCause, "point not found: ", cause)
}

func QdrantVectorNotFoundForPoint() Message {
	return Message{ID: MessageQdrantVectorNotFound, Fallback: "vector not found for point"}
}

func QdrantVectorInputVariantUnsupported(variant any) Message {
	value := fmt.Sprintf("%T", variant)
	return Message{ID: MessageQdrantInputVariantUnsupported, Fallback: "vector input variant " + value + " is not implemented", Data: map[string]any{"Variant": value}}
}

func QdrantPositiveExampleRequired() Message {
	return Message{ID: MessageQdrantPositiveRequired, Fallback: "at least one positive example is required"}
}

func QdrantPositiveAverageFailed() Message {
	return Message{ID: MessageQdrantPositiveAverageFailed, Fallback: "failed to compute positive vector average: no valid vectors"}
}

func QdrantNegativeAverageFailed() Message {
	return Message{ID: MessageQdrantNegativeAverageFailed, Fallback: "failed to compute negative vector average: no valid vectors"}
}

func QdrantRecommendationDimensionMismatch(positive, negative int) Message {
	return Message{ID: MessageQdrantRecommendDimMismatch, Fallback: "dimension mismatch: positive vectors have dimension " + strconv.Itoa(positive) + ", negative vectors have dimension " + strconv.Itoa(negative), Data: map[string]any{"Positive": positive, "Negative": negative}}
}

func QdrantUnsupportedVectorKind() Message {
	return Message{ID: MessageQdrantVectorKindUnsupported, Fallback: "unsupported vector kind"}
}

func QdrantScanPointsFailed(cause error) Message {
	return qdrantCauseMessage(MessageQdrantScanPointsFailed, "failed to scan points: ", cause)
}

func QdrantInvalidPointsSelector() Message {
	return Message{ID: MessageQdrantSelectorInvalid, Fallback: "invalid points selector"}
}

func QdrantUnsupportedVectorsType() Message {
	return Message{ID: MessageQdrantVectorsTypeUnsupported, Fallback: "unsupported vectors type"}
}

func QdrantGetCollectionPointsFailed(cause error) Message {
	return qdrantCauseMessage(MessageQdrantGetCollectionPointsFailed, "failed to get collection points: ", cause)
}

func QdrantFullSnapshotsRequireBaseStorage() Message {
	return Message{ID: MessageQdrantFullSnapshotStorage, Fallback: "full snapshots require base storage"}
}

func QdrantCreateBackupFailed(cause error) Message {
	return qdrantCauseMessage(MessageQdrantCreateBackupFailed, "failed to create backup: ", cause)
}

func QdrantGetNodesFailed(cause error) Message {
	return qdrantCauseMessage(MessageQdrantGetNodesFailed, "failed to get nodes: ", cause)
}

func QdrantGetEdgesFailed(cause error) Message {
	return qdrantCauseMessage(MessageQdrantGetEdgesFailed, "failed to get edges: ", cause)
}

func qdrantCauseMessage(id MessageID, prefix string, cause error) Message {
	causeText := cause.Error()
	return Message{ID: id, Fallback: prefix + causeText, Data: map[string]any{"Cause": causeText}}
}
