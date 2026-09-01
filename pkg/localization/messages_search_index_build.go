package localization

import "strconv"

const (
	MessageSearchVectorFileMagicInvalid           MessageID = "search.vector_file_magic_invalid"
	MessageSearchVectorFileVersionUnsupported     MessageID = "search.vector_file_version_unsupported"
	MessageSearchVectorFileDimensionsMismatch     MessageID = "search.vector_file_dimensions_mismatch"
	MessageSearchVectorMetaDimensionsMismatch     MessageID = "search.vector_meta_dimensions_mismatch"
	MessageSearchGPUNotEnabled                    MessageID = "search.gpu_not_enabled"
	MessageSearchVectorDimensionsInvalid          MessageID = "search.vector_dimensions_invalid"
	MessageSearchIVFPQVectorStoreRequired         MessageID = "search.ivfpq_vector_store_required"
	MessageSearchIVFPQDimensionsInvalid           MessageID = "search.ivfpq_dimensions_invalid"
	MessageSearchIVFPQSegmentsInvalid             MessageID = "search.ivfpq_segments_invalid"
	MessageSearchIVFPQTrainingVectorsInsufficient MessageID = "search.ivfpq_training_vectors_insufficient"
	MessageSearchIVFCoarseTrainingFailed          MessageID = "search.ivf_coarse_training_failed"
	MessageSearchPQCodebookTrainingFailed         MessageID = "search.pq_codebook_training_failed"
	MessageSearchHNSWGPUBuildDimensionInvalid     MessageID = "search.hnsw_gpu_build_dimension_invalid"
	MessageSearchGPUAcceleratorUnavailable        MessageID = "search.gpu_accelerator_unavailable"
	MessageSearchCUDANotAvailable                 MessageID = "search.cuda_not_available"
	MessageSearchVulkanNotAvailable               MessageID = "search.vulkan_not_available"
	MessageSearchVulkanDeviceNotInitialized       MessageID = "search.vulkan_device_not_initialized"
	MessageSearchIVFPQVectorStoreUnavailable      MessageID = "search.ivfpq_vector_store_unavailable"
)

func SearchVectorFileMagicInvalid() Message {
	return Message{ID: MessageSearchVectorFileMagicInvalid, Fallback: "invalid vector file magic"}
}

func SearchVectorFileVersionUnsupported(version byte) Message {
	return Message{ID: MessageSearchVectorFileVersionUnsupported, Fallback: "unsupported vector file version " + strconv.Itoa(int(version)), Data: map[string]any{"Version": version}}
}

func SearchVectorFileDimensionsMismatch(fileDimensions, storeDimensions int) Message {
	return Message{ID: MessageSearchVectorFileDimensionsMismatch, Fallback: "vector file dimensions " + strconv.Itoa(fileDimensions) + " != store dimensions " + strconv.Itoa(storeDimensions), Data: map[string]any{"FileDimensions": fileDimensions, "StoreDimensions": storeDimensions}}
}

func SearchVectorMetaDimensionsMismatch(metaDimensions, storeDimensions int) Message {
	return Message{ID: MessageSearchVectorMetaDimensionsMismatch, Fallback: "meta dimensions " + strconv.Itoa(metaDimensions) + " != store dimensions " + strconv.Itoa(storeDimensions), Data: map[string]any{"MetaDimensions": metaDimensions, "StoreDimensions": storeDimensions}}
}

func SearchGPUNotEnabled() Message {
	return Message{ID: MessageSearchGPUNotEnabled, Fallback: "gpu not enabled"}
}

func SearchVectorDimensionsInvalid() Message {
	return Message{ID: MessageSearchVectorDimensionsInvalid, Fallback: "invalid vector dimensions"}
}

func SearchIVFPQVectorStoreRequired() Message {
	return Message{ID: MessageSearchIVFPQVectorStoreRequired, Fallback: "vector file store is required"}
}

func SearchIVFPQDimensionsInvalid() Message {
	return Message{ID: MessageSearchIVFPQDimensionsInvalid, Fallback: "invalid dimensions"}
}

func SearchIVFPQSegmentsInvalid(dimensions, segments int) Message {
	return Message{ID: MessageSearchIVFPQSegmentsInvalid, Fallback: "invalid pq segments: dimensions=" + strconv.Itoa(dimensions) + " segments=" + strconv.Itoa(segments), Data: map[string]any{"Dimensions": dimensions, "Segments": segments}}
}

func SearchIVFPQTrainingVectorsInsufficient(vectorCount, listCount int) Message {
	return Message{ID: MessageSearchIVFPQTrainingVectorsInsufficient, Fallback: "insufficient training vectors (" + strconv.Itoa(vectorCount) + ") for ivf lists (" + strconv.Itoa(listCount) + ")", Data: map[string]any{"VectorCount": vectorCount, "ListCount": listCount}}
}

func SearchIVFCoarseTrainingFailed(cause error) Message {
	return Message{ID: MessageSearchIVFCoarseTrainingFailed, Fallback: "ivf coarse training failed: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

func SearchPQCodebookTrainingFailed(cause error) Message {
	return Message{ID: MessageSearchPQCodebookTrainingFailed, Fallback: "pq codebook training failed: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

func SearchHNSWGPUBuildDimensionInvalid(dimensions int) Message {
	return Message{ID: MessageSearchHNSWGPUBuildDimensionInvalid, Fallback: "invalid HNSW GPU build dimension " + strconv.Itoa(dimensions), Data: map[string]any{"Dimensions": dimensions}}
}

func SearchGPUAcceleratorUnavailable() Message {
	return Message{ID: MessageSearchGPUAcceleratorUnavailable, Fallback: "no GPU accelerator available"}
}

func SearchCUDANotAvailable() Message {
	return Message{ID: MessageSearchCUDANotAvailable, Fallback: "cuda: CUDA is not available on this system"}
}

func SearchVulkanNotAvailable() Message {
	return Message{ID: MessageSearchVulkanNotAvailable, Fallback: "vulkan: Vulkan is not available on this system"}
}

func SearchVulkanDeviceNotInitialized() Message {
	return Message{ID: MessageSearchVulkanDeviceNotInitialized, Fallback: "vulkan: device not initialized"}
}

func SearchIVFPQVectorStoreUnavailable() Message {
	return Message{ID: MessageSearchIVFPQVectorStoreUnavailable, Fallback: "vector file store unavailable for IVFPQ build"}
}
