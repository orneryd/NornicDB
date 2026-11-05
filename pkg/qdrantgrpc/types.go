package qdrantgrpc

import qpb "github.com/qdrant/go-client/qdrant"

// CollectionMeta holds metadata about a Qdrant collection.
//
// In the collection=database model, this metadata is stored in the collection
// database as the required `_collection_meta` node.
type CollectionMeta struct {
	Name       string
	Dimensions int
	Distance   qpb.Distance
	Status     qpb.CollectionStatus
	// Quantization is the durable collection policy; nil uses server defaults.
	Quantization *qpb.QuantizationConfig
}
