package storage

import (
	"slices"
	"time"
)

// vectorRetention names the vector images of one node update that the update
// kept unchanged from the node's predecessor (NOR-412). The semantic journal
// record omits those images and marks them retained; replay restores them
// from the stored predecessor, which ordered replay has already rebuilt. An
// update that leaves a node's vectors alone therefore journals no vectors,
// however many the node holds.
type vectorRetention uint8

const (
	// retainedManagedEmbeddings: ChunkEmbeddings and EmbedMeta, the vectors
	// the embedding owner manages (kept by reconcileManagedEmbeddingsLocked).
	retainedManagedEmbeddings vectorRetention = 1 << iota
	// retainedNamedEmbeddings: NamedEmbeddings, carried unchanged in the
	// update's image.
	retainedNamedEmbeddings

	allVectorRetention = retainedManagedEmbeddings | retainedNamedEmbeddings
)

// nodeVectorRetention reports which non-empty vector images of node are the
// predecessor's own. managedKept is the embedding owner's decision for the
// managed set; named vectors are compared because callers carry them in the
// image they write.
func nodeVectorRetention(oldNode, node *Node, managedKept bool) vectorRetention {
	var retained vectorRetention
	if managedKept && (len(oldNode.ChunkEmbeddings) > 0 || oldNode.EmbedMeta != nil) {
		retained |= retainedManagedEmbeddings
	}
	if len(oldNode.NamedEmbeddings) > 0 && sameNamedEmbeddings(node.NamedEmbeddings, oldNode.NamedEmbeddings) {
		retained |= retainedNamedEmbeddings
	}
	return retained
}

func sameNamedEmbeddings(a, b map[string][]float32) bool {
	if len(a) != len(b) {
		return false
	}
	for name, vector := range a {
		other, ok := b[name]
		if !ok || !slices.Equal(vector, other) {
			return false
		}
	}
	return true
}

// keepManagedEmbeddingsLocked puts oldNode's managed embeddings on node and
// returns the body to store. When they live in the embedding sidecar the body
// is stored without them, the sidecar stays their source, and its freshness
// stamp follows the body's new version in the same transaction. meta and
// sidecar come from retainedSidecarMetadataLocked(oldNode).
func (tx *BadgerTransaction) keepManagedEmbeddingsLocked(oldNode, node *Node, meta map[string]any, sidecar bool) (*Node, error) {
	node.ChunkEmbeddings = oldNode.ChunkEmbeddings
	node.EmbedMeta = oldNode.EmbedMeta
	if !sidecar {
		return node, nil
	}
	// An unrelated user mutation advances the body version without
	// changing the embedded source. Publish the retained sidecar's new
	// freshness stamp atomically with that body, including repeated writes.
	if !oldNode.UpdatedAt.Equal(node.UpdatedAt) {
		meta[embeddingContentUpdatedAtKey] = node.UpdatedAt.UTC().Format(time.RFC3339Nano)
		data, err := encodeValue(meta)
		if err != nil {
			return nil, err
		}
		tx.bufferSet(embeddingMetaKey(node.ID), data)
	}
	body := *node
	body.ChunkEmbeddings = nil
	body.EmbedMeta = nil
	return &body, nil
}

// restoreRetainedVectorsLocked applies a decoded journal image whose record
// omitted retained vectors: it takes them from the stored predecessor exactly
// as the original commit kept them, so replay leaves those vectors and their
// sidecar as the commit did. It reports whether the managed set was kept.
func (tx *BadgerTransaction) restoreRetainedVectorsLocked(oldNode, node *Node, retained vectorRetention) (*Node, bool, error) {
	if retained&retainedNamedEmbeddings != 0 {
		node.NamedEmbeddings = oldNode.NamedEmbeddings
	}
	if retained&retainedManagedEmbeddings == 0 {
		return node, false, nil
	}
	meta, sidecar, err := tx.retainedSidecarMetadataLocked(oldNode)
	if err != nil {
		return nil, false, err
	}
	body, err := tx.keepManagedEmbeddingsLocked(oldNode, node, meta, sidecar)
	if err != nil {
		return nil, false, err
	}
	return body, true, nil
}
