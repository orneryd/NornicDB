package storage

import "sync"

// NodeRuntimeMetadata carries query provenance and storage visibility state.
// Database is never persisted; the two flags retain their existing stored keys.
// These values are not graph properties and do not appear in public JSON.
//
// For example, attach query provenance without changing a node's properties:
//
//	metadata := node.RuntimeMetadata()
//	metadata.Database = "analytics"
//	node.SetRuntimeMetadata(metadata)
type NodeRuntimeMetadata struct {
	Database                   string
	EmbeddingsStoredSeparately bool
	VisibilitySuppressed       bool
}

// nodeRuntime is immutable after attachment. Copies of a node share this record;
// changing one image must never modify the state observed by another image.
// A node without a record observes the zero value, so ordinary graph-data
// construction and copying a zero node need no record allocation.
type nodeRuntime struct {
	metadata             NodeRuntimeMetadata
	embeddingOwner       *BadgerEngine
	embeddingOwnerID     NodeID
	embeddingRevision    EmbeddingBodyRevision
	embeddingAttempt     *EmbeddingAttempt
	embeddingPublication embeddingPublicationProof
	// Vector-work admission: the write argument's (consumed before native body
	// ownership) and the embedding identity's (carried by detached copies).
	entityWorkOrigin     uint64
	entityOriginBound    bool
	embeddingWorkOrigin  uint64
	embeddingOriginBound bool
}

// RuntimeMetadata returns this node's runtime metadata by value. An unset node
// has zero metadata, so ordinary graph-data construction needs no runtime record.
func (node *Node) RuntimeMetadata() NodeRuntimeMetadata {
	if node == nil || node.runtime == nil {
		return NodeRuntimeMetadata{}
	}
	return node.runtime.metadata
}

// SetRuntimeMetadata replaces metadata on this image while preserving embedding
// identity and publication evidence. Other node copies keep their prior values.
// Setting unchanged metadata does not allocate. Like other Node field updates,
// this operation requires exclusive ownership of the node image.
func (node *Node) SetRuntimeMetadata(metadata NodeRuntimeMetadata) {
	state := node.runtimeState()
	if state.metadata == metadata {
		return
	}
	state.metadata = metadata
	node.setRuntimeState(state)
}

func (node *Node) runtimeState() nodeRuntime {
	if node == nil || node.runtime == nil {
		return nodeRuntime{}
	}
	return *node.runtime
}

func (node *Node) setRuntimeState(state nodeRuntime) {
	if node.runtimeState() == state {
		return
	}
	if state == (nodeRuntime{}) {
		node.runtime = nil
		return
	}
	// Detached query projections often differ only by the same namespace.
	// Share those immutable metadata-only values; snapshot/attempt/publication
	// identities always remain attached to their own record.
	identity := state
	identity.metadata = NodeRuntimeMetadata{}
	if identity == (nodeRuntime{}) && len(state.metadata.Database) <= 256 {
		runtimeMetadataRecords.RLock()
		record := runtimeMetadataRecords.values[state.metadata]
		runtimeMetadataRecords.RUnlock()
		if record == nil {
			runtimeMetadataRecords.Lock()
			record = runtimeMetadataRecords.values[state.metadata]
			if record == nil {
				if len(runtimeMetadataRecords.values) >= 1024 {
					runtimeMetadataRecords.values = make(map[NodeRuntimeMetadata]*nodeRuntime)
				}
				record = &nodeRuntime{metadata: state.metadata}
				runtimeMetadataRecords.values[state.metadata] = record
			}
			runtimeMetadataRecords.Unlock()
		}
		node.runtime = record
		return
	}
	next := new(nodeRuntime)
	*next = state
	node.runtime = next
}

// Only bounded metadata values are interned, never nodes, body versions or
// acceptance tokens. Eviction affects reuse alone: attached records stay valid.
var runtimeMetadataRecords = struct {
	sync.RWMutex
	values map[NodeRuntimeMetadata]*nodeRuntime
}{values: make(map[NodeRuntimeMetadata]*nodeRuntime)}
