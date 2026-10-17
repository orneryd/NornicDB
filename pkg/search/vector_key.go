package search

import (
	"strconv"
	"strings"
)

// vectorKeyKind is what a vector-entry key addresses within its node.
type vectorKeyKind uint8

const (
	vectorKeyMain     vectorKeyKind = iota // the node itself
	vectorKeyChunk                         // "{nodeID}-chunk-{i}"
	vectorKeyNamed                         // "{nodeID}-named-{vectorName}"
	vectorKeyProperty                      // "{nodeID}-prop-{propertyKey}"
)

// vectorKey is a decoded vector-entry key.
type vectorKey struct {
	node  string
	kind  vectorKeyKind
	chunk int    // vectorKeyChunk: the chunk index
	name  string // vectorKeyNamed / vectorKeyProperty: the vector or property name
}

// decodeVectorKey is the one decoder of vector-entry keys. Node IDs can
// contain '-' (UUIDs etc.), so a suffix counts only when it matches a known
// pattern (for chunks: an integer). The format is ambiguous for node IDs
// that themselves end in such a suffix (#1415); callers that need certainty
// use the node metadata maps.
func decodeVectorKey(id string) vectorKey {
	if idx := strings.LastIndex(id, "-chunk-"); idx >= 0 {
		if chunk, err := strconv.Atoi(id[idx+len("-chunk-"):]); err == nil {
			return vectorKey{node: id[:idx], kind: vectorKeyChunk, chunk: chunk}
		}
	}
	if idx := strings.LastIndex(id, "-named-"); idx >= 0 && idx+len("-named-") < len(id) {
		return vectorKey{node: id[:idx], kind: vectorKeyNamed, name: id[idx+len("-named-"):]}
	}
	if idx := strings.LastIndex(id, "-prop-"); idx >= 0 && idx+len("-prop-") < len(id) {
		return vectorKey{node: id[:idx], kind: vectorKeyProperty, name: id[idx+len("-prop-"):]}
	}
	return vectorKey{node: id, kind: vectorKeyMain}
}

func normalizeVectorResultIDToNodeID(id string) string {
	return decodeVectorKey(id).node
}
