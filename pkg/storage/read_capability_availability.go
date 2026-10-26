package storage

import "reflect"

func readCapabilityPresent[T any](engine Engine) bool {
	available := HasReadCapability[T](engine)
	return available
}

type historicalLabelNodes interface {
	GetNodesByLabelVisibleAt(string, MVCCVersion) ([]*Node, error)
}
type historicalOutgoingEdges interface {
	GetOutgoingEdgesVisibleAt(NodeID, MVCCVersion) ([]*Edge, error)
}
type historicalIncomingEdges interface {
	GetIncomingEdgesVisibleAt(NodeID, MVCCVersion) ([]*Edge, error)
}
type historicalTypedEdges interface {
	GetEdgesByTypeVisibleAt(string, MVCCVersion) ([]*Edge, error)
}
type historicalBetweenEdges interface {
	GetEdgesBetweenVisibleAt(NodeID, NodeID, MVCCVersion) ([]*Edge, error)
}
type snapshotNode interface {
	GetNodeVisibleAt(NodeID, MVCCVersion) (*Node, error)
}
type snapshotEdge interface {
	GetEdgeVisibleAt(EdgeID, MVCCVersion) (*Edge, error)
}
type nodeHead interface {
	GetNodeCurrentHead(NodeID) (MVCCHead, error)
}
type edgeHead interface {
	GetEdgeCurrentHead(EdgeID) (MVCCHead, error)
}
type materializedAccess interface{ RecordMaterializedAccess(string) }

// Namespaced reads always remain operations of n. The dependency checks only
// determine whether n can serve its conditional forwards; they never return an
// underlying reader to n's caller. Methods with a complete core fallback (node
// projection, query hydration, adjacency, ordinary streams and counts) need no
// dependency. The batch forward is governed by its existing live support flag
// in the discovery cursor.
func (n *NamespacedEngine) ReadCapabilityAvailable(request reflect.Type) bool {
	if ReadCapabilityIncludes[historicalLabelNodes](request) && !readCapabilityPresent[scopedHistoricalLabelNodes](n.inner) && !readCapabilityPresent[historicalLabelNodes](n.inner) {
		return false
	}
	if ReadCapabilityIncludes[historicalOutgoingEdges](request) && !readCapabilityPresent[historicalOutgoingEdges](n.inner) {
		return false
	}
	if ReadCapabilityIncludes[historicalIncomingEdges](request) && !readCapabilityPresent[historicalIncomingEdges](n.inner) {
		return false
	}
	if ReadCapabilityIncludes[historicalTypedEdges](request) && !readCapabilityPresent[historicalTypedEdges](n.inner) {
		return false
	}
	if ReadCapabilityIncludes[historicalBetweenEdges](request) && !readCapabilityPresent[historicalBetweenEdges](n.inner) {
		return false
	}
	if ReadCapabilityIncludes[snapshotNode](request) && !readCapabilityPresent[snapshotNode](n.inner) {
		return false
	}
	if ReadCapabilityIncludes[snapshotEdge](request) && !readCapabilityPresent[snapshotEdge](n.inner) {
		return false
	}
	if ReadCapabilityIncludes[nodeHead](request) && !readCapabilityPresent[nodeHead](n.inner) {
		return false
	}
	if ReadCapabilityIncludes[edgeHead](request) && !readCapabilityPresent[edgeHead](n.inner) {
		return false
	}
	if ReadCapabilityIncludes[TemporalLookupEngine](request) && !readCapabilityPresent[NamespaceTemporalLookupProvider](n.inner) {
		return false
	}
	// Current temporal visibility also owns the ordinary same-view schema
	// predicate when the inner view has no namespace predicate. A declared
	// provider's refusal and constrained schemas still fail at execution.
	if ReadCapabilityIncludes[TemporalCurrentNodeEngine](request) && !readCapabilityPresent[NamespaceTemporalCurrentNodeProvider](n.inner) {
		if _, declared := engineCapability[NamespaceTemporalCurrentNodeProvider](n.inner, capabilityViewMetadata, nil); declared {
			return false
		}
	}
	if ReadCapabilityIncludes[materializedAccess](request) && !readCapabilityPresent[materializedAccess](n.inner) {
		return false
	}
	if ReadCapabilityIncludes[PropertyKeyLookup](request) && !readCapabilityPresent[NamespacePropertyKeyLookup](n.inner) {
		return false
	}
	if ReadCapabilityIncludes[ReadSnapshotter](request) && !readCapabilityPresent[ReadSnapshotter](n.inner) {
		return false
	}
	return true
}

// A composite can promise a conditional operation only when every readable
// constituent can serve it. Resolve all dependencies before any constituent
// can deliver a row. In particular, a live read is not a historical fallback.
func (c *CompositeEngine) ReadCapabilityAvailable(request reflect.Type) bool {
	historical := ReadCapabilityIncludes[historicalLabelNodes](request) || ReadCapabilityIncludes[historicalOutgoingEdges](request) || ReadCapabilityIncludes[historicalIncomingEdges](request) || ReadCapabilityIncludes[historicalTypedEdges](request) || ReadCapabilityIncludes[historicalBetweenEdges](request) || ReadCapabilityIncludes[snapshotNode](request) || ReadCapabilityIncludes[snapshotEdge](request) || ReadCapabilityIncludes[nodeHead](request) || ReadCapabilityIncludes[edgeHead](request)
	if !historical {
		return true
	}
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return false
		}
		if ReadCapabilityIncludes[snapshotNode](request) && !readCapabilityPresent[snapshotNode](engine) {
			return false
		}
		if ReadCapabilityIncludes[snapshotEdge](request) && !readCapabilityPresent[snapshotEdge](engine) {
			return false
		}
		if ReadCapabilityIncludes[nodeHead](request) && !readCapabilityPresent[nodeHead](engine) {
			return false
		}
		if ReadCapabilityIncludes[edgeHead](request) && !readCapabilityPresent[edgeHead](engine) {
			return false
		}
		if ReadCapabilityIncludes[historicalLabelNodes](request) && !readCapabilityPresent[historicalLabelNodes](engine) {
			return false
		}
		if ReadCapabilityIncludes[historicalOutgoingEdges](request) && !readCapabilityPresent[historicalOutgoingEdges](engine) {
			return false
		}
		if ReadCapabilityIncludes[historicalIncomingEdges](request) && !readCapabilityPresent[historicalIncomingEdges](engine) {
			return false
		}
		if ReadCapabilityIncludes[historicalTypedEdges](request) && !readCapabilityPresent[historicalTypedEdges](engine) {
			return false
		}
		if ReadCapabilityIncludes[historicalBetweenEdges](request) && !readCapabilityPresent[historicalBetweenEdges](engine) {
			return false
		}
	}
	return true
}
