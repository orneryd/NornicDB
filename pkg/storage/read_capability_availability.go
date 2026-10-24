package storage

import (
	"reflect"
	"time"
)

func readCapabilityPresent[T any](engine Engine) bool {
	_, available := ReadCapabilityForEngine[T](engine)
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
	if ReadCapabilityIncludes[ProjectedLabelNodeReader](request) && !readCapabilityPresent[scopedProjectedLabelNodes](n.inner) && !readCapabilityPresent[ProjectedLabelNodeReader](n.inner) {
		return false
	}
	if ReadCapabilityIncludes[historicalLabelNodes](request) && !readCapabilityPresent[scopedHistoricalLabelNodes](n.inner) && !readCapabilityPresent[MVCCIndexedVisibilityEngine](n.inner) {
		return false
	}
	if (ReadCapabilityIncludes[historicalOutgoingEdges](request) || ReadCapabilityIncludes[historicalIncomingEdges](request) || ReadCapabilityIncludes[historicalTypedEdges](request) || ReadCapabilityIncludes[historicalBetweenEdges](request)) && !readCapabilityPresent[MVCCIndexedVisibilityEngine](n.inner) {
		return false
	}
	if (ReadCapabilityIncludes[snapshotNode](request) || ReadCapabilityIncludes[snapshotEdge](request)) && !readCapabilityPresent[MVCCVisibilityEngine](n.inner) {
		return false
	}
	if (ReadCapabilityIncludes[nodeHead](request) || ReadCapabilityIncludes[edgeHead](request)) && !readCapabilityPresent[MVCCHeadEngine](n.inner) {
		return false
	}
	if ReadCapabilityIncludes[TemporalLookupEngine](request) && !readCapabilityPresent[NamespaceTemporalLookupProvider](n.inner) {
		return false
	}
	if ReadCapabilityIncludes[interface {
		IsCurrentTemporalNode(*Node, time.Time) (bool, error)
	}](request) && !readCapabilityPresent[NamespaceTemporalCurrentNodeProvider](n.inner) {
		return false
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
	projected := ReadCapabilityIncludes[ProjectedLabelNodeReader](request)
	historical := ReadCapabilityIncludes[historicalLabelNodes](request) || ReadCapabilityIncludes[historicalOutgoingEdges](request) || ReadCapabilityIncludes[historicalIncomingEdges](request) || ReadCapabilityIncludes[historicalTypedEdges](request) || ReadCapabilityIncludes[historicalBetweenEdges](request)
	snapshot := ReadCapabilityIncludes[snapshotNode](request) || ReadCapabilityIncludes[snapshotEdge](request)
	heads := ReadCapabilityIncludes[nodeHead](request) || ReadCapabilityIncludes[edgeHead](request)
	if !projected && !historical && !snapshot && !heads {
		return true
	}
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return false
		}
		if projected && !readCapabilityPresent[ProjectedLabelNodeReader](engine) {
			return false
		}
		if historical && !readCapabilityPresent[MVCCIndexedVisibilityEngine](engine) {
			return false
		}
		if snapshot && !readCapabilityPresent[MVCCVisibilityEngine](engine) {
			return false
		}
		if heads && !readCapabilityPresent[MVCCHeadEngine](engine) {
			return false
		}
	}
	return true
}
