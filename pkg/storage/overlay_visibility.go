package storage

// The write-behind overlay serves acknowledged bodies until the flush lands
// them, so every read that merges it with storage must leave out what the
// visibility layer hides from that read, exactly as its storage path does.
// Scans use the adapters here; point reads pass a complete bufferedReadVisibility
// to entityStateLocked. TestOverlayBodiesAreReadThroughVisibility rejects raw
// body reads outside these owners. Each takes the read's visibility and applies
// hidesNode or hidesEdge, the predicates the storage paths use. A hidden
// buffered body stays in the touched or
// suppressed set it comes with, so it masks its stored predecessor instead of
// revealing it. All of them run where the caller holds the buffer's read
// lock (withReadView's capture, captureReadView's cached callback).

// visibleBufferedNodes keeps the nodes of a fresh overlay slice that a read
// with visibility v sees.
func (b *BadgerEngine) visibleBufferedNodes(v readVisibility, nodes []*Node) []*Node {
	if v == storedRead || len(nodes) == 0 || !b.visibilityFiltered() {
		return nodes
	}
	nowNanos := DecayScoringTime()
	visible := nodes[:0]
	for _, node := range nodes {
		if !b.hidesNode(v, node, nowNanos) {
			visible = append(visible, node)
		}
	}
	return visible
}

// visibleBufferedEdges is visibleBufferedNodes for relationships.
func (b *BadgerEngine) visibleBufferedEdges(v readVisibility, edges []*Edge) []*Edge {
	if v == storedRead || len(edges) == 0 || !b.visibilityFiltered() {
		return edges
	}
	nowNanos := DecayScoringTime()
	visible := edges[:0]
	for _, edge := range edges {
		if !b.hidesEdge(v, edge, nowNanos) {
			visible = append(visible, edge)
		}
	}
	return visible
}

// bufferedReadVisibility is a read's stack-only policy. Omission means a
// physical buffered read; no policy is retained on the shared buffer.
type bufferedReadVisibility struct {
	engine     *BadgerEngine
	visibility readVisibility
}

// bufferedLabelNodes is the overlay's label scan for a read with visibility v.
func (b *BadgerEngine) bufferedLabelNodes(w *WriteBehindBuffer, v readVisibility, label string) ([]*Node, map[NodeID]bool) {
	nodes, touched := w.labelOverlayLocked(label)
	return b.visibleBufferedNodes(v, nodes), touched
}

// bufferedAllNodes is the overlay's node scan for a read with visibility v.
func (b *BadgerEngine) bufferedAllNodes(w *WriteBehindBuffer, v readVisibility) ([]*Node, map[NodeID]bool) {
	nodes, touched := w.allNodesOverlayLocked()
	return b.visibleBufferedNodes(v, nodes), touched
}

// bufferedAllEdges is the overlay's relationship scan for a read with
// visibility v.
func (b *BadgerEngine) bufferedAllEdges(w *WriteBehindBuffer, v readVisibility) ([]*Edge, map[EdgeID]bool) {
	edges, touched := w.allEdgesOverlayLocked()
	return b.visibleBufferedEdges(v, edges), touched
}

// bufferedTypeEdges is the overlay's relationship-type scan for a read with
// visibility v.
func (b *BadgerEngine) bufferedTypeEdges(w *WriteBehindBuffer, v readVisibility, edgeType string) ([]*Edge, map[EdgeID]bool) {
	edges, touched := w.typeOverlayLocked(edgeType)
	return b.visibleBufferedEdges(v, edges), touched
}

// bufferedAdjacency is the overlay's adjacency of nodeID for a read with
// visibility v.
func (b *BadgerEngine) bufferedAdjacency(w *WriteBehindBuffer, v readVisibility, nodeID NodeID, outgoing bool) (live []*Edge, suppressed map[EdgeID]struct{}, nodeDeleted bool) {
	live, suppressed, nodeDeleted = w.edgeAdjacencyOverlayLocked(nodeID, outgoing)
	return b.visibleBufferedEdges(v, live), suppressed, nodeDeleted
}

// commitsSince visits the operations of each commit the buffer acknowledged
// after since and has not applied yet, newest first within a generation,
// until visit returns false.
func (w *WriteBehindBuffer) commitsSince(since MVCCVersion, visit func(MVCCVersion, []Operation) bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	for _, buf := range append([]*CommitBuffer{w.active}, w.draining...) {
		for i := len(buf.commits) - 1; i >= 0; i-- {
			c := &buf.commits[i]
			if c.version.Compare(since) <= 0 {
				break
			}
			if !visit(c.version, c.ops) {
				return
			}
		}
	}
}

// bufferedLiveAdjacency is the overlay's live adjacency index of id for a
// read with visibility v, without the buffer's global tombstones.
func (b *BadgerEngine) bufferedLiveAdjacency(w *WriteBehindBuffer, v readVisibility, id NodeID, outgoing bool) []*Edge {
	return b.visibleBufferedEdges(v, w.liveAdjacencyLocked(id, outgoing))
}

// bufferedEdgesBetween is the overlay's relationships from source to target
// for a read with visibility v.
func (b *BadgerEngine) bufferedEdgesBetween(w *WriteBehindBuffer, v readVisibility, source, target NodeID) (live []*Edge, suppressed map[EdgeID]struct{}, sourceDeleted, targetDeleted bool) {
	live, suppressed, sourceDeleted, targetDeleted = w.edgesBetweenOverlayLocked(source, target)
	return b.visibleBufferedEdges(v, live), suppressed, sourceDeleted, targetDeleted
}

// bufferedKeyCandidates is the overlay's pending holders of a key value for a
// read with visibility v.
func (b *BadgerEngine) bufferedKeyCandidates(w *WriteBehindBuffer, v readVisibility, label, property string, valueKey interface{}) []*Node {
	return b.visibleBufferedNodes(v, w.keyCandidatesLocked(label, property, valueKey))
}
