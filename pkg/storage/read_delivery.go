package storage

// The read delivery owner (NOR-160).
//
// A read pipeline carries images, not results: an edgeImage or nodeImage is
// either a retained image (a cache, the write-behind buffer or a
// transaction's snapshot keeps it, so the read may only look at it) or a value
// decoded for this read alone. Producers (caches, indexed readers,
// transaction overlays) only borrow or decode; they never copy. The
// write-behind overlay is not an image producer: its values leave it only
// through its own exits (bufferedEntityResult), copied once the read has
// selected them.
// Selection, visibility filtering, overlay replacement and cache fallback all
// run on images. A read turns images into results only here, after it has
// chosen exactly what it hands out: a retained image is copied once, a
// decoded value is handed over. So no read path can copy a value it then
// drops, or copy a value twice: an image has no copy until delivery, and
// delivery is the last step. The types make a missed delivery a compile
// error, since an image is not a *Edge or *Node result.

// edgeImage is a relationship on a read path before delivery.
type edgeImage struct {
	*Edge
	retained bool
}

// nodeImage is a node on a read path before delivery.
type nodeImage struct {
	*Node
	retained bool
}

func borrowedEdge(edge *Edge) edgeImage { return edgeImage{Edge: edge, retained: true} }
func decodedEdge(edge *Edge) edgeImage  { return edgeImage{Edge: edge} }
func borrowedNode(node *Node) nodeImage { return nodeImage{Node: node, retained: true} }
func decodedNode(node *Node) nodeImage  { return nodeImage{Node: node} }

// deliverEdge is the one place a read hands out a relationship.
func deliverEdge(image edgeImage) *Edge {
	if image.Edge == nil || !image.retained {
		return image.Edge
	}
	return copyEdge(image.Edge)
}

// deliverEdges hands out a chosen population (nil stays nil).
func deliverEdges(images []edgeImage) []*Edge {
	if images == nil {
		return nil
	}
	out := make([]*Edge, len(images))
	for i, image := range images {
		out[i] = deliverEdge(image)
	}
	return out
}

// deliverNode is the one place a read hands out a node, in its delivered
// shape: a retained image is copied once straight into that shape, a decoded
// node is reshaped in place.
func deliverNode(image nodeImage, properties []string, strip bool) *Node {
	return shapeNode(image.Node, properties, strip, !image.retained)
}

// deliverNodes hands out a chosen population in one shape (nil stays nil).
func deliverNodes(images []nodeImage, properties []string, strip bool) []*Node {
	if images == nil {
		return nil
	}
	out := make([]*Node, len(images))
	for i, image := range images {
		out[i] = deliverNode(image, properties, strip)
	}
	return out
}

// decodedEdges and borrowedEdges mark a whole population.
func decodedEdges(edges []*Edge) []edgeImage  { return markEdges(edges, false) }
func borrowedEdges(edges []*Edge) []edgeImage { return markEdges(edges, true) }

func markEdges(edges []*Edge, retained bool) []edgeImage {
	if edges == nil {
		return nil
	}
	out := make([]edgeImage, len(edges))
	for i, edge := range edges {
		out[i] = edgeImage{Edge: edge, retained: retained}
	}
	return out
}

func decodedNodes(nodes []*Node) []nodeImage {
	if nodes == nil {
		return nil
	}
	out := make([]nodeImage, len(nodes))
	for i, node := range nodes {
		out[i] = decodedNode(node)
	}
	return out
}
