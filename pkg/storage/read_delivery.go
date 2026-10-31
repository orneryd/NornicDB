package storage

// The read delivery owner (NOR-160).
//
// A read pipeline carries images, not results: a nodeImage is
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
// error, since an image is not a *Node result.
//
// A relationship needs no image: every edge producer hands over a value private
// to this read (a decode, or a copy made once the edge is selected), so edge
// reads carry *Edge from producer to caller.

// nodeImage is a node on a read path before delivery.
type nodeImage struct {
	*Node
	retained bool
}

func borrowedNode(node *Node) nodeImage { return nodeImage{Node: node, retained: true} }
func decodedNode(node *Node) nodeImage  { return nodeImage{Node: node} }

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
