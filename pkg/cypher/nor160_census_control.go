package cypher

import "github.com/orneryd/nornicdb/pkg/storage"

// NOR160ControlCrossCopy copies its own argument through another package's
// exported copier and drops the copy on one path: a copy it made, not a
// receipt (K1, must be reported).
func NOR160ControlCrossCopy(n *storage.Node, keep bool) *storage.Node {
	c := storage.CopyNode(n)
	if !keep {
		return nil
	}
	return c
}

// The boundary (received provenance). A helper that passes on a node a
// public read delivered, and a caller that drops it: the receiver's own
// handling (clean). A copy of a received node, dropped: clean. A cache this
// package owns, copying its own retained node and dropping the copy: in scope
// (K1, must be reported).
func nor160ControlCypherHelper(c *storage.NOR160ControlCache, id storage.NodeID) *storage.Node {
	return c.GetCopied(id)
}

func NOR160ControlCypherDrops(c *storage.NOR160ControlCache, id storage.NodeID, hidden func(*storage.Node) bool) *storage.Node {
	n := nor160ControlCypherHelper(c, id)
	if hidden(n) {
		return nil
	}
	return n
}

func NOR160ControlCypherCopyReceived(c *storage.NOR160ControlCache, id storage.NodeID, keep bool) *storage.Node {
	n := storage.CopyNode(c.GetCopied(id))
	if !keep {
		return nil
	}
	return n
}

type NOR160ControlCypherCache struct{ nodes map[string]*storage.Node }

func (c *NOR160ControlCypherCache) Get(k string, hidden func(*storage.Node) bool) *storage.Node {
	n := storage.CopyNode(c.nodes[k])
	if hidden(n) {
		return nil
	}
	return n
}

// NOR160ControlStripReceived strips copies a storage stream handed to its
// callback: its own handling. NOR160ControlUseLent does the same to nodes
// storage lends; the lend stays storage's member.
func NOR160ControlStripReceived(c *storage.NOR160ControlCache) error {
	return c.StreamCopiesFor(func(n *storage.Node) error {
		n.ChunkEmbeddings = nil
		return nil
	})
}

func NOR160ControlUseLent(c *storage.NOR160ControlCache) error {
	return c.StreamLent(func(n *storage.Node) error {
		n.ChunkEmbeddings = nil
		return nil
	})
}

// NOR160ControlDropsFresh drops what storage's GetFreshOrSame returned to it:
// a received value, even though the read may hand back the argument itself.
func NOR160ControlDropsFresh(c *storage.NOR160ControlCache, n *storage.Node, keep bool) *storage.Node {
	f := c.GetFreshOrSame(n)
	if !keep {
		return nil
	}
	return f
}

// NOR160ControlReceiver drops a node that storage's public read delivered to
// it. The census must prove the receipt and not report it.
func NOR160ControlReceiver(c *storage.NOR160ControlCache, id storage.NodeID, hidden func(*storage.Node) bool) *storage.Node {
	n := c.GetCopied(id)
	if hidden(n) {
		return nil
	}
	return n
}
