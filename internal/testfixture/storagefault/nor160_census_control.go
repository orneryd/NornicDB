package storagefault

import "github.com/orneryd/nornicdb/pkg/storage"

type NOR160ControlFixtureCache struct {
	nodes map[storage.NodeID]*storage.Node
}

func (c *NOR160ControlFixtureCache) GetShared(id storage.NodeID) *storage.Node { return c.nodes[id] }
