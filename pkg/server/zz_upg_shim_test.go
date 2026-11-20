package server

// v152 upgrade lane (not for merge): calls whose signatures differ between
// b6d8e762a and v1.5.x, so the same writer compiles on each old build.

import (
	"github.com/orneryd/nornicdb/pkg/storage"
)

func upgAllNodes(e storage.Engine) ([]*storage.Node, error) { return e.AllNodes() }
func upgGetNode(e storage.Engine, id storage.NodeID) (*storage.Node, error) { return e.GetNode(id) }
func upgAllEdges(e storage.Engine) ([]*storage.Edge, error) { return e.AllEdges() }
func upgNodesByLabel(e storage.Engine, l string) ([]*storage.Node, error) { return e.GetNodesByLabel(l) }
