package nornicdb_test

// v152 upgrade lane (not for merge): calls whose signatures differ between
// b6d8e762a and v1.5.x, so the same writer compiles on each old build.

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func upgAllNodes(e storage.Engine) ([]*storage.Node, error) { return e.AllNodes(context.Background()) }
func upgGetNode(e storage.Engine, id storage.NodeID) (*storage.Node, error) { return e.GetNode(context.Background(), id) }
func upgAllEdges(e storage.Engine) ([]*storage.Edge, error) { return e.AllEdges(context.Background()) }
func upgNodesByLabel(e storage.Engine, l string) ([]*storage.Node, error) { return e.GetNodesByLabel(context.Background(), l) }

func upgEnsureSearch(db *nornicdb.DB) error {
	_, err := db.EnsureSearchIndexes(context.Background(), "nornic", nil, nornicdb.SearchIndexBuildWait)
	return err
}
