// Author: Codex (#1636 lane). Public foreground consumers of the DB lifetime.
package nornicdb_test

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type classReaderEngine struct {
	storage.Engine
	enter func()
}

func (e *classReaderEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	e.enter()
	return e.Engine.GetNode(id)
}

func TestOwnerClassSearchReaderReentry(t *testing.T) {
	for _, name := range []string{"database", "hybrid", "similar"} {
		t.Run(name, func(t *testing.T) {
			db := classSearchDB(t)
			var children sync.WaitGroup
			defer func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
				children.Wait()
			}()
			if name == "hybrid" {
				// Finish native startup before forcing this public reader's cold
				// construction. A startup constructor must not satisfy its control.
				if err := db.BuildSearchIndexes(context.Background()); err != nil {
					t.Fatal(err)
				}
				db.ResetSearchService("nornic")
			}
			var calls atomic.Int64
			var blocked atomic.Bool
			enter := func() {
				calls.Add(1)
				child := make(chan struct{})
				children.Add(1)
				go func() { defer children.Done(); db.SetAllDatabasesProvider(nil); close(child) }()
				select {
				case <-child:
				case <-time.After(100 * time.Millisecond):
					blocked.Store(true)
				}
			}
			db.SetDbSearchOptionsResolver(func(name string) search.ServiceOptions { enter(); return search.DefaultServiceOptions(name) })
			switch name {
			case "database":
				_, _ = db.SearchDatabase(context.Background(), "reader", db.GetStorage(), "lifetime", nil, 1)
			case "hybrid":
				_, _ = db.HybridSearchWithOptions(context.Background(), "lifetime", nil, nil)
			case "similar":
				_, _ = db.FindSimilarInDatabase(context.Background(), &classReaderEngine{Engine: db.GetStorage(), enter: enter}, "missing", 1)
			}
			if calls.Load() == 0 {
				t.Fatal("public reader did not reach its extension control")
			}
			if blocked.Load() {
				t.Error("public search reader holds DB state lock across an extension")
			}
		})
	}
}
