package cypher

// Final build (2dbc67e96): NewService takes the dimensions directly.

import (
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func nrpNewService(s storage.Engine, dimensions int) *search.Service {
	return search.NewService(s, dimensions, nil)
}
