package contract

import (
	"github.com/orneryd/nornicdb/pkg/search"
	"testing"
)

// Develop retains its approved exact-scoring scope. Compression is release-only.
type Policy struct{ Name string }

func Policies() []Policy                                 { return []Policy{{"exact"}} }
func (p Policy) Options(*search.SearchOptions)           {}
func (p Policy) Prepare(t *testing.T, s *search.Service) {}
