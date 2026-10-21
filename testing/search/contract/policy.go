package contract

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

type Policy struct {
	Name    string
	rescore bool
}

func Policies() []Policy { return []Policy{{"binary_rescore_on", true}, {"binary_rescore_off", false}} }
func (p Policy) Options(options *search.SearchOptions) {
	value := p.rescore
	options.QuantizationRescore = &value
}
func (p Policy) Prepare(t *testing.T, s *search.Service) {
	require.NoError(t, s.ConfigureHNSWQuantization(context.Background(), search.HNSWQuantizationConfig{Mode: search.HNSWQuantizationBinary}))
	require.NoError(t, s.BuildIndexes(context.Background()))
	require.Eventually(t, func() bool {
		options := &search.SearchOptions{Limit: 2}
		p.Options(options)
		query := make([]float32, s.VectorIndexDimensions())
		query[0] = 1
		response, err := s.Search(context.Background(), "", query, options)
		return err == nil && strings.Contains(response.SearchMethod, "hnsw")
	}, 5*time.Second, 10*time.Millisecond, "matrix must exercise a published compressed graph")
}
