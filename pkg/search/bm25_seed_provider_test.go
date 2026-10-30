package search

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type seedProviderStub struct {
	ids      []string
	maxTerms int
	perTerm  int
}

func (s *seedProviderStub) LexicalSeedDocIDs(maxTerms, perTerm int) []string {
	s.maxTerms = maxTerms
	s.perTerm = perTerm
	return append([]string(nil), s.ids...)
}

func TestBM25SeedDocIDs_NoProvider(t *testing.T) {
	require.Nil(t, bm25SeedDocIDs(nil))
}

func TestBM25SeedDocIDs_UsesConfiguredBounds(t *testing.T) {
	t.Setenv("NORNICDB_KMEANS_SEED_MAX_TERMS", "4")
	t.Setenv("NORNICDB_KMEANS_SEED_DOCS_PER_TERM", "0")

	stub := &seedProviderStub{
		ids: []string{"doc-1", "doc-2"},
	}
	got := bm25SeedDocIDs(stub)

	require.Equal(t, []string{"doc-1", "doc-2"}, got)
	require.Equal(t, 16, stub.maxTerms, "max terms should clamp to minimum bound")
	require.Equal(t, 1, stub.perTerm, "docs per term should clamp to minimum bound")
}
