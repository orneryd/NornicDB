package embed

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// closingMockEmbedder counts Close calls.
type closingMockEmbedder struct {
	mockEmbedder
	closes int
}

func (e *closingMockEmbedder) Close() error {
	e.closes++
	return nil
}

// Closing a cached embedder closes the embedder it wraps (a local model),
// so a holder that closes what it was given releases the model behind the
// cache too. One wrapping an embedder without Close closes nothing.
func TestCachedEmbedderCloseClosesWrapped(t *testing.T) {
	base := &closingMockEmbedder{}
	var cached Embedder = NewCachedEmbedder(base, 16)
	closer, ok := cached.(interface{ Close() error })
	require.True(t, ok, "a cached embedder can be closed")
	require.NoError(t, closer.Close())
	require.Equal(t, 1, base.closes)

	closer, ok = Embedder(NewCachedEmbedder(&mockEmbedder{}, 16)).(interface{ Close() error })
	require.True(t, ok)
	require.NoError(t, closer.Close())
}
