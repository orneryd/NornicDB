package embed

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Plan 04-05-01: D-06 Backend() method on Embedder interface.
//
// The closed enum is {gpu, cpu, cuda, metal, vulkan} per CONTEXT D-06a.
// All 5 implementers (Ollama, OpenAI, LocalGGUFEmbedder for each build tag,
// Cached) MUST return one of these strings. The mode label on
// nornicdb_embed_processed_total / duration_seconds / ffi_panics_total is
// bound from this value at observability bag construction.

// allowedBackends mirrors observability.AllowedEmbedBackends but lives in
// pkg/embed to keep the leaf-package boundary intact (D-01a — pkg/embed
// does not import pkg/observability for closed-enum lookups; the canonical
// authority is observability.AllowedEmbedBackends, this is the same set
// repeated locally for the assertion).
var allowedBackends = map[string]bool{
	"gpu":    true,
	"cpu":    true,
	"cuda":   true,
	"metal":  true,
	"vulkan": true,
}

// TestEmbedderBackend_AllImplementers asserts every Embedder implementer
// returns a value in the closed enum (D-06a).
func TestEmbedderBackend_AllImplementers(t *testing.T) {
	cases := []struct {
		name string
		emb  Embedder
		want string
	}{
		{
			name: "ollama",
			emb:  NewOllama(nil),
			want: "cpu", // RESEARCH §Q9 A1: HTTP-out — process-perspective is cpu.
		},
		{
			name: "openai",
			// API-key required to be non-empty for NewOpenAI to construct;
			// the dummy key is fine — Backend() is independent of the key.
			emb:  NewOpenAI(DefaultOpenAIConfig("sk-test")),
			want: "cpu",
		},
		{
			name: "cached_wraps_ollama",
			emb:  NewCachedEmbedder(NewOllama(nil), 10),
			want: "cpu", // delegates
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotNil(t, tc.emb)
			got := tc.emb.Backend()
			assert.Equal(t, tc.want, got, "%s.Backend() mismatch", tc.name)
			assert.Truef(t, allowedBackends[got],
				"Backend() returned %q which is NOT in closed enum {gpu,cpu,cuda,metal,vulkan}", got)
		})
	}
}

// TestEmbedderBackend_CachedDelegates asserts CachedEmbedder.Backend()
// returns whatever the wrapped embedder reports (so a cached metal embedder
// reports "metal", not "cpu" — preserves the dynamic CUDA→CPU fallback
// truth captured at the wrapped layer per D-06).
func TestEmbedderBackend_CachedDelegates(t *testing.T) {
	wrapped := &fixedBackendMock{backend: "cuda"}
	cached := NewCachedEmbedder(wrapped, 10)

	assert.Equal(t, "cuda", cached.Backend(),
		"CachedEmbedder.Backend() must delegate to wrapped (D-06)")

	// Sanity: the wrapped value is what we configured.
	assert.Equal(t, "cuda", wrapped.Backend())
}

// TestEmbedderBackend_LocalGGUFStub asserts the !localllm stub returns "cpu"
// (the stub never executes a model — process-perspective is the trivial CPU
// fallback path).
func TestEmbedderBackend_LocalGGUFStub(t *testing.T) {
	// LocalGGUFEmbedder zero-value is the stub when build tag !localllm;
	// when build tag localllm, this exercises the production type whose
	// Backend() reads localGGUFBackend (build-tagged var).
	var stub LocalGGUFEmbedder
	got := stub.Backend()
	assert.Truef(t, allowedBackends[got],
		"LocalGGUFEmbedder.Backend()=%q must be in closed enum", got)
}

// fixedBackendMock is a test double that implements Embedder with a
// configurable Backend() return value — used to verify CachedEmbedder
// delegation.
type fixedBackendMock struct {
	backend string
}

func (m *fixedBackendMock) Embed(ctx context.Context, text string) ([]float32, error) {
	return nil, nil
}

func (m *fixedBackendMock) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	return nil, nil
}

func (m *fixedBackendMock) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return []string{text}, nil
}

func (m *fixedBackendMock) Dimensions() int { return 0 }
func (m *fixedBackendMock) Model() string   { return "fixed-mock" }
func (m *fixedBackendMock) Backend() string { return m.backend }
