package embed

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// --- ChunkText coverage (0% on all embedder types) ---

func TestCachedEmbedder_ChunkText(t *testing.T) {
	mock := &mockEmbedder{}
	cached := NewCachedEmbedder(mock, 100)

	chunks, err := cached.ChunkText("hello world", 512, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 || chunks[0] != "hello world" {
		t.Errorf("Expected [\"hello world\"], got %v", chunks)
	}
}

func TestOllamaEmbedder_ChunkText(t *testing.T) {
	e := NewOllama(nil)
	chunks, err := e.ChunkText("some text", 512, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 || chunks[0] != "some text" {
		t.Errorf("Expected [\"some text\"], got %v", chunks)
	}
}

func TestOpenAIEmbedder_ChunkText(t *testing.T) {
	e := NewOpenAI(DefaultOpenAIConfig("fake-key"))
	chunks, err := e.ChunkText("some text", 512, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 || chunks[0] != "some text" {
		t.Errorf("Expected [\"some text\"], got %v", chunks)
	}
}

func TestLocalGGUFStub_ChunkText(t *testing.T) {
	e := &LocalGGUFEmbedder{}
	_, err := e.ChunkText("text", 512, 50)
	if err == nil {
		t.Fatal("Expected error from stub ChunkText")
	}
}

// --- NewCachedEmbedder default size path (66.7% → covers maxSize <= 0) ---

func TestNewCachedEmbedder_DefaultSize(t *testing.T) {
	mock := &mockEmbedder{}

	cached := NewCachedEmbedder(mock, 0)
	if cached.maxSize != 10000 {
		t.Errorf("Expected default maxSize 10000, got %d", cached.maxSize)
	}

	cached = NewCachedEmbedder(mock, -5)
	if cached.maxSize != 10000 {
		t.Errorf("Expected default maxSize 10000 for negative input, got %d", cached.maxSize)
	}
}

// --- CachedEmbedder error propagation from base embedder ---

type errorEmbedder struct{}

func (e *errorEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	return nil, fmt.Errorf("embed failed")
}
func (e *errorEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	return nil, fmt.Errorf("batch failed")
}
func (e *errorEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return nil, fmt.Errorf("chunk failed")
}
func (e *errorEmbedder) Dimensions() int { return 0 }
func (e *errorEmbedder) Model() string   { return "error" }
func (e *errorEmbedder) Backend() string { return "cpu" } // Plan 04-05 D-06: closed enum

func TestCachedEmbedder_Embed_BaseError(t *testing.T) {
	cached := NewCachedEmbedder(&errorEmbedder{}, 100)
	_, err := cached.Embed(context.Background(), "test")
	if err == nil {
		t.Fatal("Expected error propagation from base embedder")
	}
}

func TestCachedEmbedder_EmbedBatch_BaseError(t *testing.T) {
	cached := NewCachedEmbedder(&errorEmbedder{}, 100)
	_, err := cached.EmbedBatch(context.Background(), []string{"a", "b"})
	if err == nil {
		t.Fatal("Expected error propagation from base batch")
	}
}

func TestCachedEmbedder_ChunkText_BaseError(t *testing.T) {
	cached := NewCachedEmbedder(&errorEmbedder{}, 100)
	_, err := cached.ChunkText("text", 512, 50)
	if err == nil {
		t.Fatal("Expected error propagation from base ChunkText")
	}
}

// --- LocalGGUFStub remaining methods ---

func TestLocalGGUFStub_MetadataAndClose(t *testing.T) {
	e := &LocalGGUFEmbedder{}

	if e.Dimensions() != 0 {
		t.Errorf("Expected 0 dimensions from stub, got %d", e.Dimensions())
	}
	if e.Model() != "" {
		t.Errorf("Expected empty model from stub, got %q", e.Model())
	}
	stats := e.Stats()
	if stats.EmbedCount != 0 {
		t.Errorf("Expected 0 embed count, got %d", stats.EmbedCount)
	}
	if err := e.Close(); err != nil {
		t.Errorf("Expected nil from stub Close, got %v", err)
	}
}

func TestNewLocalGGUF_Stub(t *testing.T) {
	_, err := NewLocalGGUF(&Config{Provider: "local"})
	if err == nil {
		t.Fatal("Expected error from stub NewLocalGGUF")
	}
}

// --- OllamaEmbedder bad JSON response ---

func TestOllamaEmbedder_Embed_BadJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("not json"))
	}))
	defer srv.Close()

	cfg := DefaultOllamaConfig()
	cfg.APIURL = srv.URL
	e := NewOllama(cfg)

	_, err := e.Embed(context.Background(), "test")
	if err == nil {
		t.Fatal("Expected error for bad JSON response")
	}
}

// --- OpenAI bad JSON response ---

func TestOpenAIEmbedder_EmbedBatch_BadJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("{broken"))
	}))
	defer srv.Close()

	cfg := DefaultOpenAIConfig("key")
	cfg.APIURL = srv.URL
	e := NewOpenAI(cfg)

	_, err := e.EmbedBatch(context.Background(), []string{"a"})
	if err == nil {
		t.Fatal("Expected error for bad JSON")
	}
}

// --- Connection refused (covers http.Client.Do error branch) ---

func TestOpenAIEmbedder_EmbedBatch_ConnectionRefused(t *testing.T) {
	cfg := DefaultOpenAIConfig("key")
	cfg.APIURL = "http://127.0.0.1:1"
	e := NewOpenAI(cfg)

	_, err := e.EmbedBatch(context.Background(), []string{"a"})
	if err == nil {
		t.Fatal("Expected error for connection refused")
	}
}
