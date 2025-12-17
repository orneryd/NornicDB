// Package embed tests for embedding generation clients.
package embed

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDefaultOllamaConfig(t *testing.T) {
	config := DefaultOllamaConfig()

	if config.Provider != "ollama" {
		t.Errorf("expected ollama, got %s", config.Provider)
	}
	if config.APIURL != "http://localhost:11434" {
		t.Errorf("expected localhost:11434, got %s", config.APIURL)
	}
	if config.APIPath != "/api/embeddings" {
		t.Errorf("expected /api/embeddings, got %s", config.APIPath)
	}
	if config.Model != "bge-m3" {
		t.Errorf("expected mxbai-embed-large, got %s", config.Model)
	}
	if config.Dimensions != 1024 {
		t.Errorf("expected 1024 dimensions, got %d", config.Dimensions)
	}
	if config.Timeout != 30*time.Second {
		t.Errorf("expected 30s timeout, got %v", config.Timeout)
	}
}

func TestDefaultOpenAIConfig(t *testing.T) {
	config := DefaultOpenAIConfig("test-key")

	if config.Provider != "openai" {
		t.Errorf("expected openai, got %s", config.Provider)
	}
	if config.APIURL != "https://api.openai.com" {
		t.Errorf("expected api.openai.com, got %s", config.APIURL)
	}
	if config.APIPath != "/v1/embeddings" {
		t.Errorf("expected /v1/embeddings, got %s", config.APIPath)
	}
	if config.APIKey != "test-key" {
		t.Errorf("expected test-key, got %s", config.APIKey)
	}
	if config.Model != "text-embedding-3-small" {
		t.Errorf("expected text-embedding-3-small, got %s", config.Model)
	}
	if config.Dimensions != 1536 {
		t.Errorf("expected 1536 dimensions, got %d", config.Dimensions)
	}
}

func TestNewOllama(t *testing.T) {
	t.Run("with config", func(t *testing.T) {
		config := &Config{
			Provider:   "ollama",
			APIURL:     "http://custom:8080",
			Model:      "custom-model",
			Dimensions: 512,
			Timeout:    10 * time.Second,
		}
		embedder := NewOllama(config)

		if embedder.config.APIURL != "http://custom:8080" {
			t.Error("should use custom config")
		}
	})

	t.Run("with nil config", func(t *testing.T) {
		embedder := NewOllama(nil)

		if embedder.config.APIURL != "http://localhost:11434" {
			t.Error("should use default config")
		}
	})
}

func TestNewOpenAI(t *testing.T) {
	t.Run("with config", func(t *testing.T) {
		config := &Config{
			Provider: "openai",
			APIKey:   "sk-test",
		}
		embedder := NewOpenAI(config)

		if embedder.config.APIKey != "sk-test" {
			t.Error("should use custom config")
		}
	})

	t.Run("with nil config", func(t *testing.T) {
		embedder := NewOpenAI(nil)

		if embedder.config.Provider != "openai" {
			t.Error("should use default config")
		}
	})
}

func TestOllamaEmbedder(t *testing.T) {
	// Create mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Error("expected JSON content type")
		}

		var req ollamaRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("failed to decode request: %v", err)
		}

		resp := ollamaResponse{
			Embedding: []float32{0.1, 0.2, 0.3, 0.4},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	config := &Config{
		Provider:   "ollama",
		APIURL:     server.URL,
		APIPath:    "/api/embeddings",
		Model:      "test-model",
		Dimensions: 4,
		Timeout:    5 * time.Second,
	}

	embedder := NewOllama(config)

	t.Run("Embed", func(t *testing.T) {
		embedding, err := embedder.Embed(context.Background(), "hello world")
		if err != nil {
			t.Fatalf("Embed() error = %v", err)
		}

		if len(embedding) != 4 {
			t.Errorf("expected 4 dimensions, got %d", len(embedding))
		}
	})

	t.Run("EmbedBatch", func(t *testing.T) {
		texts := []string{"hello", "world"}
		embeddings, err := embedder.EmbedBatch(context.Background(), texts)
		if err != nil {
			t.Fatalf("EmbedBatch() error = %v", err)
		}

		if len(embeddings) != 2 {
			t.Errorf("expected 2 embeddings, got %d", len(embeddings))
		}
	})

	t.Run("Dimensions", func(t *testing.T) {
		if embedder.Dimensions() != 4 {
			t.Errorf("expected 4 dimensions, got %d", embedder.Dimensions())
		}
	})

	t.Run("Model", func(t *testing.T) {
		if embedder.Model() != "test-model" {
			t.Errorf("expected test-model, got %s", embedder.Model())
		}
	})
}

func TestOllamaEmbedderError(t *testing.T) {
	// Create mock server that returns error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	}))
	defer server.Close()

	config := &Config{
		Provider: "ollama",
		APIURL:   server.URL,
		APIPath:  "/api/embeddings",
		Timeout:  5 * time.Second,
	}

	embedder := NewOllama(config)

	_, err := embedder.Embed(context.Background(), "test")
	if err == nil {
		t.Error("expected error for server error response")
	}
}

func TestOpenAIEmbedder(t *testing.T) {
	// Create mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Error("expected Authorization header")
		}

		var req openaiRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("failed to decode request: %v", err)
		}

		resp := openaiResponse{
			Data: []struct {
				Embedding []float32 `json:"embedding"`
				Index     int       `json:"index"`
			}{
				{Embedding: []float32{0.1, 0.2, 0.3}, Index: 0},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	config := &Config{
		Provider:   "openai",
		APIURL:     server.URL,
		APIPath:    "/v1/embeddings",
		APIKey:     "test-key",
		Model:      "text-embedding-3-small",
		Dimensions: 3,
		Timeout:    5 * time.Second,
	}

	embedder := NewOpenAI(config)

	t.Run("Embed", func(t *testing.T) {
		embedding, err := embedder.Embed(context.Background(), "hello world")
		if err != nil {
			t.Fatalf("Embed() error = %v", err)
		}

		if len(embedding) != 3 {
			t.Errorf("expected 3 dimensions, got %d", len(embedding))
		}
	})

	t.Run("EmbedBatch", func(t *testing.T) {
		texts := []string{"hello"}
		embeddings, err := embedder.EmbedBatch(context.Background(), texts)
		if err != nil {
			t.Fatalf("EmbedBatch() error = %v", err)
		}

		if len(embeddings) != 1 {
			t.Errorf("expected 1 embedding, got %d", len(embeddings))
		}
	})

	t.Run("Dimensions", func(t *testing.T) {
		if embedder.Dimensions() != 3 {
			t.Errorf("expected 3 dimensions, got %d", embedder.Dimensions())
		}
	})

	t.Run("Model", func(t *testing.T) {
		if embedder.Model() != "text-embedding-3-small" {
			t.Errorf("expected text-embedding-3-small, got %s", embedder.Model())
		}
	})
}

func TestOpenAIEmbedderError(t *testing.T) {
	// Create mock server that returns error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte("invalid api key"))
	}))
	defer server.Close()

	config := &Config{
		Provider: "openai",
		APIURL:   server.URL,
		APIPath:  "/v1/embeddings",
		APIKey:   "bad-key",
		Timeout:  5 * time.Second,
	}

	embedder := NewOpenAI(config)

	_, err := embedder.Embed(context.Background(), "test")
	if err == nil {
		t.Error("expected error for auth error response")
	}
}

func TestOpenAIEmbedderNoEmbedding(t *testing.T) {
	// Create mock server that returns empty data
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := openaiResponse{
			Data: []struct {
				Embedding []float32 `json:"embedding"`
				Index     int       `json:"index"`
			}{},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	config := &Config{
		Provider: "openai",
		APIURL:   server.URL,
		APIPath:  "/v1/embeddings",
		APIKey:   "test-key",
		Timeout:  5 * time.Second,
	}

	embedder := NewOpenAI(config)

	_, err := embedder.Embed(context.Background(), "test")
	if err == nil {
		t.Error("expected error for no embedding returned")
	}
}

func TestNewEmbedder(t *testing.T) {
	t.Run("ollama provider", func(t *testing.T) {
		config := &Config{
			Provider: "ollama",
		}
		embedder, err := NewEmbedder(config)
		if err != nil {
			t.Fatalf("NewEmbedder() error = %v", err)
		}

		if _, ok := embedder.(*OllamaEmbedder); !ok {
			t.Error("expected OllamaEmbedder")
		}
	})

	t.Run("openai provider", func(t *testing.T) {
		config := &Config{
			Provider: "openai",
			APIKey:   "test-key",
		}
		embedder, err := NewEmbedder(config)
		if err != nil {
			t.Fatalf("NewEmbedder() error = %v", err)
		}

		if _, ok := embedder.(*OpenAIEmbedder); !ok {
			t.Error("expected OpenAIEmbedder")
		}
	})

	t.Run("openai without key", func(t *testing.T) {
		config := &Config{
			Provider: "openai",
		}
		_, err := NewEmbedder(config)
		if err == nil {
			t.Error("expected error for missing API key")
		}
	})

	t.Run("unknown provider", func(t *testing.T) {
		config := &Config{
			Provider: "unknown",
		}
		_, err := NewEmbedder(config)
		if err == nil {
			t.Error("expected error for unknown provider")
		}
	})
}

func TestEmbedderInterface(t *testing.T) {
	// Verify both embedders implement the interface
	var _ Embedder = (*OllamaEmbedder)(nil)
	var _ Embedder = (*OpenAIEmbedder)(nil)
}

func TestConfigStruct(t *testing.T) {
	config := Config{
		Provider:   "test",
		APIURL:     "http://test",
		APIPath:    "/test",
		APIKey:     "key",
		Model:      "model",
		Dimensions: 100,
		Timeout:    time.Minute,
	}

	if config.Provider != "test" {
		t.Error("wrong provider")
	}
	if config.APIURL != "http://test" {
		t.Error("wrong API URL")
	}
	if config.APIPath != "/test" {
		t.Error("wrong API path")
	}
	if config.APIKey != "key" {
		t.Error("wrong API key")
	}
	if config.Model != "model" {
		t.Error("wrong model")
	}
	if config.Dimensions != 100 {
		t.Error("wrong dimensions")
	}
	if config.Timeout != time.Minute {
		t.Error("wrong timeout")
	}
}

func TestOllamaEmbedBatchError(t *testing.T) {
	// Server that fails on second request
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if requestCount > 1 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("error"))
			return
		}
		resp := ollamaResponse{
			Embedding: []float32{0.1, 0.2, 0.3},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	config := &Config{
		Provider: "ollama",
		APIURL:   server.URL,
		APIPath:  "/api/embeddings",
		Timeout:  5 * time.Second,
	}

	embedder := NewOllama(config)

	texts := []string{"first", "second"}
	_, err := embedder.EmbedBatch(context.Background(), texts)
	if err == nil {
		t.Error("expected error for failed batch item")
	}
}

func TestContextCancellation(t *testing.T) {
	// Server that delays response
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		resp := ollamaResponse{Embedding: []float32{0.1}}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	config := &Config{
		Provider: "ollama",
		APIURL:   server.URL,
		APIPath:  "/api/embeddings",
		Timeout:  5 * time.Second,
	}

	embedder := NewOllama(config)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := embedder.Embed(ctx, "test")
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}
