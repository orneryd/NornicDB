// Package embed provides embedding generation clients for vector search.
// This file implements the Apple NLEmbedding provider for macOS.

//go:build darwin

package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// AppleEmbedder implements Embedder using macOS NLEmbedding via the menu bar app.
//
// This provider calls a local HTTP endpoint exposed by the NornicDB menu bar app,
// which uses Apple's NaturalLanguage framework for embeddings.
//
// Benefits:
//   - No model download required (built into macOS)
//   - Privacy-preserving (all local)
//   - Fast on Apple Silicon
//   - Works offline
//
// Limitations:
//   - Only works on macOS
//   - Requires the NornicDB menu bar app to be running
//   - Lower quality than dedicated embedding models
//   - ~512 dimensions (vs 1024 for BGE-M3)
//
// Example:
//
//	config := embed.DefaultAppleConfig()
//	embedder := embed.NewApple(config)
//
//	embedding, err := embedder.Embed(ctx, "hello world")
//	if err != nil {
//		log.Fatal(err)
//	}
//	// embedding is ~512 float32 values
type AppleEmbedder struct {
	config *Config
	client *http.Client
}

// DefaultAppleConfig returns configuration for Apple NLEmbedding.
//
// Default settings:
//   - Provider: apple
//   - API URL: http://localhost:7475 (menu bar app embedding service)
//   - Dimensions: 512 (NLEmbedding default)
//   - Timeout: 10 seconds (should be fast)
func DefaultAppleConfig() *Config {
	return &Config{
		Provider:   "apple",
		APIURL:     "http://localhost:7475",
		APIPath:    "/embed",
		Model:      "NLEmbedding",
		Dimensions: 512,
		Timeout:    10 * time.Second,
	}
}

// NewApple creates an Apple NLEmbedding embedder.
//
// Example:
//
//	embedder := embed.NewApple(nil) // Uses defaults
//
//	embedding, err := embedder.Embed(ctx, "graph database")
//	if err != nil {
//		log.Fatal(err)
//	}
func NewApple(config *Config) *AppleEmbedder {
	if config == nil {
		config = DefaultAppleConfig()
	}
	if config.Timeout == 0 {
		config.Timeout = 10 * time.Second
	}
	if config.APIURL == "" {
		config.APIURL = "http://localhost:7475"
	}
	if config.APIPath == "" {
		config.APIPath = "/embed"
	}
	if config.Dimensions == 0 {
		config.Dimensions = 512
	}

	return &AppleEmbedder{
		config: config,
		client: &http.Client{
			Timeout: config.Timeout,
		},
	}
}

// appleEmbedRequest is the request to the Apple embedding service
type appleEmbedRequest struct {
	Text  string   `json:"text,omitempty"`
	Texts []string `json:"texts,omitempty"`
}

// appleEmbedResponse is the response from the Apple embedding service
type appleEmbedResponse struct {
	Embedding  []float32   `json:"embedding,omitempty"`
	Embeddings [][]float32 `json:"embeddings,omitempty"`
	Error      string      `json:"error,omitempty"`
}

// Embed generates embedding for single text using Apple NLEmbedding.
func (a *AppleEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	if text == "" {
		return nil, fmt.Errorf("empty text")
	}

	reqBody := appleEmbedRequest{Text: text}
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	url := a.config.APIURL + a.config.APIPath
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("apple embedding service unavailable (is the menu bar app running?): %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("apple embedding service returned %d: %s", resp.StatusCode, string(body))
	}

	var result appleEmbedResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	if result.Error != "" {
		return nil, fmt.Errorf("apple embedding error: %s", result.Error)
	}

	if len(result.Embedding) == 0 {
		return nil, fmt.Errorf("no embedding returned")
	}

	return result.Embedding, nil
}

// EmbedBatch generates embeddings for multiple texts.
func (a *AppleEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	reqBody := appleEmbedRequest{Texts: texts}
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	url := a.config.APIURL + a.config.APIPath + "/batch"
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("apple embedding service unavailable (is the menu bar app running?): %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("apple embedding service returned %d: %s", resp.StatusCode, string(body))
	}

	var result appleEmbedResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	if result.Error != "" {
		return nil, fmt.Errorf("apple embedding error: %s", result.Error)
	}

	return result.Embeddings, nil
}

// Dimensions returns the embedding vector dimension (~512 for NLEmbedding).
func (a *AppleEmbedder) Dimensions() int {
	return a.config.Dimensions
}

// Model returns "NLEmbedding".
func (a *AppleEmbedder) Model() string {
	return "NLEmbedding"
}

// IsAvailable checks if the Apple embedding service is available.
func (a *AppleEmbedder) IsAvailable() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	url := a.config.APIURL + "/health"
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return false
	}

	resp, err := a.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}
