// Package heimdall - LiteLLM-backed Generator for chat completions.
// When Heimdall provider is "litellm", NewManager uses this implementation.
package heimdall

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

// defaultLiteLLMBaseURL is the default address of a local LiteLLM proxy.
// LiteLLM's proxy listens on :4000 by default and serves the OpenAI-compatible
// /v1/chat/completions endpoint.
const defaultLiteLLMBaseURL = "http://localhost:4000"

func init() {
	RegisterHeimdallProvider("litellm", newLiteLLMGenerator)
}

// liteLLMGenerator reuses the OpenAI-compatible generator (LiteLLM's proxy speaks
// the same protocol) but reports its own provider name for logging/observability.
type liteLLMGenerator struct {
	*openAIGenerator
}

// ModelPath implements Generator, tagging logs with the litellm provider.
func (g *liteLLMGenerator) ModelPath() string {
	return "litellm:" + g.model
}

// newLiteLLMGenerator creates a Generator that talks to a LiteLLM proxy.
//
// LiteLLM (https://github.com/BerriAI/litellm) is an AI gateway that exposes an
// OpenAI-compatible API in front of 100+ providers (OpenAI, Anthropic, Azure,
// Bedrock, Vertex, Gemini, Ollama, local models, …), and adds routing, fallbacks,
// virtual keys, and spend tracking. Because the proxy speaks the OpenAI chat
// protocol, this reuses openAIGenerator with LiteLLM-proxy-friendly defaults:
//   - base URL defaults to http://localhost:4000 (the proxy's default port);
//   - the configured model is used as-is (a model name/alias defined on the proxy,
//     e.g. "gpt-4o" or "anthropic/claude-sonnet-4-20250514");
//   - the API key is optional — a proxy without a master key accepts any value,
//     so an empty key falls back to a placeholder (matching the vllm provider).
func newLiteLLMGenerator(cfg Config) (Generator, error) {
	baseURL := cfg.APIURL
	if baseURL == "" {
		baseURL = defaultLiteLLMBaseURL
	}
	baseURL = strings.TrimSuffix(baseURL, "/")
	model := strings.TrimSpace(cfg.Model)
	if model == "" {
		return nil, fmt.Errorf("litellm provider requires NORNICDB_HEIMDALL_MODEL (a model/alias served by your LiteLLM proxy)")
	}
	apiKey := cfg.APIKey
	if apiKey == "" {
		apiKey = "EMPTY" // LiteLLM proxy without a master key accepts any value
	}
	return &liteLLMGenerator{
		openAIGenerator: &openAIGenerator{
			baseURL: baseURL,
			apiKey:  apiKey,
			model:   model,
			client: &http.Client{
				Timeout: 120 * time.Second,
			},
		},
	}, nil
}
