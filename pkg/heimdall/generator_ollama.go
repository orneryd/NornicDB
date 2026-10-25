// Package heimdall - Ollama-backed Generator for chat completions.
// When Heimdall provider is "ollama", NewManager uses this implementation.
package heimdall

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
)

const defaultOllamaAPIURL = "http://localhost:11434"
const ollamaChatPath = "/api/chat"

// ollamaGenerator implements Generator by calling Ollama's /api/chat API.
type ollamaGenerator struct {
	chatCompletionGenerator
	apiURL string
	model  string
	client *http.Client
}

// ollamaChatRequest is the request body for Ollama /api/chat.
type ollamaChatRequest struct {
	Model       string         `json:"model"`
	Messages    []chatMessage  `json:"messages"`
	Tools       []chatTool     `json:"tools,omitempty"`
	Stream      bool           `json:"stream,omitempty"`
	Options     *ollamaOptions `json:"options,omitempty"`
	NumPredict  *int           `json:"num_predict,omitempty"`
	Temperature *float32       `json:"temperature,omitempty"`
}

type ollamaOptions struct {
	NumPredict  int     `json:"num_predict,omitempty"`
	Temperature float32 `json:"temperature,omitempty"`
}

// ollamaChatResponse is the non-streaming response.
type ollamaChatResponse struct {
	Message chatMessage `json:"message"`
	Done    bool        `json:"done"`
}

// ollamaChatChunk is one line of NDJSON for streaming.
type ollamaChatChunk struct {
	Message struct {
		Content string `json:"content"`
	} `json:"message"`
	Done bool `json:"done"`
}

func init() {
	RegisterHeimdallProvider("ollama", newOllamaGenerator)
}

// newOllamaGenerator creates a Generator that uses the Ollama /api/chat API.
func newOllamaGenerator(cfg Config) (Generator, error) {
	apiURL := cfg.APIURL
	if apiURL == "" {
		apiURL = defaultOllamaAPIURL
	}
	configuredURL := strings.TrimSpace(apiURL)
	apiURL, err := observability.NormalizeRequestBaseURL(configuredURL)
	if err != nil {
		return nil, fmt.Errorf("ollama provider URL: %w", observability.RedactClientError(err, configuredURL))
	}
	model := cfg.Model
	if model == "" {
		model = "llama3.2"
	}
	g := &ollamaGenerator{
		apiURL: apiURL,
		model:  model,
		client: &http.Client{Timeout: 120 * time.Second},
	}
	g.chatCompletionGenerator = chatCompletionGenerator{
		complete:    g.completeChat,
		credentials: func() (string, string) { return g.apiURL, "" },
	}
	return g, nil
}

// requestChat owns request serialization, authentication and response admission for all generation modes.
func (g *ollamaGenerator) requestChat(ctx context.Context, messages []chatMessage, tools []chatTool, params GenerateParams, stream bool) (*http.Response, error) {
	numPredict := params.MaxTokens
	if numPredict == 0 {
		numPredict = 1024
	}
	temp := params.Temperature

	reqBody := ollamaChatRequest{
		Model:       g.model,
		Messages:    messages,
		Tools:       tools,
		Stream:      stream,
		NumPredict:  &numPredict,
		Temperature: &temp,
	}

	return requestProviderChat(ctx, g.client, "ollama", g.apiURL, ollamaChatPath, "", reqBody)
}

// completeChat decodes the shared non-streaming response for plain and tool rounds.
func (g *ollamaGenerator) completeChat(ctx context.Context, messages []chatMessage, tools []chatTool, params GenerateParams) (chatMessage, error) {
	resp, err := g.requestChat(ctx, messages, tools, params, false)
	if err != nil {
		return chatMessage{}, err
	}
	defer resp.Body.Close()

	var chatResp ollamaChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&chatResp); err != nil {
		return chatMessage{}, fmt.Errorf("ollama decode: %w", observability.RedactError(err, observability.ClientCredentials(g.apiURL)...))
	}
	return chatResp.Message, nil
}

// GenerateStream implements Generator.
func (g *ollamaGenerator) GenerateStream(ctx context.Context, prompt string, params GenerateParams, callback func(token string) error) (err error) {
	defer func() {
		err = observability.RedactClientError(err, g.apiURL)
	}()
	resp, err := g.requestChat(ctx, []chatMessage{{Role: "user", Content: prompt}}, nil, params, true)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var chunk ollamaChatChunk
		if err := json.Unmarshal(line, &chunk); err != nil {
			continue
		}
		if chunk.Message.Content != "" {
			if err := callback(chunk.Message.Content); err != nil {
				return err
			}
		}
		if chunk.Done {
			break
		}
	}
	return scanner.Err()
}

// Close implements Generator (no-op for HTTP client).
func (g *ollamaGenerator) Close() error {
	return nil
}

// ModelPath implements Generator (returns display name for logging).
func (g *ollamaGenerator) ModelPath() string {
	return "ollama:" + g.model
}
