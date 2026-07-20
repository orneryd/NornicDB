// Package heimdall - Ollama-backed Generator for chat completions.
// When Heimdall provider is "ollama", NewManager uses this implementation.
package heimdall

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const defaultOllamaAPIURL = "http://localhost:11434"
const ollamaChatPath = "/api/chat"

// ollamaGenerator implements Generator by calling Ollama's /api/chat API.
type ollamaGenerator struct {
	apiURL string
	model  string
	client *http.Client
}

// ollamaChatRequest is the request body for Ollama /api/chat.
type ollamaChatRequest struct {
	Model       string              `json:"model"`
	Messages    []ollamaMessageWire `json:"messages"`
	Tools       []ollamaTool        `json:"tools,omitempty"`
	Stream      bool                `json:"stream,omitempty"`
	Options     *ollamaOptions      `json:"options,omitempty"`
	NumPredict  *int                `json:"num_predict,omitempty"`
	Temperature *float32            `json:"temperature,omitempty"`
}

type ollamaMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ollamaMessageWire supports tool_calls and tool result (role "tool" with content).
type ollamaMessageWire struct {
	Role       string               `json:"role"`
	Content    string               `json:"content"`
	ToolCalls  []ollamaToolCallWire `json:"tool_calls,omitempty"`
	ToolCallID string               `json:"tool_call_id,omitempty"`
}

type ollamaToolCallWire struct {
	Id       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type ollamaTool struct {
	Type     string         `json:"type"`
	Function ollamaToolFunc `json:"function"`
}

type ollamaToolFunc struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

type ollamaOptions struct {
	NumPredict  int     `json:"num_predict,omitempty"`
	Temperature float32 `json:"temperature,omitempty"`
}

// ollamaChatResponse is the non-streaming response.
type ollamaChatResponse struct {
	Message struct {
		Role      string               `json:"role"`
		Content   string               `json:"content"`
		ToolCalls []ollamaToolCallWire `json:"tool_calls,omitempty"`
	} `json:"message"`
	Done bool `json:"done"`
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
	apiURL = strings.TrimSuffix(apiURL, "/")
	model := cfg.Model
	if model == "" {
		model = "llama3.2"
	}
	return &ollamaGenerator{
		apiURL: apiURL,
		model:  model,
		client: &http.Client{
			Timeout: 120 * time.Second,
		},
	}, nil
}

// Generate implements Generator.
func (g *ollamaGenerator) Generate(ctx context.Context, prompt string, params GenerateParams) (string, error) {
	numPredict := params.MaxTokens
	if numPredict == 0 {
		numPredict = 1024
	}
	temp := params.Temperature

	reqBody := ollamaChatRequest{
		Model: g.model,
		Messages: []ollamaMessageWire{
			{Role: "user", Content: prompt},
		},
		Stream:      false,
		NumPredict:  &numPredict,
		Temperature: &temp,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("ollama request: %w", err)
	}

	url := g.apiURL + ollamaChatPath
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("ollama request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := g.client.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("ollama request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("ollama returned %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var chatResp ollamaChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&chatResp); err != nil {
		return "", fmt.Errorf("ollama decode: %w", err)
	}
	return chatResp.Message.Content, nil
}

// GenerateWithTools implements GeneratorWithTools (agentic loop, one round).
func (g *ollamaGenerator) GenerateWithTools(ctx context.Context, messages []ToolRoundMessage, tools []MCPTool, params GenerateParams) (content string, toolCalls []ParsedToolCall, err error) {
	wire := make([]ollamaMessageWire, 0, len(messages))
	for _, m := range messages {
		msg := ollamaMessageWire{Role: m.Role, Content: m.Content, ToolCallID: m.ToolCallID}
		if len(m.ToolCalls) > 0 {
			msg.ToolCalls = make([]ollamaToolCallWire, len(m.ToolCalls))
			for i, tc := range m.ToolCalls {
				msg.ToolCalls[i] = ollamaToolCallWire{
					Id:   tc.Id,
					Type: "function",
					Function: struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					}{Name: tc.Name, Arguments: tc.Arguments},
				}
			}
		}
		wire = append(wire, msg)
	}
	ollamaTools := make([]ollamaTool, len(tools))
	for i, t := range tools {
		ollamaTools[i] = ollamaTool{
			Type:     "function",
			Function: ollamaToolFunc{Name: t.Name, Description: t.Description, Parameters: t.InputSchema},
		}
	}
	numPredict := params.MaxTokens
	if numPredict == 0 {
		numPredict = 1024
	}
	temp := params.Temperature
	reqBody := ollamaChatRequest{
		Model:       g.model,
		Messages:    wire,
		Tools:       ollamaTools,
		Stream:      false,
		NumPredict:  &numPredict,
		Temperature: &temp,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return "", nil, fmt.Errorf("ollama request: %w", err)
	}
	url := g.apiURL + ollamaChatPath
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return "", nil, fmt.Errorf("ollama request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := g.client.Do(httpReq)
	if err != nil {
		return "", nil, fmt.Errorf("ollama request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", nil, fmt.Errorf("ollama returned %d: %s", resp.StatusCode, string(bodyBytes))
	}
	var chatResp ollamaChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&chatResp); err != nil {
		return "", nil, fmt.Errorf("ollama decode: %w", err)
	}
	content = chatResp.Message.Content
	for _, tc := range chatResp.Message.ToolCalls {
		args := tc.Function.Arguments
		toolCalls = append(toolCalls, ParsedToolCall{
			Id:        tc.Id,
			Name:      tc.Function.Name,
			Arguments: args,
		})
	}
	return content, toolCalls, nil
}

// GenerateStream implements Generator.
func (g *ollamaGenerator) GenerateStream(ctx context.Context, prompt string, params GenerateParams, callback func(token string) error) error {
	numPredict := params.MaxTokens
	if numPredict == 0 {
		numPredict = 1024
	}
	temp := params.Temperature

	reqBody := ollamaChatRequest{
		Model: g.model,
		Messages: []ollamaMessageWire{
			{Role: "user", Content: prompt},
		},
		Stream:      true,
		NumPredict:  &numPredict,
		Temperature: &temp,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("ollama request: %w", err)
	}

	url := g.apiURL + ollamaChatPath
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("ollama request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := g.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("ollama request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ollama returned %d: %s", resp.StatusCode, string(bodyBytes))
	}

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
