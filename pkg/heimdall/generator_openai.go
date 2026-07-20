// Package heimdall - OpenAI-backed Generator for chat completions.
// When Heimdall provider is "openai", NewManager uses this implementation.
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
	"unicode/utf8"
)

const defaultOpenAIBaseURL = "https://api.openai.com"
const defaultVLLMBaseURL = "http://localhost:8000"
const defaultOpenAIModel = "gpt-4o-mini"
const openAIChatPath = "/v1/chat/completions"

// openAIMaxContentPerMessage is OpenAI's per-message content limit (10MB).
// Truncate tool results and other content to stay under this to avoid 400 string_above_max_length.
const openAIMaxContentPerMessage = 10*1024*1024 - 8*1024 // ~10MB minus 8KB margin

// OpenAI context limits (best practice: stay under total context, reserve space for output).
const openAIContextLimit = 128000 // gpt-4o / gpt-4o-mini typical limit
const openAIOutputReserve = 4096  // reserve for model response
const openAIMaxInputTokens = openAIContextLimit - openAIOutputReserve
const openAIMaxTokensPerToolResult = 16384 // cap each tool result so one round doesn't dominate

// looksLikeLocalModel returns true if the model name is clearly a local/GGUF model
// (e.g. from config default or YAML) so we do not send it to the OpenAI API.
func looksLikeLocalModel(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return true
	}
	if strings.Contains(s, ".gguf") {
		return true
	}
	return false
}

// openAIGenerator implements Generator by calling OpenAI (or compatible) chat API.
// Supports both non-streaming (Generate) and streaming (GenerateStream) via stream: true and SSE.
type openAIGenerator struct {
	baseURL string
	apiKey  string
	model   string
	client  *http.Client
}

// openAIChatRequest is the request body for OpenAI chat completions.
type openAIChatRequest struct {
	Model       string          `json:"model"`
	Messages    []openAIMsgWire `json:"messages"`
	Tools       []openAITool    `json:"tools,omitempty"`
	Stream      bool            `json:"stream,omitempty"`
	MaxTokens   int             `json:"max_tokens,omitempty"`
	Temperature float32         `json:"temperature,omitempty"`
}

type openAIMsg struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// openAIMsgWire is the wire format for messages (can include tool_calls and tool_call_id).
type openAIMsgWire struct {
	Role       string               `json:"role"`
	Content    string               `json:"content"`
	ToolCalls  []openAIToolCallWire `json:"tool_calls,omitempty"`
	ToolCallID string               `json:"tool_call_id,omitempty"`
}

type openAIToolCallWire struct {
	Id       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type openAITool struct {
	Type     string `json:"type"`
	Function struct {
		Name        string          `json:"name"`
		Description string          `json:"description"`
		Parameters  json.RawMessage `json:"parameters"`
	} `json:"function"`
}

// openAIChatResponse is the non-streaming response.
type openAIChatResponse struct {
	Choices []struct {
		Message struct {
			Content   string               `json:"content"`
			ToolCalls []openAIToolCallWire `json:"tool_calls,omitempty"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
}

// openAIChatChunk is one SSE chunk for streaming.
type openAIChatChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
}

func init() {
	RegisterHeimdallProvider("openai", newOpenAIGenerator)
	RegisterHeimdallProvider("vllm", newVLLMGenerator)
}

// newVLLMGenerator creates a Generator that uses a vLLM server's OpenAI-compatible API.
// vLLM serves the same /v1/chat/completions endpoint, so this reuses openAIGenerator
// with vLLM-friendly defaults: no API key required, base URL defaults to localhost:8000,
// and the configured model name is used as-is (no looksLikeLocalModel filtering).
func newVLLMGenerator(cfg Config) (Generator, error) {
	baseURL := cfg.APIURL
	if baseURL == "" {
		baseURL = defaultVLLMBaseURL
	}
	baseURL = strings.TrimSuffix(baseURL, "/")
	model := strings.TrimSpace(cfg.Model)
	if model == "" {
		return nil, fmt.Errorf("vllm provider requires NORNICDB_HEIMDALL_MODEL (the model served by vLLM)")
	}
	apiKey := cfg.APIKey
	if apiKey == "" {
		apiKey = "EMPTY" // vLLM accepts any key when auth is disabled
	}
	return &openAIGenerator{
		baseURL: baseURL,
		apiKey:  apiKey,
		model:   model,
		client: &http.Client{
			Timeout: 120 * time.Second,
		},
	}, nil
}

// newOpenAIGenerator creates a Generator that uses the OpenAI chat completions API.
func newOpenAIGenerator(cfg Config) (Generator, error) {
	baseURL := cfg.APIURL
	if baseURL == "" {
		baseURL = defaultOpenAIBaseURL
	}
	baseURL = strings.TrimSuffix(baseURL, "/")
	model := strings.TrimSpace(cfg.Model)
	if looksLikeLocalModel(model) {
		model = defaultOpenAIModel
	}
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("openai provider requires NORNICDB_HEIMDALL_API_KEY")
	}
	return &openAIGenerator{
		baseURL: baseURL,
		apiKey:  cfg.APIKey,
		model:   model,
		client: &http.Client{
			Timeout: 120 * time.Second,
		},
	}, nil
}

// Generate implements Generator.
func (g *openAIGenerator) Generate(ctx context.Context, prompt string, params GenerateParams) (string, error) {
	reqBody := openAIChatRequest{
		Model: g.model,
		Messages: []openAIMsgWire{
			{Role: "user", Content: prompt},
		},
		Stream:      false,
		MaxTokens:   params.MaxTokens,
		Temperature: params.Temperature,
	}
	if reqBody.MaxTokens == 0 {
		reqBody.MaxTokens = 1024
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("openai request: %w", err)
	}

	url := g.baseURL + openAIChatPath
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("openai request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+g.apiKey)

	resp, err := g.client.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("openai request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("openai returned %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var chatResp openAIChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&chatResp); err != nil {
		return "", fmt.Errorf("openai decode: %w", err)
	}
	if len(chatResp.Choices) == 0 {
		return "", fmt.Errorf("openai: no choices in response")
	}
	return chatResp.Choices[0].Message.Content, nil
}

// truncateContentToTokenEstimate truncates content so estimated tokens <= maxTokens.
// Uses EstimateTokens (chars/4); backs up to rune boundary.
func truncateContentToTokenEstimate(content string, maxTokens int) string {
	if maxTokens <= 0 {
		return ""
	}
	maxChars := maxTokens * 4 // EstimateTokens uses ~4 chars per token
	if len(content) <= maxChars {
		return content
	}
	trunc := content[:maxChars]
	// Ensure we never return invalid UTF-8 after byte-length truncation.
	// DecodeLastRuneInString returns (RuneError, 1) for invalid trailing bytes,
	// so we trim until the suffix is a complete rune boundary.
	for len(trunc) > 0 {
		r, size := utf8.DecodeLastRuneInString(trunc)
		if r != utf8.RuneError || size != 1 {
			break
		}
		trunc = trunc[:len(trunc)-1]
	}
	return trunc + "\n\n[Truncated for context limit.]"
}

// trimMessagesForContext keeps messages within maxInputTokens by capping tool result
// sizes and, if needed, dropping oldest assistant+tool rounds (always keeps system + user).
func trimMessagesForContext(messages []ToolRoundMessage, maxInputTokens int) []ToolRoundMessage {
	if len(messages) <= 2 {
		return messages
	}
	// First pass: cap each tool result to openAIMaxTokensPerToolResult
	out := make([]ToolRoundMessage, 0, len(messages))
	for _, m := range messages {
		msg := m
		if msg.Role == "tool" && EstimateTokens(msg.Content) > openAIMaxTokensPerToolResult {
			msg.Content = truncateContentToTokenEstimate(msg.Content, openAIMaxTokensPerToolResult)
		}
		out = append(out, msg)
	}
	messages = out
	if EstimateToolRoundMessagesTokens(messages) <= maxInputTokens {
		return messages
	}
	// Second pass: drop oldest assistant+tool round(s); keep [system, user] and recent rounds
	for len(messages) > 2 && EstimateToolRoundMessagesTokens(messages) > maxInputTokens {
		// Remove one round: from index 2, remove assistant and all following tool messages until next assistant or end
		i := 2
		for i < len(messages) && messages[i].Role != "assistant" {
			i++
		}
		// i is first assistant after system+user, or len(messages)
		if i >= len(messages) {
			break
		}
		j := i + 1
		for j < len(messages) && messages[j].Role == "tool" {
			j++
		}
		// Remove messages[i:j]
		messages = append(messages[:i], messages[j:]...)
	}
	return messages
}

// truncateForOpenAI ensures content is within OpenAI's per-message limit.
// Truncates at a UTF-8 rune boundary to avoid invalid encoding.
func truncateForOpenAI(content string) string {
	if len(content) <= openAIMaxContentPerMessage {
		return content
	}
	max := openAIMaxContentPerMessage
	if max > len(content) {
		max = len(content)
	}
	// Back up to start of last rune so we don't split UTF-8.
	for max > 0 && !utf8.RuneStart(content[max-1]) {
		max--
	}
	suffix := fmt.Sprintf("\n\n[Content truncated for API limit; %d chars omitted]", len(content)-max)
	// Leave room for suffix so total length stays under limit.
	if max+len(suffix) > openAIMaxContentPerMessage {
		max = openAIMaxContentPerMessage - len(suffix)
		for max > 0 && !utf8.RuneStart(content[max-1]) {
			max--
		}
	}
	return content[:max] + suffix
}

// GenerateWithTools implements GeneratorWithTools (agentic loop, one round).
// Trims messages to stay within model context (128K) and returns a friendly error if context is exceeded.
func (g *openAIGenerator) GenerateWithTools(ctx context.Context, messages []ToolRoundMessage, tools []MCPTool, params GenerateParams) (content string, toolCalls []ParsedToolCall, err error) {
	messages = trimMessagesForContext(messages, openAIMaxInputTokens)
	wire := make([]openAIMsgWire, 0, len(messages))
	for _, m := range messages {
		msg := openAIMsgWire{Role: m.Role, Content: truncateForOpenAI(m.Content), ToolCallID: m.ToolCallID}
		if len(m.ToolCalls) > 0 {
			msg.ToolCalls = make([]openAIToolCallWire, len(m.ToolCalls))
			for i, tc := range m.ToolCalls {
				msg.ToolCalls[i] = openAIToolCallWire{
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
	openAITools := make([]openAITool, len(tools))
	for i, t := range tools {
		openAITools[i] = openAITool{
			Type: "function",
			Function: struct {
				Name        string          `json:"name"`
				Description string          `json:"description"`
				Parameters  json.RawMessage `json:"parameters"`
			}{Name: t.Name, Description: t.Description, Parameters: t.InputSchema},
		}
	}
	maxTok := params.MaxTokens
	if maxTok == 0 {
		maxTok = 1024
	}
	reqBody := openAIChatRequest{
		Model:       g.model,
		Messages:    wire,
		Tools:       openAITools,
		Stream:      false,
		MaxTokens:   maxTok,
		Temperature: params.Temperature,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return "", nil, fmt.Errorf("openai request: %w", err)
	}
	url := g.baseURL + openAIChatPath
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return "", nil, fmt.Errorf("openai request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+g.apiKey)
	resp, err := g.client.Do(httpReq)
	if err != nil {
		return "", nil, fmt.Errorf("openai request: %w", err)
	}
	defer resp.Body.Close()
	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		bodyStr := string(bodyBytes)
		if resp.StatusCode == http.StatusBadRequest && strings.Contains(bodyStr, "context_length_exceeded") {
			return "", nil, fmt.Errorf("conversation is too long for the model (context limit exceeded). Please start a new chat or ask a shorter question")
		}
		return "", nil, fmt.Errorf("openai returned %d: %s", resp.StatusCode, bodyStr)
	}
	var chatResp openAIChatResponse
	if err := json.Unmarshal(bodyBytes, &chatResp); err != nil {
		return "", nil, fmt.Errorf("openai decode: %w", err)
	}
	if len(chatResp.Choices) == 0 {
		return "", nil, fmt.Errorf("openai: no choices in response")
	}
	msg := chatResp.Choices[0].Message
	content = msg.Content
	for _, tc := range msg.ToolCalls {
		toolCalls = append(toolCalls, ParsedToolCall{
			Id:        tc.Id,
			Name:      tc.Function.Name,
			Arguments: tc.Function.Arguments,
		})
	}
	return content, toolCalls, nil
}

// GenerateStream implements Generator.
func (g *openAIGenerator) GenerateStream(ctx context.Context, prompt string, params GenerateParams, callback func(token string) error) error {
	reqBody := openAIChatRequest{
		Model: g.model,
		Messages: []openAIMsgWire{
			{Role: "user", Content: prompt},
		},
		Stream:      true,
		MaxTokens:   params.MaxTokens,
		Temperature: params.Temperature,
	}
	if reqBody.MaxTokens == 0 {
		reqBody.MaxTokens = 1024
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("openai request: %w", err)
	}

	url := g.baseURL + openAIChatPath
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("openai request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+g.apiKey)

	resp, err := g.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("openai request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("openai returned %d: %s", resp.StatusCode, string(bodyBytes))
	}

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			break
		}
		var chunk openAIChatChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			continue
		}
		if len(chunk.Choices) > 0 && chunk.Choices[0].Delta.Content != "" {
			if err := callback(chunk.Choices[0].Delta.Content); err != nil {
				return err
			}
		}
		if len(chunk.Choices) > 0 && chunk.Choices[0].FinishReason != nil {
			break
		}
	}
	return scanner.Err()
}

// Close implements Generator (no-op for HTTP client).
func (g *openAIGenerator) Close() error {
	return nil
}

// ModelPath implements Generator (returns display name for logging).
func (g *openAIGenerator) ModelPath() string {
	return "openai:" + g.model
}
