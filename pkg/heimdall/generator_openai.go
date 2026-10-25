// Package heimdall - OpenAI-backed Generator for chat completions.
// When Heimdall provider is "openai", NewManager uses this implementation.
package heimdall

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/pkg/observability"
)

const defaultOpenAIBaseURL = "https://api.openai.com"
const defaultVLLMBaseURL = "http://localhost:8000"
const defaultLiteLLMBaseURL = "http://localhost:4000"
const defaultOrcaBaseURL = "https://api.orcarouter.ai"
const defaultOrcaModel = "orcarouter/auto"
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
	chatCompletionGenerator
	baseURL     string
	apiKey      string
	modelPrefix string
	model       string
	client      *http.Client
}

// openAIChatRequest is the request body for OpenAI chat completions.
type openAIChatRequest struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	Tools       []chatTool    `json:"tools,omitempty"`
	Stream      bool          `json:"stream,omitempty"`
	MaxTokens   int           `json:"max_tokens,omitempty"`
	Temperature float32       `json:"temperature,omitempty"`
}

// openAIChatResponse is the non-streaming response.
type openAIChatResponse struct {
	Choices []struct {
		Message      chatMessage `json:"message"`
		FinishReason string      `json:"finish_reason"`
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
	for _, provider := range []string{"openai", "vllm", "litellm", "orca"} {
		RegisterHeimdallProvider(provider, newOpenAIGenerator)
	}
}

// newOpenAIGenerator owns construction for OpenAI-compatible providers. Provider
// policy is selected here before the shared HTTP client and completion owner are
// initialized. Empty Provider retains the direct OpenAI constructor contract.
func newOpenAIGenerator(cfg Config) (Generator, error) {
	provider := strings.ToLower(strings.TrimSpace(cfg.Provider))
	baseURL, apiKey, model := cfg.APIURL, cfg.APIKey, strings.TrimSpace(cfg.Model)
	modelPrefix := "openai"
	if provider == "orca" {
		// Orca historically validates credentials before processing its URL and
		// only strips a terminal /v1; it does not normalize the configured URL.
		if strings.TrimSpace(apiKey) == "" {
			return nil, fmt.Errorf("orca provider requires NORNICDB_HEIMDALL_API_KEY")
		}
		baseURL = strings.TrimSpace(baseURL)
		if baseURL == "" {
			baseURL = defaultOrcaBaseURL
		}
		if parsed, err := url.Parse(baseURL); err == nil {
			parsed.Path = strings.TrimSuffix(strings.TrimSuffix(parsed.Path, "/"), "/v1")
			parsed.RawPath = strings.TrimSuffix(strings.TrimSuffix(parsed.RawPath, "/"), "/v1")
			baseURL = parsed.String()
		}
		if looksLikeLocalModel(model) {
			model = defaultOrcaModel
		}
		modelPrefix = "orca"
	} else {
		defaultURL := defaultOpenAIBaseURL
		switch provider {
		case "vllm":
			defaultURL = defaultVLLMBaseURL
		case "litellm":
			defaultURL, modelPrefix = defaultLiteLLMBaseURL, "litellm"
		default:
			provider = "openai"
		}
		if baseURL == "" {
			baseURL = defaultURL
		}
		configuredURL := strings.TrimSpace(baseURL)
		var err error
		baseURL, err = observability.NormalizeRequestBaseURL(configuredURL)
		if err != nil {
			return nil, fmt.Errorf("%s provider URL: %w", provider, observability.RedactClientError(err, configuredURL, cfg.APIKey))
		}
		if provider == "openai" {
			if looksLikeLocalModel(model) {
				model = defaultOpenAIModel
			}
			if apiKey == "" {
				return nil, fmt.Errorf("openai provider requires NORNICDB_HEIMDALL_API_KEY")
			}
		} else {
			if model == "" {
				if provider == "vllm" {
					return nil, fmt.Errorf("vllm provider requires NORNICDB_HEIMDALL_MODEL (the model served by vLLM)")
				}
				return nil, fmt.Errorf("litellm provider requires NORNICDB_HEIMDALL_MODEL (a model/alias served by your LiteLLM proxy)")
			}
			if apiKey == "" {
				apiKey = "EMPTY"
			}
		}
	}
	g := &openAIGenerator{
		baseURL: baseURL, apiKey: apiKey, model: model, modelPrefix: modelPrefix,
		client: &http.Client{Timeout: 120 * time.Second},
	}
	g.chatCompletionGenerator = chatCompletionGenerator{
		complete:         g.completeChat,
		credentials:      func() (string, string) { return g.baseURL, g.apiKey },
		toolContextLimit: openAIMaxInputTokens,
		toolContent:      truncateForOpenAI,
	}
	return g, nil
}

// chatStatusError retains the safe response facts required by provider policy.
type chatStatusError struct {
	provider string
	status   int
	body     string
}

func (e *chatStatusError) Error() string {
	return fmt.Sprintf("%s returned %d: %s", e.provider, e.status, e.body)
}

// requestProviderChat is the common JSON chat transport owner. Wire request
// types, streaming decoders and provider-specific response policy stay at callers.
// A successful response transfers body ownership; every rejected response closes here.
func requestProviderChat(ctx context.Context, client *http.Client, provider, baseURL, route, apiKey string, payload any) (_ *http.Response, err error) {
	defer func() { err = observability.RedactError(err, observability.ClientCredentials(baseURL, apiKey)...) }()
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("%s request: %w", provider, err)
	}
	endpoint, err := observability.ComposeRequestURL(baseURL, route, observability.AppendRequestRoute)
	if err != nil {
		return nil, fmt.Errorf("%s request: %w", provider, err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("%s request: %w", provider, err)
	}
	request.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		request.Header.Set("Authorization", "Bearer "+apiKey)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%s request: %w", provider, err)
	}
	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		return nil, &chatStatusError{provider: provider, status: response.StatusCode, body: observability.RedactText(string(body), observability.ClientCredentials(baseURL, apiKey)...)}
	}
	return response, nil
}

// requestChat owns request serialization, authentication and response admission for all generation modes.
func (g *openAIGenerator) requestChat(ctx context.Context, messages []chatMessage, tools []chatTool, params GenerateParams, stream bool) (*http.Response, error) {
	reqBody := openAIChatRequest{
		Model:       g.model,
		Messages:    messages,
		Tools:       tools,
		Stream:      stream,
		MaxTokens:   params.MaxTokens,
		Temperature: params.Temperature,
	}
	if reqBody.MaxTokens == 0 {
		reqBody.MaxTokens = 1024
	}

	resp, err := requestProviderChat(ctx, g.client, "openai", g.baseURL, openAIChatPath, g.apiKey, reqBody)
	var rejected *chatStatusError
	if tools != nil && errors.As(err, &rejected) && rejected.status == http.StatusBadRequest && strings.Contains(rejected.body, "context_length_exceeded") {
		return nil, fmt.Errorf("conversation is too long for the model (context limit exceeded). Please start a new chat or ask a shorter question")
	}
	return resp, err
}

// completeChat decodes the shared non-streaming response for plain and tool rounds.
func (g *openAIGenerator) completeChat(ctx context.Context, messages []chatMessage, tools []chatTool, params GenerateParams) (chatMessage, error) {
	resp, err := g.requestChat(ctx, messages, tools, params, false)
	if err != nil {
		return chatMessage{}, err
	}
	defer resp.Body.Close()

	var chatResp openAIChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&chatResp); err != nil {
		return chatMessage{}, fmt.Errorf("openai decode: %w", observability.RedactError(err, observability.ClientCredentials(g.baseURL, g.apiKey)...))
	}
	if len(chatResp.Choices) == 0 {
		return chatMessage{}, fmt.Errorf("openai: no choices in response")
	}
	return chatResp.Choices[0].Message, nil
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

// GenerateStream implements Generator.
func (g *openAIGenerator) GenerateStream(ctx context.Context, prompt string, params GenerateParams, callback func(token string) error) (err error) {
	defer func() {
		err = observability.RedactClientError(err, g.baseURL, g.apiKey)
	}()
	resp, err := g.requestChat(ctx, []chatMessage{{Role: "user", Content: prompt}}, nil, params, true)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

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
	prefix := g.modelPrefix
	if prefix == "" {
		prefix = "openai"
	}
	return prefix + ":" + g.model
}
