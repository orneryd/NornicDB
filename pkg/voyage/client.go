// Package voyage provides a small HTTP client for Voyage AI embedding and rerank APIs.
package voyage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	DefaultBaseURL           = "https://api.voyageai.com"
	DefaultEmbeddingModel    = "voyage-4-large"
	DefaultContextModel      = "voyage-context-4"
	DefaultRerankModel       = "rerank-2.5"
	DefaultOutputDimension   = 1024
	defaultTimeout           = 30 * time.Second
	defaultMaxRetries        = 2
	defaultRetryBackoff      = 100 * time.Millisecond
	pathEmbeddings           = "/v1/embeddings"
	pathContextualEmbeddings = "/v1/contextualizedembeddings"
	pathMultimodalEmbeddings = "/v1/multimodalembeddings"
	pathRerank               = "/v1/rerank"
	inputTypeQuery           = "query"
	inputTypeDocument        = "document"
	defaultOutputDType       = "float"
	defaultRerankReturnDocs  = false
)

// Config configures a Voyage API client.
type Config struct {
	APIKey       string
	BaseURL      string
	HTTPClient   *http.Client
	Timeout      time.Duration
	MaxRetries   int
	RetryBackoff time.Duration
}

// Client calls Voyage AI HTTP APIs.
type Client struct {
	apiKey       string
	baseURL      string
	httpClient   *http.Client
	maxRetries   int
	retryBackoff time.Duration
}

// NewClient returns a Voyage API client.
func NewClient(cfg Config) (*Client, error) {
	apiKey := strings.TrimSpace(cfg.APIKey)
	if apiKey == "" {
		return nil, fmt.Errorf("voyage API key is required")
	}
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: timeout}
	}
	maxRetries := cfg.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	} else if maxRetries == 0 {
		maxRetries = defaultMaxRetries
	}
	retryBackoff := cfg.RetryBackoff
	if retryBackoff < 0 {
		retryBackoff = 0
	} else if retryBackoff == 0 {
		retryBackoff = defaultRetryBackoff
	}
	return &Client{
		apiKey:       apiKey,
		baseURL:      baseURL,
		httpClient:   httpClient,
		maxRetries:   maxRetries,
		retryBackoff: retryBackoff,
	}, nil
}

// Usage reports provider token accounting returned by Voyage.
type Usage struct {
	TotalTokens int `json:"total_tokens"`
}

// EmbeddingData is one embedding item in a Voyage response.
type EmbeddingData struct {
	Embedding []float32 `json:"embedding"`
	Index     int       `json:"index"`
	Text      string    `json:"text,omitempty"`
}

// EmbeddingOptions controls text embeddings.
type EmbeddingOptions struct {
	Model           string
	InputType       string
	Truncation      bool
	OutputDimension int
	OutputDType     string
}

// EmbeddingResponse is the normalized Voyage embeddings response.
type EmbeddingResponse struct {
	Data  []EmbeddingData
	Model string
	Usage Usage
}

type embeddingRequest struct {
	Input           []string `json:"input"`
	Model           string   `json:"model"`
	InputType       string   `json:"input_type,omitempty"`
	Truncation      bool     `json:"truncation"`
	OutputDimension int      `json:"output_dimension,omitempty"`
	OutputDType     string   `json:"output_dtype,omitempty"`
}

type embeddingResponseWire struct {
	Object string          `json:"object"`
	Data   []EmbeddingData `json:"data"`
	Model  string          `json:"model"`
	Usage  Usage           `json:"usage"`
}

// EmbedText embeds text inputs with Voyage's text embedding endpoint.
func (c *Client) EmbedText(ctx context.Context, inputs []string, opts EmbeddingOptions) (*EmbeddingResponse, error) {
	if len(inputs) == 0 {
		return &EmbeddingResponse{}, nil
	}
	model := strings.TrimSpace(opts.Model)
	if model == "" {
		model = DefaultEmbeddingModel
	}
	outputDType := strings.TrimSpace(opts.OutputDType)
	if outputDType == "" {
		outputDType = defaultOutputDType
	}
	req := embeddingRequest{
		Input:           inputs,
		Model:           model,
		InputType:       normalizeInputType(opts.InputType),
		Truncation:      opts.Truncation,
		OutputDimension: opts.OutputDimension,
		OutputDType:     outputDType,
	}
	var wire embeddingResponseWire
	if err := c.postJSON(ctx, pathEmbeddings, req, &wire); err != nil {
		return nil, err
	}
	return &EmbeddingResponse{Data: wire.Data, Model: wire.Model, Usage: wire.Usage}, nil
}

// ContextualizedOptions controls contextualized chunk embeddings.
type ContextualizedOptions struct {
	Model              string
	InputType          string
	OutputDimension    int
	OutputDType        string
	EnableAutoChunking bool
	ChunkSize          int
	ChunkOverlap       int
	// ChunkOverlapSet sends ChunkOverlap even when it is explicitly zero.
	// Positive values remain implicitly set for source compatibility.
	ChunkOverlapSet bool
}

// ContextualizedResult is one query/document result from the contextualized endpoint.
type ContextualizedResult struct {
	Data  []EmbeddingData `json:"data"`
	Index int             `json:"index"`
}

// ContextualizedResponse is the normalized Voyage contextualized response.
type ContextualizedResponse struct {
	Data           []ContextualizedResult
	Model          string
	Usage          Usage
	ChunkerVersion string
}

type contextualizedRequest struct {
	Inputs             any    `json:"inputs"`
	Model              string `json:"model"`
	InputType          string `json:"input_type,omitempty"`
	OutputDimension    int    `json:"output_dimension,omitempty"`
	OutputDType        string `json:"output_dtype,omitempty"`
	EnableAutoChunking bool   `json:"enable_auto_chunking,omitempty"`
	ChunkSize          int    `json:"chunk_size,omitempty"`
	ChunkOverlap       *int   `json:"chunk_overlap,omitempty"`
}

type contextualizedResponseWire struct {
	Data           []ContextualizedResult `json:"data"`
	Model          string                 `json:"model"`
	Usage          Usage                  `json:"usage"`
	ChunkerVersion string                 `json:"chunker_version"`
}

// EmbedContextualized embeds document chunks or full documents with Voyage context-aware embeddings.
func (c *Client) EmbedContextualized(ctx context.Context, inputs any, opts ContextualizedOptions) (*ContextualizedResponse, error) {
	model := strings.TrimSpace(opts.Model)
	if model == "" {
		model = DefaultContextModel
	}
	outputDType := strings.TrimSpace(opts.OutputDType)
	if outputDType == "" {
		outputDType = defaultOutputDType
	}
	var chunkOverlap *int
	if opts.ChunkOverlapSet || opts.ChunkOverlap != 0 {
		value := opts.ChunkOverlap
		chunkOverlap = &value
	}
	req := contextualizedRequest{
		Inputs:             inputs,
		Model:              model,
		InputType:          normalizeInputType(opts.InputType),
		OutputDimension:    opts.OutputDimension,
		OutputDType:        outputDType,
		EnableAutoChunking: opts.EnableAutoChunking,
		ChunkSize:          opts.ChunkSize,
		ChunkOverlap:       chunkOverlap,
	}
	var wire contextualizedResponseWire
	if err := c.postJSON(ctx, pathContextualEmbeddings, req, &wire); err != nil {
		return nil, err
	}
	return &ContextualizedResponse{
		Data:           wire.Data,
		Model:          wire.Model,
		Usage:          wire.Usage,
		ChunkerVersion: wire.ChunkerVersion,
	}, nil
}

// MultimodalOptions controls multimodal embeddings.
type MultimodalOptions struct {
	Model           string
	InputType       string
	Truncation      bool
	OutputDimension int
	// OutputDType is retained for source compatibility. Voyage multimodal
	// responses are floating-point vectors unless output_encoding is requested.
	OutputDType string
}

// EmbedMultimodal embeds text/image multimodal inputs. Inputs are intentionally
// represented as JSON-compatible values so callers can supply the Voyage wire shape.
func (c *Client) EmbedMultimodal(ctx context.Context, inputs []any, opts MultimodalOptions) (*EmbeddingResponse, error) {
	if err := validateMultimodalInputs(inputs); err != nil {
		return nil, err
	}
	model := strings.TrimSpace(opts.Model)
	if model == "" {
		model = DefaultMultimodalModel
	}
	req := map[string]any{
		"inputs":     inputs,
		"model":      model,
		"input_type": normalizeInputType(opts.InputType),
		"truncation": opts.Truncation,
	}
	if opts.OutputDimension > 0 {
		req["output_dimension"] = opts.OutputDimension
	}
	var wire embeddingResponseWire
	if err := c.postJSON(ctx, pathMultimodalEmbeddings, req, &wire); err != nil {
		return nil, err
	}
	return &EmbeddingResponse{Data: wire.Data, Model: wire.Model, Usage: wire.Usage}, nil
}

// RerankOptions controls Voyage reranking.
type RerankOptions struct {
	Model           string
	TopK            int
	ReturnDocuments bool
	Truncation      bool
}

// RerankResult is one normalized rerank result.
type RerankResult struct {
	Index          int
	RelevanceScore float64
	Document       string
}

// RerankResponse is the normalized Voyage rerank response.
type RerankResponse struct {
	Data  []RerankResult
	Model string
	Usage Usage
}

type rerankRequest struct {
	Query           string   `json:"query"`
	Documents       []string `json:"documents"`
	Model           string   `json:"model"`
	TopK            int      `json:"top_k,omitempty"`
	ReturnDocuments bool     `json:"return_documents"`
	Truncation      bool     `json:"truncation"`
}

type rerankItemWire struct {
	Index          int     `json:"index"`
	RelevanceScore float64 `json:"relevance_score"`
	Document       string  `json:"document,omitempty"`
}

type rerankResponseWire struct {
	Data    []rerankItemWire `json:"data"`
	Results []rerankItemWire `json:"results"`
	Model   string           `json:"model"`
	Usage   Usage            `json:"usage"`
}

// Rerank calls Voyage's reranker endpoint.
func (c *Client) Rerank(ctx context.Context, query string, documents []string, opts RerankOptions) (*RerankResponse, error) {
	if len(documents) == 0 {
		return &RerankResponse{}, nil
	}
	model := strings.TrimSpace(opts.Model)
	if model == "" {
		model = DefaultRerankModel
	}
	req := rerankRequest{
		Query:           query,
		Documents:       documents,
		Model:           model,
		TopK:            opts.TopK,
		ReturnDocuments: opts.ReturnDocuments,
		Truncation:      opts.Truncation,
	}
	if !opts.ReturnDocuments {
		req.ReturnDocuments = defaultRerankReturnDocs
	}
	var wire rerankResponseWire
	if err := c.postJSON(ctx, pathRerank, req, &wire); err != nil {
		return nil, err
	}
	items := wire.Data
	if len(items) == 0 {
		items = wire.Results
	}
	out := make([]RerankResult, 0, len(items))
	for _, item := range items {
		out = append(out, RerankResult{
			Index:          item.Index,
			RelevanceScore: item.RelevanceScore,
			Document:       item.Document,
		})
	}
	return &RerankResponse{Data: out, Model: wire.Model, Usage: wire.Usage}, nil
}

func (c *Client) postJSON(ctx context.Context, path string, request any, response any) error {
	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("marshal voyage request: %w", err)
	}
	var lastErr error
	attempts := c.maxRetries + 1
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 && c.retryBackoff > 0 {
			timer := time.NewTimer(c.retryBackoff * time.Duration(attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		err = c.postOnce(ctx, path, body, response)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isRetryable(err) {
			break
		}
	}
	return lastErr
}

type apiError struct {
	StatusCode int
	Body       string
}

func (e apiError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("voyage API returned status %d", e.StatusCode)
	}
	return fmt.Sprintf("voyage API returned status %d: %s", e.StatusCode, e.Body)
}

// Retryable reports whether the API response could succeed on a later attempt.
// Request-validation 4xx responses are permanent; rate limits and server
// failures may recover and are safe to retry.
func (e apiError) Retryable() bool {
	return e.StatusCode == http.StatusTooManyRequests || e.StatusCode >= 500
}

func (c *Client) postOnce(ctx context.Context, path string, body []byte, response any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create voyage request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send voyage request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return apiError{StatusCode: resp.StatusCode, Body: strings.TrimSpace(string(raw))}
	}
	if err := json.NewDecoder(resp.Body).Decode(response); err != nil {
		return fmt.Errorf("decode voyage response: %w", err)
	}
	return nil
}

func isRetryable(err error) bool {
	var apiErr apiError
	if errors.As(err, &apiErr) {
		return apiErr.Retryable()
	}
	return strings.Contains(err.Error(), "send voyage request")
}

func normalizeInputType(inputType string) string {
	switch strings.ToLower(strings.TrimSpace(inputType)) {
	case inputTypeQuery:
		return inputTypeQuery
	case inputTypeDocument:
		return inputTypeDocument
	default:
		return ""
	}
}
