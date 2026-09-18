package embed

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/textchunk"
	voyageapi "github.com/orneryd/nornicdb/pkg/voyage"
)

const (
	VoyageModeText           = "text"
	VoyageModeContextualized = "contextualized"
	VoyageModeMultimodal     = "multimodal"

	// VoyageContextualizedMaxChunkTokens is Voyage's maximum auto-chunk size
	// for voyage-context-4 contextualized chunk embeddings.
	VoyageContextualizedMaxChunkTokens = 32000

	// VoyageContextualizedSafeRequestBytes is a conservative upper bound for
	// client-side contextualized requests. UTF-8 byte length is an upper bound
	// for the number of text tokens, leaving 20% of Voyage's 120K-token request
	// budget for provider-added input text.
	VoyageContextualizedSafeRequestBytes = 96000

	// VoyageContextualizedMaxInputs is a conservative cap on client-side chunks
	// in one contextualized request. A client-side request contains one group.
	VoyageContextualizedMaxInputs = 1000
)

// DefaultVoyageConfig returns a default Voyage embedding configuration.
func DefaultVoyageConfig(apiKey string) *Config {
	return &Config{
		Provider:   "voyage",
		APIURL:     voyageapi.DefaultBaseURL,
		APIPath:    "/v1/embeddings",
		APIKey:     apiKey,
		Model:      voyageapi.DefaultEmbeddingModel,
		Dimensions: voyageapi.DefaultOutputDimension,
		Timeout:    30 * time.Second,
		Mode:       VoyageModeText,
	}
}

func resolveVoyageConfig(config *Config) *Config {
	defaults := DefaultVoyageConfig("")
	if config == nil {
		return defaults
	}
	cfg := *config
	cfg.Provider = "voyage"
	if strings.TrimSpace(cfg.APIURL) == "" {
		cfg.APIURL = defaults.APIURL
	}
	if strings.TrimSpace(cfg.APIPath) == "" {
		cfg.APIPath = defaults.APIPath
	}
	if strings.TrimSpace(cfg.Model) == "" {
		if normalizeVoyageMode(cfg.Mode) == VoyageModeContextualized {
			cfg.Model = voyageapi.DefaultContextModel
		} else {
			cfg.Model = defaults.Model
		}
	}
	if cfg.Dimensions <= 0 {
		cfg.Dimensions = defaults.Dimensions
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaults.Timeout
	}
	return &cfg
}

// VoyageEmbedder implements Embedder for Voyage AI embeddings.
type VoyageEmbedder struct {
	config       *Config
	client       *voyageapi.Client
	mode         string
	textModel    string
	contextModel string
}

// NewVoyage creates a Voyage embedder.
func NewVoyage(config *Config) (*VoyageEmbedder, error) {
	if config == nil {
		config = DefaultVoyageConfig("")
	}
	cfg := *config
	cfg.Provider = strings.TrimSpace(strings.ToLower(cfg.Provider))
	if cfg.Provider == "" {
		cfg.Provider = "voyage"
	}
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("Voyage requires an API key")
	}
	if strings.TrimSpace(cfg.APIURL) == "" {
		cfg.APIURL = voyageapi.DefaultBaseURL
	}
	mode := normalizeVoyageMode(cfg.Mode)
	if mode == VoyageModeMultimodal {
		return nil, fmt.Errorf("Voyage multimodal mode is not supported for managed text embeddings")
	}
	model := strings.TrimSpace(cfg.Model)
	if model == "" {
		if mode == VoyageModeContextualized {
			model = voyageapi.DefaultContextModel
		} else {
			model = voyageapi.DefaultEmbeddingModel
		}
		cfg.Model = model
	}
	textModel := model
	contextModel := voyageapi.DefaultContextModel
	if mode == VoyageModeContextualized {
		if strings.HasPrefix(model, "voyage-context") {
			contextModel = model
			textModel = voyageapi.DefaultEmbeddingModel
		}
	} else {
		contextModel = voyageapi.DefaultContextModel
	}
	if cfg.Dimensions <= 0 {
		cfg.Dimensions = voyageapi.DefaultOutputDimension
	}
	client, err := voyageapi.NewClient(voyageapi.Config{
		APIKey:  cfg.APIKey,
		BaseURL: cfg.APIURL,
		Timeout: cfg.Timeout,
	})
	if err != nil {
		return nil, err
	}
	return &VoyageEmbedder{
		config:       &cfg,
		client:       client,
		mode:         mode,
		textModel:    textModel,
		contextModel: contextModel,
	}, nil
}

// Embed generates a query embedding for a single text string.
func (e *VoyageEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	return e.EmbedWithInputType(ctx, text, InputTypeQuery)
}

// EmbedBatch generates document embeddings for multiple texts.
func (e *VoyageEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	return e.EmbedBatchWithInputType(ctx, texts, InputTypeDocument)
}

func (e *VoyageEmbedder) EmbedWithInputType(ctx context.Context, text, inputType string) ([]float32, error) {
	vecs, err := e.EmbedBatchWithInputType(ctx, []string{text}, inputType)
	if err != nil || len(vecs) == 0 {
		return nil, err
	}
	return vecs[0], nil
}

func (e *VoyageEmbedder) EmbedBatchWithInputType(ctx context.Context, texts []string, inputType string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	resp, err := e.client.EmbedText(ctx, texts, voyageapi.EmbeddingOptions{
		Model:           e.textModel,
		InputType:       inputType,
		Truncation:      true,
		OutputDimension: e.config.Dimensions,
		OutputDType:     "float",
	})
	if err != nil {
		return nil, err
	}
	results := make([][]float32, len(texts))
	for _, item := range resp.Data {
		if item.Index >= 0 && item.Index < len(results) {
			results[item.Index] = item.Embedding
		}
	}
	return results, nil
}

func (e *VoyageEmbedder) EmbedDocumentChunks(ctx context.Context, text string, maxTokens, overlap int) (*DocumentChunkResult, error) {
	if e.mode != VoyageModeContextualized {
		chunks, err := e.ChunkText(text, maxTokens, overlap)
		if err != nil {
			return nil, err
		}
		embeddings, err := e.EmbedBatchWithInputType(ctx, chunks, InputTypeDocument)
		if err != nil {
			return nil, err
		}
		return &DocumentChunkResult{Chunks: chunks, Embeddings: embeddings, Model: e.Model()}, nil
	}
	model := e.contextModel
	chunkSize := voyageContextualizedChunkSize(maxTokens)
	if len(text) > VoyageContextualizedSafeRequestBytes {
		return e.embedLongContextualizedDocument(ctx, text, model, chunkSize, overlap)
	}
	resp, err := e.client.EmbedContextualized(ctx, []string{text}, voyageapi.ContextualizedOptions{
		Model:              model,
		InputType:          InputTypeDocument,
		OutputDimension:    e.config.Dimensions,
		OutputDType:        "float",
		EnableAutoChunking: true,
		ChunkSize:          chunkSize,
		ChunkOverlap:       overlap,
	})
	if err != nil {
		return nil, err
	}
	return contextualizedDocumentResult(resp, model), nil
}

// embedLongContextualizedDocument preserves contextualized embeddings for
// documents that cannot fit Voyage's 120K-token request budget. It makes
// conservative byte-bounded, locally chunked requests: chunks in each request
// retain context with their neighbouring chunks, while each group is safely
// below the provider's total-token and input-count limits.
func (e *VoyageEmbedder) embedLongContextualizedDocument(ctx context.Context, text, model string, chunkSize, overlap int) (*DocumentChunkResult, error) {
	chunks, err := e.ChunkText(text, chunkSize, overlap)
	if err != nil {
		return nil, err
	}

	result := &DocumentChunkResult{Model: model}
	for start := 0; start < len(chunks); {
		end, requestBytes := start, 0
		for end < len(chunks) && end-start < VoyageContextualizedMaxInputs {
			chunkBytes := len(chunks[end])
			if end > start && requestBytes+chunkBytes > VoyageContextualizedSafeRequestBytes {
				break
			}
			requestBytes += chunkBytes
			end++
		}
		// ChunkText uses byte length as its conservative token counter, so a
		// single chunk cannot exceed the request byte budget.
		if end == start {
			return nil, fmt.Errorf("contextualized chunk exceeds safe request size")
		}

		resp, err := e.client.EmbedContextualized(ctx, [][]string{chunks[start:end]}, voyageapi.ContextualizedOptions{
			Model:           model,
			InputType:       InputTypeDocument,
			OutputDimension: e.config.Dimensions,
			OutputDType:     "float",
		})
		if err != nil {
			return nil, err
		}
		batch := contextualizedDocumentResult(resp, model)
		result.Chunks = append(result.Chunks, batch.Chunks...)
		result.Embeddings = append(result.Embeddings, batch.Embeddings...)
		result.TotalTokens += batch.TotalTokens
		if batch.ChunkerVersion != "" {
			result.ChunkerVersion = batch.ChunkerVersion
		}
		start = end
	}
	return result, nil
}

func contextualizedDocumentResult(resp *voyageapi.ContextualizedResponse, model string) *DocumentChunkResult {
	result := &DocumentChunkResult{Model: model}
	if resp == nil {
		return result
	}
	result.ChunkerVersion = resp.ChunkerVersion
	result.TotalTokens = resp.Usage.TotalTokens
	for _, document := range resp.Data {
		for _, item := range document.Data {
			result.Chunks = append(result.Chunks, item.Text)
			result.Embeddings = append(result.Embeddings, item.Embedding)
		}
	}
	return result
}

func (e *VoyageEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return textchunk.ChunkByTokenCount(text, maxTokens, overlap, func(value string) (int, error) {
		return len(value), nil
	})
}

func (e *VoyageEmbedder) Dimensions() int {
	return e.config.Dimensions
}

func (e *VoyageEmbedder) Model() string {
	return e.config.Model
}

func (e *VoyageEmbedder) Backend() string {
	return "cpu"
}

func voyageContextualizedChunkSize(maxTokens int) int {
	if maxTokens <= 0 || maxTokens > VoyageContextualizedMaxChunkTokens {
		return VoyageContextualizedMaxChunkTokens
	}
	return maxTokens
}

func normalizeVoyageMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case VoyageModeContextualized:
		return VoyageModeContextualized
	case VoyageModeMultimodal:
		return VoyageModeMultimodal
	default:
		return VoyageModeText
	}
}
