package embed

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/pkg/textchunk"
	voyageapi "github.com/orneryd/nornicdb/pkg/voyage"
)

const (
	VoyageModeText           = "text"
	VoyageModeContextualized = "contextualized"
	VoyageModeMultimodal     = "multimodal"

	// VoyageContextualizedDefaultChunkTokens is NornicDB's default provider
	// chunk size when Voyage contextualized auto-chunking is enabled.
	VoyageContextualizedDefaultChunkTokens = 512

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
		switch normalizeVoyageMode(cfg.Mode) {
		case VoyageModeContextualized:
			cfg.Model = voyageapi.DefaultContextModel
		case VoyageModeMultimodal:
			cfg.Model = voyageapi.DefaultMultimodalModel
		default:
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
	model := strings.TrimSpace(cfg.Model)
	if model == "" {
		switch mode {
		case VoyageModeContextualized:
			model = voyageapi.DefaultContextModel
		case VoyageModeMultimodal:
			model = voyageapi.DefaultMultimodalModel
		default:
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
	if e.mode == VoyageModeMultimodal {
		inputs := make([]any, len(texts))
		for index, text := range texts {
			inputs[index] = voyageapi.MultimodalInput{Content: []voyageapi.MultimodalPart{{Type: "text", Text: text}}}
		}
		resp, err := e.client.EmbedMultimodal(ctx, inputs, voyageapi.MultimodalOptions{
			Model: e.config.Model, InputType: inputType, Truncation: false, OutputDimension: e.config.Dimensions,
		})
		if err != nil {
			return nil, err
		}
		return orderedVoyageEmbeddings(resp, len(texts)), nil
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
	return orderedVoyageEmbeddings(resp, len(texts)), nil
}

func orderedVoyageEmbeddings(resp *voyageapi.EmbeddingResponse, count int) [][]float32 {
	results := make([][]float32, count)
	if resp == nil {
		return results
	}
	for _, item := range resp.Data {
		if item.Index >= 0 && item.Index < len(results) {
			results[item.Index] = item.Embedding
		}
	}
	return results
}

// UsesDocumentProperties reports whether this configured provider consumes a
// structured node property instead of flattened text.
func (e *VoyageEmbedder) UsesDocumentProperties() bool {
	return e.mode == VoyageModeMultimodal
}

// EmbedDocumentPropertyChunks embeds a managed document. The Voyage package
// owns the property name, structured schema, and input validation.
func (e *VoyageEmbedder) EmbedDocumentPropertyChunks(ctx context.Context, fallbackText string, properties map[string]any, _, _ int) (*DocumentChunkResult, error) {
	if e.mode != VoyageModeMultimodal {
		return e.EmbedDocumentChunks(ctx, fallbackText, 0, 0)
	}
	parts := []voyageapi.MultimodalPart{{Type: "text", Text: fallbackText}}
	if raw, ok := properties[voyageapi.MultimodalContentProperty]; ok {
		parsed, err := voyageapi.ParseMultimodalContent(raw)
		if err != nil {
			return nil, err
		}
		parts = parsed
	}
	if err := voyageapi.ValidateMultimodalContent(parts); err != nil {
		return nil, err
	}
	resp, err := e.client.EmbedMultimodal(ctx, []any{voyageapi.MultimodalInput{Content: parts}}, voyageapi.MultimodalOptions{
		Model: e.config.Model, InputType: InputTypeDocument, Truncation: false, OutputDimension: e.config.Dimensions,
	})
	if err != nil {
		return nil, err
	}
	chunks := make([]string, 0, len(parts))
	for _, part := range parts {
		if part.Type == "text" {
			chunks = append(chunks, part.Text)
		}
	}
	return &DocumentChunkResult{Chunks: chunks, Embeddings: orderedVoyageEmbeddings(resp, 1), Model: e.Model()}, nil
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
	chunkOverlap, chunkOverlapSet := voyageChunkOverlap(overlap)
	resp, err := e.client.EmbedContextualized(ctx, []string{text}, voyageapi.ContextualizedOptions{
		Model:              model,
		InputType:          InputTypeDocument,
		OutputDimension:    e.config.Dimensions,
		OutputDType:        "float",
		EnableAutoChunking: true,
		ChunkSize:          chunkSize,
		ChunkOverlap:       chunkOverlap,
		ChunkOverlapSet:    chunkOverlapSet,
	})
	if err != nil {
		return nil, err
	}
	return contextualizedDocumentResult(resp, model), nil
}

// EmbedDocumentBatchChunks batches documents that fit Voyage's request budget
// and falls back to the bounded long-document path for oversized inputs.
func (e *VoyageEmbedder) EmbedDocumentBatchChunks(ctx context.Context, texts []string, maxTokens, overlap int) ([]*DocumentChunkResult, error) {
	results := make([]*DocumentChunkResult, len(texts))
	if len(texts) == 0 {
		return results, nil
	}
	if e.mode != VoyageModeContextualized {
		for i, text := range texts {
			result, err := e.EmbedDocumentChunks(ctx, text, maxTokens, overlap)
			if err != nil {
				return nil, err
			}
			results[i] = result
		}
		return results, nil
	}

	shortTexts := make([]string, 0, len(texts))
	shortIndexes := make([]int, 0, len(texts))
	requestBytes := 0
	for i, text := range texts {
		if len(text) > VoyageContextualizedSafeRequestBytes {
			result, err := e.embedLongContextualizedDocument(ctx, text, e.contextModel, voyageContextualizedChunkSize(maxTokens), overlap)
			if err != nil {
				return nil, err
			}
			results[i] = result
			continue
		}
		if len(shortTexts) >= VoyageContextualizedMaxInputs || requestBytes+len(text) > VoyageContextualizedSafeRequestBytes {
			return nil, fmt.Errorf("contextualized document batch exceeds provider request limits")
		}
		requestBytes += len(text)
		shortTexts = append(shortTexts, text)
		shortIndexes = append(shortIndexes, i)
	}
	if len(shortTexts) == 0 {
		return results, nil
	}

	chunkOverlap, chunkOverlapSet := voyageChunkOverlap(overlap)
	resp, err := e.client.EmbedContextualized(ctx, shortTexts, voyageapi.ContextualizedOptions{
		Model:              e.contextModel,
		InputType:          InputTypeDocument,
		OutputDimension:    e.config.Dimensions,
		OutputDType:        "float",
		EnableAutoChunking: true,
		ChunkSize:          voyageContextualizedChunkSize(maxTokens),
		ChunkOverlap:       chunkOverlap,
		ChunkOverlapSet:    chunkOverlapSet,
	})
	if err != nil {
		return nil, err
	}
	for _, document := range resp.Data {
		if document.Index < 0 || document.Index >= len(shortIndexes) {
			continue
		}
		result := &DocumentChunkResult{Model: e.contextModel, ChunkerVersion: resp.ChunkerVersion}
		for _, item := range document.Data {
			result.Chunks = append(result.Chunks, item.Text)
			result.Embeddings = append(result.Embeddings, item.Embedding)
		}
		results[shortIndexes[document.Index]] = result
	}
	return results, nil
}

// embedLongContextualizedDocument splits only at the request boundary. Voyage
// still performs token-based, boundary-aware auto-chunking inside every
// segment, so chunk size has identical semantics for short and long documents.
func (e *VoyageEmbedder) embedLongContextualizedDocument(ctx context.Context, text, model string, chunkSize, overlap int) (*DocumentChunkResult, error) {
	result := &DocumentChunkResult{Model: model}
	chunkOverlap, chunkOverlapSet := voyageChunkOverlap(overlap)
	for _, segment := range splitVoyageContextualizedSegments(text, VoyageContextualizedSafeRequestBytes) {
		resp, err := e.client.EmbedContextualized(ctx, []string{segment}, voyageapi.ContextualizedOptions{
			Model:              model,
			InputType:          InputTypeDocument,
			OutputDimension:    e.config.Dimensions,
			OutputDType:        "float",
			EnableAutoChunking: true,
			ChunkSize:          chunkSize,
			ChunkOverlap:       chunkOverlap,
			ChunkOverlapSet:    chunkOverlapSet,
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
	}
	return result, nil
}

func voyageChunkOverlap(overlap int) (int, bool) {
	if overlap < 0 {
		return 0, false
	}
	return overlap, true
}

func splitVoyageContextualizedSegments(text string, maxBytes int) []string {
	if text == "" {
		return []string{""}
	}
	if maxBytes <= 0 || len(text) <= maxBytes {
		return []string{text}
	}
	segments := make([]string, 0, len(text)/maxBytes+1)
	for start := 0; start < len(text); {
		hardEnd := start + maxBytes
		if hardEnd >= len(text) {
			segments = append(segments, text[start:])
			break
		}
		for hardEnd > start && !utf8.RuneStart(text[hardEnd]) {
			hardEnd--
		}
		end := hardEnd
		lastBoundary := 0
		for offset, r := range text[start:hardEnd] {
			if unicode.IsSpace(r) || strings.ContainsRune(".!?;:。！？", r) {
				lastBoundary = offset + utf8.RuneLen(r)
			}
		}
		if lastBoundary >= maxBytes/2 {
			end = start + lastBoundary
		}
		segments = append(segments, text[start:end])
		start = end
	}
	return segments
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

// EmbeddingSpace returns a stable identity for compatible vectors. It includes
// the endpoint because custom Voyage-compatible services are distinct spaces.
func (e *VoyageEmbedder) EmbeddingSpace() string {
	return fmt.Sprintf("voyage:%s:%s:%d:%s", e.mode, e.Model(), e.Dimensions(), strings.TrimRight(e.config.APIURL, "/"))
}

func voyageContextualizedChunkSize(maxTokens int) int {
	if maxTokens <= 0 {
		return VoyageContextualizedDefaultChunkTokens
	}
	if maxTokens > VoyageContextualizedMaxChunkTokens {
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
