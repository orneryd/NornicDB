package embeddingutil

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// EmbedTextOptions controls which properties and labels are used when building embedding text.
type EmbedTextOptions struct {
	Include       []string
	Exclude       []string
	IncludeLabels bool
}

// BuildText creates canonical embedding text from node properties and labels.
func BuildText(properties map[string]interface{}, labels []string, opts *EmbedTextOptions) string {
	if opts == nil {
		opts = &EmbedTextOptions{IncludeLabels: true}
	}
	var parts []string

	if opts.IncludeLabels && len(labels) > 0 {
		parts = append(parts, fmt.Sprintf("labels: %s", strings.Join(labels, ", ")))
	}

	// One rule decides which properties feed the text: storage's, which also
	// decides when an update changes a node's embedding source (#963). Keys
	// are taken in order so the same content always gives the same text.
	policy := storage.EmbeddingTextPolicy{Include: opts.Include, Exclude: opts.Exclude, IncludeLabels: opts.IncludeLabels}
	keys := make([]string, 0, len(properties))
	for key := range properties {
		if policy.FeedsText(key) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	for _, key := range keys {
		val := properties[key]

		var strVal string
		switch v := val.(type) {
		case string:
			strVal = v
		case []interface{}:
			strs := make([]string, 0, len(v))
			for _, item := range v {
				if s, ok := item.(string); ok {
					strs = append(strs, s)
				} else {
					strs = append(strs, fmt.Sprintf("%v", item))
				}
			}
			strVal = strings.Join(strs, ", ")
		case bool:
			strVal = fmt.Sprintf("%v", v)
		case int, int64, float64:
			strVal = fmt.Sprintf("%v", v)
		case nil:
			strVal = "null"
		default:
			if b, err := json.Marshal(v); err == nil {
				strVal = string(b)
			} else {
				strVal = fmt.Sprintf("%v", v)
			}
		}
		parts = append(parts, fmt.Sprintf("%s: %s", key, strVal))
	}

	result := strings.Join(parts, "\n")
	if result == "" {
		return "node"
	}
	return result
}

// IsMetadataPropertyKey reports whether a property key is internal embedding metadata.
func IsMetadataPropertyKey(key string) bool {
	return storage.IsEmbeddingMetadataProperty(key)
}

// TextPolicy is opts as storage's embedding text policy.
func TextPolicy(opts *EmbedTextOptions) storage.EmbeddingTextPolicy {
	if opts == nil {
		return storage.EmbeddingTextPolicy{IncludeLabels: true}
	}
	return storage.EmbeddingTextPolicy{Include: opts.Include, Exclude: opts.Exclude, IncludeLabels: opts.IncludeLabels}
}

// InvalidateManagedEmbeddings clears worker-managed embedding state on a node.
func InvalidateManagedEmbeddings(node *storage.Node) {
	if node == nil {
		return
	}
	node.ChunkEmbeddings = nil
	node.EmbedMeta = nil
}

// EmbedTextOptionsFromConfig maps runtime embedding worker config to text builder options.
func EmbedTextOptionsFromConfig(cfg *config.Config) *EmbedTextOptions {
	if cfg == nil {
		return &EmbedTextOptions{IncludeLabels: true}
	}
	return EmbedTextOptionsFromFields(
		cfg.EmbeddingWorker.PropertiesInclude,
		cfg.EmbeddingWorker.PropertiesExclude,
		cfg.EmbeddingWorker.IncludeLabels,
	)
}

// EmbedTextOptionsFromFields builds text options from raw include/exclude settings.
func EmbedTextOptionsFromFields(include []string, exclude []string, includeLabels bool) *EmbedTextOptions {
	return &EmbedTextOptions{
		Include:       include,
		Exclude:       exclude,
		IncludeLabels: includeLabels,
	}
}

// ApplyManagedEmbedding writes worker-compatible embedding payload to node fields.
func ApplyManagedEmbedding(node *storage.Node, embeddings [][]float32, model string, dimensions int, embeddedAt time.Time) {
	if node == nil {
		return
	}
	node.ChunkEmbeddings = embeddings
	if node.EmbedMeta == nil {
		node.EmbedMeta = make(map[string]any)
	}
	node.EmbedMeta["chunk_count"] = len(embeddings)
	node.EmbedMeta["embedding_model"] = model
	node.EmbedMeta["embedding_dimensions"] = dimensions
	node.EmbedMeta["has_embedding"] = len(embeddings) > 0
	node.EmbedMeta["embedded_at"] = embeddedAt.Format(time.RFC3339)
}

