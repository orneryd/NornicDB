package nornicdb

import (
	"context"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/envutil"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type queryExpansionPassageResolver struct {
	db     *DB
	engine storage.Engine
}

func (r *queryExpansionPassageResolver) ResolvePassages(ctx context.Context, sources []search.ExpansionSource) ([]search.ExpansionSource, error) {
	if r == nil || r.db == nil || r.engine == nil {
		return nil, nil
	}
	embedder, config := r.embeddingInputs()
	chunker, ok := embedder.(deterministicTextChunker)
	if !ok || config == nil {
		return nil, nil
	}

	resolved := make([]search.ExpansionSource, 0, len(sources))
	for _, source := range sources {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		chunkIndex, isChunk, valid := queryExpansionChunkIndex(source.VectorID)
		if !valid {
			continue
		}
		node, err := r.engine.GetNode(storage.NodeID(source.NodeID))
		if err != nil || node == nil {
			continue
		}
		options := embeddingutil.EmbedTextOptionsFromFields(config.PropertiesInclude, config.PropertiesExclude, config.IncludeLabels)
		text := embeddingutil.BuildText(node.Properties, node.Labels, options)
		if isChunk {
			chunks, err := chunker.ChunkText(text, config.ChunkSize, config.ChunkOverlap)
			if err != nil || chunkIndex >= len(chunks) {
				continue
			}
			text = chunks[chunkIndex]
		}
		if text == "" {
			continue
		}
		source.Text = truncateExpansionPassage(text, envutil.GetInt("NORNICDB_SEARCH_QUERY_EXPANSION_MAX_PASSAGE_CHARS", 2048))
		resolved = append(resolved, source)
	}
	return resolved, nil
}

func (r *queryExpansionPassageResolver) embeddingInputs() (embed.Embedder, *EmbedWorkerConfig) {
	r.db.mu.RLock()
	queue := r.db.embedQueue
	config := r.db.embedWorkerConfig
	r.db.mu.RUnlock()
	if queue == nil || config == nil {
		return nil, nil
	}
	queue.mu.Lock()
	embedder := queue.embedder
	queue.mu.Unlock()
	copy := *config
	copy.PropertiesInclude = append([]string(nil), config.PropertiesInclude...)
	copy.PropertiesExclude = append([]string(nil), config.PropertiesExclude...)
	return embedder, &copy
}

func queryExpansionChunkIndex(vectorID string) (index int, isChunk bool, valid bool) {
	if strings.Contains(vectorID, "-named-") || strings.Contains(vectorID, "-prop-") {
		return 0, false, false
	}
	marker := strings.LastIndex(vectorID, "-chunk-")
	if marker < 0 {
		return 0, false, vectorID != ""
	}
	index, err := strconv.Atoi(vectorID[marker+len("-chunk-"):])
	if err != nil || index < 0 {
		return 0, false, false
	}
	return index, true, true
}

func truncateExpansionPassage(text string, limit int) string {
	if limit < 128 {
		limit = 128
	}
	if len(text) <= limit {
		return text
	}
	end := limit
	for end > 0 && !utf8.RuneStart(text[end]) {
		end--
	}
	return text[:end]
}
