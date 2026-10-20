package nornicdb

import (
	"fmt"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/embeddingsource"
	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// nodeEmbeddingResult carries a producer's answer without deciding acceptance.
// Errors and absent documents use the same outcome boundary as vector answers.
type nodeEmbeddingResult struct {
	chunks   []string
	vectors  [][]float32
	metadata map[string]any
	err      error
}

func documentNodeEmbeddingResult(document *embed.DocumentChunkResult, err error) nodeEmbeddingResult {
	if document == nil {
		return nodeEmbeddingResult{err: err}
	}
	return nodeEmbeddingResult{chunks: document.Chunks, vectors: document.Embeddings, metadata: documentResultMeta(document), err: err}
}

// persistEmbeddedNode is the acceptance owner for every node dispatch shape.
// No node mutation or outcome commit precedes complete vector validation.
func (ew *EmbedWorker) persistEmbeddedNode(node *storage.Node, source embeddingutil.EmbedText, result nodeEmbeddingResult, provider embed.Embedder) bool {
	if result.err != nil {
		fmt.Printf("⚠️ Failed to embed node %s: %s\n", node.ID, compactWorkerError(result.err, 300))
		return ew.failNodeEmbedding(node, result.err)
	}
	dimensions := provider.Dimensions()
	count := len(result.chunks)
	if count == 0 {
		// Image-only providers need not supply chunk text. Their result must
		// still contain at least one usable vector.
		count = max(1, len(result.vectors))
	}
	if err := embed.CheckEmbeddings(result.vectors, count, dimensions); err != nil {
		fmt.Printf("⚠️ Failed to embed node %s: %s\n", node.ID, compactWorkerError(err, 300))
		return ew.failNodeEmbedding(node, err)
	}
	chunks, embeddings, providerMeta := result.chunks, result.vectors, result.metadata
	embeddingutil.ApplyManagedEmbedding(node, embeddings, provider.Model(), dimensions, time.Now())
	if spaceProvider, ok := provider.(embed.EmbeddingSpaceProvider); ok {
		if space := spaceProvider.EmbeddingSpace(); space != "" {
			node.EmbedMeta["embedding_space"] = space
		}
	}
	for key, value := range providerMeta {
		node.EmbedMeta[key] = value
	}
	// Bind the provider's consumed inputs to the text policy used to build this
	// request. Carry this record unchanged through retries and storage.
	consumed, present, _ := embeddingsource.Read(providerMeta)
	if !present {
		consumed = embeddingsource.Source{Text: true}
		if structured, ok := provider.(embed.DocumentPropertyChunkEmbedder); ok && structured.UsesDocumentProperties() {
			consumed.AllProperties = true
		}
	}
	consumed.Include, consumed.Exclude, consumed.IncludeLabels = source.Policy.Include, source.Policy.Exclude, source.Policy.IncludeLabels
	node.EmbedMeta[embeddingsource.MetaKey] = consumed.Metadata()
	// A property-only result cannot own spans into unconsumed fallback text.
	if !consumed.Text {
		source = embeddingutil.EmbedText{}
	}
	embeddingutil.RecordChunkSpans(node, source, chunks)
	return ew.writeEmbeddedNode(node)
}
