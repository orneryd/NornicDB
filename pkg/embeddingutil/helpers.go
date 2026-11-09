package embeddingutil

import (
	"encoding/json"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/convert"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/textchunk"
)

// EmbedTextOptions controls which properties and labels are used when building embedding text.
type EmbedTextOptions struct {
	Include       []string
	Exclude       []string
	IncludeLabels bool
}

// BuildText creates canonical embedding text from node properties and labels.
func BuildText(properties map[string]interface{}, labels []string, opts *EmbedTextOptions) string {
	return BuildEmbedText(properties, labels, opts).Text
}

// EmbedText is a node's embedding text, with where each string property's
// value lies in it, so a chunk of the text can be traced to its properties.
type EmbedText struct {
	Policy storage.EmbeddingTextPolicy
	Text   string
	fields []embedTextField
}

// embedTextField is where a string property's value lies in the text, in
// bytes: the value is copied verbatim after "key: ".
type embedTextField struct {
	key, value string
	start      int
}

// BuildEmbedText creates canonical embedding text from node properties and
// labels, as BuildText, and records where each string property's value lies.
func BuildEmbedText(properties map[string]interface{}, labels []string, opts *EmbedTextOptions) EmbedText {
	if opts == nil {
		opts = &EmbedTextOptions{IncludeLabels: true}
	}
	var parts []string
	var fields []embedTextField
	length := 0
	add := func(part string) {
		if len(parts) > 0 {
			length++ // the "\n" joining parts
		}
		parts = append(parts, part)
		length += len(part)
	}

	if opts.IncludeLabels && len(labels) > 0 {
		add(fmt.Sprintf("labels: %s", strings.Join(labels, ", ")))
	}

	// One rule decides which properties feed the text: storage's, which also
	// decides when an update changes a node's embedding source (#963). Keys
	// are taken in order so the same content always gives the same text.
	policy := storage.EmbeddingTextPolicy{Include: append([]string(nil), opts.Include...), Exclude: append([]string(nil), opts.Exclude...), IncludeLabels: opts.IncludeLabels}
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
		part := fmt.Sprintf("%s: %s", key, strVal)
		if value, ok := val.(string); ok {
			start := length + len(part) - len(value)
			if len(parts) > 0 {
				start++
			}
			fields = append(fields, embedTextField{key: key, value: value, start: start})
		}
		add(part)
	}

	result := strings.Join(parts, "\n")
	if result == "" {
		return EmbedText{Text: "node", Policy: policy}
	}
	return EmbedText{Text: result, fields: fields, Policy: policy}
}

// ChunkSpan is where a chunk of a node's embedding text lies in one of its
// string properties: [Start, End) in Unicode code points of the value, as
// Cypher's substring() counts. A chunk that runs across two properties has
// one span in each.
type ChunkSpan struct {
	Chunk      int
	Property   string
	Start, End int
}

// ChunkSpans finds each chunk in the text (textchunk.Align) and returns the
// parts of the string property values it covers, with leading and trailing
// whitespace left out. A chunk not found in the text, or one covering only
// labels, keys or non-string values, has no span.
func (t EmbedText) ChunkSpans(chunks []string) []ChunkSpan {
	var spans []ChunkSpan
	for chunk, found := range textchunk.Align(t.Text, chunks) {
		if !found.Found() {
			continue
		}
		for _, field := range t.fields {
			start := max(found.Start, field.start) - field.start
			end := min(found.End, field.start+len(field.value)) - field.start
			if start >= end {
				continue
			}
			covered := field.value[start:end]
			start += len(covered) - len(strings.TrimLeftFunc(covered, unicode.IsSpace))
			end -= len(covered) - len(strings.TrimRightFunc(covered, unicode.IsSpace))
			if start >= end {
				continue
			}
			startRunes := utf8.RuneCountInString(field.value[:start])
			spans = append(spans, ChunkSpan{
				Chunk:    chunk,
				Property: field.key,
				Start:    startRunes,
				End:      startRunes + utf8.RuneCountInString(field.value[start:end]),
			})
		}
	}
	return spans
}

// chunkSpansKey holds a managed embedding's chunk spans in EmbedMeta: one
// [chunk, property, start, end] list per span.
const chunkSpansKey = "chunk_spans"

// RecordChunkSpans stores where each of node's chunks lies in its string
// properties, from the text the chunks were cut from. It replaces any spans
// of an earlier embedding, and stores none when the chunks don't match the
// node's chunk embeddings one to one (a multimodal document's single vector).
func RecordChunkSpans(node *storage.Node, source EmbedText, chunks []string) {
	if node == nil || node.EmbedMeta == nil {
		return
	}
	delete(node.EmbedMeta, chunkSpansKey)
	if len(chunks) == 0 || len(chunks) != len(node.ChunkEmbeddings) {
		return
	}
	encoded := make([]any, 0, len(chunks))
	for _, span := range source.ChunkSpans(chunks) {
		encoded = append(encoded, []any{int64(span.Chunk), span.Property, int64(span.Start), int64(span.End)})
	}
	node.EmbedMeta[chunkSpansKey] = encoded
}

// StoredChunkSpans returns the chunk spans RecordChunkSpans stored in meta,
// whichever integer types the stored form decoded into; ok is false when
// meta holds none (an embedding made before spans were recorded).
func StoredChunkSpans(meta map[string]any) (spans []ChunkSpan, ok bool) {
	raw, ok := meta[chunkSpansKey].([]any)
	if !ok {
		return nil, false
	}
	spans = make([]ChunkSpan, 0, len(raw))
	for _, entry := range raw {
		fields, isList := entry.([]any)
		if !isList || len(fields) != 4 {
			continue
		}
		chunk, chunkOK := storedInt(fields[0])
		property, propertyOK := fields[1].(string)
		start, startOK := storedInt(fields[2])
		end, endOK := storedInt(fields[3])
		if chunkOK && propertyOK && startOK && endOK && start < end {
			spans = append(spans, ChunkSpan{Chunk: chunk, Property: property, Start: start, End: end})
		}
	}
	return spans, true
}

// ChunkCount is the number of chunks meta's embedding has, whichever
// integer type the stored form decoded into; -1 when meta records none.
func ChunkCount(meta map[string]any) int {
	if count, ok := storedInt(meta["chunk_count"]); ok {
		return count
	}
	return -1
}

func storedInt(value any) (int, bool) {
	// Metadata accepts the historical concrete integer carriers, without
	// extending admission to floats, strings, named scalars or native uint.
	switch value.(type) {
	case int, int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		number, _ := convert.ReadNumber(value)
		if signed, ok := number.Signed(); ok {
			return int(signed), true
		}
		unsigned, _ := number.Unsigned()
		return int(unsigned), true
	default:
		return 0, false
	}
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
	return storage.EmbeddingTextPolicy{Include: append([]string(nil), opts.Include...), Exclude: append([]string(nil), opts.Exclude...), IncludeLabels: opts.IncludeLabels}
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
