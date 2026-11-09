// Package indexing tests
package indexing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSearchableProperties(t *testing.T) {
	// These should match the Neo4j node_search fulltext index
	expectedProps := []string{
		"content",
		"text",
		"title",
		"name",
		"description",
		"path",
		"workerRole",
		"requirements",
	}

	assert.Equal(t, expectedProps, SearchableProperties)
}

func TestTokenizeForBM25(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{
			input:    "Hello World",
			expected: []string{"hello", "world"},
		},
		{
			input:    "TypeScript, JavaScript, and Go!",
			expected: []string{"typescript", "javascript", "and", "go"},
		},
		{
			input:    "user@example.com",
			expected: []string{"user", "example", "com"},
		},
		{
			input:    "file.ts:42",
			expected: []string{"file", "ts", "42"},
		},
		{
			input:    "",
			expected: nil,
		},
		{
			input:    "   ",
			expected: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			result := TokenizeForBM25(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestSanitizeText(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "clean text",
			input:    "Hello World",
			expected: "Hello World",
		},
		{
			name:     "with newlines and tabs",
			input:    "Line1\nLine2\tTabbed",
			expected: "Line1\nLine2\tTabbed",
		},
		{
			name:     "with control characters",
			input:    "Hello\x00World\x01Test",
			expected: "Hello World Test",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := SanitizeText(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// Benchmark tokenization
func BenchmarkTokenizeForBM25(b *testing.B) {
	text := "This is a sample text for benchmarking the tokenization function used in BM25 full-text search indexing."

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		TokenizeForBM25(text)
	}
}

func BenchmarkExtractSearchableText(b *testing.B) {
	props := map[string]interface{}{
		"title":       "Sample Document Title",
		"content":     "This is the main content of the document with various words.",
		"description": "A brief description of what this document contains.",
		"path":        "/some/file/path.ts",
		"name":        "document-name",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ExtractSearchableText(props)
	}
}
