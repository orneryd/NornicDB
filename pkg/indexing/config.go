// Package indexing provides content processing utilities for NornicDB search indexing.
//
// This package handles the extraction and processing of textual content from node
// properties for full-text search indexing. It provides utilities for tokenization,
// text sanitization, and property extraction that are used by the search system.
//
// Architecture Note:
//
// NornicDB follows a clear separation of concerns:
//   - **Application layer**: File discovery, reading, embedding generation
//   - **NornicDB responsibilities**: Storage, search, relationships, persistence
//
// This package specifically handles the NornicDB side of content processing:
//   - Extracting searchable text from node properties
//   - Tokenizing text for BM25 full-text search
//   - Sanitizing text to handle encoding issues
//   - Providing consistent text processing across the system
//
// Key Functions:
//   - ExtractSearchableText(): Extracts text from standard node properties
//   - TokenizeForBM25(): Tokenizes text for full-text search indexing
//   - SanitizeText(): Cleans text to handle Unicode and encoding issues
//
// Example Usage:
//
//	// Extract text from node properties for indexing
//	properties := map[string]interface{}{
//		"title":       "Introduction to Machine Learning",
//		"content":     "Machine learning is a subset of artificial intelligence...",
//		"description": "A comprehensive guide to ML fundamentals",
//		"path":        "/docs/ml-intro.md",
//		"author":      "Dr. Smith", // Not searchable
//		"created_at":  time.Now(),  // Not searchable
//	}
//
//	// Extract searchable text
//	searchableText := indexing.ExtractSearchableText(properties)
//	// Result: "Introduction to Machine Learning Machine learning is a subset... A comprehensive guide... /docs/ml-intro.md"
//
//	// Tokenize for BM25 indexing
//	tokens := indexing.TokenizeForBM25(searchableText)
//	// Result: ["introduction", "to", "machine", "learning", "is", "a", "subset", ...]
//
//	// Sanitize problematic text
//	cleanText := indexing.SanitizeText("Text with\x00invalid\uD800chars")
//	// Result: "Text with invalid chars" (control chars replaced)
//
// Searchable Properties:
//
// The package defines standard properties that are indexed for full-text search:
//   - **content**: Main textual content (documents, descriptions)
//   - **text**: Alternative text field
//   - **title**: Document or entity titles
//   - **name**: Entity names
//   - **description**: Descriptive text
//   - **path**: File paths (for file-based nodes)
//   - **workerRole**: Role descriptions (for agent/worker nodes)
//   - **requirements**: Requirement specifications
//
// These properties match the Neo4j fulltext index configuration for compatibility.
//
// Text Processing Pipeline:
//
// 1. **Extraction**: ExtractSearchableText() concatenates relevant properties
// 2. **Sanitization**: SanitizeText() removes problematic Unicode characters
// 3. **Tokenization**: TokenizeForBM25() splits into searchable tokens
// 4. **Indexing**: Tokens are indexed by the search system
//
// Integration with Search:
//
// This package is used by:
//   - Full-text search indexing (BM25)
//   - Search query processing
//   - Content analysis and filtering
//   - Consistent text handling across NornicDB
//
// ELI12 (Explain Like I'm 12):
//
// Think of this package like a librarian organizing books:
//
//  1. **Extracting text**: The librarian looks at each book and writes down
//     the important information - title, summary, topic - on index cards.
//
//  2. **Cleaning text**: Sometimes books have smudged or weird characters,
//     so the librarian fixes them so they're readable.
//
//  3. **Making word lists**: The librarian breaks down all the text into
//     individual words, like "machine", "learning", "artificial", etc.
//
//  4. **Organizing for search**: All these words go into a big filing system
//     so when someone asks "find me books about machine learning", the
//     librarian can quickly find the right books.
//
// This package does the same thing but for computer data instead of books!
package indexing

import (
	"strings"
	"unicode"
)

// SearchableProperties defines which node properties are indexed for full-text search.
// These match the Neo4j node_search fulltext index configuration.
var SearchableProperties = []string{
	"content",
	"text",
	"title",
	"name",
	"description",
	"path",
	"workerRole",
	"requirements",
}

// ExtractSearchableText extracts and concatenates searchable text from node properties.
//
// This function examines node properties and extracts text from fields that are
// suitable for full-text search indexing. The extracted text is concatenated
// with spaces to form a single searchable string.
//
// Searchable properties (in order):
//   - content: Main textual content
//   - text: Alternative text field
//   - title: Document/node title
//   - name: Entity name
//   - description: Descriptive text
//   - path: File path (for file nodes)
//   - workerRole: Role description (for agent nodes)
//   - requirements: Requirement specifications
//
// Parameters:
//   - properties: Map of node properties to extract from
//
// Returns:
//   - Concatenated searchable text, or empty string if no searchable properties found
//
// Example:
//
//	properties := map[string]interface{}{
//		"title":       "Machine Learning Guide",
//		"content":     "This guide covers the fundamentals of ML...",
//		"description": "A beginner-friendly introduction",
//		"path":        "/docs/ml-guide.md",
//		"author":      "Dr. Jane Smith",    // Not searchable
//		"created_at":  "2023-01-15",        // Not searchable
//		"tags":        []string{"AI", "ML"}, // Not searchable (not string)
//	}
//
//	text := indexing.ExtractSearchableText(properties)
//	// Result: "Machine Learning Guide This guide covers the fundamentals of ML... A beginner-friendly introduction /docs/ml-guide.md"
//
// Use Cases:
//   - Building full-text search indexes
//   - Preparing content for BM25 scoring
//   - Consistent text extraction across the system
//   - Content analysis and processing
//
// Note: Only string-type properties are extracted. Other types (numbers, arrays, etc.) are ignored.
func ExtractSearchableText(properties map[string]interface{}) string {
	var parts []string

	for _, prop := range SearchableProperties {
		if val, ok := properties[prop]; ok {
			if str, ok := val.(string); ok && len(str) > 0 {
				parts = append(parts, str)
			}
		}
	}

	return strings.Join(parts, " ")
}

// TokenizeForBM25 tokenizes text for BM25 full-text search indexing.
//
// This function implements a simple but effective tokenization strategy:
//   - Converts text to lowercase for case-insensitive search
//   - Splits on whitespace and punctuation
//   - Preserves alphanumeric characters
//   - Removes empty tokens
//
// Parameters:
//   - text: Input text to tokenize
//
// Returns:
//   - Slice of lowercase tokens suitable for BM25 indexing
//
// Example:
//
//	text := "Machine Learning: A Comprehensive Guide (2023)"
//	tokens := indexing.TokenizeForBM25(text)
//	// Result: ["machine", "learning", "a", "comprehensive", "guide", "2023"]
//
//	// Handles punctuation and special characters
//	text = "Hello, world! How are you?"
//	tokens = indexing.TokenizeForBM25(text)
//	// Result: ["hello", "world", "how", "are", "you"]
//
//	// Preserves numbers and mixed alphanumeric
//	text = "Python3.9 and Node.js v18.0"
//	tokens = indexing.TokenizeForBM25(text)
//	// Result: ["python3", "9", "and", "node", "js", "v18", "0"]
//
// Algorithm:
//  1. Convert to lowercase
//  2. Iterate through each character
//  3. Collect alphanumeric characters into tokens
//  4. Split on non-alphanumeric characters
//  5. Return non-empty tokens
//
// Use Cases:
//   - BM25 full-text search indexing
//   - Query term extraction
//   - Text analysis and processing
//   - Search relevance scoring
//
// Performance:
//   - Time complexity: O(n) where n is text length
//   - Space complexity: O(t) where t is number of tokens
//   - Suitable for real-time indexing
func TokenizeForBM25(text string) []string {
	text = strings.ToLower(text)

	var tokens []string
	var current strings.Builder

	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			current.WriteRune(r)
		} else {
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		}
	}

	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}

	return tokens
}

// SanitizeText cleans text by removing or replacing problematic Unicode characters.
//
// This function handles common text encoding issues that can cause problems
// in search indexing, storage, or display. It's particularly useful when
// processing text from various sources with different encodings.
//
// Cleaning operations:
//   - Replaces control characters (0x00-0x08, 0x0B, 0x0E-0x1F) with spaces
//   - Preserves tab (0x09), newline (0x0A), and carriage return (0x0D)
//   - Replaces invalid surrogate pairs (0xD800-0xDFFF) with replacement character
//   - Preserves all other valid Unicode characters
//
// Parameters:
//   - text: Input text that may contain problematic characters
//
// Returns:
//   - Cleaned text safe for indexing and storage
//
// Example:
//
//	// Clean text with control characters
//	dirtyText := "Hello\x00World\x01Test"
//	cleanText := indexing.SanitizeText(dirtyText)
//	// Result: "Hello World Test" (null bytes replaced with spaces)
//
//	// Handle invalid Unicode
//	invalidText := "Text with\uD800invalid\uDFFFsurrogates"
//	cleanText = indexing.SanitizeText(invalidText)
//	// Result: "Text with\uFFFDinvalid\uFFFDsurrogates" (surrogates replaced)
//
//	// Preserve valid whitespace
//	validText := "Line 1\nLine 2\tTabbed\rCarriage"
//	cleanText = indexing.SanitizeText(validText)
//	// Result: "Line 1\nLine 2\tTabbed\rCarriage" (unchanged)
//
// Common Issues Handled:
//   - Null bytes from binary data mixed with text
//   - Control characters from legacy systems
//   - Invalid UTF-8 sequences
//   - Surrogate pairs from UTF-16 conversion errors
//   - Non-printable characters that break indexing
//
// Use Cases:
//   - Preprocessing text before indexing
//   - Cleaning user input
//   - Handling text from external systems
//   - Preparing content for storage
//   - Fixing encoding issues in imported data
//
// Performance:
//   - Time complexity: O(n) where n is text length
//   - Space complexity: O(n) for the result string
//   - Efficient for typical text processing workloads
func SanitizeText(text string) string {
	if len(text) == 0 {
		return text
	}

	var result strings.Builder
	result.Grow(len(text))

	for _, r := range text {
		// Skip problematic control characters (keep tab, newline, CR)
		if (r >= 0x00 && r <= 0x08) || r == 0x0B || (r >= 0x0E && r <= 0x1F) {
			result.WriteRune(' ')
			continue
		}

		// Skip surrogate pairs (invalid in Go strings)
		if r >= 0xD800 && r <= 0xDFFF {
			result.WriteRune('\uFFFD')
			continue
		}

		result.WriteRune(r)
	}

	return result.String()
}
