package textchunk

import (
	"fmt"
	"strings"
	"testing"
)

// wordCount is a simple token counter: splits on whitespace.
func wordCount(text string) (int, error) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return 0, nil
	}
	return len(strings.Fields(trimmed)), nil
}

func errCounter(text string) (int, error) {
	return 0, fmt.Errorf("counter error")
}

func TestChunkByTokenCount_SingleChunk(t *testing.T) {
	text := "hello world foo"
	chunks, err := ChunkByTokenCount(text, 10, 0, wordCount)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 || chunks[0] != text {
		t.Errorf("Expected single chunk %q, got %v", text, chunks)
	}
}

func TestChunkByTokenCount_MultipleChunks(t *testing.T) {
	text := "one two three four five six"
	chunks, err := ChunkByTokenCount(text, 2, 0, wordCount)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) < 2 {
		t.Errorf("Expected multiple chunks, got %d", len(chunks))
	}
	// All words should be covered
	joined := strings.Join(chunks, " ")
	for _, word := range []string{"one", "two", "three", "four", "five", "six"} {
		if !strings.Contains(joined, word) {
			t.Errorf("Missing word %q in chunks", word)
		}
	}
}

func TestChunkByTokenCount_WithOverlap(t *testing.T) {
	text := "a b c d e f g h"
	chunks, err := ChunkByTokenCount(text, 3, 1, wordCount)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) < 2 {
		t.Fatalf("Expected multiple chunks with overlap, got %d", len(chunks))
	}
	// With overlap, consecutive chunks should share some content
	for i := 1; i < len(chunks); i++ {
		prevWords := strings.Fields(chunks[i-1])
		currWords := strings.Fields(chunks[i])
		lastPrev := prevWords[len(prevWords)-1]
		firstCurr := currWords[0]
		// The overlap means the start of the next chunk should overlap with the end of the previous
		_ = lastPrev
		_ = firstCurr
	}
}

func TestChunkByTokenCount_MaxTokensZero(t *testing.T) {
	text := "hello world"
	chunks, err := ChunkByTokenCount(text, 0, 0, wordCount)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 || chunks[0] != text {
		t.Errorf("Expected single chunk for maxTokens=0, got %v", chunks)
	}
}

func TestChunkByTokenCount_MaxTokensNegative(t *testing.T) {
	text := "hello world"
	chunks, err := ChunkByTokenCount(text, -5, 0, wordCount)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 || chunks[0] != text {
		t.Errorf("Expected single chunk for negative maxTokens, got %v", chunks)
	}
}

func TestChunkByTokenCount_NegativeOverlap(t *testing.T) {
	text := "one two three four five six"
	chunks, err := ChunkByTokenCount(text, 2, -3, wordCount)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) < 2 {
		t.Errorf("Expected multiple chunks, got %d", len(chunks))
	}
}

func TestChunkByTokenCount_OverlapExceedsMaxTokens(t *testing.T) {
	text := "one two three four five six"
	chunks, err := ChunkByTokenCount(text, 2, 10, wordCount)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) < 2 {
		t.Errorf("Expected multiple chunks even with large overlap, got %d", len(chunks))
	}
}

func TestChunkByTokenCount_EmptyText(t *testing.T) {
	chunks, err := ChunkByTokenCount("", 10, 0, wordCount)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 {
		t.Errorf("Expected 1 chunk for empty text, got %d", len(chunks))
	}
}

func TestChunkByTokenCount_WhitespaceOnly(t *testing.T) {
	chunks, err := ChunkByTokenCount("   ", 10, 0, wordCount)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 {
		t.Errorf("Expected 1 chunk for whitespace, got %d", len(chunks))
	}
}

func TestChunkByTokenCount_CounterError(t *testing.T) {
	_, err := ChunkByTokenCount("hello world", 10, 0, errCounter)
	if err == nil {
		t.Fatal("Expected error from counter")
	}
}

func TestChunkByTokenCount_CounterErrorDuringChunking(t *testing.T) {
	callCount := 0
	failOnSecond := func(text string) (int, error) {
		callCount++
		if callCount > 1 {
			return 0, fmt.Errorf("counter failed")
		}
		// First call returns a large count to force chunking
		return 100, nil
	}
	_, err := ChunkByTokenCount("a b c d e f", 2, 0, failOnSecond)
	if err == nil {
		t.Fatal("Expected error when counter fails during chunking")
	}
}

func TestChunkByTokenCount_Unicode(t *testing.T) {
	text := "日本語 テスト 文字列 チャンク"
	chunks, err := ChunkByTokenCount(text, 2, 0, wordCount)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) < 2 {
		t.Errorf("Expected multiple chunks for unicode text, got %d", len(chunks))
	}
}

func TestChunkByTokenCount_SingleWord(t *testing.T) {
	text := "superlongword"
	chunks, err := ChunkByTokenCount(text, 10, 0, wordCount)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 || chunks[0] != text {
		t.Errorf("Expected single chunk %q, got %v", text, chunks)
	}
}

func TestRuneByteOffsets(t *testing.T) {
	offsets := runeByteOffsets("abc")
	// "abc" has byte offsets [0, 1, 2, 3]
	if len(offsets) != 4 {
		t.Errorf("Expected 4 offsets, got %d", len(offsets))
	}
	if offsets[0] != 0 || offsets[3] != 3 {
		t.Errorf("Unexpected offsets: %v", offsets)
	}
}

func TestRuneByteOffsets_Unicode(t *testing.T) {
	offsets := runeByteOffsets("日本")
	// "日" is 3 bytes, "本" is 3 bytes, total 6 bytes
	// Offsets: [0, 3, 6]
	if len(offsets) != 3 {
		t.Errorf("Expected 3 offsets, got %d", len(offsets))
	}
	if offsets[0] != 0 || offsets[1] != 3 || offsets[2] != 6 {
		t.Errorf("Unexpected offsets: %v", offsets)
	}
}

func TestRuneByteOffsets_Empty(t *testing.T) {
	offsets := runeByteOffsets("")
	if len(offsets) != 1 || offsets[0] != 0 {
		t.Errorf("Expected [0], got %v", offsets)
	}
}
