package textchunk

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// All tests rely on a deterministic token counter so the assertions are
// stable across runs and never depend on a real tokenizer.

// ============================================================================
// maxTokens<=0 + empty text → returns the original empty text (line 13-15).
// ============================================================================

func TestChunkByTokenCount_MaxTokensZeroEmptyText(t *testing.T) {
	chunks, err := ChunkByTokenCount("", 0, 0, wordCount)
	require.NoError(t, err)
	require.Equal(t, []string{""}, chunks,
		"empty text with maxTokens<=0 must yield a single empty chunk")
}

func TestChunkByTokenCount_MaxTokensNegativeWhitespaceOnly(t *testing.T) {
	chunks, err := ChunkByTokenCount("   ", -1, 0, wordCount)
	require.NoError(t, err)
	// trimmed becomes "" → first branch returns [text] (the original).
	require.Equal(t, []string{"   "}, chunks)
}

// ============================================================================
// overlap == maxTokens with maxTokens==1 path: clamp overlap to 0 (line 23-25).
// ============================================================================

func TestChunkByTokenCount_OverlapEqualsMaxTokensOne(t *testing.T) {
	// maxTokens=1 forces overlap to be clamped to 0 (since maxTokens-1=0).
	// This exercises the "if overlap < 0 { overlap = 0 }" branch inside the
	// outer clamp. Use a 4-token text so we get multiple chunks.
	text := "one two three four"
	chunks, err := ChunkByTokenCount(text, 1, 5, wordCount)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(chunks), 2,
		"chunking with maxTokens=1 must split into multiple chunks")
	// With overlap clamped to 0, the chunks must not duplicate any token.
	totalTokens := 0
	for _, chunk := range chunks {
		totalTokens += len(strings.Fields(chunk))
	}
	require.Equal(t, 4, totalTokens,
		"clamped-to-zero overlap must not produce duplicate tokens")
}

// ============================================================================
// Single-character text (offsets len <= 1) returns [text] (line 41-43).
// Note: a single character yields offsets [0, 1] (len=2), so the branch is
// hit only by a truly empty text. Empty text totalTokens==0 short-circuits
// earlier, but a token counter that fails on empty text but reports a large
// count for any non-empty input bypasses the early returns.
// ============================================================================

func TestChunkByTokenCount_OffsetsLenOneShortCircuit(t *testing.T) {
	// Force totalTokens > maxTokens with a counter that ignores actual
	// content, then hand in a text whose rune offsets are <= 1. The empty
	// string has offsets [0] (len=1) and reaches the runeByteOffsets
	// branch only when the count returned exceeds maxTokens.
	counter := func(text string) (int, error) {
		return 10, nil // always over the limit
	}
	chunks, err := ChunkByTokenCount("", 5, 0, counter)
	require.NoError(t, err)
	require.Equal(t, []string{""}, chunks,
		"text with <=1 offsets must short-circuit and return the original")
}

// ============================================================================
// Single-rune body that exceeds maxTokens still produces a single chunk
// containing that rune (line 52-56: end<=start fallback path).
// ============================================================================

func TestChunkByTokenCount_EndLessThanStartFallback(t *testing.T) {
	// A counter that *always* reports more than maxTokens forces the binary
	// search in maxFittingChunkEnd to return `start` (since no end satisfies
	// the predicate), driving the `if end <= start` fallback in
	// ChunkByTokenCount. With 3 runes and maxTokens=1, every prefix is
	// considered too large.
	alwaysOver := func(text string) (int, error) {
		return 99, nil
	}
	text := "abc"
	chunks, err := ChunkByTokenCount(text, 1, 0, alwaysOver)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c"}, chunks,
		"single-rune fallback must produce one chunk per rune")
}

// ============================================================================
// Single-rune body with maxTokens=1 AND end>=len(offsets)-1 cap path.
// ============================================================================

func TestChunkByTokenCount_FallbackCapAtEnd(t *testing.T) {
	// Same forcing as above but with a 1-rune text — the fallback should
	// cap end at len(offsets)-1 and return the single character.
	alwaysOver := func(text string) (int, error) {
		return 99, nil
	}
	chunks, err := ChunkByTokenCount("x", 1, 0, alwaysOver)
	require.NoError(t, err)
	require.Equal(t, []string{"x"}, chunks)
}

// ============================================================================
// Overlap error-propagation path (line 71-73).
// The counter is wired to fail only inside overlappingChunkStart by
// returning successfully for the first three calls (which feed
// maxFittingChunkEnd) and erroring on subsequent calls.
// ============================================================================

func TestChunkByTokenCount_OverlapCounterErrorPropagates(t *testing.T) {
	// Strategy: the first call sees the whole text and must report a
	// count above maxTokens to drive chunking. Each subsequent call comes
	// from either maxFittingChunkEnd (input strictly starts at offset 0)
	// or overlappingChunkStart (input does NOT start at offset 0 — it is
	// a tail slice of the original). Use that distinction to fail only
	// when the overlap helper invokes the counter.
	full := "alpha bravo charlie delta echo foxtrot"
	counter := func(text string) (int, error) {
		// First call: counts the entire text — drives the chunking branch.
		if text == full {
			return 100, nil
		}
		// Prefix call from maxFittingChunkEnd starts at offset 0, i.e.
		// the slice has the same prefix as `full`.
		if strings.HasPrefix(full, text) {
			return len(strings.Fields(text)), nil
		}
		// Suffix call from overlappingChunkStart — fail here.
		return 0, fmt.Errorf("counter failed inside overlap calculation")
	}
	_, err := ChunkByTokenCount(full, 2, 1, counter)
	require.Error(t, err)
	require.Contains(t, err.Error(), "counter failed inside overlap calculation")
}

// ============================================================================
// Overlap nextStart <= start clamp (line 74-76) and nextStart > end clamp
// (line 77-79).
// ============================================================================

func TestChunkByTokenCount_OverlapClampPaths(t *testing.T) {
	// Counter that reports tokens roughly proportional to length so the
	// overlap binary search exercises both clamp branches.
	counter := func(text string) (int, error) {
		return len(strings.Fields(text)) * 2, nil
	}
	// Construct a long enough text that the overlap clamp can land in
	// either direction. Asserting that no error is returned and we still
	// get >=2 chunks is sufficient to exercise the clamps.
	text := strings.Repeat("token ", 50)
	chunks, err := ChunkByTokenCount(text, 4, 3, counter)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(chunks), 2)
}

// ============================================================================
// "len(chunks) == 0 → trimmed empty → return original" path (line 84-89).
// This is hit only when the for-loop runs but never appends a chunk because
// every body is whitespace-only.
// ============================================================================

func TestChunkByTokenCount_AllChunksEmptyReturnsOriginal(t *testing.T) {
	// A text composed entirely of spaces with a counter that reports it as
	// over the limit. Each fallback chunk is a single space character,
	// which is trimmed to "" and dropped.
	alwaysOver := func(text string) (int, error) {
		return 99, nil
	}
	text := "     "
	chunks, err := ChunkByTokenCount(text, 1, 0, alwaysOver)
	require.NoError(t, err)
	// All single-space chunks trim to "" → fallback returns [text].
	require.Equal(t, []string{text}, chunks)
}

// ============================================================================
// "all chunks empty + trimmed=='' returns [text]" path (line 86-88).
// Empty-string text is handled earlier; this case verifies the fallback
// returns the input verbatim when nothing was appended.
// ============================================================================

func TestChunkByTokenCount_AllChunksEmptyEmptyText(t *testing.T) {
	// Force the loop branch by passing text whose rune offset chain has >1
	// entries but which trims to empty.
	alwaysOver := func(text string) (int, error) {
		return 99, nil
	}
	chunks, err := ChunkByTokenCount(" ", 1, 0, alwaysOver)
	require.NoError(t, err)
	// One-char " " produces offsets [0,1]; loop hits fallback → chunk = "" after trim → not appended.
	// len(chunks)==0 → trimmed=="" → return original " ".
	require.Equal(t, []string{" "}, chunks)
}

// ============================================================================
// overlappingChunkStart bubbles its own counter error (line 121-123).
// ============================================================================

func TestOverlappingChunkStart_BubblesCounterError(t *testing.T) {
	offsets := runeByteOffsets("hello world foo bar")
	failing := func(text string) (int, error) {
		return 0, fmt.Errorf("boom inside overlap")
	}
	_, err := overlappingChunkStart("hello world foo bar", offsets, 0, len(offsets)-1, 1, failing)
	require.Error(t, err)
	require.Contains(t, err.Error(), "boom inside overlap")
}

// ============================================================================
// maxFittingChunkEnd direct test — happy path returns the largest end whose
// prefix still fits, and bubbles up counter errors.
// ============================================================================

func TestMaxFittingChunkEnd_HappyPathAndError(t *testing.T) {
	text := "a b c d e f"
	offsets := runeByteOffsets(text)
	end, err := maxFittingChunkEnd(text, offsets, 0, 4, wordCount)
	require.NoError(t, err)
	require.Greater(t, end, 0)
	// Prefix should contain at most 4 words.
	require.LessOrEqual(t, len(strings.Fields(text[offsets[0]:offsets[end]])), 4)

	_, err = maxFittingChunkEnd(text, offsets, 0, 4, func(s string) (int, error) {
		return 0, fmt.Errorf("boom inside max-fitting")
	})
	require.Error(t, err)
}
