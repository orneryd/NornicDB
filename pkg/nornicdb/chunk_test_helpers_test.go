package nornicdb

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/textchunk"
)

func countTestTokens(text string) (int, error) {
	return len(strings.Fields(text)), nil
}

func mustCountTestTokens(text string) int {
	tokens, err := countTestTokens(text)
	if err != nil {
		panic(err)
	}
	return tokens
}

func chunkTestText(text string, maxTokens, overlap int) ([]string, error) {
	return textchunk.ChunkByTokenCount(text, maxTokens, overlap, countTestTokens)
}
