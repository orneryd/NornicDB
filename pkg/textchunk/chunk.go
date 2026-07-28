package textchunk

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/util"
)

// CountFunc returns the token count for the provided text.
type CountFunc func(text string) (int, error)

// ChunkByTokenCount deterministically splits text into chunks that fit within
// maxTokens according to the supplied token counter.
func ChunkByTokenCount(text string, maxTokens, overlap int, countTokens CountFunc) ([]string, error) {
	if maxTokens <= 0 {
		trimmed := strings.TrimSpace(text)
		if trimmed == "" {
			return []string{text}, nil
		}
		return []string{trimmed}, nil
	}
	if overlap < 0 {
		overlap = 0
	}
	if overlap >= maxTokens {
		overlap = maxTokens - 1
		if overlap < 0 {
			overlap = 0
		}
	}

	totalTokens, err := countTokens(text)
	if err != nil {
		return nil, err
	}
	if totalTokens <= maxTokens {
		trimmed := strings.TrimSpace(text)
		if trimmed == "" {
			return []string{text}, nil
		}
		return []string{trimmed}, nil
	}

	offsets := runeByteOffsets(text)
	if len(offsets) <= 1 {
		return []string{text}, nil
	}

	chunks := make([]string, 0, util.SafePreallocSum(totalTokens/maxTokens, 1))
	start := 0
	for start < len(offsets)-1 {
		end, err := maxFittingChunkEnd(text, offsets, start, maxTokens, countTokens)
		if err != nil {
			return nil, err
		}
		if end <= start {
			end = start + 1
			if end > len(offsets)-1 {
				end = len(offsets) - 1
			}
		}

		chunk := strings.TrimSpace(text[offsets[start]:offsets[end]])
		if chunk != "" {
			chunks = append(chunks, chunk)
		}

		if end >= len(offsets)-1 {
			break
		}

		nextStart := end
		if overlap > 0 {
			nextStart, err = overlappingChunkStart(text, offsets, start, end, overlap, countTokens)
			if err != nil {
				return nil, err
			}
			if nextStart <= start {
				nextStart = start + 1
			}
			if nextStart > end {
				nextStart = end
			}
		}
		start = nextStart
	}

	if len(chunks) == 0 {
		trimmed := strings.TrimSpace(text)
		if trimmed == "" {
			return []string{text}, nil
		}
		return []string{trimmed}, nil
	}
	return chunks, nil
}

func maxFittingChunkEnd(text string, offsets []int, start, maxTokens int, countTokens CountFunc) (int, error) {
	lo := start + 1
	hi := len(offsets) - 1
	best := start
	for lo <= hi {
		mid := lo + (hi-lo)/2
		count, err := countTokens(text[offsets[start]:offsets[mid]])
		if err != nil {
			return 0, err
		}
		if count <= maxTokens {
			best = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return best, nil
}

func overlappingChunkStart(text string, offsets []int, chunkStart, chunkEnd, overlap int, countTokens CountFunc) (int, error) {
	lo := chunkStart + 1
	hi := chunkEnd
	best := chunkEnd
	for lo <= hi {
		mid := lo + (hi-lo)/2
		count, err := countTokens(text[offsets[mid]:offsets[chunkEnd]])
		if err != nil {
			return 0, err
		}
		if count <= overlap {
			best = mid
			hi = mid - 1
		} else {
			lo = mid + 1
		}
	}
	return best, nil
}

func runeByteOffsets(text string) []int {
	offsets := make([]int, 0, util.SafePreallocSum(len(text), 1))
	for i := range text {
		offsets = append(offsets, i)
	}
	offsets = append(offsets, len(text))
	return offsets
}
