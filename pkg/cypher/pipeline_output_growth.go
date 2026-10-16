package cypher

import (
	"strings"

	cyphertext "github.com/orneryd/nornicdb/pkg/cypher/internal/text"
)

func reservePipelineString(out *strings.Builder, additional int, probe *pipelineCancellation) bool {
	return cyphertext.ReserveString(out, additional, functionCancellation(probe))
}

func appendPipelineByte(out *strings.Builder, value byte, probe *pipelineCancellation) bool {
	return cyphertext.AppendByte(out, value, functionCancellation(probe))
}

func appendPipelineRune(out *strings.Builder, value rune, probe *pipelineCancellation) bool {
	return cyphertext.AppendRune(out, value, functionCancellation(probe))
}

func appendPipelineBytes(out *strings.Builder, value []byte, probe *pipelineCancellation) bool {
	return cyphertext.AppendBytes(out, value, functionCancellation(probe))
}

func appendPipelineItems[T any](out, items []T, probe *pipelineCancellation) []T {
	result, _ := cyphertext.AppendItems(out, items, functionCancellation(probe))
	return result
}

func appendPipelineItem[T any](out []T, item T, probe *pipelineCancellation) []T {
	result, _ := cyphertext.AppendItem(out, item, functionCancellation(probe))
	return result
}
