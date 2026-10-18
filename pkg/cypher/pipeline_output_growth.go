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
	if probe == nil {
		return append(out, items...)
	}
	if len(out)+len(items) <= pipelineCancellationInterval {
		if !probe.check() {
			return nil
		}
		return append(out, items...)
	}
	result, _ := cyphertext.AppendItems(out, items, functionCancellation(probe))
	return result
}

// appendPipelineItem appends item for one unit of work. A slice of up to one
// probe interval appends natively, inlined, so a caller's small stack buffer
// stays on its stack; a longer one grows in checked copy units. The result
// is nil once the statement is cancelled.
func appendPipelineItem[T any](out []T, item T, probe *pipelineCancellation) []T {
	if probe == nil {
		return append(out, item)
	}
	if len(out) < pipelineCancellationInterval {
		if !probe.check() {
			return nil
		}
		return append(out, item)
	}
	result, _ := cyphertext.AppendItem(out, item, functionCancellation(probe))
	return result
}
