package cypher

import (
	"fmt"
	"strings"
)

func joinPipelineValues(value interface{}, separator string, probe *pipelineCancellation) string {
	items := toAnySliceWithCancellation(value, probe)
	var out strings.Builder
	for index, item := range items {
		if probe != nil && !probe.check() {
			return ""
		}
		if index > 0 && !appendPipelineString(&out, separator, probe) {
			return ""
		}
		appendPipelineDisplayValue(&out, item, probe)
		if probe != nil && probe.err != nil {
			return ""
		}
	}
	return out.String()
}

func pipelineDisplayKey(value interface{}, probe *pipelineCancellation) string {
	var out strings.Builder
	if !appendPipelineString(&out, fmt.Sprintf("%T:", value), probe) {
		return ""
	}

	appendPipelineDisplayValue(&out, value, probe)
	return out.String()
}

func pipelineDisplayString(value interface{}, probe *pipelineCancellation) string {
	var out strings.Builder
	appendPipelineDisplayValue(&out, value, probe)
	return out.String()
}

func pipelineStringsEqual(left, right string, probe *pipelineCancellation) bool {
	order := comparePipelineStrings(left, right, probe)
	return order == 0 && (probe == nil || probe.err == nil)
}
