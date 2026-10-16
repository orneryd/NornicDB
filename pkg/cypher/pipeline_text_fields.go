package cypher

import (
	"strings"
	"unicode"
)

// fieldsWithCancellation preserves strings.Fields' Unicode whitespace policy.
// A field is a view of the source; output growth uses the shared copy owner.
func fieldsWithCancellation(text string, probe *pipelineCancellation) []string {
	if probe == nil {
		return strings.Fields(text)
	}
	fields := make([]string, 0)
	start := -1
	for index, current := range text {
		if !probe.check() {
			return nil
		}
		if unicode.IsSpace(current) {
			if start >= 0 {
				fields = appendPipelineItem(fields, text[start:index], probe)
				if probe.err != nil {
					return nil
				}
				start = -1
			}
		} else if start < 0 {
			start = index
		}
	}
	if start >= 0 {
		fields = appendPipelineItem(fields, text[start:], probe)
	}
	return fields
}
