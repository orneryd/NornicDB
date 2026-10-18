package cypher

import "strings"

// Match starts in each backwards page, retaining enough overlap to include a
// delimiter across pages. Large needles use the same bounded string comparator.
func lastIndexTextWithCancellation(text, needle string, probe *pipelineCancellation) int {
	if probe == nil {
		return strings.LastIndex(text, needle)
	}
	if len(needle) == 0 {
		return len(text)
	}
	if len(needle) <= 64 {
		for end := len(text); end > 0; {
			if !probe.check() {
				return -1
			}
			start := max(0, end-4096)
			if index := strings.LastIndex(text[start:min(len(text), end+len(needle)-1)], needle); index >= 0 {
				return start + index
			}
			end = start
		}
		return -1
	}
	for index := len(text) - len(needle); index >= 0; index-- {
		if !probe.check() {
			return -1
		}
		if text[index] == needle[0] && comparePipelineStrings(text[index:index+len(needle)], needle, probe) == 0 {
			if probe.err != nil {
				return -1
			}
			return index
		}
	}
	return -1
}
