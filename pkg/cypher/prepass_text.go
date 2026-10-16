package cypher

import "strings"

// Text scanners share the statement's existing probe. No complete source
// string is passed to an unchecked standard-library search or replacement.
func indexTextWithCancellation(text, needle string, probe *pipelineCancellation) int {
	if probe == nil {
		return strings.Index(text, needle)
	}
	if needle == "" {
		return 0
	}
	if len(needle) <= 64 {
		for start := 0; start < len(text); start += 4096 {
			if !probe.check() {
				return -1
			}
			end := min(start+4096+len(needle)-1, len(text))
			if index := strings.Index(text[start:end], needle); index >= 0 {
				return start + index
			}
		}
		return -1
	}
	for index := 0; index+len(needle) <= len(text); index++ {
		if probe != nil && !probe.check() {
			return -1
		}
		if text[index] == needle[0] && comparePipelineStrings(text[index:index+len(needle)], needle, probe) == 0 {
			return index
		}
	}
	return -1
}

func containsTextWithCancellation(text, needle string, probe *pipelineCancellation) bool {
	return indexTextWithCancellation(text, needle, probe) >= 0
}

func indexByteWithCancellation(text string, needle byte, probe *pipelineCancellation) int {
	if probe == nil {
		return strings.IndexByte(text, needle)
	}
	for start := 0; start < len(text); start += 4096 {
		if !probe.check() {
			return -1
		}
		if index := strings.IndexByte(text[start:min(start+4096, len(text))], needle); index >= 0 {
			return start + index
		}
	}
	return -1
}

func indexAnyWithCancellation(text, characters string, probe *pipelineCancellation) int {
	if probe == nil {
		return strings.IndexAny(text, characters)
	}
	for index := 0; index < len(text); index++ {
		if probe != nil && !probe.check() {
			return -1
		}
		if strings.IndexByte(characters, text[index]) >= 0 {
			return index
		}
	}
	return -1
}

// The lexical owners use nonempty ASCII needles (backticks and underscores).
func replaceTextWithCancellation(text, old, replacement string, probe *pipelineCancellation) string {
	if probe == nil {
		return strings.ReplaceAll(text, old, replacement)
	}
	var out strings.Builder
	changed := false
	for {
		if probe != nil && !probe.check() {
			return ""
		}
		index := indexTextWithCancellation(text, old, probe)
		if index < 0 {
			break
		}
		changed = true
		if !appendPipelineString(&out, text[:index], probe) || !appendPipelineString(&out, replacement, probe) {
			return ""
		}
		text = text[index+len(old):]
	}
	if probe.err != nil {
		return ""
	}
	if !changed {
		return text
	}
	if !appendPipelineString(&out, text, probe) {
		return ""
	}
	return out.String()
}
