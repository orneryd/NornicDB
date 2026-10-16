package cypher

import (
	"unicode"
	"unicode/utf8"
)

func trimPipelineCharacters(text, cutset string, leading, trailing bool, probe *pipelineCancellation) string {
	characters := map[rune]struct{}{}
	for _, character := range cutset {
		if !probe.check() { return "" }
		characters[character] = struct{}{}
	}
	contains := func(character rune)bool {
		if cutset == "" { return unicode.IsSpace(character) }
		_, present := characters[character]
		return present
	}
	if leading {
		for len(text)>0 {
			if !probe.check() { return "" }
			character,size := utf8.DecodeRuneInString(text)
			if !contains(character) {break}
			text = text[size:]
		}
	}
	if trailing {
		for len(text)>0 {
			if !probe.check() { return "" }
			character,size := utf8.DecodeLastRuneInString(text)
			if !contains(character) {break}
			text = text[:len(text)-size]
		}
	}
	return text
}
