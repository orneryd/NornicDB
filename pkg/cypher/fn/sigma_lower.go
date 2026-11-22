package fn

import (
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// lowerCaseSigma owns the contextual Sigma decision for the pinned Neo4j
// 5.26.30/JDK21 profile. General Unicode mapping still belongs to x/text.
// Each input code point is visited once; punctuation admission is deferred by
// one significant token rather than looking ahead or rescanning per Sigma.
func lowerCaseSigma(text string) string {
	const (
		outside uint8 = iota
		letters
		numbers
		danda
		suffix
		wordBridge
		numberBridge
	)
	var state uint8
	var out strings.Builder
	copied, lastSigma := 0, -1
	seenCased, earlierCased := false, false
	finish := func() {
		if lastSigma >= 0 && earlierCased {
			if out.Len() == 0 {
				out.Grow(len(text))
			}
			out.WriteString(text[copied:lastSigma])
			out.WriteString("ς")
			copied = lastSigma + len("Σ")
		}
		seenCased, earlierCased, lastSigma = false, false, -1
	}
	var previous, beforePrevious rune
	previousAt := -1
	for at := 0; at < len(text); {
		r, width := cyphertext.ReadCodepoint(text, at)
		// Java's random-access boundary check starts on the previous low
		// surrogate. Its reverse-cursor recovery creates a boundary unless
		// it terminates at input start or CharacterIterator.DONE (U+FFFF).
		// This cuts casing context, not the surrounding lexical grammar.
		if previous > 0xffff && previousAt > 0 && beforePrevious != 0xffff {
			finish()
		}
		beforePrevious, previous, previousAt = previous, r, at
		info := sigmaContext(r)
		kind := info & sigmaKindMask
		if kind == sigmaIgnore {
			at += width
			continue
		}
		if state == wordBridge || state == numberBridge {
			if state == wordBridge && kind == sigmaLetter {
				state = letters
			} else if state == numberBridge && kind == sigmaNumber {
				state = numbers
			} else {
				finish()
				state = outside
			}
		}
		admitted := false
		switch kind {
		case sigmaLetter:
			if state == danda || state == suffix {
				finish()
			}
			state = letters
			admitted = true
		case sigmaNumber:
			if state == suffix {
				finish()
			}
			state = numbers
			admitted = true
		case sigmaEnclosing:
			admitted = state == letters || state == numbers
			if !admitted {
				finish()
				state = outside
			}
		case sigmaDanda:
			if state == letters {
				state = danda
			} else {
				finish()
				state = outside
			}
		case sigmaBothPunctuation:
			if state == letters {
				state = wordBridge
			} else if state == numbers {
				state = numberBridge
			} else {
				finish()
				state = outside
			}
		case sigmaWordPunctuation:
			if state == letters {
				state = wordBridge
			} else {
				finish()
				state = outside
			}
		case sigmaNumberPunctuation:
			if state == numbers {
				state = numberBridge
			} else {
				finish()
				state = outside
			}
		case sigmaNumberSuffix:
			if state == numbers {
				state = suffix
			} else {
				finish()
				state = outside
			}
		default:
			finish()
			state = outside
		}
		if admitted && info&sigmaCased != 0 {
			earlierCased = seenCased
			seenCased = true
			lastSigma = -1
			if r == 'Σ' {
				lastSigma = at
			}
		}
		at += width
	}
	finish()
	if copied > 0 {
		out.WriteString(text[copied:])
		text = out.String()
	}
	return cases.Lower(language.Und, cases.HandleFinalSigma(false)).String(text)
}
