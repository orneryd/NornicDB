package search

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/pkg/search/stemmer"
	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

const bm25TokenizerVersion = "unicode-nfkc-casefold-v1"

// AnalyzerFingerprint identifies the tokenization and stemming contract used
// to build a BM25 index.
type AnalyzerFingerprint struct {
	Tokenizer         string
	StemmerID         string
	StemmerAPIVersion int
	StemmerVersion    string
	StemmerSHA256     string
}

// BuildSettings renders the stable persisted-index compatibility fragment.
func (f AnalyzerFingerprint) BuildSettings() string {
	stemmerID := f.StemmerID
	if stemmerID == "" {
		stemmerID = stemmer.NoneID
	}
	stemmerAPI := "none"
	if f.StemmerAPIVersion > 0 {
		stemmerAPI = fmt.Sprintf("%d", f.StemmerAPIVersion)
	}
	stemmerVersion := f.StemmerVersion
	if stemmerVersion == "" {
		stemmerVersion = stemmer.NoneID
	}
	stemmerDigest := f.StemmerSHA256
	if stemmerDigest == "" {
		stemmerDigest = stemmer.NoneID
	}
	return fmt.Sprintf("tokenizer=%s;stemmer=%s;stemmer_api=%s;stemmer_version=%s;stemmer_sha256=%s",
		f.Tokenizer, stemmerID, stemmerAPI, stemmerVersion, stemmerDigest)
}

// Analyzer converts text into BM25 terms.
type Analyzer interface {
	Analyze(text string) []string
	Fingerprint() AnalyzerFingerprint
}

type textAnalyzer struct {
	fingerprint AnalyzerFingerprint
	stem        func(string) string
	stemTokens  func([]string) []string
}

// DefaultTextAnalyzer returns NornicDB's language-neutral BM25 analyzer.
func DefaultTextAnalyzer() Analyzer {
	return textAnalyzer{
		fingerprint: AnalyzerFingerprint{
			Tokenizer:         bm25TokenizerVersion,
			StemmerID:         stemmer.NoneID,
			StemmerAPIVersion: 0,
			StemmerVersion:    stemmer.NoneID,
			StemmerSHA256:     stemmer.NoneID,
		},
	}
}

// NewStemmedTextAnalyzer returns a BM25 analyzer backed by a registered stemmer.
func NewStemmedTextAnalyzer(reg stemmer.Registration) Analyzer {
	return textAnalyzer{
		fingerprint: AnalyzerFingerprint{
			Tokenizer:         bm25TokenizerVersion,
			StemmerID:         reg.ID,
			StemmerAPIVersion: reg.APIVersion,
			StemmerVersion:    reg.Version,
			StemmerSHA256:     reg.Digest,
		},
		stem:       reg.Stem,
		stemTokens: reg.StemTokens,
	}
}

func normalizeAnalyzer(analyzer Analyzer) Analyzer {
	if analyzer == nil {
		return DefaultTextAnalyzer()
	}
	return analyzer
}

func (a textAnalyzer) Fingerprint() AnalyzerFingerprint {
	fp := a.fingerprint
	if fp.Tokenizer == "" {
		fp.Tokenizer = bm25TokenizerVersion
	}
	if fp.StemmerID == "" {
		fp.StemmerID = stemmer.NoneID
	}
	if fp.StemmerVersion == "" {
		fp.StemmerVersion = stemmer.NoneID
	}
	if fp.StemmerSHA256 == "" {
		fp.StemmerSHA256 = stemmer.NoneID
	}
	return fp
}

func (a textAnalyzer) Analyze(text string) []string {
	text = cases.Fold().String(norm.NFKC.String(text))
	words := strings.FieldsFunc(text, func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsDigit(c) && !unicode.IsMark(c)
	})
	if len(words) == 0 {
		return words
	}
	if a.stemTokens != nil {
		return a.stemTokens(words)
	}
	if a.stem == nil {
		return words
	}
	out := words[:0]
	for _, word := range words {
		stemmed := a.stem(word)
		if stemmed == "" {
			continue
		}
		out = append(out, stemmed)
	}
	return out
}

// locatedTerm is one BM25 term of a text with the [start, end) bytes of the
// text it was read from.
type locatedTerm struct {
	term       string
	start, end int
}

// locatingAnalyzer is an Analyzer that can also say where in a text each
// of its terms was read, for a hit's term matches (#1060).
type locatingAnalyzer interface {
	locateIn(text string, terms map[string]*queryTerm, buffer []byte, visit func(term *queryTerm, start, end int)) []byte
}

// queryTerm is one analyzed query term whose occurrences a search reports.
type queryTerm struct {
	term  string
	idf   float64 // its BM25 weight: higher for a rarer term
	index int     // its position among the query's distinct terms
}

// locate returns the terms Analyze returns for text, in order, each with
// the bytes of text it was read from (TestLocateReadsAnalyzeTerms).
func (a textAnalyzer) locate(text string) []locatedTerm {
	var words []locatedTerm
	scanWords(text, nil, func(word []byte, start, end int) {
		words = append(words, locatedTerm{term: string(word), start: start, end: end})
	})
	if len(words) == 0 {
		return nil
	}
	if a.stemTokens != nil {
		tokens := make([]string, len(words))
		for index := range words {
			tokens[index] = words[index].term
		}
		stemmed := a.stemTokens(tokens)
		if len(stemmed) != len(words) {
			return nil
		}
		for index := range words {
			words[index].term = stemmed[index]
		}
		return words
	}
	if a.stem == nil {
		return words
	}
	out := words[:0]
	for _, located := range words {
		located.term = a.stem(located.term)
		if located.term != "" {
			out = append(out, located)
		}
	}
	return out
}

// locateIn calls visit with each occurrence in text of a term in terms, in
// order, with the bytes of text it was read from: locate's terms, filtered.
// Without stemming a word is looked up as it is read, in buffer (returned
// for reuse), so a long text costs one pass and no allocation per word.
func (a textAnalyzer) locateIn(text string, terms map[string]*queryTerm, buffer []byte, visit func(term *queryTerm, start, end int)) []byte {
	if a.stem != nil || a.stemTokens != nil {
		for _, located := range a.locate(text) {
			if term, ok := terms[located.term]; ok {
				visit(term, located.start, located.end)
			}
		}
		return buffer
	}
	return scanWords(text, buffer, func(word []byte, start, end int) {
		if term, ok := terms[string(word)]; ok {
			visit(term, start, end)
		}
	})
}

// scanWords calls emit with each of Analyze's words of text (before
// stemming), case-folded and NFKC-normalized, in order, with the bytes of
// text it was read from. The word is built in word's storage, reused between
// calls and returned. Analyze normalizes the whole text before splitting
// it; scanWords normalizes it segment by segment (norm.Iter, the same NFKC
// result) so each word keeps its source bytes. ASCII, which NFKC leaves
// alone and case folding lowercases, is read byte by byte. A word read from
// a segment that NFKC splits ("½" is "1⁄2") gets that whole segment.
func scanWords(text string, word []byte, emit func(word []byte, start, end int)) []byte {
	word = word[:0]
	start, end := -1, 0
	if isASCII(text) {
		for index := 0; index < len(text); index++ {
			c := text[index]
			if 'A' <= c && c <= 'Z' {
				c += 'a' - 'A'
			}
			if !('a' <= c && c <= 'z') && !('0' <= c && c <= '9') {
				if start >= 0 {
					emit(word, start, end)
					word, start = word[:0], -1
				}
				continue
			}
			if start < 0 {
				start = index
			}
			word = append(word, c)
			end = index + 1
		}
		if start >= 0 {
			emit(word, start, end)
		}
		return word
	}
	fold := cases.Fold()
	var folded [64]byte
	var iter norm.Iter
	iter.InitString(norm.NFKC, text)
	// The iterator can return part of a segment's output before it moves
	// past the segment ("½"): words read from it wait until it moves, and
	// end there.
	type waiting struct {
		word  string
		start int
	}
	var held []waiting
	for !iter.Done() {
		segmentStart := iter.Pos()
		segment := iter.Next()
		segmentEnd := iter.Pos()
		var out []byte
		if len(segment) == 1 && segment[0] < utf8.RuneSelf {
			out = folded[:1]
			out[0] = segment[0]
			if 'A' <= out[0] && out[0] <= 'Z' {
				out[0] += 'a' - 'A'
			}
		} else {
			fold.Reset()
			if n, _, err := fold.Transform(folded[:], segment, true); err == nil {
				out = folded[:n]
			} else {
				out = []byte(fold.String(string(segment)))
			}
		}
		advanced := segmentEnd > segmentStart
		if advanced {
			// What was held precedes this segment's words.
			for _, h := range held {
				emit([]byte(h.word), h.start, segmentEnd)
			}
			held = held[:0]
			if start >= 0 && end < 0 {
				end = segmentEnd
			}
		}
		for index := 0; index < len(out); {
			r, size := utf8.DecodeRune(out[index:])
			index += size
			if !unicode.IsLetter(r) && !unicode.IsDigit(r) && !unicode.IsMark(r) {
				if start >= 0 {
					if end < 0 {
						held = append(held, waiting{word: string(word), start: start})
					} else {
						emit(word, start, end)
					}
					word, start = word[:0], -1
				}
				continue
			}
			if start < 0 {
				start = segmentStart
			}
			word = utf8.AppendRune(word, r)
			if advanced {
				end = segmentEnd
			} else {
				end = -1
			}
		}
	}
	if start >= 0 {
		emit(word, start, end)
	}
	return word
}

func isASCII(text string) bool {
	for index := 0; index < len(text); index++ {
		if text[index] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

func analyzerFingerprint(analyzer Analyzer) AnalyzerFingerprint {
	return normalizeAnalyzer(analyzer).Fingerprint()
}
