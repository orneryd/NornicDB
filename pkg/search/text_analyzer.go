package search

import (
	"fmt"
	"strings"
	"unicode"

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
	locate(text string) []locatedTerm
}

// locate returns the terms Analyze returns for text, in order, each with
// the bytes of text it was read from (TestLocateReadsAnalyzeTerms). Analyze
// normalizes the whole text before splitting it; locate normalizes it
// segment by segment (norm.Iter, the same NFKC result) so each word keeps
// the source bytes of the segments it came from. A word read from a
// segment that NFKC splits ("½" is "1⁄2") gets that whole segment.
func (a textAnalyzer) locate(text string) []locatedTerm {
	var words []locatedTerm
	var word strings.Builder
	start, end := -1, 0
	flush := func() {
		if start >= 0 {
			words = append(words, locatedTerm{term: word.String(), start: start, end: end})
		}
		word.Reset()
		start = -1
	}
	fold := cases.Fold()
	var iter norm.Iter
	iter.InitString(norm.NFKC, text)
	pending := 0 // words[pending:] may still end at -1
	for !iter.Done() {
		segmentStart := iter.Pos()
		segment := fold.String(string(iter.Next()))
		segmentEnd := iter.Pos()
		// The iterator can return part of a segment's output before it
		// moves past the segment ("½"): those words end where it next moves.
		sourceEnd := segmentEnd
		if segmentEnd == segmentStart {
			sourceEnd = -1
		}
		for _, r := range segment {
			if !unicode.IsLetter(r) && !unicode.IsDigit(r) && !unicode.IsMark(r) {
				flush()
				continue
			}
			if start < 0 {
				start = segmentStart
			}
			word.WriteRune(r)
			end = sourceEnd
		}
		if segmentEnd > segmentStart {
			if start >= 0 && end < 0 {
				end = segmentEnd
			}
			for index := pending; index < len(words); index++ {
				if words[index].end < 0 {
					words[index].end = segmentEnd
				}
			}
			pending = len(words)
		}
	}
	flush()
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

func analyzerFingerprint(analyzer Analyzer) AnalyzerFingerprint {
	return normalizeAnalyzer(analyzer).Fingerprint()
}
