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

func analyzerFingerprint(analyzer Analyzer) AnalyzerFingerprint {
	return normalizeAnalyzer(analyzer).Fingerprint()
}
