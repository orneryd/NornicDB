package search

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode"

	"github.com/orneryd/nornicdb/pkg/envutil"
)

// ExpansionSource is a corpus passage selected by semantic retrieval.
// Text is request-local and must not be logged by expansion implementations.
type ExpansionSource struct {
	VectorID      string
	NodeID        string
	SemanticRank  int
	SemanticScore float64
	Text          string
}

// ExpansionCandidate records the evidence used to select a lexical expansion.
type ExpansionCandidate struct {
	Text             string
	Tokens           []string
	BestSemanticRank int
	SemanticSupport  float64
	PassageSupport   int
	IDF              float64
	Score            float64
}

// QueryExpansionResult contains bounded terms suitable for appending to a BM25 query.
type QueryExpansionResult struct {
	Terms      []string
	Candidates int
	Sources    int
}

// QueryExpander produces request-local lexical terms from semantic passages.
type QueryExpander interface {
	Name() string
	Expand(context.Context, string, []ExpansionSource) (QueryExpansionResult, error)
}

// QueryExpansionConfig controls bounded dense pseudo-relevance feedback.
type QueryExpansionConfig struct {
	SourceTopK        int
	MaxCandidates     int
	MaxTerms          int
	MaxPhraseWords    int
	MinPassageSupport int
	MinIDF            float64
	DiceThreshold     float64
	UseDice           bool
	IDF               func(string) float64
}

// DensePRFDiceExpander implements dense-seeded pseudo-relevance feedback.
// Dice is used only for lexical variant normalization after evidence ranking.
type DensePRFDiceExpander struct {
	config QueryExpansionConfig
}

// NewDensePRFDiceExpander creates a bounded expander. A nil IDF function uses
// a neutral value of one, which keeps the scorer useful in isolated tests.
func NewDensePRFDiceExpander(config QueryExpansionConfig) *DensePRFDiceExpander {
	if config.SourceTopK < 1 || config.SourceTopK > 20 {
		config.SourceTopK = 10
	}
	if config.MaxCandidates < 16 {
		config.MaxCandidates = 256
	}
	if config.MaxTerms < 1 {
		config.MaxTerms = 10
	}
	if config.MaxPhraseWords < 1 || config.MaxPhraseWords > 3 {
		config.MaxPhraseWords = 3
	}
	if config.MinPassageSupport < 1 {
		config.MinPassageSupport = 1
	}
	if config.DiceThreshold < 0 || config.DiceThreshold > 1 {
		config.DiceThreshold = 0.85
	}
	if config.IDF == nil {
		config.IDF = func(string) float64 { return 1 }
	}
	return &DensePRFDiceExpander{config: config}
}

func (e *DensePRFDiceExpander) Name() string { return "dense_prf_dice" }

func (e *DensePRFDiceExpander) Expand(ctx context.Context, query string, sources []ExpansionSource) (QueryExpansionResult, error) {
	if err := ctx.Err(); err != nil {
		return QueryExpansionResult{}, err
	}
	if len(sources) == 0 {
		return QueryExpansionResult{}, nil
	}

	queryTerms := make(map[string]struct{}, len(tokenize(query)))
	for _, token := range tokenize(query) {
		queryTerms[token] = struct{}{}
	}
	type evidence struct {
		candidate ExpansionCandidate
		sources   map[string]struct{}
	}
	candidates := make(map[string]*evidence)
	for sourceIndex, source := range sources {
		if err := ctx.Err(); err != nil {
			return QueryExpansionResult{}, err
		}
		if source.Text == "" || source.SemanticRank < 1 || source.SemanticScore < 0 || math.IsNaN(source.SemanticScore) || math.IsInf(source.SemanticScore, 0) {
			continue
		}
		sourceKey := source.VectorID
		if sourceKey == "" {
			sourceKey = source.NodeID
		}
		if sourceKey == "" {
			sourceKey = string(rune(sourceIndex + 1))
		}
		for _, tokens := range expansionNgrams(source.Text, e.config.MaxPhraseWords) {
			term := strings.Join(tokens, " ")
			if _, original := queryTerms[term]; original || sameNormalizedTokens(tokens, tokenize(query)) {
				continue
			}
			idf := phraseIDF(tokens, e.config.IDF)
			if idf < e.config.MinIDF {
				continue
			}
			item := candidates[term]
			if item == nil {
				item = &evidence{candidate: ExpansionCandidate{Text: term, Tokens: tokens, BestSemanticRank: source.SemanticRank, IDF: idf}, sources: make(map[string]struct{})}
				candidates[term] = item
			}
			if source.SemanticRank < item.candidate.BestSemanticRank {
				item.candidate.BestSemanticRank = source.SemanticRank
			}
			if _, counted := item.sources[sourceKey]; !counted {
				item.sources[sourceKey] = struct{}{}
				item.candidate.SemanticSupport += source.SemanticScore / float64(source.SemanticRank)
			}
		}
	}

	ranked := make([]ExpansionCandidate, 0, len(candidates))
	for _, item := range candidates {
		item.candidate.PassageSupport = len(item.sources)
		if item.candidate.PassageSupport < e.config.MinPassageSupport {
			continue
		}
		item.candidate.Score = item.candidate.IDF * item.candidate.SemanticSupport * (1 + math.Log1p(float64(item.candidate.PassageSupport-1)))
		ranked = append(ranked, item.candidate)
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].Score != ranked[j].Score {
			return ranked[i].Score > ranked[j].Score
		}
		return ranked[i].Text < ranked[j].Text
	})
	if len(ranked) > e.config.MaxCandidates {
		ranked = ranked[:e.config.MaxCandidates]
	}

	result := QueryExpansionResult{Candidates: len(ranked), Sources: len(sources)}
	selected := make([]ExpansionCandidate, 0, e.config.MaxTerms)
	for _, candidate := range ranked {
		if len(selected) == e.config.MaxTerms {
			break
		}
		if e.config.UseDice && isDiceVariant(candidate, selected, e.config.DiceThreshold) {
			continue
		}
		selected = append(selected, candidate)
		result.Terms = append(result.Terms, candidate.Text)
	}
	return result, nil
}

func expansionNgrams(text string, maxWords int) [][]string {
	words := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	var result [][]string
	for start := 0; start < len(words); start++ {
		if len(words[start]) < 2 || isStopWord(words[start]) {
			continue
		}
		for width := 1; width <= maxWords && start+width <= len(words); width++ {
			phrase := words[start : start+width]
			valid := true
			for _, word := range phrase {
				if len(word) < 2 || isStopWord(word) {
					valid = false
					break
				}
			}
			if !valid {
				break
			}
			result = append(result, append([]string(nil), phrase...))
		}
	}
	return result
}

func phraseIDF(tokens []string, idf func(string) float64) float64 {
	if len(tokens) == 0 {
		return 0
	}
	total := 0.0
	for _, token := range tokens {
		value := idf(token)
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return 0
		}
		total += value
	}
	return total / float64(len(tokens))
}

func sameNormalizedTokens(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if normalizeDiceToken(left[i]) != normalizeDiceToken(right[i]) {
			return false
		}
	}
	return true
}

func isDiceVariant(candidate ExpansionCandidate, selected []ExpansionCandidate, threshold float64) bool {
	for _, prior := range selected {
		if len(candidate.Tokens) != len(prior.Tokens) {
			continue
		}
		if len(candidate.Tokens) == 1 {
			if characterDice(candidate.Tokens[0], prior.Tokens[0]) >= threshold {
				return true
			}
			continue
		}
		if wordBigramDice(candidate.Tokens, prior.Tokens) >= threshold || alignedTokenDice(candidate.Tokens, prior.Tokens) >= threshold {
			return true
		}
	}
	return false
}

func characterDice(left, right string) float64 {
	leftBigrams := tokenBigrams(normalizeDiceToken(left))
	rightBigrams := tokenBigrams(normalizeDiceToken(right))
	return multisetDice(leftBigrams, rightBigrams)
}

func wordBigramDice(left, right []string) float64 {
	if len(left) < 2 || len(right) < 2 {
		return -1
	}
	return multisetDice(wordBigrams(left), wordBigrams(right))
}

func alignedTokenDice(left, right []string) float64 {
	if len(left) != len(right) || len(left) == 0 {
		return -1
	}
	total := 0.0
	for i := range left {
		total += characterDice(left[i], right[i])
	}
	return total / float64(len(left))
}

func tokenBigrams(token string) []string {
	runes := []rune(token)
	if len(runes) < 2 {
		return nil
	}
	bigrams := make([]string, 0, len(runes)-1)
	for i := 0; i < len(runes)-1; i++ {
		bigrams = append(bigrams, string(runes[i:i+2]))
	}
	return bigrams
}

func wordBigrams(tokens []string) []string {
	if len(tokens) < 2 {
		return nil
	}
	bigrams := make([]string, 0, len(tokens)-1)
	for i := 0; i < len(tokens)-1; i++ {
		bigrams = append(bigrams, normalizeDiceToken(tokens[i])+"\x00"+normalizeDiceToken(tokens[i+1]))
	}
	return bigrams
}

func multisetDice(left, right []string) float64 {
	if len(left) == 0 && len(right) == 0 {
		return 1
	}
	if len(left) == 0 || len(right) == 0 {
		return 0
	}
	counts := make(map[string]int, len(left))
	for _, value := range left {
		counts[value]++
	}
	intersection := 0
	for _, value := range right {
		if counts[value] > 0 {
			counts[value]--
			intersection++
		}
	}
	return 2 * float64(intersection) / float64(len(left)+len(right))
}

func normalizeDiceToken(token string) string {
	token = strings.ToLower(token)
	if len(token) > 3 && strings.HasSuffix(token, "s") {
		return strings.TrimSuffix(token, "s")
	}
	return token
}

func queryExpansionEnabled() bool {
	return envutil.GetBoolStrict("NORNICDB_SEARCH_QUERY_EXPANSION_ENABLED", false)
}

func queryExpansionConfigFromEnv() QueryExpansionConfig {
	return QueryExpansionConfig{
		SourceTopK:        envutil.GetInt("NORNICDB_SEARCH_QUERY_EXPANSION_SOURCE_TOP_K", 10),
		MaxCandidates:     envutil.GetInt("NORNICDB_SEARCH_QUERY_EXPANSION_MAX_CANDIDATES", 256),
		MaxTerms:          envutil.GetInt("NORNICDB_SEARCH_QUERY_EXPANSION_MAX_TERMS", 10),
		MaxPhraseWords:    envutil.GetInt("NORNICDB_SEARCH_QUERY_EXPANSION_MAX_PHRASE_WORDS", 3),
		MinPassageSupport: envutil.GetInt("NORNICDB_SEARCH_QUERY_EXPANSION_MIN_PASSAGE_SUPPORT", 1),
		MinIDF:            envutil.GetFloat("NORNICDB_SEARCH_QUERY_EXPANSION_MIN_IDF", 0),
		DiceThreshold:     envutil.GetFloat("NORNICDB_SEARCH_QUERY_EXPANSION_DICE_THRESHOLD", 0.85),
		UseDice:           envutil.GetBoolStrict("NORNICDB_SEARCH_QUERY_EXPANSION_DICE_ENABLED", true),
	}
}

func queryExpansionCacheKeySuffix() string {
	if !queryExpansionEnabled() {
		return "query_expansion=disabled"
	}
	config := NewDensePRFDiceExpander(queryExpansionConfigFromEnv()).config
	return fmt.Sprintf(
		"query_expansion=%s:%d:%d:%d:%d:%g:%g:%t",
		envutil.Get("NORNICDB_SEARCH_QUERY_EXPANSION_PROVIDER", "dense_prf_dice"),
		config.SourceTopK,
		config.MaxCandidates,
		config.MaxTerms,
		config.MaxPhraseWords,
		config.MinIDF,
		config.DiceThreshold,
		config.UseDice,
	)
}

func expansionSourcesFromScored(scored []ScoredCandidate, limit int) []ExpansionSource {
	if limit < 1 {
		return nil
	}
	sources := make([]ExpansionSource, 0, limit)
	seenVectors := make(map[string]struct{}, limit)
	seenNodes := make(map[string]struct{}, limit)
	for rank, hit := range scored {
		if len(sources) == limit {
			break
		}
		if hit.ID == "" || hit.Score < 0 || math.IsNaN(hit.Score) || math.IsInf(hit.Score, 0) {
			continue
		}
		nodeID := normalizeVectorResultIDToNodeID(hit.ID)
		if _, exists := seenVectors[hit.ID]; exists {
			continue
		}
		if _, exists := seenNodes[nodeID]; exists {
			continue
		}
		seenVectors[hit.ID] = struct{}{}
		seenNodes[nodeID] = struct{}{}
		sources = append(sources, ExpansionSource{VectorID: hit.ID, NodeID: nodeID, SemanticRank: rank + 1, SemanticScore: hit.Score})
	}
	return sources
}
