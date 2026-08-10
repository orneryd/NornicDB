package search

import (
	"context"
	"testing"
)

func BenchmarkModifiedDice(b *testing.B) {
	left := []string{"adverse", "reactions", "from", "nonsteroidal", "antiinflammatory", "drugs"}
	right := []string{"adverse", "reaction", "from", "nonsteroidal", "antiinflammatory", "drug"}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = characterDice(left[index%len(left)], right[index%len(right)])
		_ = wordBigramDice(left, right)
		_ = alignedTokenDice(left, right)
	}
}

func BenchmarkDensePRF(b *testing.B) {
	expander := NewDensePRFDiceExpander(QueryExpansionConfig{
		SourceTopK:        10,
		MaxCandidates:     256,
		MaxTerms:          10,
		MaxPhraseWords:    3,
		MinPassageSupport: 1,
		UseDice:           true,
		DiceThreshold:     0.85,
		IDF:               func(string) float64 { return 1.5 },
	})
	sources := make([]ExpansionSource, 10)
	for index := range sources {
		sources[index] = ExpansionSource{
			VectorID:      "passage-" + string(rune('a'+index)),
			SemanticRank:  index + 1,
			SemanticScore: 0.9 - float64(index)*0.03,
			Text:          "nonsteroidal antiinflammatory drugs can cause nausea dizziness and adverse reactions in sensitive patients",
		}
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := expander.Expand(ctx, "ibuprofen side effects", sources); err != nil {
			b.Fatal(err)
		}
	}
}
