package search

import (
	"errors"
	"math/rand"
	"reflect"
	"slices"
	"testing"
)

// Model the contract from a complete annotated population, independently of
// the streaming iterator's projection, passes and borrowed-vector lifetimes.
func lexicalConstructionModel(ids []string, vectors [][]float32, hints nodeLexicalHints) []hnswBuildPair {
	pairs := make([]hnswBuildPair, len(ids))
	anchors := make(map[vectorNodeID]bool)
	for i, id := range ids {
		hint, ok := lexicalHintForVectorID(id, hints)
		pairs[i] = hnswBuildPair{id: id, vec: vectors[i], hint: hint, hasHint: ok}
		if ok && id == mainKeyForLexicalHint(hint) {
			anchors[hint.ID] = true
		}
	}
	var seeds, rest []hnswBuildPair
	for _, pair := range pairs {
		if pair.hasHint && (!anchors[pair.hint.ID] || pair.id == mainKeyForLexicalHint(pair.hint)) {
			seeds = append(seeds, pair)
		} else {
			rest = append(rest, pair)
		}
	}
	slices.SortFunc(seeds, compareLexicalBuildPairs)
	return append(seeds, rest...)
}

func TestVectorEntryLexicalProjectionEquivalent(t *testing.T) {
	rng := rand.New(rand.NewSource(73))
	for scenario := 0; scenario < 80; scenario++ {
		var values []LexicalSeedHint
		var ids []string
		for i, owner := range []string{"other", "\x00n\x05otherv", " white ", "named-only", "chunks-only", "no-hint", "text-only"} {
			if owner != "no-hint" {
				values = append(values, LexicalSeedHint{ID: owner, Rank: uint32(i), Signature: uint64(i + 1)})
			}
			if owner == "text-only" {
				continue
			}
			if owner != "named-only" && owner != "chunks-only" && scenario%3 != 0 {
				ids = append(ids, nodeMainVectorKey(owner))
			}
			ids = append(ids, nodeNamedVectorKey(owner, "v"), nodePropertyVectorKey(owner, "p"), nodeChunkVectorKey(owner, 1), nodeChunkVectorKey(owner, 2))
		}
		hints := nodeHintsFromValues(values)
		ids = append(ids, "\x00invalid")
		rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
		vectors := make([][]float32, len(ids))
		for i := range ids {
			vectors[i] = []float32{float32(i), 1}
		}
		want := lexicalConstructionModel(ids, vectors, hints)
		for _, size := range []int{1, 3, 128} {
			// A file/read lease can reuse its chunk buffer. Seed vectors must
			// survive later scans; copy emitted pairs before the next chunk.
			iterate := func(size int, visit func([]string, [][]float32) error) error {
				buffer := make([][]float32, size)
				for i := range buffer {
					buffer[i] = make([]float32, 2)
				}
				for start := 0; start < len(ids); start += size {
					end := min(start+size, len(ids))
					for i := start; i < end; i++ {
						copy(buffer[i-start], vectors[i])
					}
					if err := visit(ids[start:end], buffer[:end-start]); err != nil {
						return err
					}
				}
				return nil
			}
			var got []hnswBuildPair
			err := hnswVectorChunkIterator(iterate, hints)(size, func(pairs []hnswBuildPair) error {
				for _, pair := range pairs {
					pair.vec = slices.Clone(pair.vec)
					got = append(got, pair)
				}
				return nil
			})
			if err != nil || !reflect.DeepEqual(want, got) {
				t.Fatalf("scenario %d batch %d: err=%v\nwant=%+v\ngot=%+v", scenario, size, err, want, got)
			}
		}
	}
	t.Log("240 exact sequence/vector/hint comparisons, including borrowed chunk buffers")
}

func TestVectorEntryLexicalProjectionPassesAndErrors(t *testing.T) {
	hints := nodeHintsFromValues([]LexicalSeedHint{{ID: " doc ", Rank: 1}, {ID: "\x00n\x03docv", Rank: 2}})
	ids := []string{nodeMainVectorKey(" doc "), nodeMainVectorKey("\x00n\x03docv"), nodeChunkVectorKey(" doc ", 1)}
	vectors := [][]float32{{1, 0}, {0, 1}, {1, 1}}
	for _, tc := range []struct {
		name string
		hints nodeLexicalHints
		passes int
	}{{"no hints", nil, 1}, {"all mains", hints, 2}} {
		t.Run(tc.name, func(t *testing.T) {
			passes := 0
			iterate := func(_ int, visit func([]string, [][]float32) error) error {
				passes++
				return visit(ids, vectors)
			}
			var got []hnswBuildPair
			if err := hnswVectorChunkIterator(iterate, tc.hints)(0, func(pairs []hnswBuildPair) error {
				got = append(got, pairs...)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if passes != tc.passes || !reflect.DeepEqual(lexicalConstructionModel(ids, vectors, tc.hints), got) {
				t.Fatalf("passes=%d want=%d; pairs=%+v", passes, tc.passes, got)
			}
		})
	}
	sentinel := errors.New("iterator or visitor failure")
	for _, failPass := range []int{1, 2, 3} {
		passes := 0
		iterate := func(_ int, visit func([]string, [][]float32) error) error {
			passes++
			if passes == failPass {
				return sentinel
			}
			// No main entries: exercise discovery, fallback and streaming.
			return visit(ids[2:], vectors[2:])
		}
		if err := hnswVectorChunkIterator(iterate, hints)(1, func([]hnswBuildPair) error { return nil }); !errors.Is(err, sentinel) {
			t.Fatalf("pass %d lost iterator error: %v", failPass, err)
		}
	}
	iterate := func(_ int, visit func([]string, [][]float32) error) error { return visit(ids, vectors) }
	if err := hnswVectorChunkIterator(iterate, hints)(1, func([]hnswBuildPair) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("lost visitor error: %v", err)
	}
}
