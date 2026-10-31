// Author: Codex, NOR-160 public storage callback ownership, 29 October 2026.
package storage

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/readownership"
	"github.com/stretchr/testify/require"
)

// Callbacks are public read results too. Keep their actual returned containers
// until the entire operation finishes, and mutate a predicate's selected image
// while requiring accepted full images and subsequent reads to stay unchanged.
func TestNOR160CallerOwnedPublicCallbackResults(t *testing.T) {
	for _, kind := range []string{"badger", "memory", "namespaced", "wal", "traced", "composite"} {
		t.Run(kind, func(t *testing.T) {
			t.Run("MatchEdgesBetween/predicate", func(t *testing.T) {
				fixture := nor160PublicEngineFixtureFor(t, kind)
				engine := fixture.engine
				_, err := engine.GetOutgoingEdges(fixture.a)
				require.NoError(t, err)
				edge, err := engine.GetEdge(fixture.edge)
				require.NoError(t, err)
				want := readownership.Capture(edge)
				var candidateImage any
				for pass := 0; pass < 3; pass++ {
					calls := 0
					matched, err := MatchEdgesBetween(engine, fixture.a, fixture.b, "T", []string{"nested"}, func(candidate *Edge) bool {
						calls++
						image := readownership.Capture(candidate)
						if candidateImage == nil {
							candidateImage = image
							// This must exercise a selected mutable value, not an
							// empty header or a declined optional capability.
							require.Contains(t, candidate.Properties, "nested")
						} else {
							require.Equal(t, candidateImage, image, "a previous predicate changed the next candidate")
						}
						readownership.Mutate(candidate)
						return true
					})
					require.NoError(t, err)
					require.Equal(t, 1, calls)
					require.Len(t, matched, 1)
					require.Equal(t, want, readownership.Capture(matched[0]), "predicate mutation changed the full accepted image")
				}
			})
			t.Run("StreamNodeChunks/retained-container", func(t *testing.T) {
				fixture := nor160PublicEngineFixtureFor(t, kind)
				streamer, ok := fixture.engine.(StreamingEngine)
				if !ok {
					// The traced wrapper exposes no chunk stream; callers use
					// their non-streaming fallback. Any other engine must stream.
					require.Equal(t, "traced", kind)
					return
				}
				var chunks [][]*Node
				var images []any
				err := streamer.StreamNodeChunks(context.Background(), 1, func(chunk []*Node) error {
					require.Len(t, chunk, 1)
					chunks = append(chunks, chunk)
					images = append(images, readownership.Capture(chunk))
					return nil
				})
				require.NoError(t, err)
				require.Len(t, chunks, 2)
				for index, chunk := range chunks {
					require.Equal(t, images[index], readownership.Capture(chunk), "a later callback overwrote an earlier caller-owned chunk")
				}
				readownership.Mutate(chunks[0])
				require.Equal(t, images[1], readownership.Capture(chunks[1]), "one caller-owned chunk changed another")
			})
		})
	}
}
