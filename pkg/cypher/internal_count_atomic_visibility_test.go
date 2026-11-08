package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// countStorePublisher publishes one node into the store it decorates at the
// moment a counter read starts. That is the instant the superseded shape had
// already asked its separate presence probe, so the counter it then reads
// counts a node no read returns; an atomic count-store read instead sees the
// published node in its own snapshot and leaves the count to the rows.
//
// The node carries the counted label and an internal one at once: a MATCH
// returns no row for it, while its label counter still counts it.
type countStorePublisher struct {
	storage.Engine

	published bool
	publish   func() error
}

func (p *countStorePublisher) publishMixedNode() error {
	if p.published {
		return nil
	}
	p.published = true
	if p.publish == nil {
		return nil
	}
	return p.publish()
}

// ReadCountStore forwards the presence probe the current count owners
// ask before they read a counter (fail-first evidence only).
func (p *countStorePublisher) ReadCountStore(ctx context.Context, request storage.CountStoreReadRequest) (storage.CountStoreRead, error) {
	if err := p.publishMixedNode(); err != nil {
		return storage.CountStoreRead{}, err
	}
	return p.Engine.(storage.CountStoreReadEngine).ReadCountStore(ctx, request)
}

// NodeCountByLabel is the label counter read the count owners took after a
// separate presence probe.
func (p *countStorePublisher) NodeCountByLabel(ctx context.Context, label string) (int64, error) {
	if err := p.publishMixedNode(); err != nil {
		return 0, err
	}
	counter, ok := p.Engine.(storage.LabelStatsEngine)
	if !ok {
		return 0, fmt.Errorf("count-store publisher: %T keeps no label counts", p.Engine)
	}
	return counter.NodeCountByLabel(ctx, label)
}

// TestCountStoreCountsVisibleRowsWhenHiddenNodeCommits is the public-executor
// regression for the count-store visibility race: a node that commits after the
// eligibility proof and before the counter read used to be counted, so count()
// returned rows no read contains. Every parser configuration answers the rows
// of the same pattern.
func TestCountStoreCountsVisibleRowsWhenHiddenNodeCommits(t *testing.T) {
	ctx := context.Background()
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })

			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor177race")
			for _, id := range []string{"a", "b", "c"} {
				_, err := store.CreateNode(ctx, &storage.Node{
					ID:         storage.NodeID(id),
					Labels:     []string{"Nor177Race"},
					Properties: map[string]interface{}{"id": id},
				})
				require.NoError(t, err, id)
			}

			publisher := &countStorePublisher{Engine: store}
			publisher.publish = func() error {
				_, err := store.CreateNode(ctx, &storage.Node{
					ID:         storage.NodeID("nor177race:hidden"),
					Labels:     []string{"Nor177Race", "_internal"},
					Properties: map[string]interface{}{"id": "hidden"},
				})
				return err
			}
			exec := NewStorageExecutor(publisher)

			result, err := exec.Execute(ctx, "MATCH (n:Nor177Race) RETURN count(n) AS v", nil)
			require.NoError(t, err)
			require.True(t, publisher.published, "the counter read started after the eligibility proof")

			rows, err := exec.Execute(ctx, "MATCH (n:Nor177Race) RETURN n.id AS v ORDER BY v", nil)
			require.NoError(t, err)
			require.Equal(t, int64(len(rows.Rows)), result.Rows[0][0],
				"count() answers the rows the same pattern returns")
			require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows)
		})
	}
}
