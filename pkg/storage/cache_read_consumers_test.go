package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

func TestCachedNestedValuesAcrossPopulationReaders(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, route := range []string{"visible", "all", "first-label", "transaction-point"} {
			t.Run(fmt.Sprintf("buffered=%v/%s", buffered, route), func(t *testing.T) {
				b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: buffered, WriteBehindInterval: time.Hour}))
				require.NoError(t, err)
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				t.Cleanup(func() { _ = tx.Rollback() })
				require.NoError(t, tx.SetNamespace("test"))
				require.NoError(t, tx.SetImplicit(buffered))
				id, err := tx.CreateNode(&Node{ID: "test:population-owner", Labels: []string{"PopulationOwner"}, Properties: map[string]any{"nested": map[string]any{"values": []int64{1, 2}}, "omitted": "retained"}})
				require.NoError(t, err)
				require.NoError(t, tx.CommitContext(context.Background()))
				var reader *BadgerTransaction
				if route == "transaction-point" {
					reader, err = b.BeginTransaction()
					require.NoError(t, err)
					t.Cleanup(func() { _ = reader.Rollback() })
					require.NoError(t, reader.SetNamespace("test"))
				}
				read := func() (*Node, error) {
					switch route {
					case "visible":
						return b.GetNode(id)
					case "first-label":
						return b.GetFirstNodeByLabel("PopulationOwner")
					case "transaction-point":
						return reader.GetNodeProjected(id, []string{"nested"})
					default:
						nodes, err := b.AllNodes()
						if err != nil {
							return nil, err
						}
						for _, n := range nodes {
							if n.ID == id {
								return n, nil
							}
						}
						return nil, ErrNotFound
					}
				}
				first, err := read()
				require.NoError(t, err)
				require.NotNil(t, first)
				first.Properties["nested"].(map[string]any)["values"].([]int64)[0] = 99
				second, err := read()
				require.NoError(t, err)
				require.NotNil(t, second)
				require.Equal(t, []int64{1, 2}, second.Properties["nested"].(map[string]any)["values"])
				if reader != nil {
					require.NotContains(t, second.Properties, "omitted")
				}
				require.NoError(t, b.FlushWriteBehind(nil))
				stored, err := b.GetStoredNode(id)
				require.NoError(t, err)
				require.Equal(t, []int64{1, 2}, stored.Properties["nested"].(map[string]any)["values"])
			})
		}
	}
}
