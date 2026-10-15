package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Regression for #1228 (was #1204): Cypher writes through transactions,
// whose creation paths must stamp times just like direct engine writes.
func TestTransactionCreationTimesRegression(t *testing.T) {
	for _, bulk := range []bool{false, true} {
		for _, supplied := range []bool{false, true} {
			name := "single"
			if bulk {
				name = "bulk"
			}
			if supplied {
				name += "/supplied"
			} else {
				name += "/unset"
			}
			t.Run(name, func(t *testing.T) {
				eng := NewMemoryEngine()
				t.Cleanup(func() { require.NoError(t, eng.Close()) })
				tx, err := eng.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				before := time.Now()
				created := time.Date(2026, 1, 2, 3, 4, 5, 123, time.UTC)
				updated := created.Add(time.Hour)
				a := &Node{ID: "test:a"}
				b := &Node{ID: "test:b"}
				edge := &Edge{ID: "test:e", StartNode: a.ID, EndNode: b.ID, Type: "LINKS"}
				if supplied {
					a.CreatedAt, a.UpdatedAt = created, updated
					b.CreatedAt, b.UpdatedAt = created, updated
					edge.CreatedAt, edge.UpdatedAt = created, updated
				}
				_, err = tx.CreateNode(a)
				require.NoError(t, err)
				_, err = tx.CreateNode(b)
				require.NoError(t, err)
				if bulk {
					err = tx.BulkCreateEdges([]*Edge{edge})
				} else {
					err = tx.CreateEdge(edge)
				}
				require.NoError(t, err)
				require.NoError(t, tx.Commit())
				n, err := eng.GetNode(a.ID)
				require.NoError(t, err)
				r, err := eng.GetEdge(edge.ID)
				require.NoError(t, err)
				for _, times := range [][2]time.Time{{n.CreatedAt, n.UpdatedAt}, {r.CreatedAt, r.UpdatedAt}} {
					if supplied {
						require.True(t, created.Equal(times[0]))
						require.True(t, updated.Equal(times[1]))
					} else {
						require.False(t, times[0].Before(before), "creation must be stamped")
						require.False(t, times[0].After(time.Now()))
						require.True(t, times[0].Equal(times[1]), "initial update equals creation")
					}
				}
			})
		}
	}
}
