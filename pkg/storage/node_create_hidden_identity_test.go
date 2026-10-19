package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeCreateHiddenIdentityUsesStoredExistence(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, route := range []string{"explicit", "implicit", "bulk"} {
			t.Run(fmt.Sprintf("buffered=%v/%s", buffered, route), func(t *testing.T) {
				base, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: buffered})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, base.Close()) })
				_, err = base.CreateNode(&Node{ID: "identity:n", Properties: map[string]any{"payload": "original"}})
				require.NoError(t, err)
				require.NoError(t, base.SetNodeManuallySuppressed("identity:n", true))
				replacement := &Node{ID: "identity:n", Properties: map[string]any{"payload": "replacement"}}
				if route == "bulk" {
					err = base.BulkCreateNodes([]*Node{replacement})
				} else {
					tx, beginErr := base.BeginTransaction()
					require.NoError(t, beginErr)
					require.NoError(t, tx.SetImplicit(route == "implicit"))
					_, err = tx.CreateNode(replacement)
					if err == nil {
						err = tx.Commit()
					} else {
						require.NoError(t, tx.Rollback())
					}
				}
				require.ErrorIs(t, err, ErrAlreadyExists)
				for replay := 0; replay < 2; replay++ {
					if replay == 1 {
						require.NoError(t, base.FlushWriteBehind())
					}
					node, err := base.GetStoredNode("identity:n")
					require.NoError(t, err)
					require.Equal(t, "original", node.Properties["payload"])
					count, err := base.NodeCount()
					require.NoError(t, err)
					require.EqualValues(t, 1, count)
				}
			})
		}
	}
}
