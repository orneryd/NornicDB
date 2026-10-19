package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Permanent copy of review r1's deterministic regression: an acknowledged
// holder must remain visible to NODE KEY validation after buffer retirement.
func TestWriteBehind_NodeKeyAfterBufferRetirement(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	b.writeBehind.Close()
	sm := b.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddConstraint(factKey))
	idx, ok := sm.ConstraintPropertyIndex(factKey.Name)
	require.True(t, ok)
	require.NoError(t, sm.BackfillCompositeIndex(idx.Name, nil))
	require.NoError(t, bufferedTry(t, b, factNode("holder", 1, "name", 1)))
	require.Error(t, bufferedTry(t, b, factNode("pending-duplicate", 1, "name", 1)))
	require.NoError(t, b.FlushWriteBehind())
	require.Empty(t, b.writeBehind.draining)
	require.Zero(t, b.writeBehind.PendingOps())
	require.Len(t, idx.LookupFull(int64(1), "name", int64(1)), 1)
	err := bufferedTry(t, b, factNode("retired-duplicate", 1, "name", 1))
	require.Error(t, err, "a committed holder must still reject duplicate NODE KEY after retirement")
}

func TestWriteBehind_IdleFilledKeyConstraints(t *testing.T) {
	for _, kind := range []ConstraintType{ConstraintNodeKey, ConstraintUnique} {
		for _, retired := range []bool{false, true} {
			name := string(kind) + "/idle"
			if retired {
				name = string(kind) + "/retired"
			}
			t.Run(name, func(t *testing.T) {
				b := newWriteBehindTestEngine(t, time.Hour)
				b.writeBehind.Close()
				constraint := factKey
				constraint.Type = kind
				sm := b.GetSchemaForNamespace("test")
				require.NoError(t, sm.AddConstraint(constraint))
				idx, owned := sm.ConstraintPropertyIndex(constraint.Name)
				require.True(t, owned)
				require.NoError(t, sm.BackfillCompositeIndex(idx.Name, nil))
				if retired {
					require.NoError(t, bufferedTry(t, b, factNode("holder", 1, "name", 1)))
					require.NoError(t, b.FlushWriteBehind())
				} else {
					_, err := b.CreateNode(factNode("holder", 1, "name", 1))
					require.NoError(t, err)
				}
				require.Empty(t, b.writeBehind.draining)
				require.Zero(t, b.writeBehind.PendingOps())
				require.Len(t, idx.LookupFull(int64(1), "name", int64(1)), 1)
				require.Error(t, bufferedTry(t, b, factNode("duplicate", 1, "name", 1)))
				require.NoError(t, bufferedTry(t, b, factNode("different", 1, "name", 2)))
			})
		}
	}
}
