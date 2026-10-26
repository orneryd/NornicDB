// Author: Lane #1644 (Codex). Public check/repair outcomes must distinguish
// an unread required fact from a genuinely unresolved dictionary identity.
package storage

import (
	"errors"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"testing"
)

type foundationDictionaryReadFault struct {
	calls, failAt int
	kind          StoredEntityReadKind
	failure       error
}

func (b *foundationDictionaryReadFault) WithStoredEntityRead(kind StoredEntityReadKind, _ string, read func() error) error {
	if kind == b.kind {
		b.calls++
		if b.calls == b.failAt {
			return b.failure
		}
	}
	return read()
}
func TestFoundationPublicDictionaryReadOutcome(t *testing.T) {
	for _, operation := range []string{"check", "repair-node", "repair-edge"} {
		t.Run(operation, func(t *testing.T) {
			base := createTestBadgerEngine(t)
			for _, id := range []NodeID{"test:a", "test:b"} {
				_, err := base.CreateNode(&Node{ID: id, Labels: []string{"Doc"}})
				require.NoError(t, err)
			}
			require.NoError(t, base.CreateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "R"}))
			cause := errors.New("required dictionary fact read failed")
			boundary := &foundationDictionaryReadFault{kind: StoredEntityNodePriorRead, failAt: 1, failure: cause}
			require.NoError(t, base.withUpdate(func(txn *badger.Txn) error {
				if operation == "repair-edge" {
					boundary.kind = StoredEntityEdgePriorRead
					key := base.outgoingIndexKeyStringLookup("test:a", "test:e")
					require.NotNil(t, key)
					return txn.Delete(key)
				}
				num, ok := base.idDict.lookupNodeNumID("test:a")
				require.True(t, ok)
				return txn.Delete(labelIndexKey("Doc", num))
			}))
			healthy, err := base.CheckIDDictionary()
			require.NoError(t, err)
			if operation == "repair-edge" {
				require.Contains(t, healthy.MismatchedEdges, EdgeID("test:e"))
			} else {
				require.Contains(t, healthy.MismatchedNodes, NodeID("test:a"))
			}
			if operation == "repair-node" {
				boundary.failAt = 2
			} // check succeeds; repair's required fact read fails.
			base.updateStoredEntityOwners(func(registry storedEntityObserverRegistry) {
				registry["test"] = storedEntityObserverEntry{boundary: boundary}
			})
			if operation == "check" {
				_, err = base.CheckIDDictionary()
			} else {
				_, err = base.RepairIDDictionary()
			}
			require.GreaterOrEqual(t, boundary.calls, boundary.failAt, "the public outcome must reach the failing fact read")
			require.ErrorIs(t, err, cause, "a storage failure cannot become an empty report or an ordinary unresolved identity")
		})
	}
}
