package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// B03/report6: the store's key limit. A commit whose keys the store cannot hold
// is refused with ErrStoreKeyTooLong before the write buffer's WAL or the
// prepared hand-off accepts it, and the journal holds no record of it. Every
// case sizes one family of a commit's keys over the limit with the commit's
// other keys within it, so the refusal proves that family is admitted; a control
// of the same shape within the limit commits and is there after crashClose and
// the recovery at open. Both write modes and both commit modes.
//
// The sizes below are the whole proof of each case, so each key's length is
// stated with the builder that writes it. The namespace of every transaction is
// test (4 bytes) and the label admissionStored counts is Admission (9 bytes).
const (
	// keyAdmissionNamespace is the namespace of every transaction below. Its
	// length is part of a count key and of the property dictionary's key.
	keyAdmissionNamespace = "test"
	// keyAdmissionLabel is the label admissionStored counts the nodes of.
	keyAdmissionLabel = "Admission"

	// A node body key is prefixNode + the node's ID (nodeKey): the ID's length
	// plus 1.
	keyAdmissionNodeID     = 60000 // key 60001 bytes
	keyAdmissionNodeIDOver = 65000 // key 65001 bytes

	// countDeltaKey for a label is [prefixMVCCMeta, kind] + namespace + 0x00 +
	// label + 0x00 + 8 bytes: the label's length plus 16. The label's index key
	// (labelIndexKey) is its length plus 10.
	keyAdmissionLabelLength     = 64960 // count key 64976 bytes, index key 64970
	keyAdmissionLabelLengthOver = 64990 // count key 65006 bytes, index key 65000

	// edgeBetweenIndexKey is prefix + start 8 + end 8 + type + 0x00 + edge 8:
	// the type's length plus 26, longer than the type's index key (plus 10) and
	// its edge-type count key (plus 16).
	keyAdmissionEdgeType     = 64960 // lookup key 64986 bytes
	keyAdmissionEdgeTypeOver = 64980 // lookup key 65006 bytes

	// propKeyForwardKey is prefix + varint(namespace) + namespace +
	// varint(name) + name: the name's length plus 9 in this namespace.
	keyAdmissionPropertyName     = 64960 // forward key 64969 bytes
	keyAdmissionPropertyNameOver = 64995 // forward key 65004 bytes
)

// keyAdmissionCase is one commit whose keys do or do not fit the store.
type keyAdmissionCase struct {
	name string
	// stage writes the commit's changes.
	stage func(ctx context.Context, tx *BadgerTransaction) error
	// refused reports that one of the commit's keys is more than the store
	// holds.
	refused bool
	// names, when set, is the key the refusal must be about, as the error
	// names it.
	names string
	// nodes is how many nodes labelled Admission an applied commit leaves.
	nodes int
}

// keyAdmissionNodeKey creates one node whose ID, test: followed by n's, is
// idBytes bytes long.
func keyAdmissionNodeKey(idBytes int) func(context.Context, *BadgerTransaction) error {
	return func(ctx context.Context, tx *BadgerTransaction) error {
		id := keyAdmissionNamespace + ":" + strings.Repeat("n", idBytes-len(keyAdmissionNamespace)-1)
		_, err := tx.CreateNode(ctx, &Node{ID: NodeID(id), Labels: []string{keyAdmissionLabel}})
		return err
	}
}

// keyAdmissionLabelKey creates one node with the label admissionStored counts
// and a second label of labelBytes bytes.
func keyAdmissionLabelKey(labelBytes int) func(context.Context, *BadgerTransaction) error {
	return func(ctx context.Context, tx *BadgerTransaction) error {
		_, err := tx.CreateNode(ctx, &Node{ID: "test:key-label", Labels: []string{keyAdmissionLabel, strings.Repeat("l", labelBytes)}})
		return err
	}
}

// keyAdmissionEdgeTypeKey creates two endpoints and the relationship between
// them whose type is typeBytes bytes long.
func keyAdmissionEdgeTypeKey(typeBytes int) func(context.Context, *BadgerTransaction) error {
	return func(ctx context.Context, tx *BadgerTransaction) error {
		for _, id := range []NodeID{"test:key-start", "test:key-end"} {
			if _, err := tx.CreateNode(ctx, &Node{ID: id, Labels: []string{keyAdmissionLabel}}); err != nil {
				return err
			}
		}
		return tx.CreateEdge(ctx, &Edge{ID: "test:key-edge", Type: strings.Repeat("t", typeBytes), StartNode: "test:key-start", EndNode: "test:key-end"})
	}
}

// keyAdmissionPropertyKey creates one node with one property whose name is
// nameBytes bytes long.
func keyAdmissionPropertyKey(nameBytes int) func(context.Context, *BadgerTransaction) error {
	return func(ctx context.Context, tx *BadgerTransaction) error {
		_, err := tx.CreateNode(ctx, &Node{ID: "test:key-property", Labels: []string{keyAdmissionLabel}, Properties: map[string]any{strings.Repeat("p", nameBytes): int64(1)}})
		return err
	}
}

func keyAdmissionCases() []keyAdmissionCase {
	return []keyAdmissionCase{
		{name: "node ID within the limit", stage: keyAdmissionNodeKey(keyAdmissionNodeID), nodes: 1},
		// The node's body key and the ID dictionary's forward key of it are the
		// same length, so either may be the key the refusal names.
		{name: "node ID over the limit", stage: keyAdmissionNodeKey(keyAdmissionNodeIDOver), refused: true},
		{name: "label within the limit", stage: keyAdmissionLabelKey(keyAdmissionLabelLength), nodes: 1},
		{name: "label over the limit", stage: keyAdmissionLabelKey(keyAdmissionLabelLengthOver), refused: true, names: "the label count key of label"},
		{name: "relationship type within the limit", stage: keyAdmissionEdgeTypeKey(keyAdmissionEdgeType), nodes: 2},
		{name: "relationship type over the limit", stage: keyAdmissionEdgeTypeKey(keyAdmissionEdgeTypeOver), refused: true, names: "the relationship lookup key of relationship type"},
		{name: "property name within the limit", stage: keyAdmissionPropertyKey(keyAdmissionPropertyName), nodes: 1},
		{name: "property name over the limit", stage: keyAdmissionPropertyKey(keyAdmissionPropertyNameOver), refused: true, names: "the property-key dictionary's forward key of property"},
	}
}

// keyAdmissionJournalOperations counts the journal's entries per operation:
// every record of any kind, an accepted entity commit and a buffered commit
// among them.
func keyAdmissionJournalOperations(t *testing.T, dir string) map[OperationType]int {
	t.Helper()
	entries, err := ReadWALEntriesFromDir(context.Background(), filepath.Join(dir, "wal"))
	require.NoError(t, err)
	counts := make(map[OperationType]int, 2)
	for _, entry := range entries {
		counts[entry.Operation]++
	}
	return counts
}

func TestStoreKeyLimitRefusesTheCommitBeforeTheJournal(t *testing.T) {
	for _, c := range keyAdmissionCases() {
		for _, async := range []bool{false, true} {
			for mode, modeName := range admissionModes {
				t.Run(fmt.Sprintf("%s/async=%v/%s", c.name, async, modeName), func(t *testing.T) {
					dir := t.TempDir()
					b := admissionEngine(t, dir, async, false)
					require.Equal(t, async, b.writeBehindEnabled())
					before := keyAdmissionJournalOperations(t, dir)
					err := admissionCommit(t, b, mode, "key-admission", c.stage)
					want := c.nodes
					if c.refused {
						want = 0
						requireCommitCapacityRefusal(t, err, ErrStoreKeyTooLong, storeKeyLimit, dir, "key-admission", c.names)
						require.Equal(t, before, keyAdmissionJournalOperations(t, dir), "the journal gained no record of the refused commit")
					} else {
						require.NoError(t, err, "the commit is applied")
					}
					require.Equal(t, want, admissionStored(t, b))
					// The refused commit left nothing of itself in the journal,
					// so it is not there after crashClose and the recovery at
					// open; the applied one is.
					b = admissionReopen(t, b, dir, async, false)
					require.Equal(t, want, admissionStored(t, b), "after crashClose and the recovery at open")
				})
			}
		}
	}
}

// The positional (label, type) count keys of a commit (edgeTypeLabelCountKey)
// are [prefixMVCCMeta, sub] + namespace + 0x00 + label + 0x00 + type: the
// label's and the type's lengths plus 8 in this namespace. With the 40-byte
// label below they are 14 bytes longer than the relationship lookup keys of the
// same commit, so a relationship type whose own keys fit is still refused, and a
// type 30 bytes shorter applies the same commit.
const (
	keyAdmissionPositionalLabel    = 40
	keyAdmissionPositionalType     = 64930 // positional key 64978 bytes, lookup key 64956
	keyAdmissionPositionalTypeOver = 64960 // positional key 65008 bytes, lookup key 64986
)

// requireKeyAdmissionEdge checks whether the store holds the relationship the
// positional cases write.
func requireKeyAdmissionEdge(t *testing.T, b *BadgerEngine, held bool, stage string) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, b.FlushWriteBehind(ctx), stage)
	edge, err := b.GetEdge(ctx, "test:key-edge")
	if !held {
		require.ErrorIs(t, err, ErrNotFound, stage)
		require.Nil(t, edge, stage)
		return
	}
	require.NoError(t, err, stage)
	require.Equal(t, EdgeID("test:key-edge"), edge.ID, stage)
}

func TestStoreKeyLimitRefusesAPositionalCountKeyOverTheLimit(t *testing.T) {
	longLabel := strings.Repeat("l", keyAdmissionPositionalLabel)
	// The endpoints are stored before the relationship's own commit, so that
	// commit's positional counts take their stored labels
	// (derivePositionalCountDeltasLocked).
	endpoints := func(ctx context.Context, tx *BadgerTransaction) error {
		for _, id := range []NodeID{"test:key-start", "test:key-end"} {
			if _, err := tx.CreateNode(ctx, &Node{ID: id, Labels: []string{keyAdmissionLabel, longLabel}}); err != nil {
				return err
			}
		}
		return nil
	}
	for _, async := range []bool{false, true} {
		for mode, modeName := range admissionModes {
			for _, over := range []bool{false, true} {
				t.Run(fmt.Sprintf("async=%v/%s/over=%v", async, modeName, over), func(t *testing.T) {
					dir := t.TempDir()
					b := admissionEngine(t, dir, async, false)
					ctx := context.Background()
					require.NoError(t, admissionCommit(t, b, CommitModeExplicit, "positional-endpoints", endpoints))
					require.NoError(t, b.FlushWriteBehind(ctx))
					edgeType := keyAdmissionPositionalType
					if over {
						edgeType = keyAdmissionPositionalTypeOver
					}
					before := keyAdmissionJournalOperations(t, dir)
					err := admissionCommit(t, b, mode, "positional-edge", func(ctx context.Context, tx *BadgerTransaction) error {
						return tx.CreateEdge(ctx, &Edge{ID: "test:key-edge", Type: strings.Repeat("t", edgeType), StartNode: "test:key-start", EndNode: "test:key-end"})
					})
					if over {
						// The relationship's own keys fit the store: the
						// refusal is the positional count key's.
						requireCommitCapacityRefusal(t, err, ErrStoreKeyTooLong, storeKeyLimit, dir, "positional-edge", "")
						require.Equal(t, before, keyAdmissionJournalOperations(t, dir), "the journal gained no record of the refused commit")
					} else {
						require.NoError(t, err, "the commit is applied")
					}
					requireKeyAdmissionEdge(t, b, !over, "commit")
					b = admissionReopen(t, b, dir, async, false)
					requireKeyAdmissionEdge(t, b, !over, "after crashClose and the recovery at open")
				})
			}
		}
	}
}
