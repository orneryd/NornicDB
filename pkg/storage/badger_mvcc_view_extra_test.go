package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// mvccViewVersionsHarness adapts one entity family (node or edge) to the
// shared MVCC-version iterator scenario table, so both scan prefixes, numeric
// dictionaries and decode paths run through identical assertions.
type mvccViewVersionsHarness struct {
	name string
	// prefix is the MVCC version keyspace prefix the iterator scans.
	prefix byte
	// unknownKey builds a version key for a numeric ID that is never
	// registered in the dictionary.
	unknownKey func(version MVCCVersion) []byte
	// unknownRecord encodes a well-formed body for the unknown numeric ID.
	unknownRecord func(t *testing.T) []byte
	// createEntity stores the entity whose MVCC versions are then scanned.
	createEntity func(t *testing.T, engine *BadgerEngine)
	// versionKey allocates/resolves the numeric ID and returns the entity's
	// MVCC version key for version.
	versionKey func(t *testing.T, engine *BadgerEngine, txn *badger.Txn, version MVCCVersion) []byte
	// validRecord encodes a well-formed MVCC body for the created entity.
	validRecord func(t *testing.T) []byte
	// scan runs the entity-specific iterator, counting yield calls.
	scan func(engine *BadgerEngine, start []byte, limit int) (lastScanned []byte, reachedEnd bool, yieldCalls int, err error)
	// scanWithErr runs the iterator with a yield that returns wantErr.
	scanWithErr func(engine *BadgerEngine, start []byte, limit int, wantErr error) (lastScanned []byte, reachedEnd bool, err error)
}

var mvccViewVersionsHarnesses = []mvccViewVersionsHarness{
	{
		name:       "node",
		prefix:     prefixMVCCNode,
		unknownKey: func(version MVCCVersion) []byte { return mvccNodeVersionKey(8_888_888, version) },
		unknownRecord: func(t *testing.T) []byte {
			record, err := encodeMVCCNodeRecord(&Node{ID: "ghost:n", Labels: []string{"Ghost"}}, false)
			require.NoError(t, err)
			return record
		},
		createEntity: func(t *testing.T, engine *BadgerEngine) {
			_, err := engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"N"}})
			require.NoError(t, err)
		},
		versionKey: func(t *testing.T, engine *BadgerEngine, txn *badger.Txn, version MVCCVersion) []byte {
			key, err := engine.mvccNodeVersionKeyString(txn, "test:n1", version)
			require.NoError(t, err)
			return key
		},
		validRecord: func(t *testing.T) []byte {
			record, err := encodeMVCCNodeRecord(&Node{ID: "test:n1", Labels: []string{"N"}}, false)
			require.NoError(t, err)
			return record
		},
		scan: func(engine *BadgerEngine, start []byte, limit int) ([]byte, bool, int, error) {
			yieldCalls := 0
			lastScanned, reachedEnd, err := engine.withViewNodeMVCCVersionsFromKey(start, limit, func(NodeID, MVCCVersion, bool) error {
				yieldCalls++
				return nil
			})
			return lastScanned, reachedEnd, yieldCalls, err
		},
		scanWithErr: func(engine *BadgerEngine, start []byte, limit int, wantErr error) ([]byte, bool, error) {
			return engine.withViewNodeMVCCVersionsFromKey(start, limit, func(NodeID, MVCCVersion, bool) error {
				return wantErr
			})
		},
	},
	{
		name:       "edge",
		prefix:     prefixMVCCEdge,
		unknownKey: func(version MVCCVersion) []byte { return mvccEdgeVersionKey(9_999_999, version) },
		unknownRecord: func(t *testing.T) []byte {
			record, err := encodeMVCCEdgeRecord(&Edge{ID: "ghost:e", StartNode: "ghost:a", EndNode: "ghost:b", Type: "REL"}, false)
			require.NoError(t, err)
			return record
		},
		createEntity: func(t *testing.T, engine *BadgerEngine) {
			_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"N"}})
			require.NoError(t, err)
			_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"N"}})
			require.NoError(t, err)
			require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL"}))
		},
		versionKey: func(t *testing.T, engine *BadgerEngine, txn *badger.Txn, version MVCCVersion) []byte {
			key, err := engine.mvccEdgeVersionKeyString(txn, "test:e1", version)
			require.NoError(t, err)
			return key
		},
		validRecord: func(t *testing.T) []byte {
			record, err := encodeMVCCEdgeRecord(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL"}, false)
			require.NoError(t, err)
			return record
		},
		scan: func(engine *BadgerEngine, start []byte, limit int) ([]byte, bool, int, error) {
			yieldCalls := 0
			lastScanned, reachedEnd, err := engine.withViewEdgeMVCCVersionsFromKey(start, limit, func(EdgeID, MVCCVersion, bool) error {
				yieldCalls++
				return nil
			})
			return lastScanned, reachedEnd, yieldCalls, err
		},
		scanWithErr: func(engine *BadgerEngine, start []byte, limit int, wantErr error) ([]byte, bool, error) {
			return engine.withViewEdgeMVCCVersionsFromKey(start, limit, func(EdgeID, MVCCVersion, bool) error {
				return wantErr
			})
		},
	},
}

func TestBadgerMVCC_WithViewMVCCVersionsFromKey_NodeAndEdge(t *testing.T) {
	for _, h := range mvccViewVersionsHarnesses {
		t.Run(h.name+"/unknown numeric ID skipped but counted for pagination", func(t *testing.T) {
			engine := createMVCCBadgerEngine(t)

			version := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 1}
			unknownKey := h.unknownKey(version)
			unknownRec := h.unknownRecord(t)
			require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
				return txn.Set(unknownKey, unknownRec)
			}))

			lastScanned, reachedEnd, yieldCalls, err := h.scan(engine, []byte{h.prefix}, 1)
			require.NoError(t, err)
			require.False(t, reachedEnd)
			require.Equal(t, 0, yieldCalls)
			// The skip branch still advances the cursor: the scanned key is
			// reported so the caller can resume with nextScanStart.
			require.Equal(t, unknownKey, lastScanned)
			require.Len(t, lastScanned, 1+8+16)
		})

		t.Run(h.name+"/corrupt record surfaces msgpack decode error", func(t *testing.T) {
			engine := createMVCCBadgerEngine(t)

			h.createEntity(t, engine)
			version := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 1}
			require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
				return txn.Set(h.versionKey(t, engine, txn, version), []byte("corrupt-"+h.name+"-mvcc-record"))
			}))

			_, _, _, err := h.scan(engine, []byte{h.prefix}, 64)
			require.Error(t, err)
			require.Contains(t, err.Error(), "msgpack")
		})

		t.Run(h.name+"/limit truncates scan and yield error propagates", func(t *testing.T) {
			engine := createMVCCBadgerEngine(t)

			h.createEntity(t, engine)
			version := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 202}
			record := h.validRecord(t)
			require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
				return txn.Set(h.versionKey(t, engine, txn, version), record)
			}))

			// Limit branch: one record yielded, prefix not exhausted.
			_, reachedEnd, yieldCalls, err := h.scan(engine, []byte{h.prefix}, 1)
			require.NoError(t, err)
			require.Equal(t, 1, yieldCalls)
			require.False(t, reachedEnd)

			// Yield error propagation branch: the error is returned before
			// the matching record's key is recorded as scanned.
			wantErr := errors.New("stop-yield")
			lastScanned, _, err := h.scanWithErr(engine, []byte{h.prefix}, 64, wantErr)
			require.ErrorIs(t, err, wantErr)
			require.Empty(t, lastScanned)
		})
	}
}

func TestBadgerEngine_RebuildMVCCHeads_NilContext(t *testing.T) {
	engine := newRebuildHeadsFixture(t, 100)

	_, err := engine.CreateNode(&Node{ID: "test:nx", Labels: []string{"Doc"}, Properties: map[string]any{"v": 1}})
	require.NoError(t, err)
	require.NoError(t, engine.UpdateNode(NodeUpdateRequest{After: &Node{ID: "test:nx", Labels: []string{"Doc"}, Properties: map[string]any{"v": 2}}}))

	// nil context branch should default to Background and succeed.
	require.NoError(t, engine.RebuildMVCCHeads(nil))
	_, err = engine.GetNodeCurrentHead("test:nx")
	require.NoError(t, err)

	// Also keep a direct context path active for the same test flow.
	require.NoError(t, engine.RebuildMVCCHeads(context.Background()))
}
