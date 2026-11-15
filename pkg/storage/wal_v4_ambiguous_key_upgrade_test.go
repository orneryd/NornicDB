package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// This file is the acceptance evidence for the historical ambiguous-key upgrade
// (B05). It records four separate facts.
//
//  1. What the upgrade does with a record whose physical key bytes an old JSON
//     writer replaced. The fixtures below cover a commit whose effects reached
//     Badger and one whose effects never did. Both are repaired from the record's
//     own facts and then open, twice, with the graph their commit described: the
//     label scan, adjacency in both directions, the between lookup and the exact
//     property image the body carries.
//
//  2. Which authoritative facts a damaged record still carries. The forward
//     numeric bindings (0x1A/0x1B) and the entity bodies (0x01/0x02) are keyed by
//     string IDs, so neither their keys nor their values lose bytes. A record may
//     hold an entity's body without its binding, and an unapplied commit's entity
//     exists only inside the record, so absence from Badger says nothing about
//     recoverability.
//
//  3. Which derived numeric-keyed entries those facts determine. The repair owner
//     is production code (wal_v4_buffered_key_repair.go); this file exercises it
//     through the same functions the V4 upgrade calls, per record and through the
//     public engine open. Only the eight derived families are in scope: the label
//     index, the outgoing/incoming adjacency indexes, the relationship-type
//     index, the between-set and between-head indexes, and the node and
//     relationship reverse dictionaries.
//
//  4. What must still refuse, with the source journal left byte-identical and no
//     backup published: a damaged key in any other family, a missing body or
//     record-local binding, a damaged deletion, an unproven entry and a
//     between-head key two relationships share. An operator holding such a store
//     uses the old build that wrote the journal, which can still read it; the
//     upgrade never infers a repair for it.
//
// RED evidence, preserved from two runs. First, the previous revision of this
// file asserted only the refusal, and the investigation run recorded it for both
// fixtures:
//
//	PASS: TestV4StartupRefusesAmbiguousHistoricalWALKeysWithoutDurableProof
//	  fixture: 2 records, 5 damaged keys, 0 merged entries,
//	  families map[3:1 5:1 17:1 18:1 29:1], 1 ambiguous records without durable proof
//	PASS: TestV4StartupRefusesAmbiguousHistoricalWALKeysAlreadyApplied
//	  fixture: 2 records, 5 damaged keys, 0 merged entries,
//	  families map[3:1 5:1 17:1 18:1 29:1], 0 ambiguous records without durable proof
//
// Second, the success assertions below were written and run against that
// unchanged production source, where they failed instead of opening the store:
//
//	Error: Received unexpected error:
//	  wal: historical physical key contains ambiguous UTF-8 replacement
//	Test: TestV4UpgradeRepairsAmbiguousHistoricalWALKeys/applied=false
//
//	Error: Received unexpected error:
//	  V4 WAL migration and recovery: wal: historical physical key contains ambiguous UTF-8 replacement
//	Test: TestV4UpgradeRepairsMergedHistoricalCollisionClasses
//
// Only then did the repair owner exist. The negative cases in this file still
// assert that refusal, an unchanged source journal and no published backup.

// historicalAmbiguousFixture is one disposable store whose acknowledged
// write-behind commits were recorded the way the pre-identity JSON writer
// recorded them.
type historicalAmbiguousFixture struct {
	dir      string
	walPath  string
	original []byte
	commits  []capturedBufferedCommit
	// records counts the journalled write-behind commits in the fixture.
	records int
	// damaged counts every damaged physical key in every record.
	damaged int
	// dropped counts the damaged entries the JSON map encoding merged away.
	dropped int
	// families counts the damaged physical keys per Badger key family byte.
	families map[byte]int
	// ambiguousWithoutProof counts ambiguous records whose write-behind commit
	// holds no durable flush receipt, i.e. whose lost bytes were never applied.
	ambiguousWithoutProof int
}

// capturedBufferedCommit is one acknowledged write-behind commit as the capture
// WAL recorded it, beside what the old JSON encoding decodes back to.
type capturedBufferedCommit struct {
	entry WALEntry
	// raw is the commit exactly as the current writer recorded it: every
	// physical key byte intact, every value intact.
	raw WALBufferedCommitData
	// decoded is the same commit as a reader of the historical payload sees it:
	// keys the old encoder rewrote no longer name their original entry.
	decoded WALBufferedCommitData
	payload []byte
	// durableReceipt reports whether the write-behind apply owner already wrote
	// this commit's flush receipt (0x13+txID) into Badger.
	durableReceipt bool
	// damaged counts this record's damaged physical keys.
	damaged int
	// dropped counts this record's damaged entries that share a decoded key
	// with another entry, so the decoded map holds fewer entries than the
	// commit wrote.
	dropped int
}

// historicalAmbiguousRecords leaves behind a store whose acknowledged
// write-behind commits were recorded the way the pre-identity JSON writer
// recorded them.
//
// That writer serialized WALBufferedCommitData to plain JSON. encoding/json
// replaces every invalid UTF-8 byte in a map key with U+FFFD, and derived
// physical keys embed big-endian numeric IDs: a numeric ID whose last byte is
// 0x80..0xFF is not valid UTF-8 on its own, so every derived key carrying such
// an ID loses bytes. Two such IDs can even collapse into one decoded map key,
// which merges two entries into one.
//
// applied says whether the real write-behind apply owner wrote each commit's
// durable flush receipt (0x13+txID) in the same Badger transaction as its data:
// apply every batch but the last, or every batch. A store whose ambiguous record
// is durable holds no unresolved bytes in Badger; a store whose ambiguous record
// never reached Badger holds none either.
func historicalAmbiguousRecords(t *testing.T, applied bool, batches ...func(*BadgerTransaction) error) historicalAmbiguousFixture {
	t.Helper()
	return historicalAmbiguousRecordsFramed(t, applied, 3, batches...)
}

// historicalAmbiguousRecordsFramed is historicalAmbiguousRecords with an explicit
// source framing version, so the headerless JSON envelope (before version 3), the
// framed envelope it replaced, and the current framed envelope all reach the same
// decision.
func historicalAmbiguousRecordsFramed(t *testing.T, applied bool, framingVersion byte, batches ...func(*BadgerTransaction) error) historicalAmbiguousFixture {
	t.Helper()
	require.NotEmpty(t, batches)
	t.Cleanup(config.WithWALEnabled())
	fixture := historicalAmbiguousFixture{dir: t.TempDir(), families: map[byte]int{}}
	engine := reviewBindingEngine(t, fixture.dir, nil)
	capture, err := NewWAL(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	engine.SetBufferedCommitWAL(capture)
	captured := func() []WALBufferedCommitData {
		entries, err := ReadWALEntriesFromDir(t.Context(), capture.Config().Dir)
		require.NoError(t, err)
		out := make([]WALBufferedCommitData, 0, len(entries))
		for _, entry := range entries {
			require.Equal(t, OpBufferedCommit, entry.Operation)
			data, err := decodeBufferedWALRecord(entry)
			require.NoError(t, err)
			out = append(out, data)
		}
		return out
	}
	for index, batch := range batches {
		// Re-arm the pending delay: an earlier explicit flush adapts it, and a batch
		// this fixture leaves unapplied must stay pending until the crash close.
		engine.writeBehind.SetDelayBounds(time.Hour, time.Hour)
		tx, err := engine.BeginTransaction(t.Context(), OrdinaryBegin)
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace("hist"))
		require.NoError(t, tx.SetImplicit(true))
		require.NoError(t, batch(tx))
		require.NoError(t, tx.Commit())
		require.True(t, tx.AcknowledgedBuffered())
		// The last batch is the one an unapplied fixture never flushes.
		if applied || index < len(batches)-1 {
			require.NoError(t, engine.FlushWriteBehind(context.Background()))
		}
	}

	raw := captured()
	require.Len(t, raw, len(batches), "each acknowledged commit is one journalled record")
	stamp := time.Date(2026, 10, 12, 3, 4, 5, 6, time.UTC)
	for index, data := range raw {
		committed := capturedBufferedCommit{raw: data}
		histogram := map[byte]int{}
		for key := range data.Writes {
			if key != "" {
				histogram[key[0]]++
			}
			if utf8.ValidString(key) {
				continue
			}
			committed.damaged++
			fixture.families[key[0]]++
		}
		for _, key := range data.Deletes {
			if !utf8.ValidString(key) {
				committed.damaged++
				if key != "" {
					fixture.families[key[0]]++
				}
			}
		}
		fixture.damaged += committed.damaged
		// Entries whose damaged keys decode to the same map key are merged by the
		// JSON object encoding; silently losing them is what the upgrade's
		// all-or-nothing refusal exists to prevent.
		collapsed := map[string]int{}
		for key := range data.Writes {
			if utf8.ValidString(key) {
				continue
			}
			collapsed[historicalPhysicalKeyJSONForm(key)]++
		}
		for _, count := range collapsed {
			committed.dropped += count - 1
		}
		fixture.dropped += committed.dropped

		receipt, err := engine.observeBufferedCommitReceipt(t.Context(), bufferedReceiptRead{
			namespace: data.Namespace,
			txID:      data.TxID,
			logicalID: data.LogicalTxID,
		})
		require.NoError(t, err)
		committed.durableReceipt = receipt.present
		if committed.damaged > 0 && !receipt.present {
			fixture.ambiguousWithoutProof++
		}

		committed.payload = projectHistoricalBufferedCommit(t, data, committed.damaged > 0)
		committed.entry = WALEntry{
			Sequence:  uint64(index + 1),
			Timestamp: stamp,
			Operation: OpBufferedCommit,
			Data:      committed.payload,
			Checksum:  crc32Checksum(committed.payload),
		}
		committed.decoded, err = decodeBufferedWALRecord(committed.entry)
		require.NoError(t, err)
		require.Len(t, committed.decoded.Writes, len(data.Writes)-committed.dropped,
			"the old encoding merges exactly the entries that share a decoded key")

		fixture.commits = append(fixture.commits, committed)
		fixture.records++
		fixture.original = append(fixture.original, priorWALRecord(t, framingVersion, committed.entry)...)
		t.Logf("record %d: %d writes %v, %d damaged keys, %d merged entries, durable receipt %t",
			index, len(data.Writes), histogram, committed.damaged, committed.dropped, committed.durableReceipt)
	}
	require.NotZero(t, fixture.damaged, "the fixture must contain physical keys the old JSON writer replaced")
	if applied {
		require.Zero(t, fixture.ambiguousWithoutProof, "an applied fixture proves every commit is durable")
	}

	crashClose(t, engine, capture)
	fixture.walPath = filepath.Join(fixture.dir, "wal", "wal.log")
	require.NoError(t, os.MkdirAll(filepath.Dir(fixture.walPath), 0700))
	require.NoError(t, os.WriteFile(fixture.walPath, fixture.original, 0600))
	return fixture
}

// historicalAmbiguousFillerBatch creates 128 plain nodes so later numeric IDs
// reach the 0x80..0xFF block, one labelled node and one relationship into it.
// One node also carries the property names the killed commit reuses, so that
// commit's body names properties this store already persisted instead of
// carrying a property-key drain a historical record never had.
// Numeric IDs are not assigned in creation order, so which entity lands in the
// damaged block is not fixed; the tests read that from the captured commit.
func historicalAmbiguousFillerBatch(t *testing.T) func(*BadgerTransaction) error {
	t.Helper()
	return func(tx *BadgerTransaction) error {
		for i := 0; i < 128; i++ {
			node := &Node{ID: NodeID(fmt.Sprintf("hist:n%d", i))}
			if i == 0 {
				node.Properties = map[string]any{"sealed": "filler", "revision": int64(1)}
			}
			if _, err := tx.CreateNode(t.Context(), node); err != nil {
				return err
			}
		}
		if _, err := tx.CreateNode(t.Context(), &Node{ID: "hist:labelled", Labels: []string{"Damaged"}}); err != nil {
			return err
		}
		return tx.CreateEdge(t.Context(), &Edge{ID: "hist:edge", StartNode: "hist:n100", EndNode: "hist:labelled", Type: "LINK"})
	}
}

// historicalAmbiguousKilledBatch creates the commit whose record the fixture
// leaves unapplied: one labelled, property-bearing node and one relationship into
// it, recorded the way the old writer recorded them. Its entity body and numeric
// binding ride this record, so the record itself says which entity the damaged
// keys belong to.
func historicalAmbiguousKilledBatch(t *testing.T) func(*BadgerTransaction) error {
	t.Helper()
	return func(tx *BadgerTransaction) error {
		if _, err := tx.CreateNode(t.Context(), &Node{
			ID:         "hist:killed",
			Labels:     []string{"Damaged"},
			Properties: map[string]any{"sealed": "kept", "revision": int64(11)},
		}); err != nil {
			return err
		}
		return tx.CreateEdge(t.Context(), &Edge{ID: "hist:edge2", StartNode: "hist:n64", EndNode: "hist:killed", Type: "LINK"})
	}
}

// historicalAmbiguousUnboundUpdateBatch relabels every node the filler batch
// created. A relabel rewrites the body and writes the new label index entry, but
// the numeric ID was allocated by the earlier commit, so this record carries no
// binding for any body it writes. Its damaged label keys are therefore outside
// what the record itself proves: only the durable dictionary could complete them,
// which the upgrade does not consult.
func historicalAmbiguousUnboundUpdateBatch(t *testing.T) func(*BadgerTransaction) error {
	t.Helper()
	return func(tx *BadgerTransaction) error {
		for i := 0; i < 128; i++ {
			node := &Node{ID: NodeID(fmt.Sprintf("hist:n%d", i)), Labels: []string{"Extra"}}
			if err := tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: node, Policy: ClientNodeEdits}); err != nil {
				return err
			}
		}
		return tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{
			After:  &Node{ID: "hist:labelled", Labels: []string{"Damaged", "Extra"}},
			Policy: ClientNodeEdits,
		})
	}
}

// historicalAmbiguousCollisionBatch creates 200 identically labelled nodes in
// one commit. Two hundred distinct numeric IDs cannot all fit in the 0..127
// block, and every ID in a 0x80..0xFF block decodes to the same key, so the JSON
// object merges a whole block of label keys and of reverse keys into one entry
// each. Numeric IDs are not assigned in creation order, so the exact count is
// read from the captured commit.
func historicalAmbiguousCollisionBatch(t *testing.T) func(*BadgerTransaction) error {
	t.Helper()
	return func(tx *BadgerTransaction) error {
		for i := 0; i < 200; i++ {
			if _, err := tx.CreateNode(t.Context(), &Node{ID: NodeID(fmt.Sprintf("hist:collide%d", i)), Labels: []string{"Damaged"}}); err != nil {
				return err
			}
		}
		return nil
	}
}

// projectHistoricalBufferedCommit rewrites one current buffered commit payload
// as the old writer recorded it: plain JSON, no typed envelope, no logical
// lineage, no numeric bindings and no property dictionary identity.
func projectHistoricalBufferedCommit(t *testing.T, data WALBufferedCommitData, ambiguous bool) []byte {
	t.Helper()
	data.LogicalTxID, data.NumericBindings, data.PropertyBindings, data.PropertyDictionaryID = "", nil, nil, ""
	payload, err := json.Marshal(data)
	require.NoError(t, err)
	require.True(t, utf8.Valid(payload))
	if ambiguous {
		require.True(t, bytes.Contains(payload, []byte("\ufffd")), "the old encoder must replace the lost bytes")
	}
	return payload
}

// damagedBufferedCommit returns the fixture record whose keys the old encoder
// damaged, as the upgrade's reader sees it.
func damagedBufferedCommit(t *testing.T, fixture historicalAmbiguousFixture) capturedBufferedCommit {
	t.Helper()
	for _, commit := range fixture.commits {
		if commit.damaged > 0 {
			return commit
		}
	}
	t.Fatal("the fixture holds no damaged record")
	return capturedBufferedCommit{}
}

// cloneBufferedCommit copies the parts of a record a test mutates.
func cloneBufferedCommit(record WALBufferedCommitData) WALBufferedCommitData {
	writes := make(map[string][]byte, len(record.Writes))
	for key, value := range record.Writes {
		writes[key] = value
	}
	record.Writes = writes
	record.Deletes = append([]string(nil), record.Deletes...)
	record.Ops = append([]Operation(nil), record.Ops...)
	return record
}

// durableNumericBindings reads the store's durable forward numeric dictionary:
// the authoritative, immutable binding of every entity ID to its numeric ID. Its
// keys are string IDs, so neither they nor their 8-byte values lose bytes. The
// upgrade does not consult it; the tests use it to show what a record-local proof
// would otherwise have to borrow.
func durableNumericBindings(t *testing.T, dir string) map[string]uint64 {
	t.Helper()
	bindings := map[string]uint64{}
	store, err := fixtureowner.TrackResult[*badger.DB](t, nil)(badger.Open(badger.DefaultOptions(dir).WithReadOnly(true).WithLogger(nil)))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	require.NoError(t, store.View(func(tx *badger.Txn) error {
		for _, prefix := range []byte{prefixIDDictNodeForward, prefixIDDictEdgeForward} {
			it := tx.NewIterator(badgerPrefixIteratorOptions([]byte{prefix}))
			for it.Rewind(); it.Valid(); it.Next() {
				key := it.Item().KeyCopy(nil)
				if err := it.Item().Value(func(value []byte) error {
					if num, ok := decodeNodeRefNum(value); ok {
						bindings[string(key[1:])] = num
					}
					return nil
				}); err != nil {
					it.Close()
					return err
				}
			}
			it.Close()
		}
		return nil
	}))
	return bindings
}

// persistedNodeBody reports whether the store holds the current body of id. It
// reads the closed store directly, so it describes durable state only.
func persistedNodeBody(t *testing.T, dir, id string) bool {
	t.Helper()
	store, err := fixtureowner.TrackResult[*badger.DB](t, nil)(badger.Open(badger.DefaultOptions(dir).WithReadOnly(true).WithLogger(nil)))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	found := false
	require.NoError(t, store.View(func(tx *badger.Txn) error {
		_, err := tx.Get(append([]byte{prefixNode}, id...))
		switch {
		case err == nil:
			found = true
			return nil
		case errors.Is(err, badger.ErrKeyNotFound):
			return nil
		default:
			return err
		}
	}))
	return found
}

// assertHistoricalStoreRepaired reads the recovered graph through the public
// engine contract: the label index, both adjacency directions, the between lookup
// that the head and set indexes serve, and the property image the body carries.
func assertHistoricalStoreRepaired(t *testing.T, engine *BadgerEngine) {
	t.Helper()
	ctx := context.Background()

	labelled, err := engine.GetNodesByLabel(ctx, "Damaged")
	require.NoError(t, err)
	ids := make([]string, 0, len(labelled))
	for _, node := range labelled {
		ids = append(ids, string(node.ID))
	}
	sort.Strings(ids)
	require.Equal(t, []string{"hist:killed", "hist:labelled"}, ids,
		"the label index names exactly the nodes whose bodies carry the label")

	killed, err := engine.GetNode(ctx, "hist:killed")
	require.NoError(t, err)
	require.Equal(t, []string{"Damaged"}, killed.Labels)
	require.Equal(t, "kept", killed.Properties["sealed"], "the exact property value the commit wrote")
	require.IsType(t, int64(0), killed.Properties["revision"], "the property keeps its exact type")
	require.Equal(t, int64(11), killed.Properties["revision"])

	filler, err := engine.GetNode(ctx, "hist:n0")
	require.NoError(t, err)
	require.Equal(t, "filler", filler.Properties["sealed"])

	outgoing, err := engine.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentOutgoing, Node: "hist:n64"})
	require.NoError(t, err)
	require.Len(t, outgoing, 1)
	require.Equal(t, EdgeID("hist:edge2"), outgoing[0].ID)
	require.Equal(t, NodeID("hist:killed"), outgoing[0].EndNode)

	incoming, err := engine.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentIncoming, Node: "hist:killed"})
	require.NoError(t, err)
	require.Len(t, incoming, 1)
	require.Equal(t, EdgeID("hist:edge2"), incoming[0].ID)

	applied, err := engine.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentOutgoing, Node: "hist:n100"})
	require.NoError(t, err)
	require.Len(t, applied, 1)
	require.Equal(t, EdgeID("hist:edge"), applied[0].ID)

	between, err := engine.GetEdgeBetween(ctx, "hist:n64", "hist:killed", "LINK")
	require.NoError(t, err)
	require.Equal(t, EdgeID("hist:edge2"), between.ID, "the between head and set indexes name the relationship")

	nodes, err := engine.NodeCount(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 130, nodes)
	edges, err := engine.EdgeCount(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, edges)
}

// The inventory test establishes the population the repair has to cover: every
// key family the fixture's commits carry is a family the writer knows, and every
// family the old encoder damaged is one the repair re-derives. A damaged key in
// an unclassified or non-derived family is a refusal condition, so a missing
// entry here would silently widen the recovery.
func TestHistoricalAmbiguousDerivedKeyFamilyInventory(t *testing.T) {
	fixture := historicalAmbiguousRecords(t, true,
		historicalAmbiguousFillerBatch(t),
		historicalAmbiguousKilledBatch(t),
		historicalAmbiguousCollisionBatch(t))
	for _, commit := range fixture.commits {
		for key := range commit.raw.Writes {
			require.NotEmpty(t, key)
			require.True(t, walPhysicalKeyFamilies[key[0]], "unclassified write family 0x%02x: %q", key[0], key)
		}
		for _, key := range commit.raw.Deletes {
			require.NotEmpty(t, key)
			require.True(t, walPhysicalKeyFamilies[key[0]], "unclassified delete family 0x%02x: %q", key[0], key)
		}
	}
	for prefix := range fixture.families {
		require.True(t, walPhysicalKeyFamilies[prefix], "unclassified damaged family 0x%02x", prefix)
		_, derived := historicalDerivedKeyFamily([]byte{prefix})
		require.True(t, derived, "damaged family 0x%02x (%s) is not re-derivable", prefix, historicalPhysicalKeyFamilyName(prefix))
	}
	// The repair's population is exactly the eight families the report names.
	require.Len(t, historicalDerivedKeyFamilies, 8)
	for _, prefix := range []byte{
		prefixLabelIndex, prefixOutgoingIndex, prefixIncomingIndex, prefixEdgeTypeIndex,
		prefixEdgeBetweenIndex, prefixEdgeBetweenHead, prefixIDDictNodeReverse, prefixIDDictEdgeReverse,
	} {
		_, derived := historicalDerivedKeyFamily([]byte{prefix})
		require.True(t, derived, "family 0x%02x must be re-derivable", prefix)
	}
	// A family the writer knows but cannot re-derive is not admitted.
	for _, prefix := range []byte{prefixEmbedding, prefixMVCCNode, prefixIDDictNodeForward, prefixChangeEvent, prefixPropKeyReverse} {
		_, derived := historicalDerivedKeyFamily([]byte{prefix})
		require.False(t, derived, "family 0x%02x must not be treated as re-derivable", prefix)
	}
	// The damaged families this fixture reproduces are the ones the report names.
	for _, prefix := range []byte{prefixLabelIndex, prefixIncomingIndex, prefixEdgeBetweenIndex, prefixEdgeBetweenHead, prefixIDDictNodeReverse} {
		require.NotZero(t, fixture.families[prefix], "the fixture must damage family 0x%02x", prefix)
	}
	t.Logf("damaged families: %v", fixture.families)
}

// The label index, the reverse dictionaries and every adjacency family the
// report names are pure functions of facts the commit still names exactly: the
// forward numeric binding of each entity, the labels in its stored body, and the
// relationship's compact header. The repair restores exactly the entries the
// commit wrote, byte for byte, for a store whose ambiguous record is durable and
// for one whose record never reached Badger.
func TestV4UpgradeRepairsHistoricalAmbiguousPhysicalKeys(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprintf("applied=%t", applied), func(t *testing.T) {
			fixture := historicalAmbiguousRecords(t, applied,
				historicalAmbiguousFillerBatch(t),
				historicalAmbiguousKilledBatch(t))
			require.NotZero(t, fixture.families[prefixLabelIndex])
			require.NotZero(t, fixture.families[prefixIDDictNodeReverse])
			require.Zero(t, fixture.dropped, "this fixture damages no pair of keys into one entry")
			perFamily := map[byte]int{}
			for index, commit := range fixture.commits {
				repaired, err := repairV4BufferedCommitPhysicalKeys(commit.decoded)
				require.NoError(t, err, "record %d", index)
				require.Equal(t, commit.raw.Writes, repaired.Writes,
					"record %d: the record's own facts restore every damaged entry byte for byte", index)
				if commit.damaged == 0 {
					require.False(t, repaired.PhysicalPropertyImages,
						"record %d holds no damaged key and keeps its historical encoding", index)
					continue
				}
				require.True(t, repaired.PhysicalPropertyImages)
				require.False(t, repaired.namedPropertiesAuthoritative,
					"record %d keeps its lossy named properties non-authoritative", index)
				for key := range commit.raw.Writes {
					if !historicalPhysicalKeySurvivesJSON(key) {
						perFamily[key[0]]++
					}
				}
			}
			require.Empty(t, fixture.commits[0].decoded.Deletes)
			require.NotZero(t, perFamily[prefixLabelIndex])
			require.NotZero(t, perFamily[prefixIDDictNodeReverse])
			require.NotZero(t, perFamily[prefixIncomingIndex])
			require.NotZero(t, perFamily[prefixEdgeBetweenIndex])
			require.NotZero(t, perFamily[prefixEdgeBetweenHead])
			t.Logf("repaired families: %v", perFamily)
		})
	}
}

// Two numeric IDs whose bytes differ only in positions the old encoder replaces
// collapse into one decoded map key, so the JSON object keeps one entry and
// silently drops the others. The surviving facts still name every entity, so the
// repair restores the whole collision class; the decoded record alone cannot,
// which is why counting surviving replacement strings understates the loss.
func TestV4UpgradeRepairsMergedHistoricalCollisionClasses(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	fixture := historicalAmbiguousRecords(t, true, historicalAmbiguousCollisionBatch(t))
	t.Logf("collision fixture: %d records, %d damaged keys, %d merged entries, families %v",
		fixture.records, fixture.damaged, fixture.dropped, fixture.families)
	require.Greater(t, fixture.damaged, 2, "several keys in the damaged block lose bytes")
	require.Greater(t, fixture.dropped, 0, "the JSON object merges several damaged keys into one entry")

	commit := fixture.commits[0]
	require.Len(t, fixture.commits, 1)
	require.Len(t, commit.decoded.Writes, len(commit.raw.Writes)-commit.dropped)
	repaired, err := repairV4BufferedCommitPhysicalKeys(commit.decoded)
	require.NoError(t, err)
	require.Equal(t, commit.raw.Writes, repaired.Writes, "every merged entry is restored")
	require.Len(t, repaired.Writes, len(commit.raw.Writes))

	// A commit can carry an entity body without its numeric binding, because the
	// counter path persists those entries out of band. Without the binding the
	// record proves nothing about that body's derived keys, and the repair refuses
	// rather than consulting the durable dictionary.
	unbound := cloneBufferedCommit(commit.decoded)
	for key := range unbound.Writes {
		if len(key) > 0 && key[0] == prefixIDDictNodeForward {
			delete(unbound.Writes, key)
		}
	}
	_, err = repairV4BufferedCommitPhysicalKeys(unbound)
	require.ErrorContains(t, err, "wal: historical physical key contains ambiguous UTF-8 replacement")
	require.ErrorContains(t, err, "has no numeric binding in this record")
	t.Logf("refusal: %v", err)
	require.Len(t, durableNumericBindings(t, fixture.dir), 200,
		"the durable dictionary holds the bindings the record does not")

	// The whole store still opens, with the whole collision class repaired.
	engine := reviewBindingEngine(t, fixture.dir, nil)
	labelled, err := engine.GetNodesByLabel(context.Background(), "Damaged")
	require.NoError(t, err)
	require.Len(t, labelled, 200, "every node of the merged label block is indexed")
	require.NoError(t, engine.Close())
	reopened := reviewBindingEngine(t, fixture.dir, nil)
	labelled, err = reopened.GetNodesByLabel(context.Background(), "Damaged")
	require.NoError(t, err)
	require.Len(t, labelled, 200)
	require.NoError(t, reopened.Close())
}

// The public engine open repairs a supported historical journal, whole: the store
// opens with the graph the unapplied commit described, a second open reads the
// same state, and the original journal is preserved as the published backup. Both
// the durable-record and the unapplied-record fixtures pass, because the record's
// own facts — not a flush receipt — decide the repair.
func TestV4UpgradeRepairsAmbiguousHistoricalWALKeys(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprintf("applied=%t", applied), func(t *testing.T) {
			fixture := historicalAmbiguousRecords(t, applied,
				historicalAmbiguousFillerBatch(t),
				historicalAmbiguousKilledBatch(t))
			t.Logf("fixture: %d records, %d damaged keys, %d merged entries, families %v, %d ambiguous records without durable proof",
				fixture.records, fixture.damaged, fixture.dropped, fixture.families, fixture.ambiguousWithoutProof)
			if applied {
				require.Zero(t, fixture.ambiguousWithoutProof, "every ambiguous record in this fixture is durable")
			} else {
				require.NotZero(t, fixture.ambiguousWithoutProof, "the killed fixture keeps the last ambiguous record unapplied")
				require.False(t, persistedNodeBody(t, fixture.dir, "hist:killed"), "the record's entity exists only in its own commit")
			}
			walDir := filepath.Join(fixture.dir, "wal")
			// The file-only preflight holds no store state, so it must decide the
			// record from the record alone; it is the boundary the public library
			// Open applies before Badger is opened.
			require.NoError(t, ValidateWALFormat(walDir, WALValidateV4Upgrade))

			engine := reviewBindingEngine(t, fixture.dir, nil)
			assertHistoricalStoreRepaired(t, engine)
			require.NoError(t, engine.Close())

			// The conversion is durable, not a read-time overlay.
			converted, err := os.ReadFile(fixture.walPath)
			require.NoError(t, err)
			require.NotEqual(t, fixture.original, converted, "the repaired journal replaces the damaged bytes")
			archives, err := filepath.Glob(filepath.Join(fixture.dir, "wal.pre-v4-*"))
			require.NoError(t, err)
			require.Len(t, archives, 1, "the pre-upgrade journal is kept for rollback")
			backup, err := os.ReadFile(filepath.Join(archives[0], "wal.log"))
			require.NoError(t, err)
			require.Equal(t, fixture.original, backup, "the backup holds the source bytes unchanged")
			_, err = os.Lstat(filepath.Join(fixture.dir, "wal.before-v4-v6"))
			require.ErrorIs(t, err, os.ErrNotExist)

			// A second open needs no upgrade authority: the journal and the stored
			// graph already describe the repaired commit.
			reopened := reviewBindingEngine(t, fixture.dir, nil)
			assertHistoricalStoreRepaired(t, reopened)
			require.NoError(t, reopened.Close())
		})
	}
}

// The owner decides a damaged record whatever framing carried it: a headerless
// JSON envelope from before version 3 is read and converted through the same path
// as a framed record, so it is repaired rather than refused.
func TestV4UpgradeRepairsAmbiguousHistoricalKeysInOlderFraming(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	fixture := historicalAmbiguousRecordsFramed(t, false, 2,
		historicalAmbiguousFillerBatch(t),
		historicalAmbiguousKilledBatch(t))
	require.NotZero(t, fixture.damaged)
	require.NoError(t, ValidateWALFormat(filepath.Join(fixture.dir, "wal"), WALValidateV4Upgrade))
	engine := reviewBindingEngine(t, fixture.dir, nil)
	assertHistoricalStoreRepaired(t, engine)
	converted, err := os.ReadFile(fixture.walPath)
	require.NoError(t, err)
	require.NotEqual(t, fixture.original, converted, "the headerless journal is replaced")
	require.NoError(t, engine.Close())
	reopened := reviewBindingEngine(t, fixture.dir, nil)
	assertHistoricalStoreRepaired(t, reopened)
	require.NoError(t, reopened.Close())
}

// A record whose damaged keys its own facts do not prove still refuses the whole
// store: nothing is written, the source journal stays byte-identical and no
// backup is published. These nodes were relabelled after their numeric IDs were
// allocated, so the record carries their bodies and new label index entries but
// none of their bindings; only the durable dictionary could complete them, and it
// is not this record's proof. The operator route is the old build that wrote the
// journal, not an inferred repair.
func TestV4UpgradeRefusesAmbiguousHistoricalKeysWithoutRecordLocalProof(t *testing.T) {
	fixture := historicalAmbiguousRecords(t, false,
		historicalAmbiguousFillerBatch(t),
		historicalAmbiguousUnboundUpdateBatch(t))
	t.Logf("fixture: %d records, %d damaged keys, %d merged entries, families %v",
		fixture.records, fixture.damaged, fixture.dropped, fixture.families)
	require.NotZero(t, fixture.damaged, "the relabel record damages a label index key")
	require.NotZero(t, fixture.families[prefixLabelIndex])

	commit := fixture.commits[len(fixture.commits)-1]
	require.NotZero(t, commit.damaged)
	require.NotEmpty(t, commit.raw.Writes)
	for key := range commit.raw.Writes {
		require.NotEqual(t, byte(prefixIDDictNodeForward), key[0],
			"a binding in the record would make this a supported case")
	}
	_, err := repairV4BufferedCommitPhysicalKeys(commit.decoded)
	require.ErrorContains(t, err, "wal: historical physical key contains ambiguous UTF-8 replacement")
	require.ErrorContains(t, err, "no numeric binding in this record")

	failed, openErr := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:             fixture.dir,
		LowMemory:           true,
		AllowStorageUpgrade: true,
		CatalogUpgradeV4:    graphOnlyCatalogUpgradeV4(),
	}))
	require.ErrorContains(t, openErr, "wal: historical physical key contains ambiguous UTF-8 replacement")
	require.Nil(t, failed, "a refused upgrade must not expose an open engine")

	unchanged, err := os.ReadFile(fixture.walPath)
	require.NoError(t, err)
	require.Equal(t, fixture.original, unchanged, "a refused upgrade changes no WAL file")
	archives, err := filepath.Glob(filepath.Join(fixture.dir, "wal.pre-v4-*"))
	require.NoError(t, err)
	require.Empty(t, archives, "a refused upgrade publishes no backup")
	_, err = os.Lstat(filepath.Join(fixture.dir, "wal.before-v4-v6"))
	require.ErrorIs(t, err, os.ErrNotExist)

	// The file-only preflight refuses for the same record-local reason.
	require.ErrorContains(t, ValidateWALFormat(filepath.Join(fixture.dir, "wal"), WALValidateV4Upgrade),
		"wal: historical physical key contains ambiguous UTF-8 replacement")
	// The applied records stay exactly as they were.
	require.True(t, persistedNodeBody(t, fixture.dir, "hist:n0"))
}

// An entity that exists only inside an unapplied commit is not evidence of
// irrecoverable information: the record names the entity's body and its numeric
// binding, and both survive intact, while the durable store holds neither.
func TestHistoricalAmbiguousUnappliedRecordKeepsItsOwnFacts(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprintf("applied=%t", applied), func(t *testing.T) {
			fixture := historicalAmbiguousRecords(t, applied,
				historicalAmbiguousFillerBatch(t),
				historicalAmbiguousKilledBatch(t))
			var killed WALBufferedCommitData
			for _, commit := range fixture.commits {
				if _, ok := commit.raw.Writes[string(append([]byte{prefixNode}, "hist:killed"...))]; ok {
					killed = commit.decoded
				}
			}
			require.NotEmpty(t, killed.Writes, "the record that creates hist:killed is journalled")
			body := string(append([]byte{prefixNode}, "hist:killed"...))
			binding := string(append([]byte{prefixIDDictNodeForward}, "hist:killed"...))
			require.Equal(t, body, historicalPhysicalKeyJSONForm(body), "the body key is string-keyed and survives")
			require.Equal(t, binding, historicalPhysicalKeyJSONForm(binding), "the binding key is string-keyed and survives")
			require.NotEmpty(t, killed.Writes[body], "the decoded record keeps the body write")
			require.NotEmpty(t, killed.Writes[binding], "the decoded record keeps the binding write")

			_, boundInStore := durableNumericBindings(t, fixture.dir)["hist:killed"]
			bodyInStore := persistedNodeBody(t, fixture.dir, "hist:killed")
			t.Logf("hist:killed: body in store %t, binding in store %t", bodyInStore, boundInStore)
			require.Equal(t, applied, bodyInStore, "only the applied commit reached Badger")
			require.Equal(t, applied, boundInStore, "only the applied commit's binding reached the dictionary")
		})
	}
}

// The repair is only sound when the record proves every entry it restores. Each
// subtest removes or adds one fact, and the repair must refuse instead of
// guessing.
func TestHistoricalAmbiguousRepairRefusesIncompleteProof(t *testing.T) {
	fixture := historicalAmbiguousRecords(t, false,
		historicalAmbiguousFillerBatch(t),
		historicalAmbiguousKilledBatch(t))
	commit := damagedBufferedCommit(t, fixture)
	repaired, err := repairV4BufferedCommitPhysicalKeys(cloneBufferedCommit(commit.decoded))
	require.NoError(t, err, "the unmodified record is determined by its facts")
	require.NotEmpty(t, repaired.Writes)

	t.Run("missing node body", func(t *testing.T) {
		// The damaged label and reverse keys still name the node, but without its
		// body the commit does not say which labels the node carries.
		orphaned := cloneBufferedCommit(commit.decoded)
		body := ""
		for _, id := range []NodeID{"hist:killed", "hist:labelled"} {
			if key := string(append([]byte{prefixNode}, id...)); len(orphaned.Writes[key]) > 0 {
				body = key
			}
		}
		require.NotEmpty(t, body, "the record holds the affected node body")
		delete(orphaned.Writes, body)
		_, err := repairV4BufferedCommitPhysicalKeys(orphaned)
		require.ErrorContains(t, err, "wal: historical physical key contains ambiguous UTF-8 replacement")
		require.ErrorContains(t, err, "has no body in this record")
		require.ErrorContains(t, err, "collision-class entries cannot be proven complete")
		t.Logf("refusal: %v", err)
	})

	t.Run("missing numeric binding", func(t *testing.T) {
		// Without the forward binding the damaged label and reverse keys carry a
		// number nothing in the record resolves.
		unbound := cloneBufferedCommit(commit.decoded)
		binding := ""
		for _, id := range []NodeID{"hist:killed", "hist:labelled"} {
			if key := string(append([]byte{prefixIDDictNodeForward}, id...)); len(unbound.Writes[key]) > 0 {
				binding = key
			}
		}
		require.NotEmpty(t, binding, "the record holds the affected numeric binding")
		delete(unbound.Writes, binding)
		_, err := repairV4BufferedCommitPhysicalKeys(unbound)
		require.ErrorContains(t, err, "wal: historical physical key contains ambiguous UTF-8 replacement")
		require.ErrorContains(t, err, "no numeric binding in this record")
		t.Logf("refusal: %v", err)
	})

	t.Run("unknown key family", func(t *testing.T) {
		unclassified := cloneBufferedCommit(commit.decoded)
		unclassified.Writes[string([]byte{0x27, 0x00, 0xFF})] = []byte{}
		_, err := repairV4BufferedCommitPhysicalKeys(unclassified)
		require.ErrorContains(t, err, "not in a re-derivable numeric-key family")
		require.ErrorContains(t, err, "unclassified family")
	})

	t.Run("known non-derived key family", func(t *testing.T) {
		// An embedding chunk key carries a binary chunk index and no fact of this
		// record re-derives it.
		foreign := cloneBufferedCommit(commit.decoded)
		foreign.Writes[string(append([]byte{prefixEmbedding}, append([]byte("hist:killed"), 0x00, 0x00, 0x00, 0x00, 0x80)...))] = []byte{1}
		_, err := repairV4BufferedCommitPhysicalKeys(foreign)
		require.ErrorContains(t, err, "not in a re-derivable numeric-key family")
		require.ErrorContains(t, err, "known non-derived family")
	})

	t.Run("damaged deletion", func(t *testing.T) {
		// A damaged deletion names no entry, and which old entry it removed is
		// not a fact this record holds.
		removal := cloneBufferedCommit(commit.decoded)
		removal.Deletes = append(removal.Deletes, string(labelIndexKey("Damaged", 129)))
		_, err := repairV4BufferedCommitPhysicalKeys(removal)
		require.ErrorContains(t, err, "would justify the removal")
		require.ErrorContains(t, err, "wal: historical physical key contains ambiguous UTF-8 replacement")
	})

	t.Run("cross-store numeric bindings", func(t *testing.T) {
		// A record carrying its own numeric bindings belongs to another store's
		// dictionary, so its numeric IDs are not this store's derivation.
		bound := cloneBufferedCommit(commit.decoded)
		bound.NumericBindings = []byte{1, 0}
		_, err := repairV4BufferedCommitPhysicalKeys(bound)
		require.ErrorContains(t, err, "cross-store numeric bindings")
	})

	t.Run("shared between-head key", func(t *testing.T) {
		// Two same-typed relationships between one pair share one between-head
		// entry, whose value is whichever was written last. The record's own
		// relationship body and binding are copied under a second relationship ID,
		// which is exactly the state a commit with two such relationships carries.
		shared := cloneBufferedCommit(commit.decoded)
		var bodyKey, bindingKey string
		var body, binding []byte
		for key, value := range commit.decoded.Writes {
			if len(key) == 0 {
				continue
			}
			switch key[0] {
			case prefixEdge:
				bodyKey, body = key, value
			case prefixIDDictEdgeForward:
				bindingKey, binding = key, value
			}
		}
		require.NotEmpty(t, bodyKey)
		require.NotEmpty(t, bindingKey)
		second := EdgeID("hist:second")
		shared.Writes[string(append([]byte{prefixEdge}, second...))] = body
		shared.Writes[string(append([]byte{prefixIDDictEdgeForward}, second...))] = append([]byte(nil), encodeNumID(131)...)
		require.NotEqual(t, binding, shared.Writes[string(append([]byte{prefixIDDictEdgeForward}, second...))])
		_, err := repairV4BufferedCommitPhysicalKeys(shared)
		require.ErrorContains(t, err, "between-head key")
		t.Logf("refusal: %v", err)
	})
}

// The record's Ops carry arbitrary property values, and the old JSON writer
// damaged those too: property text containing a byte it replaced is a
// U+FFFD-bearing string now. The repair never reads them, so a poisoned Ops value
// adds no write and replaces none.
//
// The repaired record is written with the current typed encoding, which is
// lossless for physical keys. That must not promote the historical named property
// images to authority: the explicit physical-property-image marker keeps them
// historical, while a current typed record and a legacy JSON record keep the
// authority decision they had.
func TestHistoricalAmbiguousRepairIgnoresLossyOperations(t *testing.T) {
	fixture := historicalAmbiguousRecords(t, false,
		historicalAmbiguousFillerBatch(t),
		historicalAmbiguousKilledBatch(t))
	commit := damagedBufferedCommit(t, fixture)
	repaired, err := repairV4BufferedCommitPhysicalKeys(commit.decoded)
	require.NoError(t, err)
	require.NotEmpty(t, repaired.Writes)

	withoutOps := cloneBufferedCommit(commit.decoded)
	withoutOps.Ops = nil
	bare, err := repairV4BufferedCommitPhysicalKeys(withoutOps)
	require.NoError(t, err)
	require.Equal(t, repaired.Writes, bare.Writes, "the derivation uses no operation value")

	poisoned := cloneBufferedCommit(commit.decoded)
	poisoned.Ops = append(poisoned.Ops, Operation{
		Type:    OpUpdateNode,
		NodeID:  "hist:killed",
		Node:    &Node{ID: "hist:killed", Labels: []string{"Dama\ufffdged"}, Properties: map[string]any{"sealed": "Dama\ufffdged"}},
		OldNode: &Node{ID: "hist:killed", Labels: []string{"Damaged"}},
	})
	tainted, err := repairV4BufferedCommitPhysicalKeys(poisoned)
	require.NoError(t, err)
	require.Equal(t, repaired.Writes, tainted.Writes, "an Ops value must not replace a derived entry")

	// The upgraded record the migration writes, carrying the poisoned value: the
	// conversion the V4 upgrade runs in every pass.
	payload := projectHistoricalBufferedCommit(t, poisoned, true)
	entry := WALEntry{Sequence: commit.entry.Sequence, Timestamp: commit.entry.Timestamp, Operation: OpBufferedCommit, Data: payload, Checksum: crc32Checksum(payload)}
	normalized, _, err := WALV4UpgradeOptions{}.records().convert(&bytes.Buffer{}, entry)
	require.NoError(t, err)
	require.NotEqual(t, entry.Data, normalized.Data, "the repair re-encodes the record")
	require.Equal(t, crc32Checksum(normalized.Data), normalized.Checksum, "the checksum follows the new payload")
	decoded, err := decodeBufferedWALRecord(normalized)
	require.NoError(t, err)
	require.True(t, decoded.losslessPhysicalKeys, "the typed encoding keeps every physical key byte")
	require.True(t, decoded.PhysicalPropertyImages, "the marker travels with the record")
	require.False(t, decoded.namedPropertiesAuthoritative, "repaired historical properties stay non-authoritative")
	require.Equal(t, tainted.Writes, decoded.Writes, "the repaired physical images round trip")
	require.Len(t, decoded.Ops, len(poisoned.Ops), "the record keeps its operation images")

	// A record the current writer produced keeps named property authority.
	typed := WALBufferedCommitData{TxID: "typed", Writes: map[string][]byte{"k": []byte("v")}}
	typedPayload, err := marshalWALRecoveryPayload(nil, typed)
	require.NoError(t, err)
	current := WALEntry{Operation: OpBufferedCommit, Data: typedPayload, Checksum: crc32Checksum(typedPayload)}
	decodedCurrent, err := decodeBufferedWALRecord(current)
	require.NoError(t, err)
	require.True(t, decodedCurrent.losslessPhysicalKeys)
	require.False(t, decodedCurrent.PhysicalPropertyImages)
	require.True(t, decodedCurrent.namedPropertiesAuthoritative)

	// Plain legacy JSON keeps both facts false.
	legacyPayload, err := json.Marshal(WALBufferedCommitData{TxID: "legacy"})
	require.NoError(t, err)
	legacy := WALEntry{Operation: OpBufferedCommit, Data: legacyPayload, Checksum: crc32Checksum(legacyPayload)}
	decodedLegacy, err := decodeBufferedWALRecord(legacy)
	require.NoError(t, err)
	require.False(t, decodedLegacy.losslessPhysicalKeys)
	require.False(t, decodedLegacy.namedPropertiesAuthoritative)
}

// A legacy record whose physical keys survived the encoding is admitted
// unchanged: the repair only rewrites what the encoding actually damaged, and the
// ambiguity decision depends on the source encoding, not on named-property
// authority.
func TestV4UpgradeKeepsUndamagedHistoricalRecordsUnchanged(t *testing.T) {
	clean := WALBufferedCommitData{
		TxID:   "clean",
		Writes: map[string][]byte{string(labelIndexKey("Damaged", 1)): {}, string(append([]byte{prefixNode}, "hist:a"...)): {0x00}},
	}
	payload, err := json.Marshal(clean)
	require.NoError(t, err)
	entry := WALEntry{Sequence: 4, Operation: OpBufferedCommit, Data: payload, Checksum: crc32Checksum(payload)}
	normalized, _, err := WALV4UpgradeOptions{}.records().convert(&bytes.Buffer{}, entry)
	require.NoError(t, err)
	require.Equal(t, entry.Data, normalized.Data, "no damaged key means no rewrite")
	require.Equal(t, entry.Checksum, normalized.Checksum)

	decoded, err := decodeBufferedWALRecord(normalized)
	require.NoError(t, err)
	require.False(t, decoded.losslessPhysicalKeys)
	require.False(t, decoded.namedPropertiesAuthoritative)

	// A record of another operation is not this owner's concern.
	other := WALEntry{Operation: OpTxBegin, Data: []byte(`{"tx_id":"legacy"}`), Checksum: crc32Checksum([]byte(`{"tx_id":"legacy"}`))}
	unchanged, _, err := WALV4UpgradeOptions{}.records().convert(&bytes.Buffer{}, other)
	require.NoError(t, err)
	require.Equal(t, other.Data, unchanged.Data)
	require.Equal(t, other.Checksum, unchanged.Checksum)
}
