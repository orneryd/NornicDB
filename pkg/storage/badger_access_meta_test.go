package storage

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

func newTestEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	if err != nil {
		t.Fatalf("NewBadgerEngineInMemory: %v", err)
	}
	t.Cleanup(func() { engine.Close() })
	return engine
}

func TestAccessMetaOwnsCatalogAndPreservesConcurrentPolicyState(t *testing.T) {
	engine := newTestEngine(t)
	require.Equal(t, accessMetaKey("test:node"), indexEntryCatalogKey("test:node"))
	node := &Node{ID: "test:node", Labels: []string{"Old"}}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)
	meta, err := readStoredAccessMeta(t, engine, string(node.ID))
	require.NoError(t, err)
	require.Nil(t, meta, "catalog writes must not count as ON ACCESS")
	meta = &knowledgepolicy.AccessMetaEntry{TargetID: string(node.ID), TargetScope: knowledgepolicy.ScopeNode}
	meta.Fixed.AccessCount = 42
	meta.Overflow = map[string]interface{}{"custom": int64(7)}
	require.NoError(t, engine.PutAccessMeta(string(node.ID), meta))
	stale, err := readStoredAccessMeta(t, engine, string(node.ID))
	require.NoError(t, err)
	node.Labels = []string{"New"}
	require.NoError(t, engine.UpdateNode(node))
	current, err := engine.GetIndexEntryCatalog(string(node.ID))
	require.NoError(t, err)
	require.NotNil(t, current)
	stale.Fixed.AccessCount++
	require.NoError(t, engine.PutAccessMeta(string(node.ID), stale))
	got, err := engine.GetIndexEntryCatalog(string(node.ID))
	require.NoError(t, err)
	require.Equal(t, current, got)
	meta, err = readStoredAccessMeta(t, engine, string(node.ID))
	require.NoError(t, err)
	require.Equal(t, int64(43), meta.Fixed.AccessCount)
	require.Equal(t, int64(7), meta.Overflow["custom"])
	require.NoError(t, engine.DeleteIndexEntryCatalog(string(node.ID)))
	got, err = engine.GetIndexEntryCatalog(string(node.ID))
	require.NoError(t, err)
	require.Nil(t, got)
	meta, err = readStoredAccessMeta(t, engine, string(node.ID))
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.Equal(t, int64(43), meta.Fixed.AccessCount)
}

func TestAccessMetaDeletionRetainsGraphCatalog(t *testing.T) {
	for _, tc := range []struct {
		name, id string
		count    int64
		catalog  bool
	}{
		{name: "metadata only", id: "node-1", count: 10},
		{name: "indexed graph node", id: "test:delete-access", count: 42, catalog: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine := newTestEngine(t)
			if tc.catalog {
				_, err := engine.CreateNode(&Node{ID: NodeID(tc.id), Labels: []string{"Person"}})
				require.NoError(t, err)
			}
			before, err := engine.GetIndexEntryCatalog(tc.id)
			require.NoError(t, err)
			if tc.catalog {
				require.NotNil(t, before)
			} else {
				require.Nil(t, before)
			}
			require.NoError(t, engine.PutAccessMeta(tc.id, &knowledgepolicy.AccessMetaEntry{TargetID: tc.id, TargetScope: knowledgepolicy.ScopeNode, Fixed: knowledgepolicy.AccessMetaFixedFields{AccessCount: tc.count}}))
			stored, err := readStoredAccessMeta(t, engine, tc.id)
			require.NoError(t, err)
			require.NotNil(t, stored)
			require.Equal(t, tc.count, stored.Fixed.AccessCount)
			require.NoError(t, engine.DeleteAccessMeta(tc.id))
			stored, err = readStoredAccessMeta(t, engine, tc.id)
			require.NoError(t, err)
			require.Nil(t, stored)
			after, err := engine.GetIndexEntryCatalog(tc.id)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestAccessMeta_PutAndGet(t *testing.T) {
	engine := newTestEngine(t)

	entry := &knowledgepolicy.AccessMetaEntry{
		TargetID:    "node-1",
		TargetScope: knowledgepolicy.ScopeNode,
		Fixed: knowledgepolicy.AccessMetaFixedFields{
			AccessCount:    42,
			LastAccessedAt: 1000000,
		},
		LastMutatedAt: 2000000,
		MutationCount: 5,
	}

	if err := engine.PutAccessMeta("node-1", entry); err != nil {
		t.Fatalf("PutAccessMeta: %v", err)
	}

	got, err := readStoredAccessMeta(t, engine, "node-1")
	if err != nil {
		t.Fatalf("ReadAccessMeta: %v", err)
	}
	if got == nil {
		t.Fatal("expected entry, got nil")
	}
	if got.TargetID != "node-1" {
		t.Errorf("TargetID: expected node-1, got %s", got.TargetID)
	}
	if got.Fixed.AccessCount != 42 {
		t.Errorf("AccessCount: expected 42, got %d", got.Fixed.AccessCount)
	}
	if got.LastMutatedAt != 2000000 {
		t.Errorf("LastMutatedAt: expected 2000000, got %d", got.LastMutatedAt)
	}
}

func TestAccessMeta_GetNotFound(t *testing.T) {
	engine := newTestEngine(t)

	got, err := readStoredAccessMeta(t, engine, "nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Error("expected nil for missing key")
	}
}

func TestAccessMeta_Overwrite(t *testing.T) {
	engine := newTestEngine(t)

	entry1 := &knowledgepolicy.AccessMetaEntry{
		TargetID: "node-1",
		Fixed:    knowledgepolicy.AccessMetaFixedFields{AccessCount: 1},
	}
	entry2 := &knowledgepolicy.AccessMetaEntry{
		TargetID: "node-1",
		Fixed:    knowledgepolicy.AccessMetaFixedFields{AccessCount: 99},
	}

	if err := engine.PutAccessMeta("node-1", entry1); err != nil {
		t.Fatal(err)
	}
	if err := engine.PutAccessMeta("node-1", entry2); err != nil {
		t.Fatal(err)
	}

	got, err := readStoredAccessMeta(t, engine, "node-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Fixed.AccessCount != 99 {
		t.Errorf("expected overwritten count 99, got %d", got.Fixed.AccessCount)
	}
}

func TestAccessMeta_Scan(t *testing.T) {
	engine := newTestEngine(t)

	for _, id := range []string{"a", "b", "c"} {
		entry := &knowledgepolicy.AccessMetaEntry{
			TargetID: id,
			Fixed:    knowledgepolicy.AccessMetaFixedFields{AccessCount: 1},
		}
		if err := engine.PutAccessMeta(id, entry); err != nil {
			t.Fatal(err)
		}
	}

	result, err := engine.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{Mode: knowledgepolicy.AccessMetaReadAll})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Entries) != 3 {
		t.Errorf("expected 3 entries, got %d", len(result.Entries))
	}
}

func TestAccessMeta_WithKalmanFilters(t *testing.T) {
	engine := newTestEngine(t)

	entry := &knowledgepolicy.AccessMetaEntry{
		TargetID:    "node-1",
		TargetScope: knowledgepolicy.ScopeNode,
		Fixed:       knowledgepolicy.AccessMetaFixedFields{AccessCount: 10},
		KalmanFilters: map[string]*knowledgepolicy.KalmanPropertyState{
			"confidence": {
				FilteredValue: 0.75,
				Filter: knowledgepolicy.KalmanFilterState{
					X:            0.75,
					P:            1.5,
					Q:            0.001,
					R:            50.0,
					K:            0.03,
					E:            1.0,
					LastX:        0.72,
					Observations: 20,
				},
			},
		},
		Overflow: map[string]interface{}{
			"customField": int64(42),
		},
	}

	if err := engine.PutAccessMeta("node-1", entry); err != nil {
		t.Fatal(err)
	}

	got, err := readStoredAccessMeta(t, engine, "node-1")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected entry")
	}

	ks, ok := got.KalmanFilters["confidence"]
	if !ok {
		t.Fatal("expected KalmanFilters[confidence]")
	}
	if ks.FilteredValue != 0.75 {
		t.Errorf("FilteredValue: expected 0.75, got %f", ks.FilteredValue)
	}
	if ks.Filter.X != 0.75 {
		t.Errorf("Filter.X: expected 0.75, got %f", ks.Filter.X)
	}
	if ks.Filter.Observations != 20 {
		t.Errorf("Observations: expected 20, got %d", ks.Filter.Observations)
	}
}

func TestAccessMeta_WithVarianceTracker(t *testing.T) {
	engine := newTestEngine(t)

	entry := &knowledgepolicy.AccessMetaEntry{
		TargetID: "node-1",
		KalmanFilters: map[string]*knowledgepolicy.KalmanPropertyState{
			"metric": {
				FilteredValue: 5.0,
				Filter:        knowledgepolicy.KalmanFilterState{X: 5.0, P: 1.0, Q: 0.01, R: 1.0},
				Variance: &knowledgepolicy.VarianceTrackerState{
					Window:    []float64{4.9, 5.1, 5.0, 4.8},
					WindowIdx: 3,
					Mean:      4.95,
					Variance:  0.0125,
				},
			},
		},
	}

	if err := engine.PutAccessMeta("node-1", entry); err != nil {
		t.Fatal(err)
	}

	got, err := readStoredAccessMeta(t, engine, "node-1")
	if err != nil {
		t.Fatal(err)
	}

	ks := got.KalmanFilters["metric"]
	if ks.Variance == nil {
		t.Fatal("expected VarianceTracker")
	}
	if len(ks.Variance.Window) != 4 {
		t.Errorf("expected window length 4, got %d", len(ks.Variance.Window))
	}
	if ks.Variance.WindowIdx != 3 {
		t.Errorf("expected WindowIdx 3, got %d", ks.Variance.WindowIdx)
	}
}

func TestAccessMetaReadSelectionAndErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		id      string
		entry   knowledgepolicy.AccessMetaEntry
		corrupt bool
		point   bool
		scan    bool
	}{
		{name: "default empty entity ID", id: "", entry: knowledgepolicy.AccessMetaEntry{HasAccessState: true}, point: true, scan: true},
		{name: "access state", id: "test:entity", entry: knowledgepolicy.AccessMetaEntry{HasAccessState: true}, point: true, scan: true},
		{name: "catalog only", id: "test:entity", entry: knowledgepolicy.AccessMetaEntry{HasIndexKeys: true}},
		{name: "tombstone", id: "test:entity", entry: knowledgepolicy.AccessMetaEntry{IndexTombstone: true}, point: true},
		{name: "internal work", id: policyWorkMetaIDPrefix + "work", entry: knowledgepolicy.AccessMetaEntry{HasAccessState: true}, point: true},
		{name: "corrupt entity", id: "test:entity", corrupt: true},
		{name: "corrupt internal work", id: policyWorkMetaIDPrefix + "work", corrupt: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine := newTestEngine(t)
			if tc.id == "" {
				missing, err := engine.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{})
				require.NoError(t, err)
				require.Equal(t, knowledgepolicy.AccessMetaReadResult{}, missing)
			}
			data, err := msgpack.Marshal(&tc.entry)
			require.NoError(t, err)
			if tc.corrupt {
				data = []byte{0xc1}
			}
			require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error { return txn.Set(accessMetaKey(tc.id), data) }))
			point, err := engine.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{EntityID: tc.id})
			require.Nil(t, point.Entries)
			if tc.corrupt {
				require.Error(t, err)
				require.Nil(t, point.Entry)
			} else {
				require.NoError(t, err)
				if tc.point {
					require.Equal(t, &tc.entry, point.Entry)
				} else {
					require.Nil(t, point.Entry)
				}
			}
			entries, err := engine.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{Mode: knowledgepolicy.AccessMetaReadAll, EntityID: "ignored"})
			require.Nil(t, entries.Entry)
			if tc.corrupt && tc.id[0] != 0 {
				require.Error(t, err)
				require.Nil(t, entries.Entries)
			} else {
				require.NoError(t, err)
				if tc.scan {
					require.Equal(t, []*knowledgepolicy.AccessMetaEntry{&tc.entry}, entries.Entries)
				} else {
					require.Nil(t, entries.Entries)
				}
			}
		})
	}
}

func TestAccessMetaReadRejectsInvalidMode(t *testing.T) {
	engine := newTestEngine(t)
	require.NoError(t, engine.PutAccessMeta("node-1", &knowledgepolicy.AccessMetaEntry{
		TargetID: "node-1",
		Fixed:    knowledgepolicy.AccessMetaFixedFields{AccessCount: 7},
	}))
	result, err := engine.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{Mode: knowledgepolicy.AccessMetaReadMode(42), EntityID: "node-1"})
	require.ErrorIs(t, err, ErrInvalidData)
	require.Nil(t, result.Entry)
	require.Nil(t, result.Entries)
}

// readStoredAccessMeta is the point-read assertion helper: it returns the
// stored entry plus the read error.
func readStoredAccessMeta(t *testing.T, engine *BadgerEngine, entityID string) (*knowledgepolicy.AccessMetaEntry, error) {
	t.Helper()
	result, err := engine.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{Mode: knowledgepolicy.AccessMetaReadPoint, EntityID: entityID})
	return result.Entry, err
}
