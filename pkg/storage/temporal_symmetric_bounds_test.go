package storage

import (
	"encoding/binary"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestSymmetricTemporalEpochs(t *testing.T) {
	for _, relationship := range []bool{false, true} {
		for _, indexed := range []bool{false, true} {
			if relationship && indexed {
				continue
			} // Relationship enforcement uses type scans, not the node temporal index.
			t.Run(fmt.Sprintf("relationship=%t/indexed=%t", relationship, indexed), func(t *testing.T) {
				engine, cleanup := setupTestBadgerEngine(t)
				defer cleanup()
				c := Constraint{Name: "epochs", Type: ConstraintTemporal, EntityType: ConstraintEntityNode, Label: "Window", Properties: []string{"key", "start", "end"}}
				if relationship {
					c.EntityType = ConstraintEntityRelationship
				}
				schema := engine.GetSchemaForNamespace("test")
				require.NoError(t, schema.AddConstraint(c))
				if indexed {
					require.NoError(t, MarkNewTemporalConstraintsForEngine(engine, schema, []Constraint{c}))
					require.True(t, engine.temporalIndexAuthoritative(schema, c))
				}
				for _, id := range []NodeID{"test:a", "test:b"} {
					_, err := engine.CreateNode(t.Context(), &Node{ID: id})
					require.NoError(t, err)
				}
				write := func(id string, p map[string]interface{}) error {
					if relationship {
						return engine.CreateEdge(t.Context(), &Edge{ID: EdgeID("test:" + id), StartNode: "test:a", EndNode: "test:b", Type: c.Label, Properties: p})
					}
					_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID("test:" + id), Labels: []string{c.Label}, Properties: p})
					return err
				}
				cut := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
				require.NoError(t, write("past", map[string]interface{}{"key": "asset", "start": nil, "end": cut}))
				require.NoError(t, write("future", map[string]interface{}{"key": "asset", "start": cut, "end": nil}))
				require.Error(t, write("overlap", map[string]interface{}{"key": "asset", "start": cut.Add(-time.Hour), "end": cut.Add(time.Hour)}))
				require.Error(t, write("second-past", map[string]interface{}{"key": "asset", "start": nil, "end": cut.Add(-time.Hour)}))
				require.Error(t, write("second-future", map[string]interface{}{"key": "asset", "start": cut.Add(time.Hour), "end": nil}))
				require.NoError(t, write("different", map[string]interface{}{"key": "other", "start": nil, "end": cut}))
				for i, p := range []map[string]interface{}{
					{"key": "invalid", "start": nil, "end": nil},
					{"key": "invalid", "start": cut, "end": cut},
					{"key": "invalid", "start": cut, "end": cut.Add(-time.Hour)},
					{"key": "invalid", "start": "bad", "end": cut},
					{"key": "invalid", "start": cut, "end": "bad"},
				} {
					require.Error(t, write(fmt.Sprintf("invalid-%d", i), p))
				}
				if !relationship {
					// Force each canonical coverage kernel; merely omitting an
					// index mark does not guarantee a public write takes a scan.
					mode := temporalOverlapFullLabel
					if indexed {
						mode = temporalOverlapIndexed
					}
					for _, tc := range []struct {
						start, end time.Time
						hasEnd     bool
						exclude    NodeID
						overlap    bool
					}{
						{time.Time{}, cut, true, "test:past", false},
						{time.Time{}, cut, true, "", true},
						{cut, time.Time{}, false, "test:future", false},
						{cut.Add(-time.Hour), cut.Add(time.Hour), true, "", true},
					} {
						err := engine.withView(func(txn *badger.Txn) error {
							return engine.scanForTemporalOverlapInTxn(t.Context(), txn, "test", c, "asset", tc.start, tc.end, tc.hasEnd, tc.exclude, mode)
						})
						if tc.overlap {
							require.Error(t, err)
						} else {
							require.NoError(t, err)
						}
					}
					for _, at := range []time.Time{time.Date(1500, 1, 1, 0, 0, 0, 0, time.UTC), cut.Add(-time.Nanosecond), cut, cut.Add(time.Hour)} {
						got, err := engine.GetTemporalNodeAsOfInNamespace(t.Context(), "test", c.Label, "key", "asset", "start", "end", at)
						require.NoError(t, err)
						require.NotNil(t, got)
						want := NodeID("test:past")
						if !at.Before(cut) {
							want = "test:future"
						}
						require.Equal(t, want, got.ID)
					}
					old, err := engine.GetNode(t.Context(), "test:future")
					require.NoError(t, err)
					next := *old
					next.Properties = maps.Clone(old.Properties)
					next.Properties["start"] = cut.Add(-time.Hour)
					require.Error(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{Before: old, After: &next, Policy: ClientNodeEdits}))
				} else {
					old, err := engine.GetEdge(t.Context(), "test:future")
					require.NoError(t, err)
					next := *old
					next.Properties = maps.Clone(old.Properties)
					next.Properties["start"] = cut.Add(-time.Hour)
					require.Error(t, engine.UpdateEdge(t.Context(), EdgeUpdateRequest{Before: old, After: &next}))
				}
				require.NoError(t, ValidateConstraintOnCreationForEngine(t.Context(), engine, c), "existing symmetric epochs must pass DDL validation")
			})
		}
	}
}

func TestTemporalEpochSortEncodingKeepsFiniteKeys(t *testing.T) {
	for _, at := range []time.Time{time.Unix(-1, 0), time.Unix(0, 1), time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)} {
		want := make([]byte, 8)
		binary.BigEndian.PutUint64(want, uint64(at.UnixNano())^(1<<63))
		require.Equal(t, want, encodeTemporalSortTime(at))
	}
	require.Equal(t, make([]byte, 8), encodeTemporalSortTime(time.Time{}))
}

func TestReadTemporalBoundsNilRepresentations(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, missing := range []interface{}{nil, (*time.Time)(nil)} {
		start, end, hasEnd, ok := ReadTemporalBounds(missing, at)
		require.True(t, ok)
		require.True(t, start.IsZero())
		require.Equal(t, at, end)
		require.True(t, hasEnd)
		start, _, hasEnd, ok = ReadTemporalBounds(at, missing)
		require.True(t, ok)
		require.Equal(t, at, start)
		require.False(t, hasEnd)
		_, _, _, ok = ReadTemporalBounds(missing, missing)
		require.False(t, ok)
	}
	for _, bad := range []interface{}{"bad", []interface{}{1}, map[string]interface{}{"a": 1}} {
		_, _, _, ok := ReadTemporalBounds(bad, at)
		require.False(t, ok)
		_, _, _, ok = ReadTemporalBounds(at, bad)
		require.False(t, ok)
	}
}
