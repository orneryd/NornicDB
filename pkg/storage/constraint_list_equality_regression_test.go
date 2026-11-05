package storage

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
	"time"
)

func TestConstraintListEqualityRepresentations(t *testing.T) {
	type numbers []int64
	for _, tc := range []struct {
		name  string
		a, b  interface{}
		equal bool
	}{
		{"stored float list", []interface{}{float64(200)}, []float64{200}, true},
		{"numeric list", []int64{1, 2}, []interface{}{float64(1), uint32(2)}, true},
		{"named list", numbers{1}, []interface{}{int64(1)}, true},
		{"empty list", []interface{}{}, []float64{}, true},
		{"strings", []string{"x"}, []interface{}{"x"}, true},
		{"bools", []bool{true}, []interface{}{true}, true},
		{"null positions", []interface{}{nil, "x"}, []interface{}{nil, "x"}, true},
		{"different order", []int64{1, 2}, []interface{}{int64(2), int64(1)}, false},
		{"different length", []int64{1}, []interface{}{int64(1), int64(2)}, false},
		{"list versus scalar", []int64{1}, int64(1), false},
		{"empty versus null", []int64{}, nil, false},
		{"nil versus empty list", []int64(nil), []interface{}{}, false},
		{"large integers", []int64{1<<53 + 1}, []interface{}{int64(1 << 53)}, false},
		{"large integer float", []int64{1<<53 + 1}, []interface{}{float64(1 << 53)}, false},
		{"max unsigned", []uint64{math.MaxUint64}, []interface{}{uint64(math.MaxUint64)}, true},
		{"unsigned float boundary", []uint64{math.MaxUint64}, []interface{}{float64(1 << 64)}, false},
		{"signed float boundary", []int64{math.MaxInt64}, []interface{}{float64(1 << 63)}, false},
		{"signed minimum", []int64{math.MinInt64}, []interface{}{float64(-1 << 63)}, true},
		{"negative unsigned", []int64{-1}, []interface{}{uint64(math.MaxUint64)}, false},
		{"nan", []float64{math.NaN()}, []interface{}{math.NaN()}, false},
		{"float fraction", []float32{.5}, []interface{}{float64(.5)}, true},
		{"different fractions", []float64{.5}, []interface{}{float64(.6)}, false},
		{"signed zero", []float64{math.Copysign(0, -1)}, []interface{}{float64(0)}, true},
		{"nested lists", [][]int64{{1}}, []interface{}{[]interface{}{float64(1)}}, true},
		{"map leaf unchanged", []interface{}{map[string]interface{}{"x": int64(1)}}, []interface{}{map[string]interface{}{"x": float64(1)}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.equal, compareValues(tc.a, tc.b))
			require.Equal(t, tc.equal, compareValues(tc.b, tc.a))
		})
	}
}

func TestUniqueListDurableRoundTrip(t *testing.T) {
	for _, entity := range []string{"node", "relationship"} {
		for _, mode := range []string{"direct", "transaction", "pending", "restart", "composite"} {
			t.Run(entity+"/"+mode, func(t *testing.T) {
				path := t.TempDir()
				engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(path))
				if engine != nil {
					fixtureowner.Own(t, engine)
				}
				require.NoError(t, err)
				defer func() { require.NoError(t, engine.Close()) }()
				for _, id := range []NodeID{"test:a", "test:b"} {
					_, err = engine.CreateNode(t.Context(), &Node{ID: id})
					require.NoError(t, err)
				}
				typ := ConstraintEntityNode
				if entity == "relationship" {
					typ = ConstraintEntityRelationship
				}
				properties := []string{"id"}
				if mode == "composite" {
					properties = []string{"group", "id"}
				}
				require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(Constraint{Name: "unique_list", Type: ConstraintUnique, EntityType: typ, Label: "T", Properties: properties}))
				create := func(tx *BadgerTransaction, id string, value interface{}) error {
					props := map[string]interface{}{"group": "same", "id": value}
					if entity == "node" {
						n := &Node{ID: NodeID("test:" + id), Labels: []string{"T"}, Properties: props}
						if tx != nil {
							_, err := tx.CreateNode(t.Context(), n)
							return err
						}
						_, err := engine.CreateNode(t.Context(), n)
						return err
					}
					e := &Edge{ID: EdgeID("test:" + id), Type: "T", StartNode: "test:a", EndNode: "test:b", Properties: props}
					if tx != nil {
						return tx.CreateEdge(t.Context(), e)
					}
					return engine.CreateEdge(t.Context(), e)
				}
				first := []interface{}{float64(200)}
				if mode == "pending" {
					tx, err := engine.BeginTransaction(t.Context())
					require.NoError(t, err)
					defer tx.Rollback()
					require.NoError(t, create(tx, "one", first))
					err = create(tx, "two", []float64{200})
					if err == nil {
						err = tx.Commit()
					}
					asConstraintViolation(t, err)
					return
				}
				require.NoError(t, create(nil, "one", first))
				if mode == "restart" {
					require.NoError(t, engine.Close())
					engine, err = fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(path))
					if engine != nil {
						fixtureowner.Own(t, engine)
					}
					require.NoError(t, err)
				}
				if mode == "direct" {
					asConstraintViolation(t, create(nil, "two", first))
					return
				}
				tx, err := engine.BeginTransaction(t.Context())
				require.NoError(t, err)
				defer tx.Rollback()
				err = create(tx, "two", first)
				if err == nil {
					err = tx.Commit()
				}
				asConstraintViolation(t, err)
			})
		}
	}
}

func TestUniqueListPreservesLargeIntegerIdentityAfterRestart(t *testing.T) {
	path := t.TempDir()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(path))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	defer func() { engine.Close() }()
	require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(Constraint{Name: "large_list", Type: ConstraintUnique, Label: "LongList", Properties: []string{"id"}}))
	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:first", Labels: []string{"LongList"}, Properties: map[string]interface{}{"id": []int64{1 << 53}}})
	require.NoError(t, err)
	require.NoError(t, engine.Close())
	engine, err = fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(path))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:distinct", Labels: []string{"LongList"}, Properties: map[string]interface{}{"id": []int64{1<<53 + 1}}})
	require.NoError(t, err, "adjacent longs are different UNIQUE values, including after index rebuild")
	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:duplicate", Labels: []string{"LongList"}, Properties: map[string]interface{}{"id": []interface{}{int64(1 << 53)}}})
	asConstraintViolation(t, err)
}
func TestConstraintListEqualityCyclesAndBoundaries(t *testing.T) {
	a := []interface{}{nil}
	a[0] = a
	b := []interface{}{nil}
	b[0] = b
	require.True(t, compareValues(a, b))
	for _, tc := range []struct {
		a, b interface{}
		eq   bool
	}{
		{[]interface{}{nil}, []interface{}{"x"}, false},
		{[][]int64{{1}}, []interface{}{int64(1)}, false},
		{[]int64(nil), []float64(nil), true},
		{[]int64{1}, []interface{}{float64(1.5)}, false},
		{[]string{"x"}, []interface{}{true}, false},
		{[]uint32{1}, []interface{}{float32(1)}, true},
		{[]uint32{1}, []interface{}{int8(1)}, true},
		{[]int32{1}, []interface{}{int8(1)}, true},
	} {
		require.Equal(t, tc.eq, compareValues(tc.a, tc.b))
		require.Equal(t, tc.eq, compareValues(tc.b, tc.a))
	}
}
func TestConstraintListCanonicalKeysPreserveEquality(t *testing.T) {
	for _, pair := range []struct {
		a, b interface{}
		same bool
	}{
		{[]int64{200}, []float64{200}, true},
		{[]int64{1 << 53}, []int64{1<<53 + 1}, false},
		{[]int64{math.MaxInt64}, []float64{float64(1 << 63)}, false},
		{[]uint64{1 << 63}, []float64{float64(1 << 63)}, true},
		{[]uint64{math.MaxUint64}, []float64{float64(1 << 64)}, false},
		{[]int64{math.MinInt64}, []float64{float64(-1 << 63)}, true},
		{[]float32{.5}, []float64{.5}, true},
		{[]float64{math.Copysign(0, -1)}, []float64{0}, true},
	} {
		a, ok, keyErr := indexValueKey(t.Context(), pair.a)
		require.NoError(t, keyErr)
		require.True(t, ok)
		b, ok, keyErr := indexValueKey(t.Context(), pair.b)
		require.NoError(t, keyErr)
		require.True(t, ok)
		require.Equal(t, pair.same, a == b)
	}
}

func TestUniqueListRejectsDuplicateAfterBufferedRecovery(t *testing.T) {
	dir := t.TempDir()
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
	wal := engine.bufferedCommitWAL
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(Constraint{Name: "recovered_list", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "T", Properties: []string{"id"}}))
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.CreateEdge(t.Context(), &Edge{ID: "test:recovered", Type: "T", StartNode: "test:a", EndNode: "test:b", Properties: map[string]interface{}{"id": []interface{}{float64(200)}}}))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	crashClose(t, engine, wal)
	recovered := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
	wal2 := recovered.bufferedCommitWAL
	t.Cleanup(func() { require.NoError(t, wal2.Close()); require.NoError(t, recovered.Close()) })
	n, err := RecoverBufferedCommits(recovered, wal2)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	next, err := recovered.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer next.Rollback()
	require.NoError(t, next.CreateEdge(t.Context(), &Edge{ID: "test:duplicate", Type: "T", StartNode: "test:a", EndNode: "test:b", Properties: map[string]interface{}{"id": []interface{}{float64(200)}}}))
	asConstraintViolation(t, next.Commit())
}

var constraintEqualityBenchmarkResult bool

func BenchmarkConstraintEqualityStableValues(b *testing.B) {
	for _, tc := range []struct {
		name string
		a, b interface{}
	}{
		{"scalar", int64(200), float64(200)},
		{"typed_numeric_list", []float64{1, 2, 3, 4}, []float64{1, 2, 3, 4}},
		{"typed_string_list", []string{"a", "b", "c", "d"}, []string{"a", "b", "c", "d"}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				constraintEqualityBenchmarkResult = compareValues(tc.a, tc.b)
			}
		})
	}
}
