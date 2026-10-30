package qdrantgrpc

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
)

// Storage returns a homogeneous list as a typed slice and a number in the
// width its codec keeps (earlier versions narrowed a Go int to int8, uint16
// and the like). Collection metadata, payload values and range filters read
// them in any of those forms.
func TestStoredValueFormsReadBack(t *testing.T) {
	for _, dims := range []any{int8(4), uint8(200), uint16(384), int32(1536), int64(768), 3.0} {
		engine := storage.NewNamespacedEngine(runtimeFixtureOwner.Own(t, runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "test")
		_, err := engine.CreateNode(&storage.Node{
			ID: collectionMetaNodeID, Labels: []string{collectionMetaNodeLabel},
			Properties: map[string]any{"dimensions": dims, "distance": int8(qpb.Distance_Dot)},
		})
		require.NoError(t, err)
		meta, err := readCollectionMeta(engine)
		require.NoError(t, err, "%T", dims)
		require.Positive(t, meta.Dimensions, "%T", dims)
		require.Equal(t, qpb.Distance_Dot, meta.Distance, "%T", dims)
	}

	list := func(values ...*qpb.Value) *qpb.Value {
		return &qpb.Value{Kind: &qpb.Value_ListValue{ListValue: &qpb.ListValue{Values: values}}}
	}
	str := func(s string) *qpb.Value { return &qpb.Value{Kind: &qpb.Value_StringValue{StringValue: s}} }
	num := func(n int64) *qpb.Value { return &qpb.Value{Kind: &qpb.Value_IntegerValue{IntegerValue: n}} }
	dbl := func(f float64) *qpb.Value { return &qpb.Value{Kind: &qpb.Value_DoubleValue{DoubleValue: f}} }
	boolean := func(b bool) *qpb.Value { return &qpb.Value{Kind: &qpb.Value_BoolValue{BoolValue: b}} }
	for _, tc := range []struct {
		stored any
		want   *qpb.Value
	}{
		{[]string{"a", "b"}, list(str("a"), str("b"))},
		{[]any{"a", "b"}, list(str("a"), str("b"))},
		{[]int64{1, 2}, list(num(1), num(2))},
		{[]float64{0.5}, list(dbl(0.5))},
		{[]bool{true}, list(boolean(true))},
		{int8(3), num(3)},
		{uint16(300), num(300)},
		{int32(-7), num(-7)},
	} {
		require.Equal(t, tc.want.String(), anyToValue(tc.stored).String(), "%T", tc.stored)
	}

	gte := 4.0
	for _, stored := range []any{int8(5), uint16(5), int32(5), int64(5), float32(5), 5.0} {
		require.True(t, matchesRange(stored, &qpb.Range{Gte: &gte}), "%T", stored)
	}
	require.False(t, matchesRange("5", &qpb.Range{Gte: &gte}), "a string is no number")
}
