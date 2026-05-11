package observability

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
)

func TestFilteredBaggagePropagator_EmptyAllowListDropsAll(t *testing.T) {
	orig := BaggageAllowList
	BaggageAllowList = map[string]bool{}
	defer func() { BaggageAllowList = orig }()

	carrier := propagation.MapCarrier{}
	carrier.Set("baggage", "user_id=alice,session=xyz,tenant=acme")

	prop := FilteredBaggagePropagator{}
	ctx := prop.Extract(context.Background(), carrier)

	bag := baggage.FromContext(ctx)
	assert.Empty(t, bag.Members(), "empty allow-list must drop all baggage")
}

func TestFilteredBaggagePropagator_AllowsConfiguredKeys(t *testing.T) {
	orig := BaggageAllowList
	BaggageAllowList = map[string]bool{
		"tenant":  true,
		"user_id": true,
	}
	defer func() { BaggageAllowList = orig }()

	carrier := propagation.MapCarrier{}
	carrier.Set("baggage", "user_id=alice,session=xyz,tenant=acme")

	prop := FilteredBaggagePropagator{}
	ctx := prop.Extract(context.Background(), carrier)

	bag := baggage.FromContext(ctx)
	members := bag.Members()

	keys := make(map[string]string)
	for _, m := range members {
		keys[m.Key()] = m.Value()
	}

	assert.Equal(t, "alice", keys["user_id"])
	assert.Equal(t, "acme", keys["tenant"])
	assert.NotContains(t, keys, "session", "non-allowed key must be dropped")
	assert.Len(t, members, 2)
}

func TestFilteredBaggagePropagator_NoBaggagePassesThrough(t *testing.T) {
	orig := BaggageAllowList
	BaggageAllowList = map[string]bool{"tenant": true}
	defer func() { BaggageAllowList = orig }()

	carrier := propagation.MapCarrier{}

	prop := FilteredBaggagePropagator{}
	ctx := prop.Extract(context.Background(), carrier)

	bag := baggage.FromContext(ctx)
	assert.Empty(t, bag.Members())
}

func TestFilteredBaggagePropagator_InjectUnmodified(t *testing.T) {
	orig := BaggageAllowList
	BaggageAllowList = map[string]bool{}
	defer func() { BaggageAllowList = orig }()

	m1, err := baggage.NewMember("key1", "val1")
	require.NoError(t, err)
	m2, err := baggage.NewMember("key2", "val2")
	require.NoError(t, err)
	bag, err := baggage.New(m1, m2)
	require.NoError(t, err)

	ctx := baggage.ContextWithBaggage(context.Background(), bag)
	carrier := propagation.MapCarrier{}

	prop := FilteredBaggagePropagator{}
	prop.Inject(ctx, carrier)

	raw := carrier.Get("baggage")
	assert.Contains(t, raw, "key1=val1")
	assert.Contains(t, raw, "key2=val2")
}
