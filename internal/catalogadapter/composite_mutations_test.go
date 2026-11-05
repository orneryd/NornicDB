package catalogadapter

import (
	"context"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestConstituentInputConversion(t *testing.T) {
	ref := multidb.ConstituentRef{
		Alias: " alias ", DatabaseName: " db ", Type: "remote", AccessMode: "read",
		URI: "http://synthetic.invalid/?token= kept ", SecretRef: " key ",
		AuthMode: "user_password", User: " user ", Password: " password ",
	}
	for _, input := range []interface{}{ref, map[string]interface{}{
		"alias": ref.Alias, "database_name": ref.DatabaseName, "type": ref.Type,
		"access_mode": ref.AccessMode, "uri": ref.URI, "secret_ref": ref.SecretRef,
		"auth_mode": ref.AuthMode, "user": ref.User, "password": ref.Password,
	}} {
		got, ok := ConstituentRef(input, NewConstituentInput)
		require.True(t, ok)
		require.Equal(t, ref, got)
	}
	for _, input := range []interface{}{nil, 123, &ref} {
		_, ok := ConstituentRef(input, NewConstituentInput)
		require.False(t, ok)
	}
	for _, purpose := range []ConstituentPurpose{NewConstituentInput, StoredConstituentRecord} {
		stored := ref
		stored.ConnectionV1 = "sealed-catalog-record"
		got, ok := ConstituentRef(stored, purpose)
		require.True(t, ok)
		require.Equal(t, stored, got, "typed references retain their existing contract")
		got, ok = ConstituentRef(map[string]interface{}{"alias": ref.Alias, "connection_v1": stored.ConnectionV1}, purpose)
		require.True(t, ok)
		require.Equal(t, ref.Alias, got.Alias)
		if purpose == StoredConstituentRecord {
			require.Equal(t, stored.ConnectionV1, got.ConnectionV1)
		} else {
			require.Empty(t, got.ConnectionV1, "mutation maps cannot supply catalog-owned sealed state")
		}
	}
	for _, tc := range []struct {
		value map[string]interface{}
		key   string
		want  string
	}{
		{map[string]interface{}{"key": "value"}, "key", "value"},
		{map[string]interface{}{"key": 42}, "key", ""},
		{map[string]interface{}{}, "missing", ""},
	} {
		require.Equal(t, tc.want, stringField(tc.value, tc.key))
	}
}

func TestCatalogRetainConversionAndPublication(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	manager, err := multidb.NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	adapter := Catalog{Manager: manager}
	ref := multidb.ConstituentRef{Alias: "first", DatabaseName: "nornic", Type: "local", AccessMode: "read"}
	require.EqualError(t, adapter.CreateCompositeDatabase(t.Context(), "invalid", []interface{}{ref, 123}), "invalid constituent type at index 1")
	require.False(t, manager.Exists("invalid"))
	require.NoError(t, adapter.CreateCompositeDatabase(t.Context(), "catalog", []interface{}{map[string]interface{}{
		"alias": ref.Alias, "database_name": ref.DatabaseName, "type": ref.Type, "access_mode": ref.AccessMode,
		"connection_v1": "untrusted-create-map",
	}}))
	require.NoError(t, adapter.AddConstituent(t.Context(), "catalog", map[string]interface{}{
		"alias": "second", "database_name": "nornic", "type": "local", "access_mode": "read_write",
		"connection_v1": "untrusted-add-map",
	}))
	require.EqualError(t, adapter.AddConstituent(t.Context(), "catalog", 123), "invalid constituent type")
	refs, err := manager.GetCompositeConstituents("catalog")
	require.NoError(t, err)
	require.Equal(t, []multidb.ConstituentRef{ref, {Alias: "second", DatabaseName: "nornic", Type: "local", AccessMode: "read_write"}}, refs)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, adapter.CreateCompositeDatabase(ctx, "canceled", []interface{}{ref}), context.Canceled)
	require.False(t, manager.Exists("canceled"))
	require.ErrorIs(t, adapter.AddConstituent(ctx, "catalog", multidb.ConstituentRef{Alias: "third", DatabaseName: "nornic", Type: "local", AccessMode: "read"}), context.Canceled)
	refs, err = manager.GetCompositeConstituents("catalog")
	require.NoError(t, err)
	require.Len(t, refs, 2)
}
