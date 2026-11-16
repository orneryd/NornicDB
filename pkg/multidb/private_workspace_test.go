package multidb

import (
	"encoding/json"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPrivateWorkspaceOwnershipPublishesAndRestoresWithCatalog(t *testing.T) {
	engine := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = engine.Close() })
	manager, err := NewDatabaseManager(t.Context(), engine, nil)
	require.NoError(t, err)
	require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "alex-chats", PrivateOwner: "sub:alex"}))
	require.Equal(t, "sub:alex", manager.PrivateDatabaseOwner("alex-chats"))
	require.Equal(t, "alex-chats", manager.PrivateWorkspace("sub:alex").Name)
	require.ErrorIs(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "another", PrivateOwner: "sub:alex"}), ErrDatabaseExists)
	var submitted DatabaseCreateRequest
	require.NoError(t, json.Unmarshal([]byte(`{"Name":"forged","PrivateOwner":"sub:other"}`), &submitted))
	require.Empty(t, submitted.PrivateOwner)
	restored, err := NewDatabaseManager(t.Context(), engine, nil)
	require.NoError(t, err)
	require.Equal(t, "sub:alex", restored.PrivateDatabaseOwner("alex-chats"))
	require.Equal(t, "alex-chats", restored.PrivateWorkspace("sub:alex").Name)
}
