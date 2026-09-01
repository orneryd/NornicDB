package multidb

import (
	"errors"
	"fmt"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateCompositeDatabase(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Create constituent databases
	err := manager.CreateDatabase("db1")
	require.NoError(t, err)
	err = manager.CreateDatabase("db2")
	require.NoError(t, err)

	// Create composite database
	constituents := []ConstituentRef{
		{DatabaseName: "db1", Alias: "db1", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db2", Alias: "db2", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite1", constituents)
	require.NoError(t, err)

	// Verify composite database exists
	info, err := manager.GetDatabase("composite1")
	require.NoError(t, err)
	assert.Equal(t, "composite1", info.Name)
	assert.Equal(t, "composite", info.Type)
	assert.Equal(t, 2, len(info.Constituents))
	assert.Equal(t, "db1", info.Constituents[0].DatabaseName)
	assert.Equal(t, "db2", info.Constituents[1].DatabaseName)
}

func TestCreateCompositeDatabase_EdgeCases(t *testing.T) {
	t.Run("empty name rejected", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		err := manager.CreateCompositeDatabase("", nil)
		assert.ErrorIs(t, err, ErrInvalidDatabaseName)
	})

	t.Run("remote constituents require uri", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		err := manager.CreateCompositeDatabase("composite1", []ConstituentRef{
			{DatabaseName: "remote_db", Alias: "db1", Type: "remote", AccessMode: "read_write"},
		})
		require.ErrorContains(t, err, "URI cannot be empty")
	})

	t.Run("remote constituents are accepted with uri", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		err := manager.CreateCompositeDatabase("composite1", []ConstituentRef{
			{DatabaseName: "remote_db", Alias: "db1", Type: "remote", AccessMode: "read", URI: "http://remote:7474"},
		})
		require.NoError(t, err)
		assert.True(t, manager.IsCompositeDatabase("composite1"))
	})

	t.Run("remote user_password requires both user and password", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		err := manager.CreateCompositeDatabase("composite1", []ConstituentRef{
			{
				DatabaseName: "remote_db",
				Alias:        "db1",
				Type:         "remote",
				AccessMode:   "read",
				URI:          "http://remote:7474",
				AuthMode:     "user_password",
				User:         "svc",
			},
		})
		require.ErrorContains(t, err, "password cannot be empty")
	})

	t.Run("remote oidc_forwarding rejects explicit user_password fields", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		err := manager.CreateCompositeDatabase("composite1", []ConstituentRef{
			{
				DatabaseName: "remote_db",
				Alias:        "db1",
				Type:         "remote",
				AccessMode:   "read",
				URI:          "http://remote:7474",
				AuthMode:     "oidc_forwarding",
				User:         "svc",
				Password:     "pass",
			},
		})
		require.ErrorContains(t, err, "cannot be set when auth mode is oidc_forwarding")
	})

	t.Run("remote invalid auth mode rejected", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		err := manager.CreateCompositeDatabase("composite1", []ConstituentRef{
			{
				DatabaseName: "remote_db",
				Alias:        "db1",
				Type:         "remote",
				AccessMode:   "read",
				URI:          "http://remote:7474",
				AuthMode:     "token_exchange",
			},
		})
		require.ErrorContains(t, err, "auth mode")
	})

	t.Run("constituent aliases resolve to actual databases", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		require.NoError(t, manager.CreateDatabase("db1"))
		require.NoError(t, manager.CreateAlias("db1_alias", "db1"))

		err := manager.CreateCompositeDatabase("composite1", []ConstituentRef{
			{DatabaseName: "db1_alias", Alias: "alias1", Type: "local", AccessMode: "read"},
		})
		require.NoError(t, err)
		assert.True(t, manager.IsCompositeDatabase("composite1"))
	})
}

func TestConstituentRefValidate_RemoteAuthModes(t *testing.T) {
	tests := []struct {
		name        string
		ref         ConstituentRef
		errContains string
		messageID   localization.MessageID
	}{
		{
			name: "empty alias rejected",
			ref: ConstituentRef{
				DatabaseName: "db",
				Type:         "local",
				AccessMode:   "read",
			},
			errContains: "alias cannot be empty",
			messageID:   localization.MessageMultidbConstituentAliasRequired,
		},
		{
			name: "empty database name rejected",
			ref: ConstituentRef{
				Alias:      "a",
				Type:       "local",
				AccessMode: "read",
			},
			errContains: "database name cannot be empty",
			messageID:   localization.MessageMultidbConstituentDatabaseRequired,
		},
		{
			name: "invalid constituent type rejected",
			ref: ConstituentRef{
				Alias:        "a",
				DatabaseName: "db",
				Type:         "external",
				AccessMode:   "read",
			},
			errContains: "type must be 'local' or 'remote'",
			messageID:   localization.MessageMultidbConstituentTypeInvalid,
		},
		{
			name: "invalid access mode rejected",
			ref: ConstituentRef{
				Alias:        "a",
				DatabaseName: "db",
				Type:         "local",
				AccessMode:   "admin",
			},
			errContains: "access mode must be",
			messageID:   localization.MessageMultidbAccessModeInvalid,
		},
		{
			name: "remote requires uri",
			ref: ConstituentRef{
				Alias:        "a",
				DatabaseName: "db",
				Type:         "remote",
				AccessMode:   "read",
			},
			errContains: "URI cannot be empty",
			messageID:   localization.MessageMultidbRemoteURIRequired,
		},
		{
			name: "remote defaults to oidc forwarding when mode omitted",
			ref: ConstituentRef{
				Alias:        "r1",
				DatabaseName: "db",
				Type:         "remote",
				AccessMode:   "read",
				URI:          "https://remote.example/nornic-db",
			},
		},
		{
			name: "remote rejects invalid auth mode",
			ref: ConstituentRef{
				Alias:        "r1",
				DatabaseName: "db",
				Type:         "remote",
				AccessMode:   "read",
				URI:          "https://remote.example/nornic-db",
				AuthMode:     "bad_mode",
			},
			errContains: "auth mode",
			messageID:   localization.MessageMultidbRemoteAuthModeInvalid,
		},
		{
			name: "user_password requires user",
			ref: ConstituentRef{
				Alias:        "r1",
				DatabaseName: "db",
				Type:         "remote",
				AccessMode:   "read",
				URI:          "https://remote.example/nornic-db",
				AuthMode:     "user_password",
				Password:     "pass",
			},
			errContains: "user cannot be empty",
			messageID:   localization.MessageMultidbRemoteUserRequired,
		},
		{
			name: "user_password requires password",
			ref: ConstituentRef{
				Alias:        "r1",
				DatabaseName: "db",
				Type:         "remote",
				AccessMode:   "read",
				URI:          "https://remote.example/nornic-db",
				AuthMode:     "user_password",
				User:         "svc",
			},
			errContains: "password cannot be empty",
			messageID:   localization.MessageMultidbRemotePasswordRequired,
		},
		{
			name: "oidc forwarding rejects user/password",
			ref: ConstituentRef{
				Alias:        "r1",
				DatabaseName: "db",
				Type:         "remote",
				AccessMode:   "read",
				URI:          "https://remote.example/nornic-db",
				AuthMode:     "oidc_forwarding",
				User:         "svc",
				Password:     "pass",
			},
			errContains: "cannot be set when auth mode is oidc_forwarding",
			messageID:   localization.MessageMultidbOIDCCredentialsForbidden,
		},
		{
			name: "user_password valid",
			ref: ConstituentRef{
				Alias:        "r1",
				DatabaseName: "db",
				Type:         "remote",
				AccessMode:   "read",
				URI:          "https://remote.example/nornic-db",
				AuthMode:     "user_password",
				User:         "svc",
				Password:     "pass",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.ref.Validate()
			if tc.errContains == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errContains)
			var localizedErr *nornicerrors.Localized
			require.ErrorAs(t, err, &localizedErr)
			require.Equal(t, tc.messageID, localizedErr.Message.ID)
		})
	}
}

func TestGetStorageWithAuth_ForwardsTokenToRemoteFactory(t *testing.T) {
	manager, _ := setupTestManager(t)

	var capturedToken string
	manager.remoteEngineFactory = func(ref ConstituentRef, authToken string) (storage.Engine, error) {
		capturedToken = authToken
		return storage.NewMemoryEngine(), nil
	}

	err := manager.CreateCompositeDatabase("composite_remote", []ConstituentRef{
		{
			DatabaseName: "remote_db",
			Alias:        "r1",
			Type:         "remote",
			AccessMode:   "read",
			URI:          "http://remote.example:7474",
		},
	})
	require.NoError(t, err)

	engine, err := manager.GetStorageWithAuth("composite_remote", "Bearer svc-principal-token")
	require.NoError(t, err)
	require.NotNil(t, engine)

	// Trigger at least one routed read through the composite wrapper.
	_, err = engine.AllNodes()
	require.NoError(t, err)
	require.Equal(t, "Bearer svc-principal-token", capturedToken)
}

func TestGetStorageWithAuth_RemoteFactoryErrorPaths(t *testing.T) {
	t.Run("missing remote factory", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		require.NoError(t, manager.CreateCompositeDatabase("composite_remote", []ConstituentRef{
			{DatabaseName: "remote_db", Alias: "r1", Type: "remote", AccessMode: "read", URI: "http://remote.example"},
		}))

		_, err := manager.GetStorageWithAuth("composite_remote", "Bearer token")
		require.Error(t, err)
		require.Contains(t, err.Error(), "remote engine factory is not configured")
	})

	t.Run("remote factory returns error", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		require.NoError(t, manager.CreateCompositeDatabase("composite_remote", []ConstituentRef{
			{DatabaseName: "remote_db", Alias: "r1", Type: "remote", AccessMode: "read", URI: "http://remote.example"},
		}))
		manager.remoteEngineFactory = func(_ ConstituentRef, _ string) (storage.Engine, error) {
			return nil, fmt.Errorf("dial failed")
		}

		_, err := manager.GetStorageWithAuth("composite_remote", "Bearer token")
		require.Error(t, err)
		require.Contains(t, err.Error(), "dial failed")
	})

	t.Run("remote factory returns nil engine", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		require.NoError(t, manager.CreateCompositeDatabase("composite_remote", []ConstituentRef{
			{DatabaseName: "remote_db", Alias: "r1", Type: "remote", AccessMode: "read", URI: "http://remote.example"},
		}))
		manager.remoteEngineFactory = func(_ ConstituentRef, _ string) (storage.Engine, error) {
			return nil, nil
		}

		_, err := manager.GetStorageWithAuth("composite_remote", "Bearer token")
		require.Error(t, err)
		require.Contains(t, err.Error(), "returned nil")
	})
}

func TestCreateCompositeDatabase_InvalidConstituent(t *testing.T) {
	manager, _ := setupTestManager(t)

	t.Run("validation preserves constituent index", func(t *testing.T) {
		err := manager.CreateCompositeDatabase("invalid_config", []ConstituentRef{{DatabaseName: "db", Type: "local", AccessMode: "read"}})
		require.EqualError(t, err, "invalid constituent at index 0: constituent alias cannot be empty")
		var localizedErr *nornicerrors.Localized
		require.ErrorAs(t, err, &localizedErr)
		require.Equal(t, localization.MessageMultidbInvalidConstituent, localizedErr.Message.ID)
		require.Equal(t, localization.MessageMultidbConstituentAliasRequired, errors.Unwrap(err).(*nornicerrors.Localized).Message.ID)
	})

	// Try to create composite with non-existent constituent
	constituents := []ConstituentRef{
		{DatabaseName: "nonexistent", Alias: "db1", Type: "local", AccessMode: "read_write"},
	}
	err := manager.CreateCompositeDatabase("composite1", constituents)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestCreateCompositeDatabase_DuplicateAlias(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Create constituent databases
	err := manager.CreateDatabase("db1")
	require.NoError(t, err)
	err = manager.CreateDatabase("db2")
	require.NoError(t, err)

	// Try to create composite with duplicate alias
	constituents := []ConstituentRef{
		{DatabaseName: "db1", Alias: "alias1", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db2", Alias: "alias1", Type: "local", AccessMode: "read_write"}, // Duplicate alias
	}
	err = manager.CreateCompositeDatabase("composite1", constituents)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate")
}

func TestCreateCompositeDatabase_CompositeAsConstituent(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Create constituent databases
	err := manager.CreateDatabase("db1")
	require.NoError(t, err)
	err = manager.CreateDatabase("db2")
	require.NoError(t, err)

	// Create first composite
	constituents1 := []ConstituentRef{
		{DatabaseName: "db1", Alias: "db1", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db2", Alias: "db2", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite1", constituents1)
	require.NoError(t, err)

	// Try to use composite as constituent
	constituents2 := []ConstituentRef{
		{DatabaseName: "composite1", Alias: "comp1", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite2", constituents2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot use composite database")
}

func TestDropCompositeDatabase(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Create constituent databases
	err := manager.CreateDatabase("db1")
	require.NoError(t, err)
	err = manager.CreateDatabase("db2")
	require.NoError(t, err)

	// Create composite database
	constituents := []ConstituentRef{
		{DatabaseName: "db1", Alias: "db1", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db2", Alias: "db2", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite1", constituents)
	require.NoError(t, err)

	// Drop composite database
	err = manager.DropCompositeDatabase("composite1")
	require.NoError(t, err)

	// Verify composite database is gone
	_, err = manager.GetDatabase("composite1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Verify constituent databases still exist
	_, err = manager.GetDatabase("db1")
	require.NoError(t, err)
	_, err = manager.GetDatabase("db2")
	require.NoError(t, err)
}

func TestDropCompositeDatabase_Errors(t *testing.T) {
	manager, dbName := setupTestManager(t)

	err := manager.DropCompositeDatabase("missing")
	assert.ErrorIs(t, err, ErrDatabaseNotFound)

	err = manager.DropCompositeDatabase(dbName)
	require.ErrorContains(t, err, "not a composite database")
}

func TestAddConstituent(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Create constituent databases
	err := manager.CreateDatabase("db1")
	require.NoError(t, err)
	err = manager.CreateDatabase("db2")
	require.NoError(t, err)
	err = manager.CreateDatabase("db3")
	require.NoError(t, err)

	// Create composite database with 2 constituents
	constituents := []ConstituentRef{
		{DatabaseName: "db1", Alias: "db1", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db2", Alias: "db2", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite1", constituents)
	require.NoError(t, err)

	// Add third constituent
	newConstituent := ConstituentRef{DatabaseName: "db3", Alias: "db3", Type: "local", AccessMode: "read_write"}
	err = manager.AddConstituent("composite1", newConstituent)
	require.NoError(t, err)

	// Verify constituent was added
	info, err := manager.GetDatabase("composite1")
	require.NoError(t, err)
	assert.Equal(t, 3, len(info.Constituents))

	// Verify new constituent is in the list
	found := false
	for _, c := range info.Constituents {
		if c.DatabaseName == "db3" {
			found = true
			break
		}
	}
	assert.True(t, found, "db3 should be in constituents list")
}

func TestAddConstituent_DuplicateAlias(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Create constituent databases
	err := manager.CreateDatabase("db1")
	require.NoError(t, err)
	err = manager.CreateDatabase("db2")
	require.NoError(t, err)
	err = manager.CreateDatabase("db3")
	require.NoError(t, err)

	// Create composite database
	constituents := []ConstituentRef{
		{DatabaseName: "db1", Alias: "alias1", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db2", Alias: "alias2", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite1", constituents)
	require.NoError(t, err)

	// Try to add constituent with duplicate alias
	newConstituent := ConstituentRef{DatabaseName: "db3", Alias: "alias1", Type: "local", AccessMode: "read_write"}
	err = manager.AddConstituent("composite1", newConstituent)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestAddConstituent_Errors(t *testing.T) {
	manager, dbName := setupTestManager(t)
	require.NoError(t, manager.CreateDatabase("db1"))

	err := manager.AddConstituent("missing", ConstituentRef{
		DatabaseName: "db1",
		Alias:        "db1",
		Type:         "local",
		AccessMode:   "read_write",
	})
	assert.ErrorIs(t, err, ErrDatabaseNotFound)

	err = manager.AddConstituent(dbName, ConstituentRef{
		DatabaseName: "db1",
		Alias:        "db1",
		Type:         "local",
		AccessMode:   "read_write",
	})
	require.ErrorContains(t, err, "not a composite database")

	require.NoError(t, manager.CreateDatabase("db2"))
	require.NoError(t, manager.CreateCompositeDatabase("composite1", []ConstituentRef{
		{DatabaseName: "db1", Alias: "db1", Type: "local", AccessMode: "read_write"},
	}))

	err = manager.AddConstituent("composite1", ConstituentRef{
		DatabaseName: "db2",
		Alias:        "",
		Type:         "local",
		AccessMode:   "read_write",
	})
	require.ErrorContains(t, err, "alias cannot be empty")

	err = manager.AddConstituent("composite1", ConstituentRef{
		DatabaseName: "missing",
		Alias:        "missing",
		Type:         "local",
		AccessMode:   "read_write",
	})
	require.ErrorContains(t, err, "not found")
}

func TestRemoveConstituent(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Create constituent databases
	err := manager.CreateDatabase("db1")
	require.NoError(t, err)
	err = manager.CreateDatabase("db2")
	require.NoError(t, err)
	err = manager.CreateDatabase("db3")
	require.NoError(t, err)

	// Create composite database with 3 constituents
	constituents := []ConstituentRef{
		{DatabaseName: "db1", Alias: "db1", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db2", Alias: "db2", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db3", Alias: "db3", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite1", constituents)
	require.NoError(t, err)

	// Remove constituent
	err = manager.RemoveConstituent("composite1", "db2")
	require.NoError(t, err)

	// Verify constituent was removed
	info, err := manager.GetDatabase("composite1")
	require.NoError(t, err)
	assert.Equal(t, 2, len(info.Constituents))

	// Verify db2 is not in the list
	for _, c := range info.Constituents {
		assert.NotEqual(t, "db2", c.DatabaseName, "db2 should not be in constituents list")
	}
}

func TestRemoveConstituent_NotFound(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Create constituent databases
	err := manager.CreateDatabase("db1")
	require.NoError(t, err)
	err = manager.CreateDatabase("db2")
	require.NoError(t, err)

	// Create composite database
	constituents := []ConstituentRef{
		{DatabaseName: "db1", Alias: "db1", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db2", Alias: "db2", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite1", constituents)
	require.NoError(t, err)

	// Try to remove non-existent constituent
	err = manager.RemoveConstituent("composite1", "nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestRemoveConstituent_NotComposite(t *testing.T) {
	manager, dbName := setupTestManager(t)
	err := manager.RemoveConstituent(dbName, "anything")
	require.ErrorContains(t, err, "not a composite database")
}

func TestListConstituents(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Create constituent databases
	err := manager.CreateDatabase("db1")
	require.NoError(t, err)
	err = manager.CreateDatabase("db2")
	require.NoError(t, err)
	err = manager.CreateDatabase("db3")
	require.NoError(t, err)

	// Create composite database
	constituents := []ConstituentRef{
		{DatabaseName: "db1", Alias: "alias1", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db2", Alias: "alias2", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db3", Alias: "alias3", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite1", constituents)
	require.NoError(t, err)

	// List constituents
	list, err := manager.GetCompositeConstituents("composite1")
	require.NoError(t, err)
	assert.Equal(t, 3, len(list))

	// Verify all constituents are in the list
	constituentMap := make(map[string]bool)
	for _, c := range list {
		constituentMap[c.DatabaseName] = true
	}
	assert.True(t, constituentMap["db1"])
	assert.True(t, constituentMap["db2"])
	assert.True(t, constituentMap["db3"])
}

func TestListConstituents_NotFound(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Try to list constituents for non-existent composite
	_, err := manager.GetCompositeConstituents("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestGetCompositeConstituents_CopyAndTypeChecks(t *testing.T) {
	manager, dbName := setupTestManager(t)
	require.NoError(t, manager.CreateDatabase("db1"))
	require.NoError(t, manager.CreateCompositeDatabase("composite1", []ConstituentRef{
		{DatabaseName: "db1", Alias: "alias1", Type: "local", AccessMode: "read_write"},
	}))

	_, err := manager.GetCompositeConstituents(dbName)
	require.ErrorContains(t, err, "not a composite database")

	list, err := manager.GetCompositeConstituents("composite1")
	require.NoError(t, err)
	require.Len(t, list, 1)
	list[0].Alias = "mutated"

	listAgain, err := manager.GetCompositeConstituents("composite1")
	require.NoError(t, err)
	assert.Equal(t, "alias1", listAgain[0].Alias)
}

func TestConstituentRef_Validate(t *testing.T) {
	tests := []struct {
		name      string
		ref       ConstituentRef
		wantError bool
	}{
		{
			name:      "valid with database name and alias",
			ref:       ConstituentRef{DatabaseName: "db1", Alias: "alias1", Type: "local", AccessMode: "read_write"},
			wantError: false,
		},
		{
			name:      "invalid empty alias",
			ref:       ConstituentRef{DatabaseName: "db1", Alias: "", Type: "local", AccessMode: "read_write"},
			wantError: true,
		},
		{
			name:      "invalid empty database name",
			ref:       ConstituentRef{DatabaseName: "", Alias: "alias1", Type: "local", AccessMode: "read_write"},
			wantError: true,
		},
		{
			name:      "invalid type",
			ref:       ConstituentRef{DatabaseName: "db1", Alias: "alias1", Type: "invalid", AccessMode: "read_write"},
			wantError: true,
		},
		{
			name:      "invalid access mode",
			ref:       ConstituentRef{DatabaseName: "db1", Alias: "alias1", Type: "local", AccessMode: "invalid"},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.ref.Validate()
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestListCompositeDatabases(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Create constituent databases
	err := manager.CreateDatabase("db1")
	require.NoError(t, err)
	err = manager.CreateDatabase("db2")
	require.NoError(t, err)
	err = manager.CreateDatabase("db3")
	require.NoError(t, err)

	// Create composite databases
	constituents1 := []ConstituentRef{
		{DatabaseName: "db1", Alias: "db1", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db2", Alias: "db2", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite1", constituents1)
	require.NoError(t, err)

	constituents2 := []ConstituentRef{
		{DatabaseName: "db3", Alias: "db3", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite2", constituents2)
	require.NoError(t, err)

	// List composite databases
	composites := manager.ListCompositeDatabases()
	assert.Equal(t, 2, len(composites))

	// Verify both composites are in the list
	compositeMap := make(map[string]bool)
	for _, comp := range composites {
		compositeMap[comp.Name] = true
	}
	assert.True(t, compositeMap["composite1"])
	assert.True(t, compositeMap["composite2"])
}

func TestIsCompositeDatabase(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Create constituent databases
	err := manager.CreateDatabase("db1")
	require.NoError(t, err)
	err = manager.CreateDatabase("db2")
	require.NoError(t, err)

	// Create standard database
	err = manager.CreateDatabase("standard_db")
	require.NoError(t, err)

	// Create composite database
	constituents := []ConstituentRef{
		{DatabaseName: "db1", Alias: "db1", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db2", Alias: "db2", Type: "local", AccessMode: "read_write"},
	}
	err = manager.CreateCompositeDatabase("composite_db", constituents)
	require.NoError(t, err)

	// Check if composite
	assert.True(t, manager.IsCompositeDatabase("composite_db"))
	assert.False(t, manager.IsCompositeDatabase("standard_db"))
	assert.False(t, manager.IsCompositeDatabase("nonexistent"))
}
