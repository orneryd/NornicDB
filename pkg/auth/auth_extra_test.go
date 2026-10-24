package auth

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
)

// ============================================================================
// roleNameFromNodeID
// ============================================================================

func TestRoleNameFromNodeID_WithPrefix(t *testing.T) {
	result := roleNameFromNodeID(rolePrefix + "admin")
	assert.Equal(t, "admin", result)
}

func TestRoleNameFromNodeID_WithoutPrefix(t *testing.T) {
	result := roleNameFromNodeID("nornic:node-123")
	assert.Equal(t, "", result)
}

func TestRoleNameFromNodeID_Empty(t *testing.T) {
	result := roleNameFromNodeID("")
	assert.Equal(t, "", result)
}

// ============================================================================
// Authenticator.IsSecurityEnabled
// ============================================================================

func TestAuthenticator_IsSecurityEnabled_True(t *testing.T) {
	cfg := AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-that-is-long-enough-32chars"),
	}
	eng := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	a, err := NewAuthenticator(cfg, eng)
	if err != nil {
		t.Skip("cannot create authenticator:", err)
	}
	assert.True(t, a.IsSecurityEnabled())
}

func TestAuthenticator_IsSecurityEnabled_False(t *testing.T) {
	cfg := AuthConfig{
		SecurityEnabled: false,
		JWTSecret:       []byte("test-secret-that-is-long-enough-32chars"),
	}
	eng := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	a, err := NewAuthenticator(cfg, eng)
	if err != nil {
		t.Skip("cannot create authenticator:", err)
	}
	assert.False(t, a.IsSecurityEnabled())
}

// ============================================================================
// cloneJWTClaims
// ============================================================================

func TestCloneJWTClaims_Nil(t *testing.T) {
	result := cloneJWTClaims(nil)
	assert.Nil(t, result)
}

func TestCloneJWTClaims_Full(t *testing.T) {
	original := &JWTClaims{
		Sub:      "user-1",
		Username: "alice",
		Email:    "alice@example.com",
		Roles:    []string{"admin", "reader"},
		Iat:      1234567890,
		Exp:      9999999999,
	}
	clone := cloneJWTClaims(original)
	assert.Equal(t, original.Sub, clone.Sub)
	assert.Equal(t, original.Username, clone.Username)
	assert.Equal(t, original.Roles, clone.Roles)
	// Mutating original should not affect clone
	original.Roles[0] = "mutated"
	assert.Equal(t, "admin", clone.Roles[0])
}

func TestCloneJWTClaims_EmptyRoles(t *testing.T) {
	original := &JWTClaims{Sub: "x", Roles: []string{}}
	clone := cloneJWTClaims(original)
	assert.Equal(t, original.Sub, clone.Sub)
	assert.Empty(t, clone.Roles)
}

// ============================================================================
// BasicAuthCache – Set/Get by username/password
// ============================================================================

func TestBasicAuthCache_GetSet_ByCredentials(t *testing.T) {
	c := NewBasicAuthCache(10, DefaultAuthCacheTTL)
	if c == nil {
		t.Skip("cache not created")
	}
	claims := &JWTClaims{Username: "bob", Sub: "bob-id"}
	c.SetAtGeneration("bob", "password123", claims, c.Generation())

	got, ok := c.Get("bob", "password123")
	assert.True(t, ok)
	assert.Equal(t, "bob", got.Username)

	// Wrong password → miss
	_, ok2 := c.Get("bob", "wrongpass")
	assert.False(t, ok2)
}

func TestBasicAuthCache_Get_EmptyCredentials(t *testing.T) {
	c := NewBasicAuthCache(10, DefaultAuthCacheTTL)
	_, ok := c.Get("", "password")
	assert.False(t, ok)
	_, ok2 := c.Get("user", "")
	assert.False(t, ok2)
}

func TestAuthenticator_UserFromNode(t *testing.T) {
	createdAt := time.Unix(100, 0)
	updatedAt := time.Unix(200, 0)
	lastLogin := time.Unix(300, 0)
	lockedUntil := time.Unix(400, 0)

	a := &Authenticator{}
	user, err := a.userFromNode(&storage.Node{
		ID:        storage.NodeID("user:alice"),
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
		Properties: map[string]any{
			"id":            "user-1",
			"username":      "alice",
			"email":         "alice@example.com",
			"password_hash": "hash",
			"roles":         `["admin","viewer"]`,
			"created_at":    createdAt.Unix(),
			"updated_at":    updatedAt.Unix(),
			"last_login":    lastLogin.Unix(),
			"locked_until":  lockedUntil.Unix(),
			"failed_logins": int64(3),
			"disabled":      true,
			"metadata":      `{"team":"graph"}`,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "user-1", user.ID)
	assert.Equal(t, "alice", user.Username)
	assert.Equal(t, "alice@example.com", user.Email)
	assert.Equal(t, "hash", user.PasswordHash)
	assert.Equal(t, []Role{RoleAdmin, RoleViewer}, user.Roles)
	assert.Equal(t, createdAt, user.CreatedAt)
	assert.Equal(t, updatedAt, user.UpdatedAt)
	assert.Equal(t, lastLogin, user.LastLogin)
	assert.Equal(t, lockedUntil, user.LockedUntil)
	assert.Equal(t, 3, user.FailedLogins)
	assert.True(t, user.Disabled)
	assert.Equal(t, map[string]string{"team": "graph"}, user.Metadata)

	fallbackUser, err := a.userFromNode(&storage.Node{
		ID:        storage.NodeID("user:bob"),
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
		Properties: map[string]any{
			"username": "bob",
			"roles":    `not-json`,
			"metadata": `not-json`,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, createdAt, fallbackUser.CreatedAt)
	assert.Equal(t, updatedAt, fallbackUser.UpdatedAt)
	assert.Empty(t, fallbackUser.Roles)
	assert.Empty(t, fallbackUser.Metadata)
}

func TestAuthenticator_LoadUsers(t *testing.T) {
	eng := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer eng.Close()

	_, err := eng.CreateNode(&storage.Node{
		ID:     storage.NodeID("user:alice"),
		Labels: []string{"_User", "_System"},
		Properties: map[string]any{
			"id":            "user-1",
			"username":      "alice",
			"password_hash": "hash",
			"roles":         `["viewer"]`,
			"metadata":      `{"env":"test"}`,
		},
	})
	assert.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{
		ID:     storage.NodeID("system:not-user"),
		Labels: []string{"_System"},
	})
	assert.NoError(t, err)

	a := &Authenticator{storage: eng, users: make(map[string]*User)}
	assert.NoError(t, a.loadUsers())
	if assert.Contains(t, a.users, "alice") {
		assert.Equal(t, []Role{RoleViewer}, a.users["alice"].Roles)
	}
	assert.NotContains(t, a.users, "not-user")
}
