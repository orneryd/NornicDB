package auth

import (
	"testing"
	"time"
)

func TestBasicAuthCache_GetSet(t *testing.T) {
	cache := NewBasicAuthCache(4, 50*time.Millisecond)
	claims := &JWTClaims{Sub: "u1", Roles: []string{"admin"}}

	cache.SetAtGeneration("user", "pass", claims, cache.Generation())
	got, ok := cache.Get("user", "pass")
	if !ok {
		t.Fatalf("expected cache hit")
	}
	if got.Sub != "u1" {
		t.Fatalf("got sub %q, want %q", got.Sub, "u1")
	}

	// Ensure a copy is returned.
	got.Roles[0] = "mutated"
	got2, ok := cache.Get("user", "pass")
	if !ok {
		t.Fatalf("expected cache hit")
	}
	if got2.Roles[0] != "admin" {
		t.Fatalf("cache entry was mutated")
	}
}

func TestBasicAuthCache_Expires(t *testing.T) {
	cache := NewBasicAuthCache(4, 20*time.Millisecond)
	cache.SetAtGeneration("user", "pass", &JWTClaims{Sub: "u1"}, cache.Generation())

	time.Sleep(30 * time.Millisecond)
	if _, ok := cache.Get("user", "pass"); ok {
		t.Fatalf("expected cache miss after expiry")
	}
}

// TestAuthCacheKeysUseCanonicalNamespaces pins the key namespaces to the
// canonical authCacheKey owner. The caches have no per-path key helpers, so the
// namespace literals at each call site are the only thing keeping a result
// written through one path from being read through another, and keeping the
// caches' reads and writes on the same key.
func TestAuthCacheKeysUseCanonicalNamespaces(t *testing.T) {
	const shared = "YWxpY2U6cGFzc3dvcmQ="
	const tokenValue = "token-value"
	claims := &JWTClaims{Sub: "u1"}

	headerKey := authCacheKey("auth.basic.header", shared)
	credentialsKey := authCacheKey("auth.basic.credentials", shared)
	tokenKey := authCacheKey("auth.jwt.token", shared)
	if headerKey == credentialsKey || headerKey == tokenKey || credentialsKey == tokenKey {
		t.Fatalf("auth cache namespaces collide: header=%x credentials=%x token=%x", headerKey, credentialsKey, tokenKey)
	}

	cache := NewBasicAuthCache(4, time.Minute)
	cache.SetFromHeader(shared, claims)
	if _, ok := cache.entries[headerKey]; !ok {
		t.Fatal("SetFromHeader did not store under the auth.basic.header namespace")
	}
	if _, ok := cache.GetFromHeader(shared); !ok {
		t.Fatal("GetFromHeader did not read the entry SetFromHeader stored")
	}

	cache.SetAtGeneration("alice", "password", claims, cache.Generation())
	if _, ok := cache.entries[authCacheKey("auth.basic.credentials", "alice", "password")]; !ok {
		t.Fatal("SetAtGeneration did not store under the auth.basic.credentials namespace")
	}
	if _, ok := cache.Get("alice", "password"); !ok {
		t.Fatal("Get did not read the entry SetAtGeneration stored")
	}

	tokens := newTokenCache(4, time.Minute)
	tokens.set(tokenValue, claims)
	if _, ok := tokens.entries[authCacheKey("auth.jwt.token", tokenValue)]; !ok {
		t.Fatal("token cache did not store under the auth.jwt.token namespace")
	}
	if _, ok := tokens.get(tokenValue); !ok {
		t.Fatal("token cache did not read the entry it stored")
	}
}
