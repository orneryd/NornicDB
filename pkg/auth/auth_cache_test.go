package auth

import (
	"testing"
	"time"
)

func TestBasicAuthCache_GetSet(t *testing.T) {
	cache := NewBasicAuthCache(4, 50*time.Millisecond)
	claims := &JWTClaims{Sub: "u1", Roles: []string{"admin"}}

	cache.Set("user", "pass", claims, cache.Generation())
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
	cache.Set("user", "pass", &JWTClaims{Sub: "u1"}, cache.Generation())

	time.Sleep(30 * time.Millisecond)
	if _, ok := cache.Get("user", "pass"); ok {
		t.Fatalf("expected cache miss after expiry")
	}
}
