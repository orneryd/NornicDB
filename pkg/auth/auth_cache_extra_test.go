package auth

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBasicAuthCache_GetFromHeader_NilCache(t *testing.T) {
	var c *BasicAuthCache
	v, ok := c.GetFromHeader("Basic dXNlcjpwYXNz")
	assert.Nil(t, v)
	assert.False(t, ok)
}

func TestBasicAuthCache_GetFromHeader_EmptyHeader(t *testing.T) {
	c := NewBasicAuthCache(10, time.Minute)
	v, ok := c.GetFromHeader("")
	assert.Nil(t, v)
	assert.False(t, ok)
}

func TestBasicAuthCache_SetFromHeader_RoundTrip(t *testing.T) {
	c := NewBasicAuthCache(10, time.Minute)

	claims := &JWTClaims{Username: "alice", Sub: "alice-id"}
	c.SetFromHeaderAtGeneration("Basic YWxpY2U6cGFzcw==", claims, c.Generation())

	got, ok := c.GetFromHeader("Basic YWxpY2U6cGFzcw==")
	assert.True(t, ok)
	assert.Equal(t, "alice", got.Username)
}

func TestBasicAuthCache_SetFromHeader_NilClaims(t *testing.T) {
	c := NewBasicAuthCache(10, time.Minute)
	// Should not panic
	c.SetFromHeaderAtGeneration("Basic header", nil, c.Generation())
	_, ok := c.GetFromHeader("Basic header")
	assert.False(t, ok)
}

func TestBasicAuthCache_SetFromHeader_NilCache(t *testing.T) {
	var c *BasicAuthCache
	// Should not panic
	c.SetFromHeaderAtGeneration("Basic header", &JWTClaims{Username: "bob"}, c.Generation())
}

func TestBasicAuthCache_GetFromHeader_Miss(t *testing.T) {
	c := NewBasicAuthCache(10, time.Minute)
	v, ok := c.GetFromHeader("Basic bm90Y2FjaGVk")
	assert.Nil(t, v)
	assert.False(t, ok)
}

func TestBasicAuthCache_GetFromHeader_Expired(t *testing.T) {
	c := NewBasicAuthCache(10, 1*time.Nanosecond)
	claims := &JWTClaims{Username: "eve"}
	c.SetFromHeaderAtGeneration("Basic ZXZlOnBhc3M=", claims, c.Generation())

	// Let TTL expire
	time.Sleep(2 * time.Millisecond)

	v, ok := c.GetFromHeader("Basic ZXZlOnBhc3M=")
	assert.Nil(t, v)
	assert.False(t, ok)
}

func TestNewBasicAuthCache_InvalidParams(t *testing.T) {
	assert.Nil(t, NewBasicAuthCache(0, time.Minute))
	assert.Nil(t, NewBasicAuthCache(10, 0))
	assert.Nil(t, NewBasicAuthCache(-1, time.Minute))
}

func TestBasicAuthCache_SetCredentialNoopsAndEviction(t *testing.T) {
	c := NewBasicAuthCache(1, time.Minute)
	c.SetAtGeneration("", "password", &JWTClaims{Username: "alice"}, c.Generation())
	c.SetAtGeneration("alice", "", &JWTClaims{Username: "alice"}, c.Generation())
	c.SetAtGeneration("alice", "password", nil, c.Generation())
	_, ok := c.Get("alice", "password")
	assert.False(t, ok)

	c.SetAtGeneration("alice", "password", &JWTClaims{Username: "alice"}, c.Generation())
	c.SetAtGeneration("bob", "password", &JWTClaims{Username: "bob"}, c.Generation())

	_, aliceOK := c.Get("alice", "password")
	bobClaims, bobOK := c.Get("bob", "password")
	assert.True(t, bobOK)
	assert.Equal(t, "bob", bobClaims.Username)
	assert.False(t, aliceOK)
}

func TestTokenCache_RoundTripExpiryAndEviction(t *testing.T) {
	assert.Nil(t, newTokenCache(0, time.Minute))
	assert.Nil(t, newTokenCache(1, 0))

	cache := newTokenCache(1, time.Minute)
	claims, ok := cache.get("")
	assert.Nil(t, claims)
	assert.False(t, ok)

	cache.set("", &JWTClaims{Username: "ignored"})
	cache.set("token-a", nil)
	_, ok = cache.get("token-a")
	assert.False(t, ok)

	cache.set("token-a", &JWTClaims{Username: "alice", Roles: []string{"admin"}, Exp: time.Now().Add(time.Minute).Unix()})
	got, ok := cache.get("token-a")
	assert.True(t, ok)
	assert.Equal(t, "alice", got.Username)
	got.Roles[0] = "mutated"
	got2, ok := cache.get("token-a")
	assert.True(t, ok)
	assert.Equal(t, "admin", got2.Roles[0])

	cache.set("token-b", &JWTClaims{Username: "bob", Exp: time.Now().Add(time.Minute).Unix()})
	_, ok = cache.get("token-a")
	assert.False(t, ok)
	got, ok = cache.get("token-b")
	assert.True(t, ok)
	assert.Equal(t, "bob", got.Username)

	short := newTokenCache(2, time.Millisecond)
	short.set("short-token", &JWTClaims{Username: "eve", Exp: time.Now().Add(time.Minute).Unix()})
	time.Sleep(2 * time.Millisecond)
	_, ok = short.get("short-token")
	assert.False(t, ok)
}
