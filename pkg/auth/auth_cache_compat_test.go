package auth

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBasicAuthCacheLegacySetterSignatures(t *testing.T) {
	cache := NewBasicAuthCache(10, time.Minute)
	// Method values preserve the original public source signatures too.
	var set func(string, string, *JWTClaims) = cache.Set
	var setHeader func(string, *JWTClaims) = cache.SetFromHeader
	claims := &JWTClaims{Username: "alice"}
	set("alice", "password", claims)
	got, ok := cache.Get("alice", "password")
	require.True(t, ok)
	require.Equal(t, claims, got)
	setHeader("Basic YWxpY2U6cGFzc3dvcmQ=", claims)
	got, ok = cache.GetFromHeader("Basic YWxpY2U6cGFzc3dvcmQ=")
	require.True(t, ok)
	require.Equal(t, claims, got)
	cache.ForgetUser("alice")
	_, ok = cache.Get("alice", "password")
	require.False(t, ok)
	var absent *BasicAuthCache
	absent.Set("alice", "password", claims)
	absent.SetFromHeader("Basic header", claims)
}

func TestBasicAuthCacheRejectsInflightAuthenticationPublication(t *testing.T) {
	for _, header := range []bool{false, true} {
		t.Run(map[bool]string{false: "credentials", true: "header"}[header], func(t *testing.T) {
			cache := NewBasicAuthCache(10, time.Minute)
			started, finishAuthentication, published := make(chan struct{}), make(chan struct{}), make(chan struct{})
			go func() {
				generation := cache.Generation()
				close(started)
				<-finishAuthentication
				claims := &JWTClaims{Username: "alice"}
				if header {
					cache.SetFromHeaderAtGeneration("Basic YWxpY2U6b2xk", claims, generation)
				} else {
					cache.SetAtGeneration("alice", "old", claims, generation)
				}
				close(published)
			}()
			<-started
			cache.ForgetUser("alice")
			close(finishAuthentication)
			<-published
			_, cached := cache.Get("alice", "old")
			require.False(t, cached, "an old password checked before invalidation must not be published afterward")
		})
	}
}
