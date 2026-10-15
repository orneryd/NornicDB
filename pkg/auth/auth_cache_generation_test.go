package auth

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBasicAuthCachePublicationGeneration(t *testing.T) {
	for _, header := range []bool{false, true} {
		t.Run(map[bool]string{false: "credentials", true: "header"}[header], func(t *testing.T) {
			cache := NewBasicAuthCache(4, time.Minute)
			claims := &JWTClaims{Username: "alice"}
			generation := cache.Generation()
			publish := func(version uint64) {
				if header {
					cache.SetFromHeaderAtGeneration("Basic YWxpY2U6b2xk", claims, version)
				} else {
					cache.SetAtGeneration("alice", "old", claims, version)
				}
			}
			lookup := func() bool {
				if header {
					_, ok := cache.GetFromHeader("Basic YWxpY2U6b2xk")
					return ok
				}
				_, ok := cache.Get("alice", "old")
				return ok
			}
			publish(generation)
			require.True(t, lookup())
			cache.ForgetUser("alice")
			require.False(t, lookup())
			publish(generation)
			require.False(t, lookup())
			publish(cache.Generation())
			require.True(t, lookup())
		})
	}
	var disabled *BasicAuthCache
	require.Zero(t, disabled.Generation())
}
