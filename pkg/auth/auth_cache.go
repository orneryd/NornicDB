package auth

import (
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/security"
)

const (
	// DefaultAuthCacheEntries is the default max size for auth caches.
	DefaultAuthCacheEntries = 1024
	// DefaultAuthCacheTTL is the default TTL for cached auth results.
	DefaultAuthCacheTTL = 30 * time.Second
)

// BasicAuthCache caches successful Basic authentication results to avoid
// repeated bcrypt work on high-volume request paths.
type BasicAuthCache struct {
	mu         sync.Mutex
	entries    map[[32]byte]basicAuthCacheEntry
	maxEntries int
	ttl        time.Duration
}

type basicAuthCacheEntry struct {
	claims    *JWTClaims
	expiresAt time.Time
}

// NewBasicAuthCache creates a new cache for Basic auth results.
func NewBasicAuthCache(maxEntries int, ttl time.Duration) *BasicAuthCache {
	if maxEntries <= 0 || ttl <= 0 {
		return nil
	}
	return &BasicAuthCache{
		entries:    make(map[[32]byte]basicAuthCacheEntry, maxEntries),
		maxEntries: maxEntries,
		ttl:        ttl,
	}
}

// GetFromHeader retrieves cached claims using the full Basic auth header.
func (c *BasicAuthCache) GetFromHeader(authHeader string) (*JWTClaims, bool) {
	if c == nil || authHeader == "" {
		return nil, false
	}
	return c.get(basicAuthCacheKeyFromHeader(authHeader))
}

// SetFromHeader caches claims using the full Basic auth header.
func (c *BasicAuthCache) SetFromHeader(authHeader string, claims *JWTClaims) {
	if c == nil || authHeader == "" || claims == nil {
		return
	}
	c.set(basicAuthCacheKeyFromHeader(authHeader), claims)
}

// Get retrieves cached claims using username/password (when header isn't available).
func (c *BasicAuthCache) Get(username, password string) (*JWTClaims, bool) {
	if c == nil || username == "" || password == "" {
		return nil, false
	}
	return c.get(basicAuthCacheKeyFromCredentials(username, password))
}

// Set caches claims using username/password (when header isn't available).
func (c *BasicAuthCache) Set(username, password string, claims *JWTClaims) {
	if c == nil || username == "" || password == "" || claims == nil {
		return
	}
	c.set(basicAuthCacheKeyFromCredentials(username, password), claims)
}

func basicAuthCacheKeyFromHeader(authHeader string) [32]byte {
	return security.KeyedDigest("auth.basic.header", authHeader)
}

func basicAuthCacheKeyFromCredentials(username, password string) [32]byte {
	return security.KeyedDigest("auth.basic.credentials", username, password)
}

func (c *BasicAuthCache) get(key [32]byte) (*JWTClaims, bool) {
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	if now.After(entry.expiresAt) {
		delete(c.entries, key)
		return nil, false
	}
	return cloneJWTClaims(entry.claims), true
}

func (c *BasicAuthCache) set(key [32]byte, claims *JWTClaims) {
	if c == nil || claims == nil {
		return
	}

	now := time.Now()
	expiresAt := now.Add(c.ttl)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Opportunistic cleanup of expired entries.
	for k, entry := range c.entries {
		if now.After(entry.expiresAt) {
			delete(c.entries, k)
		}
	}

	// Bound the map size with simple eviction.
	for len(c.entries) >= c.maxEntries {
		for k := range c.entries {
			delete(c.entries, k)
			break
		}
	}

	c.entries[key] = basicAuthCacheEntry{
		claims:    cloneJWTClaims(claims),
		expiresAt: expiresAt,
	}
}

// tokenCache stores validated JWT claims to skip repeated signature verification.
type tokenCache struct {
	mu         sync.Mutex
	entries    map[[32]byte]tokenCacheEntry
	maxEntries int
	ttl        time.Duration
}

type tokenCacheEntry struct {
	claims    *JWTClaims
	expiresAt time.Time
}

func newTokenCache(maxEntries int, ttl time.Duration) *tokenCache {
	if maxEntries <= 0 || ttl <= 0 {
		return nil
	}
	return &tokenCache{
		entries:    make(map[[32]byte]tokenCacheEntry, maxEntries),
		maxEntries: maxEntries,
		ttl:        ttl,
	}
}

func tokenCacheKey(token string) [32]byte {
	return security.KeyedDigest("auth.jwt.token", token)
}

func (c *tokenCache) get(token string) (*JWTClaims, bool) {
	if c == nil || token == "" {
		return nil, false
	}
	now := time.Now()
	key := tokenCacheKey(token)

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	if now.After(entry.expiresAt) {
		delete(c.entries, key)
		return nil, false
	}
	return cloneJWTClaims(entry.claims), true
}

func (c *tokenCache) set(token string, claims *JWTClaims) {
	if c == nil || token == "" || claims == nil {
		return
	}

	now := time.Now()
	expiresAt := now.Add(c.ttl)
	if claims.Exp > 0 {
		exp := time.Unix(claims.Exp, 0)
		if exp.Before(expiresAt) {
			expiresAt = exp
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Opportunistic cleanup of expired entries.
	for k, entry := range c.entries {
		if now.After(entry.expiresAt) {
			delete(c.entries, k)
		}
	}

	// Bound the map size with simple eviction.
	for len(c.entries) >= c.maxEntries {
		for k := range c.entries {
			delete(c.entries, k)
			break
		}
	}

	c.entries[tokenCacheKey(token)] = tokenCacheEntry{
		claims:    cloneJWTClaims(claims),
		expiresAt: expiresAt,
	}
}

func cloneJWTClaims(in *JWTClaims) *JWTClaims {
	if in == nil {
		return nil
	}
	out := *in
	if in.Roles != nil {
		out.Roles = append([]string(nil), in.Roles...)
	}
	return &out
}
