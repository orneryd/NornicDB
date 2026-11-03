package auth

import (
	"context"
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
// repeated bcrypt work on high-volume request paths. It spares the password
// check only: every caller passes a cached result through
// Authenticator.CurrentClaims, so the roles are the account's current ones,
// and every owner registers ForgetUser with Authenticator.OnPasswordChange,
// so a changed password stops working at once.
type BasicAuthCache struct {
	mu         sync.Mutex
	entries    map[[32]byte]basicAuthCacheEntry
	maxEntries int
	ttl        time.Duration
	generation uint64
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
	return c.get(authCacheKey("auth.basic.header", authHeader))
}

// SetFromHeader caches claims using the full Basic auth header at the current
// generation. It preserves the original setter contract; authentication paths
// must use SetFromHeaderAtGeneration with a generation captured before checking
// credentials to reject publication racing a password change.
func (c *BasicAuthCache) SetFromHeader(authHeader string, claims *JWTClaims) {
	c.SetFromHeaderAtGeneration(authHeader, claims, c.Generation())
}

// SetFromHeaderAtGeneration caches claims only if generation, captured before
// authentication, still matches. For example, capture cache.Generation(), check
// credentials, then publish with the captured value so ForgetUser wins a race.
func (c *BasicAuthCache) SetFromHeaderAtGeneration(authHeader string, claims *JWTClaims, generation uint64) {
	if c == nil || authHeader == "" || claims == nil {
		return
	}
	c.set(authCacheKey("auth.basic.header", authHeader), claims, generation)
}

// Get retrieves cached claims using username/password (when header isn't available).
func (c *BasicAuthCache) Get(username, password string) (*JWTClaims, bool) {
	if c == nil || username == "" || password == "" {
		return nil, false
	}
	return c.get(authCacheKey("auth.basic.credentials", username, password))
}

// Set caches claims using username/password at the current generation. It
// preserves the original setter contract; authentication paths must instead use
// SetAtGeneration with a generation captured before checking credentials.
func (c *BasicAuthCache) Set(username, password string, claims *JWTClaims) {
	c.SetAtGeneration(username, password, claims, c.Generation())
}

// SetAtGeneration caches claims only if generation, captured before checking
// credentials, still matches. For example, capture cache.Generation() before
// authenticating and pass it here to prevent a password change from racing
// publication of a successful check of the old password.
func (c *BasicAuthCache) SetAtGeneration(username, password string, claims *JWTClaims, generation uint64) {
	if c == nil || username == "" || password == "" || claims == nil {
		return
	}
	c.set(authCacheKey("auth.basic.credentials", username, password), claims, generation)
}

// Generation captures the cache publication generation before credentials are
// authenticated. SetAtGeneration and SetFromHeaderAtGeneration publish only within that generation.
func (c *BasicAuthCache) Generation() uint64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation
}

// ForgetUser drops every cached result of username, so a password that was
// just changed authenticates no more (Authenticator.OnPasswordChange calls
// it): the next request with it is checked in full.
func (c *BasicAuthCache) ForgetUser(username string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.generation++
	for key, entry := range c.entries {
		if entry.claims != nil && entry.claims.Username == username {
			delete(c.entries, key)
		}
	}
}

// authCacheKey derives the process-local cache key for the supplied namespace
// and parts. It is the canonical owner of every auth cache key: each cache
// supplies its own namespace literal ("auth.basic.header",
// "auth.basic.credentials", "auth.jwt.token"), and the namespace keeps an entry
// written through one path from being read through another.
//
// The auth caches expose no context to their callers (the HTTP, Bolt and gRPC
// credential paths consult them synchronously), so the context-free boundary is
// an explicit context.Background(): these digests cannot be cancelled and their
// errors are not discarded.
func authCacheKey(namespace string, parts ...string) [32]byte {
	key, err := security.KeyedDigest(context.Background(), namespace, parts...)
	if err != nil {
		// Unreachable: a background context is never cancelled and HMAC writes
		// do not fail. Surface it rather than using a zero key, which would
		// collide every cache entry.
		panic("auth: derive cache key: " + err.Error())
	}
	return key
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

func (c *BasicAuthCache) set(key [32]byte, claims *JWTClaims, generation uint64) {
	if c == nil || claims == nil {
		return
	}

	now := time.Now()
	expiresAt := now.Add(c.ttl)

	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.generation {
		return
	}

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

func (c *tokenCache) get(token string) (*JWTClaims, bool) {
	if c == nil || token == "" {
		return nil, false
	}
	now := time.Now()
	key := authCacheKey("auth.jwt.token", token)

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

	c.entries[authCacheKey("auth.jwt.token", token)] = tokenCacheEntry{
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
