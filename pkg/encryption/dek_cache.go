package encryption

import (
	"context"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/kms"
)

type dekCache struct {
	mu       sync.RWMutex
	current  *kms.DataKey
	maxAge   time.Duration
	provider kms.KeyProvider
	label    string
}

func newDEKCache(provider kms.KeyProvider, maxAge time.Duration, label string) *dekCache {
	if maxAge <= 0 {
		maxAge = 24 * time.Hour
	}
	return &dekCache{
		maxAge:   maxAge,
		provider: provider,
		label:    label,
	}
}

func (c *dekCache) GetOrGenerate(ctx context.Context) (*kms.DataKey, error) {
	c.mu.RLock()
	cur := c.current
	c.mu.RUnlock()
	if cur != nil {
		if !cur.ExpiresAt.IsZero() && time.Now().UTC().Before(cur.ExpiresAt) {
			return cur, nil
		}
		if cur.ExpiresAt.IsZero() && time.Since(cur.CreatedAt) < c.maxAge {
			return cur, nil
		}
	}

	dek, err := c.provider.GenerateDataKey(ctx, kms.KeyGenOpts{
		Algorithm: "AES-256-GCM",
		TTL:       c.maxAge,
		Label:     c.label,
	})
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.current = dek
	c.mu.Unlock()
	return dek, nil
}
