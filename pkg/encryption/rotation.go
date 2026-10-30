package encryption

import (
	"context"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/kms"
)

type RotationConfig struct {
	Enabled        bool
	Interval       time.Duration
	RetentionCount int
}

type Reencryptor interface {
	ReencryptWithDataKey(ctx context.Context, key *kms.DataKey) (updated int, err error)
}

type RotationManager struct {
	provider    kms.KeyProvider
	reencryptor Reencryptor
	config      RotationConfig
	ticker      *time.Ticker
	stopOnce    sync.Once
	stopCh      chan struct{}
}

func NewRotationManager(provider kms.KeyProvider, reencryptor Reencryptor, cfg RotationConfig) *RotationManager {
	if cfg.Interval <= 0 {
		cfg.Interval = 90 * 24 * time.Hour
	}
	if cfg.RetentionCount <= 0 {
		cfg.RetentionCount = 5
	}
	return &RotationManager{
		provider:    provider,
		reencryptor: reencryptor,
		config:      cfg,
		stopCh:      make(chan struct{}),
	}
}

func (rm *RotationManager) Start(ctx context.Context) {
	if !rm.config.Enabled || rm.reencryptor == nil || rm.provider == nil {
		return
	}
	rm.ticker = time.NewTicker(rm.config.Interval)
	go rm.loop(ctx)
}

func (rm *RotationManager) Stop() {
	rm.stopOnce.Do(func() {
		close(rm.stopCh)
		if rm.ticker != nil {
			rm.ticker.Stop()
		}
	})
}

func (rm *RotationManager) loop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-rm.stopCh:
			return
		case <-rm.ticker.C:
			_, _ = rm.performRotation(ctx)
		}
	}
}

func (rm *RotationManager) performRotation(ctx context.Context) (int, error) {
	key, err := rm.provider.GenerateDataKey(ctx, kms.KeyGenOpts{
		Algorithm: "AES-256-GCM",
		TTL:       rm.config.Interval,
		Label:     "rotation",
	})
	if err != nil {
		return 0, err
	}
	return rm.reencryptor.ReencryptWithDataKey(ctx, key)
}
