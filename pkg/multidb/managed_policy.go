package multidb

import "sync"

type databaseLimitPolicy struct {
	limitsMu                           sync.RWMutex
	limits                             *Limits
	queryRateLimiter, writeRateLimiter *rateLimiter
}

func (p *databaseLimitPolicy) updateLimits(limits *Limits) {
	limits = cloneLimits(limits)
	p.limitsMu.Lock()
	defer p.limitsMu.Unlock()
	p.limits = limits
	p.queryRateLimiter = nil
	p.writeRateLimiter = nil
	if limits != nil {
		if limits.Rate.MaxQueriesPerSecond > 0 {
			p.queryRateLimiter = newRateLimiter(limits.Rate.MaxQueriesPerSecond)
		}
		if limits.Rate.MaxWritesPerSecond > 0 {
			p.writeRateLimiter = newRateLimiter(limits.Rate.MaxWritesPerSecond)
		}
	}
}

func (p *databaseLimitPolicy) snapshot() *Limits {
	if p == nil {
		return nil
	}
	p.limitsMu.RLock()
	defer p.limitsMu.RUnlock()
	return cloneLimits(p.limits)
}

type rateKind uint8

const (
	queryRate rateKind = iota
	writeRate
)

func (p *databaseLimitPolicy) rateSnapshot(kind rateKind) (*Limits, *rateLimiter) {
	if p == nil {
		return nil, nil
	}
	p.limitsMu.RLock()
	defer p.limitsMu.RUnlock()
	limiter := p.queryRateLimiter
	if kind == writeRate {
		limiter = p.writeRateLimiter
	}
	return cloneLimits(p.limits), limiter
}
