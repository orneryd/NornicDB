// Package multidb provides resource limits for multi-database support.
package multidb

import (
	"time"
)

// Limits holds resource limits for a database.
//
// All limits are optional (0 = unlimited). Limits are enforced at runtime
// to prevent any single database from consuming excessive resources.
//
// Example:
//
//	limits := &Limits{
//		Storage: StorageLimits{
//			MaxNodes: 1000000,
//			MaxEdges: 5000000,
//			MaxBytes: 10 * 1024 * 1024 * 1024, // 10GB
//		},
//		Query: QueryLimits{
//			MaxQueryTime:        60 * time.Second,
//			MaxResults:          10000,
//			MaxConcurrentQueries: 10,
//		},
//		Connection: ConnectionLimits{
//			MaxConnections: 50,
//		},
//		Rate: RateLimits{
//			MaxQueriesPerSecond: 100,
//			MaxWritesPerSecond:  50,
//		},
//	}
type Limits struct {
	Storage    StorageLimits    `json:"storage,omitempty"`
	Query      QueryLimits      `json:"query,omitempty"`
	Connection ConnectionLimits `json:"connection,omitempty"`
	Rate       RateLimits       `json:"rate,omitempty"`
}

// StorageLimits controls storage capacity per database.
//
// All limits are enforced at runtime. When a limit is exceeded, the operation
// fails with a clear error message explaining the quota limits.
//
// MaxBytes Enforcement:
//
// MaxBytes charges the MessagePack size of canonical logical entities. That
// quota measure is tracked incrementally; it is not the physical disk size.
//
// Example:
//
//	limits := StorageLimits{
//		MaxNodes: 1000000,                    // 1 million nodes
//		MaxEdges: 5000000,                    // 5 million edges
//		MaxBytes: 10 * 1024 * 1024 * 1024,   // 10GB
//	}
type StorageLimits struct {
	// MaxNodes is the maximum number of nodes allowed (0 = unlimited).
	// When exceeded, create operations fail with: "has reached max_nodes limit (N/M)"
	MaxNodes int64 `json:"max_nodes,omitempty"`

	// MaxEdges is the maximum number of edges allowed (0 = unlimited).
	// When exceeded, create operations fail with: "has reached max_edges limit (N/M)"
	MaxEdges int64 `json:"max_edges,omitempty"`

	// MaxBytes is the maximum storage size in bytes (0 = unlimited).
	// When exceeded, create operations fail with: "would exceed max_bytes limit
	// (current: X bytes, limit: Y bytes, new entity: Z bytes)"
	//
	// Size is the canonical logical entity's MessagePack length, which can differ
	// from the physical Badger or WAL size.
	MaxBytes int64 `json:"max_bytes,omitempty"`
}

// QueryLimits controls query execution per database.
type QueryLimits struct {
	// MaxQueryTime is the maximum query execution time (0 = unlimited).
	MaxQueryTime time.Duration `json:"max_query_time,omitempty"`

	// MaxResults is the maximum number of results returned (0 = unlimited).
	MaxResults int64 `json:"max_results,omitempty"`

	// MaxConcurrentQueries is the maximum concurrent queries (0 = unlimited).
	MaxConcurrentQueries int `json:"max_concurrent_queries,omitempty"`
}

// GetMaxResults returns the maximum number of results (for interface compatibility).
func (q *QueryLimits) GetMaxResults() int64 {
	if q == nil {
		return 0
	}
	return q.MaxResults
}

// ConnectionLimits controls connection count per database.
type ConnectionLimits struct {
	// MaxConnections is the maximum concurrent connections (0 = unlimited).
	MaxConnections int `json:"max_connections,omitempty"`
}

// RateLimits controls request rate per database.
type RateLimits struct {
	// MaxQueriesPerSecond is the maximum queries per second (0 = unlimited).
	MaxQueriesPerSecond int `json:"max_queries_per_second,omitempty"`

	// MaxWritesPerSecond is the maximum writes per second (0 = unlimited).
	MaxWritesPerSecond int `json:"max_writes_per_second,omitempty"`
}

// DefaultLimits returns default limits (all unlimited).
func DefaultLimits() *Limits {
	return &Limits{
		Storage: StorageLimits{
			MaxNodes: 0, // Unlimited
			MaxEdges: 0, // Unlimited
			MaxBytes: 0, // Unlimited
		},
		Query: QueryLimits{
			MaxQueryTime:         0, // Unlimited
			MaxResults:           0, // Unlimited
			MaxConcurrentQueries: 0, // Unlimited
		},
		Connection: ConnectionLimits{
			MaxConnections: 0, // Unlimited
		},
		Rate: RateLimits{
			MaxQueriesPerSecond: 0, // Unlimited
			MaxWritesPerSecond:  0, // Unlimited
		},
	}
}

// IsUnlimited returns true if all limits are unlimited (default state).
func (l *Limits) IsUnlimited() bool {
	if l == nil {
		return true
	}
	return l.Storage.MaxNodes == 0 &&
		l.Storage.MaxEdges == 0 &&
		l.Storage.MaxBytes == 0 &&
		l.Query.MaxQueryTime == 0 &&
		l.Query.MaxResults == 0 &&
		l.Query.MaxConcurrentQueries == 0 &&
		l.Connection.MaxConnections == 0 &&
		l.Rate.MaxQueriesPerSecond == 0 &&
		l.Rate.MaxWritesPerSecond == 0
}

// GetMaxResults returns the maximum number of results (for interface compatibility).
func (l *Limits) GetMaxResults() int64 {
	if l == nil || l.Query.MaxResults == 0 {
		return 0
	}
	return l.Query.MaxResults
}
