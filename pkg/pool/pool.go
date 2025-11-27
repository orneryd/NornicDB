// Package pool provides object pooling for NornicDB to reduce allocations.
//
// Object pooling reuses allocated objects instead of creating new ones,
// reducing GC pressure and improving throughput for high-frequency operations.
//
// Pooled objects:
// - Query results (rows, columns)
// - Node/Edge slices
// - String builders
// - Byte buffers
//
// Usage:
//
//	// Get a slice from pool
//	rows := pool.GetRowSlice()
//	defer pool.PutRowSlice(rows)
//
//	// Use the slice...
//	rows = append(rows, newRow)
package pool

import (
	"sync"
)

// PoolConfig configures object pooling behavior.
type PoolConfig struct {
	// Enabled controls whether pooling is active
	Enabled bool

	// MaxSize limits maximum objects kept in each pool
	MaxSize int
}

var globalConfig = PoolConfig{
	Enabled: true,
	MaxSize: 1000,
}

// Configure sets global pool configuration.
// Should be called early during initialization.
func Configure(config PoolConfig) {
	globalConfig = config

	// Reinitialize pools to ensure New functions are set correctly
	initPools()
}

// initPools reinitializes all pools with their New functions.
func initPools() {
	rowSlicePool = sync.Pool{
		New: func() any {
			return make([][]interface{}, 0, 64)
		},
	}
	nodeSlicePool = sync.Pool{
		New: func() any {
			return make([]*PooledNode, 0, 64)
		},
	}
	stringBuilderPool = sync.Pool{
		New: func() any {
			return &PooledStringBuilder{buf: make([]byte, 0, 256)}
		},
	}
	byteBufferPool = sync.Pool{
		New: func() any {
			return make([]byte, 0, 1024)
		},
	}
	mapPool = sync.Pool{
		New: func() any {
			return make(map[string]interface{}, 8)
		},
	}
	stringSlicePool = sync.Pool{
		New: func() any {
			return make([]string, 0, 16)
		},
	}
	interfaceSlicePool = sync.Pool{
		New: func() any {
			return make([]interface{}, 0, 16)
		},
	}
}

// IsEnabled returns whether pooling is enabled.
func IsEnabled() bool {
	return globalConfig.Enabled
}

// =============================================================================
// Row Slice Pool (for query results)
// =============================================================================

var rowSlicePool = sync.Pool{
	New: func() any {
		// Pre-allocate with reasonable capacity
		return make([][]interface{}, 0, 64)
	},
}

// GetRowSlice returns a row slice from the pool.
// The returned slice has length 0 but may have capacity.
// Call PutRowSlice when done.
func GetRowSlice() [][]interface{} {
	if !globalConfig.Enabled {
		return make([][]interface{}, 0, 64)
	}
	return rowSlicePool.Get().([][]interface{})[:0]
}

// PutRowSlice returns a row slice to the pool.
// The slice is cleared before being pooled.
func PutRowSlice(rows [][]interface{}) {
	if !globalConfig.Enabled {
		return
	}
	// Don't pool very large slices (memory leak prevention)
	if cap(rows) > globalConfig.MaxSize {
		return
	}
	// Clear references to allow GC of row contents
	for i := range rows {
		rows[i] = nil
	}
	rowSlicePool.Put(rows[:0])
}

// =============================================================================
// Node Slice Pool
// =============================================================================

// PooledNode is a minimal node representation for pooling.
type PooledNode struct {
	ID         string
	Labels     []string
	Properties map[string]interface{}
}

var nodeSlicePool = sync.Pool{
	New: func() any {
		return make([]*PooledNode, 0, 64)
	},
}

// GetNodeSlice returns a node slice from the pool.
func GetNodeSlice() []*PooledNode {
	if !globalConfig.Enabled {
		return make([]*PooledNode, 0, 64)
	}
	return nodeSlicePool.Get().([]*PooledNode)[:0]
}

// PutNodeSlice returns a node slice to the pool.
func PutNodeSlice(nodes []*PooledNode) {
	if !globalConfig.Enabled {
		return
	}
	if cap(nodes) > globalConfig.MaxSize {
		return
	}
	for i := range nodes {
		nodes[i] = nil
	}
	nodeSlicePool.Put(nodes[:0])
}

// =============================================================================
// String Builder Pool
// =============================================================================

var stringBuilderPool = sync.Pool{
	New: func() any {
		b := &PooledStringBuilder{
			buf: make([]byte, 0, 256),
		}
		return b
	},
}

// PooledStringBuilder is a poolable string builder.
type PooledStringBuilder struct {
	buf []byte
}

// WriteString appends a string to the builder.
func (b *PooledStringBuilder) WriteString(s string) {
	b.buf = append(b.buf, s...)
}

// WriteByte appends a byte to the builder.
func (b *PooledStringBuilder) WriteByte(c byte) {
	b.buf = append(b.buf, c)
}

// String returns the built string.
func (b *PooledStringBuilder) String() string {
	return string(b.buf)
}

// Len returns current length.
func (b *PooledStringBuilder) Len() int {
	return len(b.buf)
}

// Reset clears the builder for reuse.
func (b *PooledStringBuilder) Reset() {
	b.buf = b.buf[:0]
}

// GetStringBuilder returns a string builder from the pool.
func GetStringBuilder() *PooledStringBuilder {
	if !globalConfig.Enabled {
		return &PooledStringBuilder{buf: make([]byte, 0, 256)}
	}
	b := stringBuilderPool.Get().(*PooledStringBuilder)
	b.Reset()
	return b
}

// PutStringBuilder returns a string builder to the pool.
func PutStringBuilder(b *PooledStringBuilder) {
	if !globalConfig.Enabled || b == nil {
		return
	}
	if cap(b.buf) > 64*1024 { // Don't pool huge buffers
		return
	}
	b.Reset()
	stringBuilderPool.Put(b)
}

// =============================================================================
// Byte Buffer Pool
// =============================================================================

var byteBufferPool = sync.Pool{
	New: func() any {
		return make([]byte, 0, 1024)
	},
}

// GetByteBuffer returns a byte buffer from the pool.
func GetByteBuffer() []byte {
	if !globalConfig.Enabled {
		return make([]byte, 0, 1024)
	}
	return byteBufferPool.Get().([]byte)[:0]
}

// PutByteBuffer returns a byte buffer to the pool.
func PutByteBuffer(buf []byte) {
	if !globalConfig.Enabled {
		return
	}
	if cap(buf) > 1024*1024 { // Don't pool huge buffers (>1MB)
		return
	}
	byteBufferPool.Put(buf[:0])
}

// =============================================================================
// Map Pool (for query parameters, node properties)
// =============================================================================

var mapPool = sync.Pool{
	New: func() any {
		return make(map[string]interface{}, 8)
	},
}

// GetMap returns a map from the pool.
func GetMap() map[string]interface{} {
	if !globalConfig.Enabled {
		return make(map[string]interface{}, 8)
	}
	m := mapPool.Get().(map[string]interface{})
	// Clear existing entries
	for k := range m {
		delete(m, k)
	}
	return m
}

// PutMap returns a map to the pool.
func PutMap(m map[string]interface{}) {
	if !globalConfig.Enabled || m == nil {
		return
	}
	if len(m) > globalConfig.MaxSize {
		return
	}
	// Clear for reuse
	for k := range m {
		delete(m, k)
	}
	mapPool.Put(m)
}

// =============================================================================
// String Slice Pool
// =============================================================================

var stringSlicePool = sync.Pool{
	New: func() any {
		return make([]string, 0, 16)
	},
}

// GetStringSlice returns a string slice from the pool.
func GetStringSlice() []string {
	if !globalConfig.Enabled {
		return make([]string, 0, 16)
	}
	return stringSlicePool.Get().([]string)[:0]
}

// PutStringSlice returns a string slice to the pool.
func PutStringSlice(s []string) {
	if !globalConfig.Enabled {
		return
	}
	if cap(s) > globalConfig.MaxSize {
		return
	}
	stringSlicePool.Put(s[:0])
}

// =============================================================================
// Interface Slice Pool (for query result rows)
// =============================================================================

var interfaceSlicePool = sync.Pool{
	New: func() any {
		return make([]interface{}, 0, 16)
	},
}

// GetInterfaceSlice returns an interface slice from the pool.
func GetInterfaceSlice() []interface{} {
	if !globalConfig.Enabled {
		return make([]interface{}, 0, 16)
	}
	return interfaceSlicePool.Get().([]interface{})[:0]
}

// PutInterfaceSlice returns an interface slice to the pool.
func PutInterfaceSlice(s []interface{}) {
	if !globalConfig.Enabled || s == nil {
		return
	}
	if cap(s) > globalConfig.MaxSize {
		return
	}
	// Clear references
	for i := range s {
		s[i] = nil
	}
	interfaceSlicePool.Put(s[:0])
}
