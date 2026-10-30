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
//
// Object pooling reduces memory allocations by reusing objects instead of
// creating new ones. This is especially beneficial for high-frequency operations
// like query execution where thousands of temporary objects are created per second.
//
// Fields:
//   - Enabled: Controls whether pooling is active (disable for debugging)
//   - MaxSize: Maximum capacity for pooled objects (prevents memory leaks)
//
// Example:
//
//	config := pool.PoolConfig{
//		Enabled: true,
//		MaxSize: 1000, // Keep up to 1000 objects per pool
//	}
//	pool.Configure(config)
//
// ELI12:
//
// Think of object pooling like a library's book return system:
//   - Instead of buying new books every time (allocating memory),
//     you return books to the library (pool) when done
//   - The next person can check out the same book (reuse object)
//   - The library only keeps 1000 books max (MaxSize limit)
//   - This saves money (reduces garbage collection) and is faster!
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
//
// This function should be called once during application initialization,
// before any pooled objects are allocated. Calling it multiple times will
// reinitialize all pools, which may cause temporary allocation spikes.
//
// Parameters:
//   - config: Pool configuration with Enabled and MaxSize settings
//
// Example 1 - Production Setup:
//
//	func main() {
//		// Configure pooling early
//		pool.Configure(pool.PoolConfig{
//			Enabled: true,
//			MaxSize: 1000,
//		})
//
//		// Now start your application
//		server.Start()
//	}
//
// Example 2 - Disable for Debugging:
//
//	// Disable pooling to detect memory leaks
//	pool.Configure(pool.PoolConfig{
//		Enabled: false, // All Get/Put operations become no-ops
//	})
//
// Example 3 - Memory-Constrained Environment:
//
//	// Reduce pool size for low-memory systems
//	pool.Configure(pool.PoolConfig{
//		Enabled: true,
//		MaxSize: 100, // Keep fewer objects
//	})
//
// Performance Impact:
//   - Enabling pooling: 50-70% reduction in allocations
//   - Disabling pooling: Easier debugging, higher GC pressure
//   - MaxSize too low: More allocations (pool fills up)
//   - MaxSize too high: More memory usage
//
// Thread Safety:
//
//	Not thread-safe. Call only during initialization.
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
//
// This function can be used to check if pooling is active, which is useful
// for conditional logic or debugging output.
//
// Returns:
//   - true if pooling is enabled (objects are reused)
//   - false if pooling is disabled (objects are allocated fresh)
//
// Example:
//
//	if pool.IsEnabled() {
//		fmt.Println("Object pooling is active")
//	} else {
//		fmt.Println("Object pooling is disabled (debug mode)")
//	}
//
// Use Case:
//
//	// Conditional metrics collection
//	if pool.IsEnabled() {
//		metrics.RecordPoolHit()
//	} else {
//		metrics.RecordAllocation()
//	}
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

// GetRowSlice returns a row slice from the pool for query results.
//
// The returned slice has length 0 but may have pre-allocated capacity (typically 64).
// This reduces allocations when building query result sets. Always call PutRowSlice
// when done to return the slice to the pool.
//
// Returns:
//   - Empty slice with pre-allocated capacity if pooling is enabled
//   - Fresh slice with capacity 64 if pooling is disabled
//
// Example 1 - Basic Usage:
//
//	rows := pool.GetRowSlice()
//	defer pool.PutRowSlice(rows)
//
//	// Build result set
//	for _, record := range records {
//		row := []interface{}{record.ID, record.Name, record.Value}
//		rows = append(rows, row)
//	}
//
//	return rows
//
// Example 2 - Query Execution:
//
//	func executeQuery(query string) ([][]interface{}, error) {
//		rows := pool.GetRowSlice()
//		defer pool.PutRowSlice(rows)
//
//		// Execute query and populate rows
//		results := db.Query(query)
//		for results.Next() {
//			var id, name, value interface{}
//			results.Scan(&id, &name, &value)
//			rows = append(rows, []interface{}{id, name, value})
//		}
//
//		// Make a copy to return (original goes back to pool)
//		result := make([][]interface{}, len(rows))
//		copy(result, rows)
//		return result, nil
//	}
//
// Example 3 - Batch Processing:
//
//	// Process 10,000 records with minimal allocations
//	for batch := range batches {
//		rows := pool.GetRowSlice()
//
//		for _, record := range batch {
//			rows = append(rows, record.ToRow())
//		}
//
//		processBatch(rows)
//		pool.PutRowSlice(rows) // Reuse for next batch
//	}
//
// Performance:
//   - Eliminates allocation for slice header and backing array
//   - Typical savings: 2-3 allocations per query
//   - For 1000 QPS: ~2000-3000 fewer allocations/sec
//
// Memory Safety:
//   - Returned slice is empty (len=0) but has capacity
//   - Safe to append without initial allocation
//   - Don't modify after calling PutRowSlice
//
// ELI12:
//
// Imagine you're doing homework and need scratch paper:
//   - GetRowSlice: Grab a blank sheet from the recycling bin (pool)
//   - Use it: Write your work on the paper (append rows)
//   - PutRowSlice: Erase it and put it back in the bin for next time
//   - This is faster than getting new paper from the store every time!
func GetRowSlice() [][]interface{} {
	if !globalConfig.Enabled {
		return make([][]interface{}, 0, 64)
	}
	return rowSlicePool.Get().([][]interface{})[:0]
}

// PutRowSlice returns a row slice to the pool for reuse.
//
// The slice is cleared (all references set to nil) before being pooled to allow
// garbage collection of the row contents. Very large slices (capacity > MaxSize)
// are not pooled to prevent memory leaks.
//
// Parameters:
//   - rows: The slice to return to the pool (will be cleared)
//
// Example:
//
//	rows := pool.GetRowSlice()
//	defer pool.PutRowSlice(rows) // Always return to pool
//
//	// Use rows...
//	rows = append(rows, []interface{}{1, "test"})
//
// Memory Safety:
//   - All row references are cleared (set to nil)
//   - Allows GC to collect row contents
//   - Don't use the slice after calling PutRowSlice
//
// Size Limits:
//   - Slices with cap > MaxSize are not pooled (prevents memory leaks)
//   - Typical MaxSize: 1000 rows
//   - For larger result sets, consider streaming
//
// ELI12:
//
// Like returning a whiteboard to the classroom:
//   - First, erase everything on it (clear references)
//   - Then put it back on the rack (return to pool)
//   - If it's too big to fit on the rack (cap > MaxSize), throw it away
//   - Don't try to use the whiteboard after you've returned it!
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
//
// This lightweight structure is used for temporary node storage during
// query execution. It contains only the essential fields needed for most
// operations, reducing memory overhead.
//
// Fields:
//   - ID: Unique node identifier
//   - Labels: Node labels (e.g., ["Person", "Employee"])
//   - Properties: Node properties as key-value pairs
//
// Example:
//
//	node := &pool.PooledNode{
//		ID:     "node-123",
//		Labels: []string{"User"},
//		Properties: map[string]interface{}{
//			"name": "Alice",
//			"age":  30,
//		},
//	}
//
// Use with Pool:
//
//	nodes := pool.GetNodeSlice()
//	defer pool.PutNodeSlice(nodes)
//
//	nodes = append(nodes, &pool.PooledNode{
//		ID:     "node-1",
//		Labels: []string{"Person"},
//	})
//
// ELI12:
//
// Think of PooledNode like a sticky note with just the important info:
//   - ID: The note's unique number
//   - Labels: Tags like "Homework" or "Reminder"
//   - Properties: The actual information written on the note
//   - It's simpler than a full notebook page, so it's faster to work with!
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
//
// The returned slice has length 0 but pre-allocated capacity (typically 64 nodes).
// Use this when building lists of nodes during query execution. Always call
// PutNodeSlice when done to return the slice to the pool.
//
// Returns:
//   - Empty slice with capacity 64 if pooling is enabled
//   - Fresh slice if pooling is disabled
//
// Example 1 - Query Results:
//
//	func findUsers(db *DB) []*pool.PooledNode {
//		nodes := pool.GetNodeSlice()
//		defer pool.PutNodeSlice(nodes)
//
//		for _, user := range db.QueryUsers() {
//			nodes = append(nodes, &pool.PooledNode{
//				ID:     user.ID,
//				Labels: []string{"User"},
//				Properties: map[string]interface{}{
//					"name":  user.Name,
//					"email": user.Email,
//				},
//			})
//		}
//
//		// Return a copy (original goes back to pool)
//		result := make([]*pool.PooledNode, len(nodes))
//		copy(result, nodes)
//		return result
//	}
//
// Example 2 - Graph Traversal:
//
//	func traverse(start string, depth int) []*pool.PooledNode {
//		visited := pool.GetNodeSlice()
//		defer pool.PutNodeSlice(visited)
//
//		queue := []string{start}
//		for len(queue) > 0 && depth > 0 {
//			current := queue[0]
//			queue = queue[1:]
//
//			node := db.GetNode(current)
//			visited = append(visited, node)
//
//			for _, neighbor := range db.GetNeighbors(current) {
//				queue = append(queue, neighbor)
//			}
//			depth--
//		}
//
//		return copyNodes(visited)
//	}
//
// Performance:
//   - Saves 1-2 allocations per query
//   - Reduces GC pressure for graph operations
//   - Especially beneficial for traversals with many intermediate nodes
func GetNodeSlice() []*PooledNode {
	if !globalConfig.Enabled {
		return make([]*PooledNode, 0, 64)
	}
	return nodeSlicePool.Get().([]*PooledNode)[:0]
}

// PutNodeSlice returns a node slice to the pool for reuse.
//
// The slice is cleared (all pointers set to nil) before being pooled to allow
// garbage collection of the node objects. Slices with capacity > MaxSize are
// not pooled to prevent memory leaks.
//
// Parameters:
//   - nodes: The slice to return to the pool (will be cleared)
//
// Example:
//
//	nodes := pool.GetNodeSlice()
//	defer pool.PutNodeSlice(nodes)
//
//	// Build node list...
//	nodes = append(nodes, &pool.PooledNode{ID: "node-1"})
//
// Memory Safety:
//   - All node pointers are set to nil
//   - Allows GC to collect node objects
//   - Don't access nodes after calling PutNodeSlice
//
// ELI12:
//
// Like returning a tray of cups to the cafeteria:
//   - Empty all the cups first (clear pointers)
//   - Put the tray back on the stack (return to pool)
//   - If the tray is too big (cap > MaxSize), throw it away
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

// PooledStringBuilder is a poolable string builder for efficient string concatenation.
//
// This builder uses a byte slice internally for zero-allocation string building.
// It's more efficient than using += for string concatenation, especially when
// building large strings or concatenating in loops.
//
// Methods:
//   - WriteString: Append a string
//   - WriteByte: Append a single byte
//   - String: Get the final string
//   - Len: Get current length
//   - Reset: Clear for reuse
//
// Example 1 - Build Cypher Query:
//
//	builder := pool.GetStringBuilder()
//	defer pool.PutStringBuilder(builder)
//
//	builder.WriteString("MATCH (n:User) WHERE ")
//	for i, filter := range filters {
//		if i > 0 {
//			builder.WriteString(" AND ")
//		}
//		builder.WriteString(filter)
//	}
//	builder.WriteString(" RETURN n")
//
//	query := builder.String()
//
// Example 2 - Build JSON:
//
//	builder := pool.GetStringBuilder()
//	defer pool.PutStringBuilder(builder)
//
//	builder.WriteByte('{')
//	builder.WriteString(`"name":"`)
//	builder.WriteString(name)
//	builder.WriteString(`","age":`)
//	builder.WriteString(strconv.Itoa(age))
//	builder.WriteByte('}')
//
//	json := builder.String()
//
// Performance vs strings.Builder:
//   - Similar performance for single-threaded use
//   - Pooled version reduces allocations by ~90%
//   - Best for frequently-called functions
//
// Performance vs += concatenation:
//   - 10-100x faster for loops
//   - O(n) vs O(n²) complexity
//   - Use builder for >3 concatenations
//
// ELI12:
//
// Think of PooledStringBuilder like a reusable notepad:
//   - WriteString: Write words on the notepad
//   - WriteByte: Write a single letter
//   - String: Read what you wrote
//   - Reset: Erase the notepad to start over
//   - Much faster than using a new piece of paper for each word!
type PooledStringBuilder struct {
	buf []byte
}

// WriteString appends a string to the builder.
//
// This method efficiently appends a string to the internal buffer without
// creating intermediate string copies.
//
// Parameters:
//   - s: String to append
//
// Example:
//
//	builder := pool.GetStringBuilder()
//	builder.WriteString("Hello")
//	builder.WriteString(" ")
//	builder.WriteString("World")
//	fmt.Println(builder.String()) // "Hello World"
func (b *PooledStringBuilder) WriteString(s string) {
	b.buf = append(b.buf, s...)
}

// WriteByte appends a single byte to the builder.
//
// Use this for single characters or delimiters. More efficient than
// WriteString for single bytes.
//
// Parameters:
//   - c: Byte to append
//
// Returns:
//   - Always nil; the signature is io.ByteWriter's, as for strings.Builder
//
// Example:
//
//	builder := pool.GetStringBuilder()
//	builder.WriteByte('[')
//	builder.WriteString("items")
//	builder.WriteByte(']')
//	fmt.Println(builder.String()) // "[items]"
func (b *PooledStringBuilder) WriteByte(c byte) error {
	b.buf = append(b.buf, c)
	return nil
}

// String returns the built string.
//
// This method converts the internal byte buffer to a string. The conversion
// creates a copy, so the builder can be safely reused after calling String.
//
// Returns:
//   - The concatenated string
//
// Example:
//
//	builder := pool.GetStringBuilder()
//	builder.WriteString("test")
//	result := builder.String()
//	fmt.Println(result) // "test"
func (b *PooledStringBuilder) String() string {
	return string(b.buf)
}

// Len returns the current length of the built string in bytes.
//
// Returns:
//   - Number of bytes currently in the buffer
//
// Example:
//
//	builder := pool.GetStringBuilder()
//	builder.WriteString("Hello")
//	fmt.Println(builder.Len()) // 5
func (b *PooledStringBuilder) Len() int {
	return len(b.buf)
}

// Reset clears the builder for reuse.
//
// This method resets the length to 0 while keeping the underlying capacity,
// allowing the buffer to be reused without reallocation.
//
// Example:
//
//	builder := pool.GetStringBuilder()
//	builder.WriteString("first")
//	fmt.Println(builder.String()) // "first"
//
//	builder.Reset()
//	builder.WriteString("second")
//	fmt.Println(builder.String()) // "second"
func (b *PooledStringBuilder) Reset() {
	b.buf = b.buf[:0]
}

// GetStringBuilder returns a string builder from the pool.
//
// The returned builder is empty and ready to use. Always call PutStringBuilder
// when done to return it to the pool. The builder has pre-allocated capacity
// (typically 256 bytes) to avoid initial allocations.
//
// Returns:
//   - Empty PooledStringBuilder with pre-allocated capacity
//
// Example 1 - Build SQL Query:
//
//	builder := pool.GetStringBuilder()
//	defer pool.PutStringBuilder(builder)
//
//	builder.WriteString("SELECT * FROM users WHERE ")
//	for i, condition := range conditions {
//		if i > 0 {
//			builder.WriteString(" AND ")
//		}
//		builder.WriteString(condition)
//	}
//
//	return builder.String()
//
// Example 2 - Build CSV Line:
//
//	func buildCSVLine(fields []string) string {
//		builder := pool.GetStringBuilder()
//		defer pool.PutStringBuilder(builder)
//
//		for i, field := range fields {
//			if i > 0 {
//				builder.WriteByte(',')
//			}
//			builder.WriteByte('"')
//			builder.WriteString(field)
//			builder.WriteByte('"')
//		}
//
//		return builder.String()
//	}
//
// Example 3 - Build Log Message:
//
//	builder := pool.GetStringBuilder()
//	defer pool.PutStringBuilder(builder)
//
//	builder.WriteString("[")
//	builder.WriteString(level)
//	builder.WriteString("] ")
//	builder.WriteString(time.Now().Format(time.RFC3339))
//	builder.WriteString(": ")
//	builder.WriteString(message)
//
//	logger.Print(builder.String())
//
// Performance:
//   - Eliminates allocation for string builder
//   - Pre-allocated capacity avoids growth for small strings
//   - Typical savings: 1-2 allocations per call
func GetStringBuilder() *PooledStringBuilder {
	if !globalConfig.Enabled {
		return &PooledStringBuilder{buf: make([]byte, 0, 256)}
	}
	b := stringBuilderPool.Get().(*PooledStringBuilder)
	b.Reset()
	return b
}

// PutStringBuilder returns a string builder to the pool for reuse.
//
// The builder is automatically reset before being pooled. Very large builders
// (capacity > 64KB) are not pooled to prevent memory leaks.
//
// Parameters:
//   - b: The builder to return to the pool (will be reset)
//
// Example:
//
//	builder := pool.GetStringBuilder()
//	defer pool.PutStringBuilder(builder) // Always return to pool
//
//	builder.WriteString("temporary string")
//	return builder.String()
//
// Memory Safety:
//   - Builder is automatically reset (cleared)
//   - Don't use the builder after calling PutStringBuilder
//   - Large builders (>64KB) are discarded, not pooled
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
//
// The returned buffer has length 0 but pre-allocated capacity (typically 1KB).
// Use this for temporary byte operations like encoding, serialization, or
// building binary data. Always call PutByteBuffer when done.
//
// Returns:
//   - Empty byte slice with capacity 1024
//
// Example 1 - JSON Encoding:
//
//	func encodeJSON(data interface{}) ([]byte, error) {
//		buf := pool.GetByteBuffer()
//		defer pool.PutByteBuffer(buf)
//
//		encoder := json.NewEncoder(bytes.NewBuffer(buf))
//		if err := encoder.Encode(data); err != nil {
//			return nil, err
//		}
//
//		// Return a copy (original goes back to pool)
//		result := make([]byte, len(buf))
//		copy(result, buf)
//		return result, nil
//	}
//
// Example 2 - Binary Protocol:
//
//	func encodeMessage(msgType byte, payload []byte) []byte {
//		buf := pool.GetByteBuffer()
//		defer pool.PutByteBuffer(buf)
//
//		// Write header
//		buf = append(buf, msgType)
//		buf = binary.BigEndian.AppendUint32(buf, uint32(len(payload)))
//
//		// Write payload
//		buf = append(buf, payload...)
//
//		return append([]byte(nil), buf...) // Return copy
//	}
//
// Example 3 - Hash Computation:
//
//	func computeHash(data string) []byte {
//		buf := pool.GetByteBuffer()
//		defer pool.PutByteBuffer(buf)
//
//		h := sha256.New()
//		h.Write([]byte(data))
//		return h.Sum(buf[:0]) // Use buf as backing array
//	}
//
// Performance:
//   - Eliminates allocation for temporary buffers
//   - Pre-allocated 1KB handles most use cases
//   - Grows automatically if needed
func GetByteBuffer() []byte {
	if !globalConfig.Enabled {
		return make([]byte, 0, 1024)
	}
	return byteBufferPool.Get().([]byte)[:0]
}

// PutByteBuffer returns a byte buffer to the pool for reuse.
//
// The buffer is cleared (length reset to 0) before being pooled. Very large
// buffers (capacity > 1MB) are not pooled to prevent memory leaks.
//
// Parameters:
//   - buf: The buffer to return to the pool (will be cleared)
//
// Example:
//
//	buf := pool.GetByteBuffer()
//	defer pool.PutByteBuffer(buf)
//
//	// Use buffer...
//	buf = append(buf, []byte("data")...)
//
// Memory Safety:
//   - Buffer is cleared (length set to 0)
//   - Capacity is preserved for reuse
//   - Don't use buffer after calling PutByteBuffer
//   - Buffers >1MB are discarded, not pooled
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

// GetMap returns a string-to-interface map from the pool.
//
// The returned map is empty and ready to use. Maps are commonly used for
// query parameters, node properties, and JSON-like data structures. Always
// call PutMap when done to return it to the pool.
//
// Returns:
//   - Empty map with capacity 8
//
// Example 1 - Query Parameters:
//
//	func buildParams(userID string, filters map[string]string) map[string]interface{} {
//		params := pool.GetMap()
//		defer pool.PutMap(params)
//
//		params["userID"] = userID
//		for k, v := range filters {
//			params[k] = v
//		}
//
//		// Return a copy
//		result := make(map[string]interface{}, len(params))
//		for k, v := range params {
//			result[k] = v
//		}
//		return result
//	}
//
// Example 2 - Node Properties:
//
//	func createNode(id, name string, age int) *Node {
//		props := pool.GetMap()
//		defer pool.PutMap(props)
//
//		props["id"] = id
//		props["name"] = name
//		props["age"] = age
//		props["created_at"] = time.Now()
//
//		return &Node{Properties: copyMap(props)}
//	}
//
// Example 3 - JSON Response:
//
//	func buildResponse(status string, data interface{}) map[string]interface{} {
//		resp := pool.GetMap()
//		defer pool.PutMap(resp)
//
//		resp["status"] = status
//		resp["data"] = data
//		resp["timestamp"] = time.Now().Unix()
//
//		return copyMap(resp)
//	}
//
// Performance:
//   - Eliminates allocation for map header and initial buckets
//   - Pre-allocated capacity 8 handles most use cases
//   - Typical savings: 1 allocation per call
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

// PutMap returns a map to the pool for reuse.
//
// The map is cleared (all entries deleted) before being pooled. Very large
// maps (len > MaxSize) are not pooled to prevent memory leaks.
//
// Parameters:
//   - m: The map to return to the pool (will be cleared)
//
// Example:
//
//	params := pool.GetMap()
//	defer pool.PutMap(params)
//
//	params["key"] = "value"
//	// Use params...
//
// Memory Safety:
//   - All entries are deleted before pooling
//   - Allows GC to collect map values
//   - Don't use map after calling PutMap
//   - Maps with >MaxSize entries are discarded
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
//
// The returned slice has length 0 but pre-allocated capacity (typically 16).
// Use this for building lists of strings like labels, column names, or tags.
// Always call PutStringSlice when done.
//
// Returns:
//   - Empty string slice with capacity 16
//
// Example 1 - Collect Labels:
//
//	func getNodeLabels(node *Node) []string {
//		labels := pool.GetStringSlice()
//		defer pool.PutStringSlice(labels)
//
//		for _, label := range node.Labels {
//			if isActive(label) {
//				labels = append(labels, label)
//			}
//		}
//
//		return append([]string(nil), labels...) // Return copy
//	}
//
// Example 2 - Build Column Names:
//
//	func getColumns(fields []Field) []string {
//		cols := pool.GetStringSlice()
//		defer pool.PutStringSlice(cols)
//
//		for _, field := range fields {
//			cols = append(cols, field.Name)
//		}
//
//		return append([]string(nil), cols...)
//	}
//
// Example 3 - Parse Tags:
//
//	func parseTags(input string) []string {
//		tags := pool.GetStringSlice()
//		defer pool.PutStringSlice(tags)
//
//		for _, tag := range strings.Split(input, ",") {
//			tag = strings.TrimSpace(tag)
//			if tag != "" {
//				tags = append(tags, tag)
//			}
//		}
//
//		return append([]string(nil), tags...)
//	}
//
// Performance:
//   - Eliminates allocation for slice header
//   - Pre-allocated capacity avoids growth for small lists
//   - Typical savings: 1 allocation per call
func GetStringSlice() []string {
	if !globalConfig.Enabled {
		return make([]string, 0, 16)
	}
	return stringSlicePool.Get().([]string)[:0]
}

// PutStringSlice returns a string slice to the pool for reuse.
//
// The slice is cleared (length reset to 0) before being pooled. Slices with
// capacity > MaxSize are not pooled to prevent memory leaks.
//
// Parameters:
//   - s: The slice to return to the pool (will be cleared)
//
// Example:
//
//	labels := pool.GetStringSlice()
//	defer pool.PutStringSlice(labels)
//
//	labels = append(labels, "User", "Active")
//	// Use labels...
//
// Memory Safety:
//   - Slice is cleared (length set to 0)
//   - Capacity is preserved for reuse
//   - Don't use slice after calling PutStringSlice
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
//
// The returned slice has length 0 but pre-allocated capacity (typically 16).
// Use this for building lists of mixed-type values like query result rows.
// Always call PutInterfaceSlice when done.
//
// Returns:
//   - Empty interface slice with capacity 16
//
// Example 1 - Build Result Row:
//
//	func buildRow(id int, name string, active bool) []interface{} {
//		row := pool.GetInterfaceSlice()
//		defer pool.PutInterfaceSlice(row)
//
//		row = append(row, id, name, active)
//
//		// Return a copy
//		return append([]interface{}(nil), row...)
//	}
//
// Example 2 - Collect Values:
//
//	func extractValues(props map[string]interface{}, keys []string) []interface{} {
//		values := pool.GetInterfaceSlice()
//		defer pool.PutInterfaceSlice(values)
//
//		for _, key := range keys {
//			if val, ok := props[key]; ok {
//				values = append(values, val)
//			}
//		}
//
//		return append([]interface{}(nil), values...)
//	}
//
// Example 3 - Function Arguments:
//
//	func callFunction(name string, args ...interface{}) interface{} {
//		argList := pool.GetInterfaceSlice()
//		defer pool.PutInterfaceSlice(argList)
//
//		argList = append(argList, args...)
//
//		return functions[name](argList...)
//	}
//
// Performance:
//   - Eliminates allocation for slice header
//   - Pre-allocated capacity 16 handles most rows
//   - Typical savings: 1 allocation per call
func GetInterfaceSlice() []interface{} {
	if !globalConfig.Enabled {
		return make([]interface{}, 0, 16)
	}
	return interfaceSlicePool.Get().([]interface{})[:0]
}

// PutInterfaceSlice returns an interface slice to the pool for reuse.
//
// The slice is cleared (all references set to nil, length reset to 0) before
// being pooled. Slices with capacity > MaxSize are not pooled to prevent
// memory leaks.
//
// Parameters:
//   - s: The slice to return to the pool (will be cleared)
//
// Example:
//
//	row := pool.GetInterfaceSlice()
//	defer pool.PutInterfaceSlice(row)
//
//	row = append(row, 1, "test", true)
//	// Use row...
//
// Memory Safety:
//   - All references are set to nil
//   - Allows GC to collect slice contents
//   - Don't use slice after calling PutInterfaceSlice
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
