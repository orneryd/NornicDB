package pool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test GetMap with data already in pool entry (clear path)
func TestGetMap_WithExistingEntries(t *testing.T) {
	Configure(PoolConfig{Enabled: true, MaxSize: 1000})

	// Put a map back with data, then GetMap should clear it
	m := GetMap()
	m["key1"] = "val1"
	m["key2"] = 42
	PutMap(m)

	m2 := GetMap()
	assert.NotNil(t, m2)
	assert.Equal(t, 0, len(m2), "map should be cleared on Get")
}

// Test PutStringSlice with oversized slice (should not be pooled)
func TestPutStringSlice_Oversized(t *testing.T) {
	Configure(PoolConfig{Enabled: true, MaxSize: 3})

	bigSlice := make([]string, 0, 100)
	for i := 0; i < 50; i++ {
		bigSlice = append(bigSlice, "item")
	}
	// oversized → should be discarded, not panic
	PutStringSlice(bigSlice)
}

// Test PutInterfaceSlice with oversized slice
func TestPutInterfaceSlice_Oversized(t *testing.T) {
	Configure(PoolConfig{Enabled: true, MaxSize: 3})

	bigSlice := make([]interface{}, 0, 100)
	for i := 0; i < 50; i++ {
		bigSlice = append(bigSlice, i)
	}
	PutInterfaceSlice(bigSlice)
}

// Test PutNodeSlice with oversized slice
func TestPutNodeSlice_Oversized(t *testing.T) {
	Configure(PoolConfig{Enabled: true, MaxSize: 3})
	bigSlice := GetNodeSlice()
	// extend it beyond MaxSize manually
	for i := 0; i < 100; i++ {
		bigSlice = append(bigSlice, nil)
	}
	PutNodeSlice(bigSlice) // should not panic
}

// Test PutMap with oversized map
func TestPutMap_Oversized(t *testing.T) {
	Configure(PoolConfig{Enabled: true, MaxSize: 3})
	m := make(map[string]interface{})
	for i := 0; i < 50; i++ {
		m[string(rune('a'+i%26))+string(rune('0'+i%10))] = i
	}
	PutMap(m) // oversized → discarded
}

// Test PutMap nil
func TestPutMap_Nil(t *testing.T) {
	Configure(PoolConfig{Enabled: true, MaxSize: 1000})
	PutMap(nil) // should not panic
}

// Test GetStringSlice round-trip when enabled
func TestGetPutStringSlice_Enabled(t *testing.T) {
	Configure(PoolConfig{Enabled: true, MaxSize: 1000})

	s := GetStringSlice()
	assert.NotNil(t, s)
	s = append(s, "hello", "world")
	PutStringSlice(s)

	// Get again — should be reused (empty)
	s2 := GetStringSlice()
	assert.NotNil(t, s2)
	assert.Equal(t, 0, len(s2))
}

// Test GetInterfaceSlice round-trip when enabled
func TestGetPutInterfaceSlice_Enabled(t *testing.T) {
	Configure(PoolConfig{Enabled: true, MaxSize: 1000})

	s := GetInterfaceSlice()
	assert.NotNil(t, s)
	s = append(s, 1, "x", true)
	PutInterfaceSlice(s)

	s2 := GetInterfaceSlice()
	assert.NotNil(t, s2)
	assert.Equal(t, 0, len(s2))
}
