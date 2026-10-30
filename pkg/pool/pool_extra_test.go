package pool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test pool functions when pool is disabled (branching code)
func TestGetPut_PoolDisabled(t *testing.T) {
	Configure(PoolConfig{Enabled: false})
	defer Configure(PoolConfig{Enabled: true, MaxSize: 1000})

	// ByteBuffer
	buf := GetByteBuffer()
	assert.NotNil(t, buf)
	assert.Equal(t, 0, len(buf))
	PutByteBuffer(buf) // no-op when disabled

	// StringSlice
	ss := GetStringSlice()
	assert.NotNil(t, ss)
	PutStringSlice(ss) // no-op when disabled

	// InterfaceSlice
	is := GetInterfaceSlice()
	assert.NotNil(t, is)
	PutInterfaceSlice(is) // no-op when disabled

	// NodeSlice
	ns := GetNodeSlice()
	assert.NotNil(t, ns)
	PutNodeSlice(ns) // no-op when disabled

	// Map
	m := GetMap()
	assert.NotNil(t, m)
	PutMap(m) // no-op when disabled
}

// Test pool functions when pool is enabled
func TestGetPut_PoolEnabled(t *testing.T) {
	Configure(PoolConfig{Enabled: true, MaxSize: 1000})

	// ByteBuffer - enabled path
	buf := GetByteBuffer()
	assert.NotNil(t, buf)
	assert.Equal(t, 0, len(buf))
	buf = append(buf, []byte("test data")...)
	PutByteBuffer(buf)

	// StringSlice - enabled path
	ss := GetStringSlice()
	assert.NotNil(t, ss)
	ss = append(ss, "a", "b")
	PutStringSlice(ss)

	// InterfaceSlice - enabled path
	is := GetInterfaceSlice()
	assert.NotNil(t, is)
	is = append(is, 1, 2, 3)
	PutInterfaceSlice(is)

	// NodeSlice - enabled path
	ns := GetNodeSlice()
	assert.NotNil(t, ns)
	PutNodeSlice(ns)

	// Map - enabled path
	m := GetMap()
	assert.NotNil(t, m)
	m["key"] = "value"
	PutMap(m)
}

func TestPutByteBuffer_HugeBufNotPooled(t *testing.T) {
	Configure(PoolConfig{Enabled: true, MaxSize: 1000})

	// Huge buffer > 1MB should not be pooled (just discarded)
	hugeBuf := make([]byte, 2*1024*1024) // 2MB
	PutByteBuffer(hugeBuf)               // should not panic
}

func TestGetStringBuilder_EnabledDisabled(t *testing.T) {
	Configure(PoolConfig{Enabled: false})
	defer Configure(PoolConfig{Enabled: true, MaxSize: 1000})

	sb := GetStringBuilder()
	assert.NotNil(t, sb)
	sb.WriteString("test")
	PutStringBuilder(sb) // no-op when disabled

	Configure(PoolConfig{Enabled: true, MaxSize: 1000})
	sb2 := GetStringBuilder()
	assert.NotNil(t, sb2)
	assert.Equal(t, 0, sb2.Len())
	sb2.WriteString("hello")
	PutStringBuilder(sb2)
}

func TestGetRowSlice_RoundTrip(t *testing.T) {
	Configure(PoolConfig{Enabled: true, MaxSize: 1000})

	rows := GetRowSlice()
	assert.NotNil(t, rows)
	rows = append(rows, []interface{}{"col1", "col2"})
	PutRowSlice(rows)
}

func TestGetRowSlice_Disabled(t *testing.T) {
	Configure(PoolConfig{Enabled: false})
	defer Configure(PoolConfig{Enabled: true, MaxSize: 1000})

	rows := GetRowSlice()
	assert.NotNil(t, rows)
	PutRowSlice(rows) // no-op
}
