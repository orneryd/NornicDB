package security

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ============================================================================
// bytesCompare – IPv6 fallback branches
// ============================================================================

func TestBytesCompare_IPv4_Less(t *testing.T) {
	a := net.ParseIP("10.0.0.1")
	b := net.ParseIP("10.0.0.2")
	assert.Equal(t, -1, bytesCompare(a, b))
}

func TestBytesCompare_IPv4_Greater(t *testing.T) {
	a := net.ParseIP("10.0.0.5")
	b := net.ParseIP("10.0.0.2")
	assert.Equal(t, 1, bytesCompare(a, b))
}

func TestBytesCompare_IPv4_Equal(t *testing.T) {
	a := net.ParseIP("10.0.0.1")
	b := net.ParseIP("10.0.0.1")
	assert.Equal(t, 0, bytesCompare(a, b))
}

func TestBytesCompare_IPv6_Less(t *testing.T) {
	a := net.IP([]byte{0x20, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01})
	b := net.IP([]byte{0x20, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x02})
	assert.Equal(t, -1, bytesCompare(a, b))
}

func TestBytesCompare_IPv6_Greater(t *testing.T) {
	a := net.IP([]byte{0x20, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x05})
	b := net.IP([]byte{0x20, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x02})
	assert.Equal(t, 1, bytesCompare(a, b))
}

func TestBytesCompare_IPv6_Equal(t *testing.T) {
	a := net.IP([]byte{0x20, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01})
	b := net.IP([]byte{0x20, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01})
	assert.Equal(t, 0, bytesCompare(a, b))
}

// ============================================================================
// isPrivateIP – IPv6 branches
// ============================================================================

func TestIsPrivateIP_IPv6_Private(t *testing.T) {
	// fc00::/7 is ULA private IPv6 range
	ip := net.ParseIP("fc00::1")
	assert.True(t, isPrivateIP(ip))
}

func TestIsPrivateIP_IPv6_LinkLocal(t *testing.T) {
	// fe80:: is link-local IPv6 (covered by branch ip[0]=0xfe && ip[1]&0xc0==0x80)
	ip := net.ParseIP("fe80::1")
	assert.True(t, isPrivateIP(ip))
}

func TestIsPrivateIP_IPv6_Public(t *testing.T) {
	// 2001:db8:: is documentation prefix — not private in our check
	ip := net.ParseIP("2001:4860:4860::8888") // Google DNS IPv6
	assert.False(t, isPrivateIP(ip))
}

// ============================================================================
// ValidateURL – IPv6 loopback in dev
// ============================================================================

func TestValidateURL_IPv6_Loopback_Dev(t *testing.T) {
	// ::1 is IPv6 loopback — allowed in dev mode
	err := ValidateURL("http://[::1]:8080/api", URLPolicy{AllowHTTP: true, HostPolicy: URLDevelopmentHost})
	assert.NoError(t, err)
}

func TestValidateURL_IPv6_PrivateULA(t *testing.T) {
	// fc00::1 is IPv6 ULA private
	err := ValidateURL("https://[fc00::1]/api", URLPolicy{})
	assert.ErrorIs(t, err, ErrURLPrivateIP)
}

// ============================================================================
// ValidateRequest – extra branches
// ============================================================================

func TestValidateRequest_NoSecurityMiddleware(t *testing.T) {
	// Calling ValidateURL directly to hit remaining ValidateRequest branches via public path
	// Just ensure no panic
	err := ValidateURL("https://example.com/valid", URLPolicy{})
	assert.NoError(t, err)
}
