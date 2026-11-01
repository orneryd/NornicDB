package security

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ============================================================================
// IsRestrictedIP – IPv6 branches of the literal URL policy
// ============================================================================

func TestIsRestrictedIP_IPv6_UniqueLocal(t *testing.T) {
	// fc00::/7 is the IPv6 unique-local (private) range.
	ip := net.ParseIP("fc00::1")
	assert.True(t, IsRestrictedIP(ip, IPPolicy{}))
}

func TestIsRestrictedIP_IPv6_LinkLocal(t *testing.T) {
	// fe80::/10 is IPv6 link-local unicast.
	ip := net.ParseIP("fe80::1")
	assert.True(t, IsRestrictedIP(ip, IPPolicy{}))
}

func TestIsRestrictedIP_IPv6_Public(t *testing.T) {
	// 2001:db8:: is a documentation prefix — not private in our check.
	ip := net.ParseIP("2001:4860:4860::8888") // Google DNS IPv6
	assert.False(t, IsRestrictedIP(ip, IPPolicy{}))
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
// ValidateURL – extra branches
// ============================================================================

func TestValidateRequest_NoSecurityMiddleware(t *testing.T) {
	// Calling ValidateURL directly to hit the remaining ValidateURL branches via
	// the public path; just ensure no panic.
	err := ValidateURL("https://example.com/valid", URLPolicy{})
	assert.NoError(t, err)
}
