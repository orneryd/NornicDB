package security

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Coverage for the one IP restriction owner. The retired inRange/bytesCompare
// helpers existed only to compare an address against the IPv4 range ends, so
// their coverage now lives here as classifier boundary cases: each range end is
// checked together with the address just outside it.

func TestIsRestrictedIP_IPv4RangeBoundaries(t *testing.T) {
	cases := []struct {
		ip         string
		restricted bool
	}{
		// 10.0.0.0/8
		{"9.255.255.255", false},
		{"10.0.0.0", true},
		{"10.255.255.255", true},
		{"11.0.0.0", false},
		// 172.16.0.0/12
		{"172.15.255.255", false},
		{"172.16.0.0", true},
		{"172.31.255.255", true},
		{"172.32.0.0", false},
		// 192.168.0.0/16
		{"192.167.255.255", false},
		{"192.168.0.0", true},
		{"192.168.255.255", true},
		{"192.169.0.0", false},
		// 169.254.0.0/16 link-local
		{"169.253.255.255", false},
		{"169.254.0.0", true},
		{"169.254.255.255", true},
		{"169.255.0.0", false},
		// Address classes the literal URL policy has never rejected.
		{"0.0.0.0", false},
		{"100.64.0.1", false},
		{"224.0.0.1", false},
		{"255.255.255.255", false},
		// Loopback is its own class, unchanged by the range ends.
		{"126.255.255.255", false},
		{"127.0.0.1", true},
		{"127.255.255.255", true},
		{"128.0.0.0", false},
	}
	for _, testCase := range cases {
		t.Run(testCase.ip, func(t *testing.T) {
			ip := net.ParseIP(testCase.ip)
			require.NotNil(t, ip)
			assert.Equal(t, testCase.restricted, IsRestrictedIP(ip, IPPolicy{}))
		})
	}
}

func TestIsRestrictedIP_IPv6RangeBoundaries(t *testing.T) {
	cases := []struct {
		ip         string
		restricted bool
	}{
		// fc00::/7 unique-local
		{"fbff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", false},
		{"fc00::", true},
		{"fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", true},
		{"fe00::", false},
		// fe80::/10 link-local unicast
		{"fe7f:ffff:ffff:ffff:ffff:ffff:ffff:ffff", false},
		{"fe80::", true},
		{"febf:ffff:ffff:ffff:ffff:ffff:ffff:ffff", true},
		{"fec0::", false},
		// Loopback, unspecified, multicast and public stay in their own classes.
		{"::1", true},
		{"::", false},
		{"ff02::1", false},
		{"2001:4860:4860::8888", false},
		{"2606:2800:220:1:248:1893:25c8:1946", false},
	}
	for _, testCase := range cases {
		t.Run(testCase.ip, func(t *testing.T) {
			ip := net.ParseIP(testCase.ip)
			require.NotNil(t, ip)
			assert.Equal(t, testCase.restricted, IsRestrictedIP(ip, IPPolicy{}))
		})
	}
}

func TestIsRestrictedIP_IPv4MappedAddresses(t *testing.T) {
	cases := []struct {
		ip         string
		restricted bool
	}{
		{"::ffff:10.0.0.1", true},
		{"::ffff:127.0.0.1", true},
		{"::ffff:169.254.169.254", true},
		{"::ffff:192.168.1.1", true},
		{"::ffff:8.8.8.8", false},
		{"::ffff:0.0.0.0", false},
		{"::ffff:224.0.0.1", false},
	}
	for _, testCase := range cases {
		t.Run(testCase.ip, func(t *testing.T) {
			ip := net.ParseIP(testCase.ip)
			require.NotNil(t, ip)
			assert.Equal(t, testCase.restricted, IsRestrictedIP(ip, IPPolicy{}))
		})
	}
}

// TestIsRestrictedIP_AddressLengths keeps the classifier independent of the
// caller's net.IP representation: a 4-byte IPv4 address, its To4 form and its
// IPv4-mapped 16-byte form answer alike, and an unjudgeable length stays
// unrestricted for the owner to fail closed on separately.
func TestIsRestrictedIP_AddressLengths(t *testing.T) {
	assert.True(t, IsRestrictedIP(net.IP{10, 0, 0, 1}, IPPolicy{}), "4-byte IPv4 form")
	assert.True(t, IsRestrictedIP(net.ParseIP("10.0.0.1").To4(), IPPolicy{}), "To4 form")
	assert.True(t, IsRestrictedIP(net.ParseIP("10.0.0.1"), IPPolicy{}), "IPv4-mapped 16-byte form")

	public := net.IP([]byte{0x20, 0x01, 0x48, 0x60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x88, 0x88})
	assert.False(t, IsRestrictedIP(public, IPPolicy{}), "16-byte global IPv6 form")

	apoc := IPPolicy{RejectMulticast: true, RejectUnspecified: true}
	assert.False(t, IsRestrictedIP(nil, IPPolicy{}), "nil address")
	assert.False(t, IsRestrictedIP(nil, apoc), "nil address under the extended policy")
	assert.False(t, IsRestrictedIP(net.IP{}, IPPolicy{}), "empty address")
	assert.False(t, IsRestrictedIP(net.IP{10, 0, 0}, IPPolicy{}), "truncated IPv4 address")
	assert.False(t, IsRestrictedIP(net.IP{0xfe, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, IPPolicy{}),
		"truncated IPv6 address")
}

func TestIsRestrictedIP_PolicyFlags(t *testing.T) {
	multicastOnly := IPPolicy{RejectMulticast: true}
	unspecifiedOnly := IPPolicy{RejectUnspecified: true}
	extended := IPPolicy{RejectMulticast: true, RejectUnspecified: true}

	cases := []struct {
		ip          string
		zero        bool
		multicast   bool
		unspecified bool
		extended    bool
	}{
		// Multicast is rejected only by RejectMulticast.
		{"224.0.0.1", false, true, false, true},
		{"239.255.255.250", false, true, false, true},
		{"ff02::1", false, true, false, true},
		{"ff0e::1", false, true, false, true},
		{"::ffff:224.0.0.1", false, true, false, true},
		// Unspecified is rejected only by RejectUnspecified.
		{"0.0.0.0", false, false, true, true},
		{"::", false, false, true, true},
		{"::ffff:0.0.0.0", false, false, true, true},
		// The literal URL rule holds under every policy.
		{"10.1.2.3", true, true, true, true},
		{"172.16.0.1", true, true, true, true},
		{"192.168.1.1", true, true, true, true},
		{"169.254.10.20", true, true, true, true},
		{"127.0.0.1", true, true, true, true},
		{"fc00::1", true, true, true, true},
		{"fe80::1", true, true, true, true},
		// Public and shared address space stays allowed under every policy.
		{"93.184.216.34", false, false, false, false},
		{"203.0.113.7", false, false, false, false},
		{"100.64.0.1", false, false, false, false},
		{"2606:2800:220:1:248:1893:25c8:1946", false, false, false, false},
	}
	for _, testCase := range cases {
		t.Run(testCase.ip, func(t *testing.T) {
			ip := net.ParseIP(testCase.ip)
			require.NotNil(t, ip)
			assert.Equal(t, testCase.zero, IsRestrictedIP(ip, IPPolicy{}), "zero policy")
			assert.Equal(t, testCase.multicast, IsRestrictedIP(ip, multicastOnly), "RejectMulticast")
			assert.Equal(t, testCase.unspecified, IsRestrictedIP(ip, unspecifiedOnly), "RejectUnspecified")
			assert.Equal(t, testCase.extended, IsRestrictedIP(ip, extended), "RejectMulticast+RejectUnspecified")
		})
	}
}

// TestValidateURL_LiteralPolicyKeepsAddressClasses pins the URL-level acceptance
// the retired helper provided: the zero policy rejects loopback, private and
// link-local literals only, and multicast or unspecified literals stay accepted
// as before instead of being broadened into rejections.
func TestValidateURL_LiteralPolicyKeepsAddressClasses(t *testing.T) {
	for _, rawURL := range []string{
		"https://224.0.0.1/api",
		"https://0.0.0.0/api",
		"https://[ff02::1]/api",
		"https://[::]/api",
		"https://[::ffff:224.0.0.1]/api",
		"https://93.184.216.34/api",
		"https://100.64.0.1/api",
	} {
		assert.NoError(t, ValidateURL(rawURL, URLPolicy{}), rawURL)
	}

	for _, rawURL := range []string{
		"https://10.255.255.255/api",
		"https://172.31.255.255/api",
		"https://192.168.255.255/api",
		"https://169.254.255.255/api",
		"https://127.0.0.1/api",
		"https://[::ffff:10.0.0.1]/api",
		"https://[fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]/api",
		"https://[febf:ffff:ffff:ffff:ffff:ffff:ffff:ffff]/api",
	} {
		assert.ErrorIs(t, ValidateURL(rawURL, URLPolicy{}), ErrURLPrivateIP, rawURL)
	}
}
