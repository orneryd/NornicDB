// Package security provides security validation utilities for NornicDB.
package security

import (
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strings"

	"github.com/orneryd/nornicdb/pkg/observability"
)

const (
	MaxTokenLength  = 8192
	MaxURLLength    = 2048
	MaxHeaderLength = 4096
)

var (
	ErrTokenInvalidChars  = fmt.Errorf("token contains invalid characters (possible injection attack)")
	ErrTokenTooLong       = fmt.Errorf("token exceeds maximum length of %d characters", MaxTokenLength)
	ErrTokenEmpty         = fmt.Errorf("token must be a non-empty string")
	ErrURLInvalidProtocol = fmt.Errorf("only HTTP/HTTPS protocols are allowed")
	ErrURLPrivateIP       = fmt.Errorf("private IP addresses are not allowed")
	ErrURLLocalhost       = fmt.Errorf("localhost is not allowed in production")
	ErrURLHTTPNotAllowed  = fmt.Errorf("only HTTPS URLs are allowed in production")
	ErrURLTooLong         = fmt.Errorf("URL exceeds maximum length of %d characters", MaxURLLength)
	ErrURLInvalid         = fmt.Errorf("invalid URL format")

	tokenValidCharsPattern = regexp.MustCompile(`^[a-zA-Z0-9\-_.~+/=]+$`)
	dangerousTokenPattern  = regexp.MustCompile(`[\r\n<>'"&;(){}[\]\\]|javascript:|data:|file:|vbscript:`)
)

// ValidateToken validates OAuth/API token format to prevent injection attacks.
func ValidateToken(token string) error {
	if token == "" || strings.TrimSpace(token) == "" {
		return ErrTokenEmpty
	}
	if len(token) > MaxTokenLength {
		return ErrTokenTooLong
	}
	if dangerousTokenPattern.MatchString(token) {
		return ErrTokenInvalidChars
	}
	if !tokenValidCharsPattern.MatchString(token) {
		return ErrTokenInvalidChars
	}
	return nil
}

// URLHostPolicy identifies an existing consumer's host access contract.
type URLHostPolicy uint8

const (
	URLProductionHost URLHostPolicy = iota
	URLDevelopmentHost
	URLResolvedLoopback
	URLAnyHost
)

// URLSchemePolicy selects the existing consumer's URL syntax contract.
type URLSchemePolicy uint8

const (
	URLHTTPScheme URLSchemePolicy = iota
	// URLAbsoluteScheme accepts any absolute scheme with an authority, as OAuth issuer configuration requires.
	URLAbsoluteScheme
)

// URLPolicy selects a consumer's URL access contract. Its zero value applies
// production host restrictions, HTTPS, and MaxURLLength. A negative MaxLength
// explicitly disables the length bound; HostRejected preserves consumer errors.
type URLPolicy struct {
	SchemePolicy      URLSchemePolicy
	RejectCredentials bool
	MaxLength         int
	AllowHTTP         bool
	RejectUserinfo    bool
	HostPolicy        URLHostPolicy
	HostRejected      func(string) error
}

// ValidateURL is the shared ordered URL validation owner.
// Security configuration rejects private literal addresses; eval separately
// requires all DNS answers to be loopback unless remote evaluation is enabled.
// The zero policy retains production defaults; callers explicitly select other
// contracts rather than relying on mode booleans or compatibility wrappers.
func ValidateURL(rawURL string, policy URLPolicy) error {
	maximum := policy.MaxLength
	if maximum == 0 {
		maximum = MaxURLLength
	}
	if maximum > 0 && len(rawURL) > maximum {
		return ErrURLTooLong
	}
	if strings.TrimSpace(rawURL) == "" {
		return ErrURLInvalid
	}
	parsed, err := ParseURL(rawURL)
	if err != nil {
		return err
	}
	hostname := parsed.Hostname()
	switch policy.SchemePolicy {
	case URLHTTPScheme:
		scheme := strings.ToLower(parsed.Scheme)
		if scheme != "http" && scheme != "https" {
			return ErrURLInvalidProtocol
		}
		if scheme == "http" && !policy.AllowHTTP {
			return ErrURLHTTPNotAllowed
		}
		if hostname == "" {
			return ErrURLInvalid
		}
	case URLAbsoluteScheme:
		if parsed.Scheme == "" || parsed.Host == "" {
			return ErrURLInvalid
		}
	default:
		return ErrURLInvalid
	}
	if policy.RejectCredentials && (parsed.User != nil || len(observability.ClientCredentials(rawURL)) != 0) {
		return fmt.Errorf("URLs with credentials are not allowed")
	}
	if policy.RejectUserinfo && parsed.User != nil {
		return fmt.Errorf("URLs with userinfo are not allowed")
	}
	switch policy.HostPolicy {
	case URLAnyHost:
		return nil
	case URLResolvedLoopback:
		if isLoopbackHost(hostname) {
			return nil
		}
		if policy.HostRejected != nil {
			return policy.HostRejected(hostname)
		}
		return ErrURLPrivateIP
	case URLDevelopmentHost, URLProductionHost:
		if policy.HostPolicy == URLProductionHost {
			lower := strings.ToLower(hostname)
			if lower == "localhost" || lower == "host.docker.internal" {
				return ErrURLLocalhost
			}
		}
		if ip := net.ParseIP(hostname); ip != nil {
			if policy.HostPolicy == URLDevelopmentHost && ip.IsLoopback() {
				return nil
			}
			if isPrivateIP(ip) {
				return ErrURLPrivateIP
			}
		}
		return nil
	default:
		return ErrURLInvalid
	}
}

// isLoopbackHost retains eval's all-address DNS rule without adding DNS
// resolution to security configuration's literal-address policy.
func isLoopbackHost(hostname string) bool {
	if ip := net.ParseIP(hostname); ip != nil {
		return ip.IsLoopback()
	}
	addresses, err := net.LookupIP(hostname)
	if err != nil || len(addresses) == 0 {
		return false
	}
	for _, address := range addresses {
		if !address.IsLoopback() {
			return false
		}
	}
	return true
}

// ParseURL parses URL syntax and safely renders parse failures. It imposes no
// protocol, hostname or network policy; ValidateURL applies the selected
// consumer contract in its required order.
func ParseURL(rawURL string) (*url.URL, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrURLInvalid, observability.RedactError(err, observability.ClientCredentials(rawURL)...))
	}
	return parsed, nil
}

// isPrivateIP checks if an IP address is in a private range.
func isPrivateIP(ip net.IP) bool {
	if ip.IsLoopback() {
		return true
	}

	if ip.To4() != nil {
		privateRanges := []struct {
			start net.IP
			end   net.IP
		}{
			{net.ParseIP("10.0.0.0"), net.ParseIP("10.255.255.255")},
			{net.ParseIP("172.16.0.0"), net.ParseIP("172.31.255.255")},
			{net.ParseIP("192.168.0.0"), net.ParseIP("192.168.255.255")},
			{net.ParseIP("169.254.0.0"), net.ParseIP("169.254.255.255")},
		}

		for _, r := range privateRanges {
			if inRange(ip, r.start, r.end) {
				return true
			}
		}
	} else {
		if ip.IsPrivate() {
			return true
		}
		if len(ip) == 16 && ip[0] == 0xfe && (ip[1]&0xc0) == 0x80 {
			return true
		}
	}

	return false
}

func inRange(ip, start, end net.IP) bool {
	return bytesCompare(ip, start) >= 0 && bytesCompare(ip, end) <= 0
}

func bytesCompare(a, b net.IP) int {
	a4 := a.To4()
	b4 := b.To4()
	if a4 != nil && b4 != nil {
		for i := 0; i < len(a4); i++ {
			if a4[i] < b4[i] {
				return -1
			}
			if a4[i] > b4[i] {
				return 1
			}
		}
		return 0
	}
	if string(a) < string(b) {
		return -1
	}
	if string(a) > string(b) {
		return 1
	}
	return 0
}

// ValidateHeaderValue validates HTTP header values to prevent injection attacks.
func ValidateHeaderValue(value string) error {
	if len(value) > MaxHeaderLength {
		return fmt.Errorf("header value exceeds maximum length of %d characters", MaxHeaderLength)
	}
	if strings.ContainsAny(value, "\r\n\x00") {
		return fmt.Errorf("header value contains invalid control characters")
	}
	return nil
}

// SanitizeString removes dangerous characters from user input.
func SanitizeString(input string) string {
	input = strings.ReplaceAll(input, "\x00", "")

	var result strings.Builder
	for _, r := range input {
		if r >= 32 || r == '\t' || r == '\n' {
			result.WriteRune(r)
		}
	}

	return strings.TrimSpace(result.String())
}
