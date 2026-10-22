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

// ValidateURL validates URLs to prevent SSRF attacks.
func ValidateURL(rawURL string, isDevelopment, allowHTTP bool) error {
	if len(rawURL) > MaxURLLength {
		return ErrURLTooLong
	}
	if rawURL == "" || strings.TrimSpace(rawURL) == "" {
		return ErrURLInvalid
	}

	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrURLInvalid, observability.RedactError(err, observability.ClientCredentials(rawURL)...))
	}

	scheme := strings.ToLower(parsedURL.Scheme)
	if scheme != "http" && scheme != "https" {
		return ErrURLInvalidProtocol
	}

	if !isDevelopment && scheme == "http" && !allowHTTP {
		return ErrURLHTTPNotAllowed
	}

	hostname := parsedURL.Hostname()
	if hostname == "" {
		return ErrURLInvalid
	}

	if !isDevelopment {
		lowercaseHost := strings.ToLower(hostname)
		if lowercaseHost == "localhost" || lowercaseHost == "host.docker.internal" {
			return ErrURLLocalhost
		}
	}

	ip := net.ParseIP(hostname)
	if ip != nil {
		// In development mode, allow loopback IPs (127.x.x.x) only
		if isDevelopment && ip.IsLoopback() {
			return nil
		}
		// Block ALL private IPs (including in dev mode, except loopback)
		if isPrivateIP(ip) {
			return ErrURLPrivateIP
		}
	}

	return nil
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
