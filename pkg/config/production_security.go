package config

import (
	"fmt"
	"net"
	"net/netip"
	"strings"
)

// ValidateSecurityConfiguration rejects insecure listener and credential
// combinations before startup.
func ValidateSecurityConfiguration(config *Config) error {
	if config == nil {
		return fmt.Errorf("security configuration: config is required")
	}
	if err := validateTrustedProxies(config.Server.HTTPTrustedProxies); err != nil {
		return err
	}
	nativeHTTPS := config.Server.HTTPSEnabled && strings.TrimSpace(config.Server.HTTPTLSCert) != "" && strings.TrimSpace(config.Server.HTTPTLSKey) != ""
	if config.Server.HTTPSEnabled && !nativeHTTPS {
		return fmt.Errorf("security configuration: HTTPS certificate and key are required when HTTPS is enabled")
	}
	grpcTLS := config.Features.QdrantGRPCTLSEnabled && strings.TrimSpace(config.Features.QdrantGRPCTLSCert) != "" && strings.TrimSpace(config.Features.QdrantGRPCTLSKey) != ""
	if config.Features.QdrantGRPCTLSEnabled && !grpcTLS {
		return fmt.Errorf("security configuration: gRPC TLS certificate and key are required when gRPC TLS is enabled")
	}
	grpcClientAuthMode := strings.TrimSpace(strings.ToLower(config.Features.QdrantGRPCTLSClientAuthMode))
	switch grpcClientAuthMode {
	case "", "none", "request", "request_verify", "require_verify":
	default:
		return fmt.Errorf("security configuration: invalid gRPC TLS client auth mode %q", config.Features.QdrantGRPCTLSClientAuthMode)
	}
	if (grpcClientAuthMode == "request_verify" || grpcClientAuthMode == "require_verify") && strings.TrimSpace(config.Features.QdrantGRPCTLSClientCA) == "" {
		return fmt.Errorf("security configuration: gRPC TLS client CA is required for client auth mode %q", grpcClientAuthMode)
	}
	if !config.Auth.Enabled {
		return nil
	}
	if strings.TrimSpace(config.Auth.InitialPassword) == "" {
		return fmt.Errorf("security configuration: empty initial password is not allowed")
	}
	if config.Server.EnableCORS {
		for _, origin := range config.Server.CORSOrigins {
			if strings.TrimSpace(origin) == "*" && isPublicListener(config.Server.HTTPAddress) {
				return fmt.Errorf("security configuration: wildcard CORS origin is not allowed")
			}
		}
	}
	if config.Server.HTTPEnabled && isPublicListener(config.Server.HTTPAddress) && !nativeHTTPS && len(config.Server.HTTPTrustedProxies) == 0 {
		return fmt.Errorf("security configuration: public plaintext HTTP listener is not allowed")
	}
	if config.Server.BoltEnabled && isPublicListener(config.Server.BoltAddress) {
		if !config.Server.BoltTLSRequire {
			return fmt.Errorf("security configuration: public Bolt listener must require TLS")
		}
		if !config.Server.BoltTLSEnabled || strings.TrimSpace(config.Server.BoltTLSCert) == "" || strings.TrimSpace(config.Server.BoltTLSKey) == "" {
			return fmt.Errorf("security configuration: Bolt TLS certificate and key are required for a public Bolt listener")
		}
	}
	if config.Features.QdrantGRPCEnabled && isPublicListener(config.Features.QdrantGRPCListenAddr) {
		if !grpcTLS {
			return fmt.Errorf("security configuration: public gRPC listener must use TLS")
		}
	}
	return nil
}

func validateTrustedProxies(proxies []string) error {
	for _, proxy := range proxies {
		proxy = strings.TrimSpace(proxy)
		if proxy == "" {
			return fmt.Errorf("security configuration: trusted proxy entry must not be empty")
		}
		if _, err := netip.ParsePrefix(proxy); err == nil {
			continue
		}
		if _, err := netip.ParseAddr(proxy); err != nil {
			return fmt.Errorf("security configuration: invalid trusted proxy %q: expected an IP address or CIDR", proxy)
		}
	}
	return nil
}

func isPublicListener(address string) bool {
	host := strings.TrimSpace(address)
	if parsedHost, _, err := net.SplitHostPort(host); err == nil {
		host = parsedHost
	} else if strings.HasPrefix(host, ":") {
		host = ""
	}
	host = strings.Trim(host, "[]")
	if host == "" || host == "0.0.0.0" || host == "::" {
		return true
	}
	if strings.EqualFold(host, "localhost") {
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		return !ip.IsLoopback()
	}
	return true
}
