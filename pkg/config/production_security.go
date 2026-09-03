package config

import (
	"fmt"
	"net"
	"strings"
)

// ValidateSecurityConfiguration rejects insecure listener and credential
// combinations before startup.
func ValidateSecurityConfiguration(config *Config) error {
	if config == nil {
		return fmt.Errorf("security configuration: config is required")
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
	if config.Server.HTTPEnabled && isPublicListener(config.Server.HTTPAddress) {
		return fmt.Errorf("security configuration: public plaintext HTTP listener is not allowed")
	}
	if config.Server.BoltEnabled && isPublicListener(config.Server.BoltAddress) && !config.Server.BoltTLSRequire {
		return fmt.Errorf("security configuration: public Bolt listener must require TLS")
	}
	if config.Features.QdrantGRPCEnabled && isPublicListener(config.Features.QdrantGRPCListenAddr) {
		return fmt.Errorf("security configuration: public plaintext gRPC listener is not allowed")
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
