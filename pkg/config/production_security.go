package config

import (
	"fmt"
	"net"
	"strings"
)

// ValidateProductionSecurity rejects insecure listener and authentication
// combinations only when the configured runtime environment is production.
func ValidateProductionSecurity(config *Config) error {
	if config == nil || !strings.EqualFold(strings.TrimSpace(config.Server.Environment), "production") {
		return nil
	}
	if !config.Auth.Enabled {
		return fmt.Errorf("production security: authentication must be enabled")
	}
	if strings.TrimSpace(config.Auth.InitialPassword) == "" || config.Auth.InitialPassword == "password" {
		return fmt.Errorf("production security: default or empty initial password is not allowed")
	}
	if config.Server.EnableCORS {
		for _, origin := range config.Server.CORSOrigins {
			if strings.TrimSpace(origin) == "*" {
				return fmt.Errorf("production security: wildcard CORS origin is not allowed")
			}
		}
	}
	if config.Server.HTTPEnabled && isPublicListener(config.Server.HTTPAddress) {
		return fmt.Errorf("production security: public plaintext HTTP listener is not allowed")
	}
	if config.Server.BoltEnabled && isPublicListener(config.Server.BoltAddress) && !config.Server.BoltTLSRequire {
		return fmt.Errorf("production security: public Bolt listener must require TLS")
	}
	if config.Features.QdrantGRPCEnabled && isPublicListener(config.Features.QdrantGRPCListenAddr) {
		return fmt.Errorf("production security: public plaintext gRPC listener is not allowed")
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
