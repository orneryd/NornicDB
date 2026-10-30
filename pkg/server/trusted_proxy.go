package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strings"
)

const contextKeyTrustedProxy = contextKey("trusted-proxy")

var forwardedHeaders = []string{
	"Forwarded",
	"X-Base-Path",
	"X-Forwarded-For",
	"X-Forwarded-Host",
	"X-Forwarded-Port",
	"X-Forwarded-Prefix",
	"X-Forwarded-Proto",
	"X-Real-IP",
}

func parseTrustedProxyPrefixes(proxies []string) ([]netip.Prefix, error) {
	prefixes := make([]netip.Prefix, 0, len(proxies))
	for _, proxy := range proxies {
		proxy = strings.TrimSpace(proxy)
		prefix, err := netip.ParsePrefix(proxy)
		if err != nil {
			addr, addrErr := netip.ParseAddr(proxy)
			if addrErr != nil {
				return nil, fmt.Errorf("invalid trusted proxy %q: expected an IP address or CIDR", proxy)
			}
			addr = addr.Unmap()
			prefix = netip.PrefixFrom(addr, addr.BitLen())
		}
		prefixes = append(prefixes, prefix)
	}
	return prefixes, nil
}

func (s *Server) trustedProxyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.isTrustedProxyPeer(r.RemoteAddr) {
			for _, header := range forwardedHeaders {
				r.Header.Del(header)
			}
			next.ServeHTTP(w, r)
			return
		}

		ctx := context.WithValue(r.Context(), contextKeyTrustedProxy, true)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) isTrustedProxyPeer(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(remoteAddr))
	if err != nil {
		host = strings.Trim(strings.TrimSpace(remoteAddr), "[]")
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	addr = addr.Unmap()
	for _, prefix := range s.trustedProxyPrefixes {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}
