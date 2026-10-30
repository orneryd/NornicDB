package main

import (
	"strings"
	"testing"
)

func TestEvalURLPolicy(t *testing.T) {
	for _, tc := range []struct {
		name, endpoint  string
		remote, allowed bool
	}{
		{"local IPv4", "http://127.0.0.1:7474", false, true},
		{"local IPv6", "http://[::1]:7474", false, true},
		{"long local URL", "http://127.0.0.1:7474/" + strings.Repeat("x", 2100), false, true},
		{"userinfo rejected by default", "http://user:password@127.0.0.1:7474", false, false},
		{"remote blocked by default", "https://192.168.1.2:7474", false, false},
		{"explicit remote LAN", "http://192.168.1.2:7474", true, true},
		{"explicit long remote URL", "https://example.invalid/" + strings.Repeat("x", 2100), true, true},
		{"userinfo rejected with remote override", "http://user:password@127.0.0.1:7474", true, false},
		{"non HTTP rejected with remote override", "file:///tmp/eval", true, false},
		{"host required with remote override", "http:///eval", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("NORNICDB_ALLOW_REMOTE_EVAL", "false")
			if tc.remote {
				t.Setenv("NORNICDB_ALLOW_REMOTE_EVAL", "true")
			}
			if err := validateURL(tc.endpoint); (err == nil) != tc.allowed {
				t.Fatalf("URL policy acceptance = %v, want %v", err == nil, tc.allowed)
			}
		})
	}
}
