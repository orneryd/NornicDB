package main

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/spf13/cobra"
)

func TestResolveBindAddress(t *testing.T) {
	t.Run("uses_cli_address_when_flag_changed", func(t *testing.T) {
		cfg := config.LoadDefaults()
		cfg.Server.BoltAddress = "0.0.0.0"
		cfg.Server.HTTPAddress = "0.0.0.0"

		cmd := &cobra.Command{Use: "test"}
		cmd.Flags().String("address", "127.0.0.1", "")
		if err := cmd.Flags().Set("address", "127.0.0.1"); err != nil {
			t.Fatalf("set address flag: %v", err)
		}

		resolved := resolveBindAddress(cmd, cfg, "127.0.0.1", false)
		if resolved != "127.0.0.1" {
			t.Fatalf("expected CLI address to win, got %q", resolved)
		}
	})

	t.Run("uses_loaded_server_address_when_config_file_sets_host", func(t *testing.T) {
		cfg := config.LoadDefaults()
		cfg.Server.BoltAddress = "127.0.0.2"
		cfg.Server.HTTPAddress = "127.0.0.2"

		cmd := &cobra.Command{Use: "test"}
		cmd.Flags().String("address", "127.0.0.1", "")

		resolved := resolveBindAddress(cmd, cfg, "127.0.0.1", true)
		if resolved != "127.0.0.2" {
			t.Fatalf("expected loaded config address, got %q", resolved)
		}
	})

	t.Run("keeps_secure_default_when_no_explicit_config_exists", func(t *testing.T) {
		cfg := config.LoadDefaults()

		cmd := &cobra.Command{Use: "test"}
		cmd.Flags().String("address", "127.0.0.1", "")

		resolved := resolveBindAddress(cmd, cfg, "127.0.0.1", false)
		if resolved != "127.0.0.1" {
			t.Fatalf("expected loopback CLI default, got %q", resolved)
		}
	})

	t.Run("falls_back_to_protocol_address_when_env_explicitly_sets_it", func(t *testing.T) {
		cfg := config.LoadDefaults()
		cfg.Server.HTTPAddress = ""
		cfg.Server.BoltAddress = "127.0.0.2"
		t.Setenv("NORNICDB_BOLT_ADDRESS", "127.0.0.2")

		cmd := &cobra.Command{Use: "test"}
		cmd.Flags().String("address", "127.0.0.1", "")

		resolved := resolveBindAddress(cmd, cfg, "127.0.0.1", false)
		if resolved != "127.0.0.2" {
			t.Fatalf("expected Bolt address fallback, got %q", resolved)
		}
	})

	t.Run("defaults_to_loopback_when_empty", func(t *testing.T) {
		cmd := &cobra.Command{Use: "test"}
		cmd.Flags().String("address", "", "")

		resolved := resolveBindAddress(cmd, nil, "", false)
		if resolved != "127.0.0.1" {
			t.Fatalf("expected loopback default, got %q", resolved)
		}
	})
}
