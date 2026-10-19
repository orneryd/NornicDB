package server

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
)

func TestCanonicalRedactKeysConfigExtensions(t *testing.T) {
	previous := observability.DefaultRedactKeys
	t.Cleanup(func() { observability.DefaultRedactKeys = previous })
	observability.DefaultRedactKeys = append(append([]string(nil), previous...), "custom.sealed_value")
	const credential = "synthetic-canonical-config-719832"
	settings := map[string]string{"custom.sealed_value": credential, "custom.sealed_value_path": "public-path", "public.limit": "42"}
	displayed := redactDatabaseSettings(settings)
	if displayed["custom.sealed_value"] != observability.RedactedPlaceholder() {
		t.Error("canonical extension escaped config view; capture withheld")
	}
	if displayed["custom.sealed_value_path"] != "public-path" || displayed["public.limit"] != "42" || settings["custom.sealed_value"] != credential {
		t.Error("config projection changed public metadata or runtime values")
	}
}
