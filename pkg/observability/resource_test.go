package observability

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
)

// TestResolveInstanceID covers OBS-10: cfg.NodeID → POD_NAME → os.Hostname() → "standalone".
func TestResolveInstanceID(t *testing.T) {
	t.Run("nodeID wins", func(t *testing.T) {
		t.Setenv("POD_NAME", "pod-from-env")
		id, src := resolveInstanceID("node-from-cfg")
		assert.Equal(t, "node-from-cfg", id)
		assert.Equal(t, "config", src)
	})

	t.Run("POD_NAME used when NodeID empty", func(t *testing.T) {
		t.Setenv("POD_NAME", "pod-from-env")
		id, src := resolveInstanceID("")
		assert.Equal(t, "pod-from-env", id)
		assert.Equal(t, "POD_NAME", src)
	})

	t.Run("hostname used when NodeID and POD_NAME empty", func(t *testing.T) {
		t.Setenv("POD_NAME", "")
		// Save and restore the package-private hook for hostname injection.
		orig := hostnameFn
		t.Cleanup(func() { hostnameFn = orig })

		hostnameFn = func() (string, error) { return "test-host", nil }
		id, src := resolveInstanceID("")
		assert.Equal(t, "test-host", id)
		assert.Equal(t, "hostname", src)
	})

	t.Run("standalone fallback when hostname errors", func(t *testing.T) {
		t.Setenv("POD_NAME", "")
		orig := hostnameFn
		t.Cleanup(func() { hostnameFn = orig })

		hostnameFn = func() (string, error) { return "", errors.New("no hostname") }
		id, src := resolveInstanceID("")
		assert.Equal(t, "standalone", id)
		assert.Equal(t, "fallback", src)
	})

	t.Run("real os.Hostname leg works", func(t *testing.T) {
		t.Setenv("POD_NAME", "")
		// Use the real hostnameFn (default = os.Hostname).
		host, err := os.Hostname()
		if err != nil || host == "" {
			t.Skip("os.Hostname not available on this system")
		}
		id, src := resolveInstanceID("")
		assert.Equal(t, host, id)
		assert.Equal(t, "hostname", src)
	})
}

// TestBuildResource_MergesExtraAttrs verifies that ExtraResourceAttrs
// override semconv defaults (resource.Merge is last-wins).
func TestBuildResource_MergesExtraAttrs(t *testing.T) {
	info := ServiceInfo{
		Name:    "nornicdb",
		Version: "v1.0.0-test",
		NodeID:  "node-1",
		ExtraResourceAttrs: []attribute.KeyValue{
			attribute.String("nornicdb.cluster.mode", "standalone"),
			// Deliberately override service.name to test last-wins.
			attribute.String("service.name", "nornicdb-override"),
		},
	}
	res := buildResource(info)
	require.NotNil(t, res)

	attrs := map[string]string{}
	for _, a := range res.Attributes() {
		attrs[string(a.Key)] = a.Value.AsString()
	}

	// Extra attrs override semconv defaults.
	assert.Equal(t, "nornicdb-override", attrs["service.name"])
	assert.Equal(t, "v1.0.0-test", attrs["service.version"])
	assert.Equal(t, "node-1", attrs["service.instance.id"])
	assert.Equal(t, "standalone", attrs["nornicdb.cluster.mode"])
}
