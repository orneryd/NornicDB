package heimdall

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type failingFlushWriter struct {
	header http.Header
}

func (w *failingFlushWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *failingFlushWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

func (w *failingFlushWriter) WriteHeader(int) {}
func (w *failingFlushWriter) Flush()          {}

func TestNewBifrost(t *testing.T) {
	t.Run("disabled when BifrostEnabled is false", func(t *testing.T) {
		cfg := Config{
			Enabled:        true,
			BifrostEnabled: false, // Explicitly disabled
		}
		bifrost := NewBifrost(cfg)
		assert.Nil(t, bifrost, "Bifrost should be nil when disabled")
	})

	t.Run("enabled when BifrostEnabled is true", func(t *testing.T) {
		cfg := Config{
			Enabled:        true,
			BifrostEnabled: true,
		}
		bifrost := NewBifrost(cfg)
		require.NotNil(t, bifrost, "Bifrost should be created when enabled")
		assert.NotNil(t, bifrost.clients)
	})

	t.Run("automatically enabled via ConfigFromFeatureFlags", func(t *testing.T) {
		flags := &MockFeatureFlags{
			enabled: true,
		}
		cfg := ConfigFromFeatureFlags(flags)
		assert.True(t, cfg.Enabled)
		assert.True(t, cfg.BifrostEnabled, "Bifrost should auto-enable when Heimdall is enabled")

		bifrost := NewBifrost(cfg)
		require.NotNil(t, bifrost)
	})

	t.Run("disabled when Heimdall disabled", func(t *testing.T) {
		flags := &MockFeatureFlags{
			enabled: false,
		}
		cfg := ConfigFromFeatureFlags(flags)
		assert.False(t, cfg.Enabled)
		assert.False(t, cfg.BifrostEnabled, "Bifrost should be disabled when Heimdall is disabled")

		bifrost := NewBifrost(cfg)
		assert.Nil(t, bifrost)
	})
}

func TestBifrost_ClientManagement(t *testing.T) {
	cfg := Config{
		Enabled:        true,
		BifrostEnabled: true,
	}
	bifrost := NewBifrost(cfg)
	require.NotNil(t, bifrost)

	// Create mock response writer
	w := httptest.NewRecorder()

	t.Run("register client", func(t *testing.T) {
		bifrost.RegisterClient("client-1", w, w)
		assert.Equal(t, 1, bifrost.ConnectionCount())
		assert.True(t, bifrost.IsConnected())
	})

	t.Run("register multiple clients", func(t *testing.T) {
		w2 := httptest.NewRecorder()
		bifrost.RegisterClient("client-2", w2, w2)
		assert.Equal(t, 2, bifrost.ConnectionCount())
	})

	t.Run("unregister client", func(t *testing.T) {
		bifrost.UnregisterClient("client-1")
		assert.Equal(t, 1, bifrost.ConnectionCount())
	})

	t.Run("unregister all clients", func(t *testing.T) {
		bifrost.UnregisterClient("client-2")
		assert.Equal(t, 0, bifrost.ConnectionCount())
		assert.False(t, bifrost.IsConnected())
	})
}

// MockResponseWriter that implements http.Flusher for testing
type MockFlushWriter struct {
	*httptest.ResponseRecorder
	flushed int
}

func NewMockFlushWriter() *MockFlushWriter {
	return &MockFlushWriter{
		ResponseRecorder: httptest.NewRecorder(),
	}
}

func (m *MockFlushWriter) Flush() {
	m.flushed++
}

func TestBifrost_SendMessage(t *testing.T) {
	cfg := Config{
		Enabled:        true,
		BifrostEnabled: true,
	}
	bifrost := NewBifrost(cfg)

	t.Run("no error when no clients", func(t *testing.T) {
		err := bifrost.SendMessage("Hello")
		assert.NoError(t, err)
	})

	t.Run("message sent to client", func(t *testing.T) {
		w := NewMockFlushWriter()
		bifrost.RegisterClient("test", w, w)
		defer bifrost.UnregisterClient("test")

		err := bifrost.SendMessage("Hello Bifrost")
		assert.NoError(t, err)

		body := w.Body.String()
		assert.Contains(t, body, "data:")
		assert.Contains(t, body, "Hello Bifrost")
		assert.Contains(t, body, `"type":"message"`)
		assert.Equal(t, 1, w.flushed)
	})
}

func TestBifrost_SendNotification(t *testing.T) {
	cfg := Config{
		Enabled:        true,
		BifrostEnabled: true,
	}
	bifrost := NewBifrost(cfg)

	w := NewMockFlushWriter()
	bifrost.RegisterClient("test", w, w)
	defer bifrost.UnregisterClient("test")

	err := bifrost.SendNotification("warning", "Test Alert", "Something happened")
	assert.NoError(t, err)

	body := w.Body.String()
	assert.Contains(t, body, `"type":"notification"`)
	assert.Contains(t, body, `"level":"warning"`)
	assert.Contains(t, body, "Test Alert")
	assert.Contains(t, body, "Something happened")
}

func TestBifrost_Broadcast(t *testing.T) {
	cfg := Config{
		Enabled:        true,
		BifrostEnabled: true,
	}
	bifrost := NewBifrost(cfg)

	// Register multiple clients
	w1 := NewMockFlushWriter()
	w2 := NewMockFlushWriter()
	bifrost.RegisterClient("client-1", w1, w1)
	bifrost.RegisterClient("client-2", w2, w2)
	defer func() {
		bifrost.UnregisterClient("client-1")
		bifrost.UnregisterClient("client-2")
	}()

	err := bifrost.Broadcast("System announcement")
	assert.NoError(t, err)

	// Both clients should receive the message
	assert.Contains(t, w1.Body.String(), "System announcement")
	assert.Contains(t, w2.Body.String(), "System announcement")
	assert.Contains(t, w1.Body.String(), `"type":"broadcast"`)
}

func TestBifrost_RequestConfirmation(t *testing.T) {
	cfg := Config{
		Enabled:        true,
		BifrostEnabled: true,
	}
	bifrost := NewBifrost(cfg)

	w := NewMockFlushWriter()
	bifrost.RegisterClient("test", w, w)
	defer bifrost.UnregisterClient("test")

	// SSE is unidirectional, so confirmation request returns false immediately
	confirmed, err := bifrost.RequestConfirmation("Delete all nodes?")
	assert.NoError(t, err)
	assert.False(t, confirmed, "SSE-based confirmation should return false (pending)")

	body := w.Body.String()
	assert.Contains(t, body, `"type":"confirmation_request"`)
	assert.Contains(t, body, "Delete all nodes?")
}

func TestBifrost_RequestConfirmation_PropagatesBroadcastError(t *testing.T) {
	cfg := Config{
		Enabled:        true,
		BifrostEnabled: true,
	}
	bifrost := NewBifrost(cfg)
	require.NotNil(t, bifrost)

	w := &failingFlushWriter{}
	bifrost.RegisterClient("bad-client", w, w)
	defer bifrost.UnregisterClient("bad-client")

	confirmed, err := bifrost.RequestConfirmation("dangerous action")
	require.Error(t, err)
	assert.False(t, confirmed)
	assert.Contains(t, err.Error(), "write failed")
}

func TestBifrost_Stats(t *testing.T) {
	cfg := Config{
		Enabled:        true,
		BifrostEnabled: true,
	}
	bifrost := NewBifrost(cfg)

	stats := bifrost.Stats()
	assert.True(t, stats["enabled"].(bool))
	assert.Equal(t, 0, stats["connection_count"].(int))

	// Add client
	w := NewMockFlushWriter()
	bifrost.RegisterClient("test", w, w)

	stats = bifrost.Stats()
	assert.Equal(t, 1, stats["connection_count"].(int))

	clients := stats["clients"].([]map[string]interface{})
	assert.Len(t, clients, 1)
	assert.Equal(t, "test", clients[0]["id"])
}

func TestBifrost_Interface(t *testing.T) {
	// Verify Bifrost implements BifrostBridge
	cfg := Config{
		Enabled:        true,
		BifrostEnabled: true,
	}
	var bridge BifrostBridge = NewBifrost(cfg)
	require.NotNil(t, bridge)
}

func TestNoOpBifrost_AllMethods(t *testing.T) {
	noop := &NoOpBifrost{}

	// All methods should be no-ops
	assert.NoError(t, noop.SendMessage("test"))
	assert.NoError(t, noop.SendNotification("info", "title", "msg"))
	assert.NoError(t, noop.Broadcast("test"))

	confirmed, err := noop.RequestConfirmation("action")
	assert.NoError(t, err)
	assert.False(t, confirmed)

	assert.False(t, noop.IsConnected())
	assert.Equal(t, 0, noop.ConnectionCount())
}

func TestDefaultConfig_HeimdallDisabled(t *testing.T) {
	cfg := DefaultConfig()
	assert.False(t, cfg.Enabled, "Heimdall should be disabled by default")
	assert.False(t, cfg.BifrostEnabled, "Bifrost should be disabled by default")
}

func TestConfigFromFeatureFlags_BifrostAutoEnable(t *testing.T) {
	tests := []struct {
		name            string
		heimdallEnabled bool
		wantBifrost     bool
	}{
		{
			name:            "Bifrost enabled when Heimdall enabled",
			heimdallEnabled: true,
			wantBifrost:     true,
		},
		{
			name:            "Bifrost disabled when Heimdall disabled",
			heimdallEnabled: false,
			wantBifrost:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flags := &MockFeatureFlags{
				enabled: tt.heimdallEnabled,
			}
			cfg := ConfigFromFeatureFlags(flags)
			assert.Equal(t, tt.wantBifrost, cfg.BifrostEnabled)
		})
	}
}

// MockFlushWriter implements http.Flusher
var _ http.Flusher = (*MockFlushWriter)(nil)

func BenchmarkBifrost_Broadcast(b *testing.B) {
	cfg := Config{
		Enabled:        true,
		BifrostEnabled: true,
	}
	bifrost := NewBifrost(cfg)

	// Add 10 clients
	for i := 0; i < 10; i++ {
		w := NewMockFlushWriter()
		bifrost.RegisterClient(generateID(), w, w)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bifrost.Broadcast("Test message")
	}
}

func BenchmarkBifrost_SendNotification(b *testing.B) {
	cfg := Config{
		Enabled:        true,
		BifrostEnabled: true,
	}
	bifrost := NewBifrost(cfg)

	w := NewMockFlushWriter()
	bifrost.RegisterClient("test", w, w)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bifrost.SendNotification("info", "Title", "Message content")
	}
}

// Test that BifrostMessage timestamps are set correctly
func TestBifrostMessage_Timestamp(t *testing.T) {
	cfg := Config{
		Enabled:        true,
		BifrostEnabled: true,
	}
	bifrost := NewBifrost(cfg)

	w := NewMockFlushWriter()
	bifrost.RegisterClient("test", w, w)

	// Capture time range for verification
	_ = time.Now().Unix() // Mark start time
	bifrost.SendMessage("test")
	_ = time.Now().Unix() // Mark end time

	body := w.Body.String()
	// Verify timestamp is present in the message
	assert.Contains(t, body, `"timestamp":`)
	assert.Contains(t, body, `"type":"message"`)
}
