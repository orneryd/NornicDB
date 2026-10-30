package security

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSecurityMiddlewareWithConfig_Development(t *testing.T) {
	cfg := SecurityConfig{
		Environment: "development",
		AllowHTTP:   true,
	}
	m := NewSecurityMiddlewareWithConfig(cfg)
	assert.True(t, m.isDevelopment)
	assert.True(t, m.allowHTTP)
}

func TestNewSecurityMiddlewareWithConfig_Production(t *testing.T) {
	cfg := SecurityConfig{
		Environment: "production",
		AllowHTTP:   false,
	}
	m := NewSecurityMiddlewareWithConfig(cfg)
	assert.False(t, m.isDevelopment)
	assert.False(t, m.allowHTTP)
}

func TestNewSecurityMiddlewareWithConfig_DevShorthand(t *testing.T) {
	cfg := SecurityConfig{Environment: "dev"}
	m := NewSecurityMiddlewareWithConfig(cfg)
	assert.True(t, m.isDevelopment)
}

func TestNewSecurityMiddlewareWithConfig_Empty(t *testing.T) {
	m := NewSecurityMiddlewareWithConfig(SecurityConfig{})
	assert.True(t, m.isDevelopment) // empty environment defaults to dev
}

func TestSecurityMiddleware_Wrap_PassesThrough(t *testing.T) {
	m := NewSecurityMiddlewareWithConfig(SecurityConfig{Environment: "development", AllowHTTP: true})

	handlerCalled := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	})

	wrapped := m.Wrap(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	wrapped.ServeHTTP(w, req)

	assert.True(t, handlerCalled)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestSecurityMiddleware_Wrap_BlocksBadHeader(t *testing.T) {
	m := NewSecurityMiddlewareWithConfig(SecurityConfig{Environment: "production"})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	wrapped := m.Wrap(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	// Inject a header with null byte (injection attack)
	req.Header.Set("X-Custom", "value\x00injection")
	w := httptest.NewRecorder()
	wrapped.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}
