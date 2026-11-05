package security

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateURL_TooLong(t *testing.T) {
	long := "https://example.com/" + string(make([]byte, MaxURLLength+1))
	err := ValidateURL(long, URLPolicy{})
	assert.ErrorIs(t, err, ErrURLTooLong)
}

func TestValidateURL_Empty(t *testing.T) {
	err := ValidateURL("", URLPolicy{})
	assert.ErrorIs(t, err, ErrURLInvalid)

	err = ValidateURL("   ", URLPolicy{})
	assert.ErrorIs(t, err, ErrURLInvalid)
}

func TestValidateURL_BadScheme(t *testing.T) {
	err := ValidateURL("ftp://example.com/data", URLPolicy{})
	assert.ErrorIs(t, err, ErrURLInvalidProtocol)
}

func TestValidateURL_HTTPNotAllowed(t *testing.T) {
	err := ValidateURL("http://example.com/api", URLPolicy{})
	assert.ErrorIs(t, err, ErrURLHTTPNotAllowed)
}

func TestValidateURL_HTTPAllowedInDev(t *testing.T) {
	// isDevelopment=true → HTTP is allowed
	err := ValidateURL("http://example.com/api", URLPolicy{AllowHTTP: true, HostPolicy: URLDevelopmentHost})
	assert.NoError(t, err)
}

func TestValidateURL_HTTPAllowedByFlag(t *testing.T) {
	err := ValidateURL("http://example.com/api", URLPolicy{AllowHTTP: true})
	assert.NoError(t, err)
}

func TestValidateURL_Localhost_Production(t *testing.T) {
	err := ValidateURL("https://localhost:8080/api", URLPolicy{})
	assert.ErrorIs(t, err, ErrURLLocalhost)
}

func TestValidateURL_Localhost_Dev(t *testing.T) {
	// isDevelopment=true → localhost is allowed (no hostname check applied)
	err := ValidateURL("https://localhost:8080/api", URLPolicy{AllowHTTP: true, HostPolicy: URLDevelopmentHost})
	assert.NoError(t, err)
}

func TestValidateURL_LoopbackIP_Dev(t *testing.T) {
	// 127.0.0.1 is loopback → allowed in dev mode
	err := ValidateURL("http://127.0.0.1:9200/index", URLPolicy{AllowHTTP: true, HostPolicy: URLDevelopmentHost})
	assert.NoError(t, err)
}

func TestValidateURL_PrivateIP_10x(t *testing.T) {
	err := ValidateURL("https://10.0.0.1/api", URLPolicy{})
	assert.ErrorIs(t, err, ErrURLPrivateIP)
}

func TestValidateURL_PrivateIP_172(t *testing.T) {
	err := ValidateURL("https://172.20.0.1/api", URLPolicy{})
	assert.ErrorIs(t, err, ErrURLPrivateIP)
}

func TestValidateURL_PrivateIP_192168(t *testing.T) {
	err := ValidateURL("https://192.168.1.100/api", URLPolicy{})
	assert.ErrorIs(t, err, ErrURLPrivateIP)
}

func TestValidateURL_PrivateIP_LinkLocal(t *testing.T) {
	err := ValidateURL("https://169.254.1.1/api", URLPolicy{})
	assert.ErrorIs(t, err, ErrURLPrivateIP)
}

func TestValidateURL_PublicIP(t *testing.T) {
	// Public IP should pass
	err := ValidateURL("https://8.8.8.8/api", URLPolicy{})
	assert.NoError(t, err)
}

func TestValidateURL_ValidPublicHTTPS(t *testing.T) {
	err := ValidateURL("https://example.com/path?q=1", URLPolicy{})
	assert.NoError(t, err)
}

func TestIsPrivateIP_Loopback(t *testing.T) {
	// 127.0.0.2 is loopback → private → blocked in production
	err := ValidateURL("https://127.0.0.2/api", URLPolicy{})
	assert.ErrorIs(t, err, ErrURLPrivateIP)
}
