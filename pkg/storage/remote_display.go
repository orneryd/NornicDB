package storage

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// These are the client's actual credential representations. The display owner
// handles wire encodings and URL components; execution keeps the original config.
func remoteDisplayCredentials(cfg RemoteEngineConfig) []string {
	var credentials []string
	user, password, token := strings.TrimSpace(cfg.User), strings.TrimSpace(cfg.Password), strings.TrimSpace(cfg.AuthToken)
	if cfg.User != "" {
		// URI normalization can move a username-only credential into User.
		// Retain that owned scalar as well as the Basic user:password form.
		credentials = append(credentials, cfg.User, user)
	}
	if cfg.Password != "" {
		credentials = append(credentials, cfg.Password, password)
	}
	if token != "" {
		credentials = append(credentials, cfg.AuthToken, token)
	}
	if cfg.User != "" || cfg.Password != "" {
		credentials = append(credentials, cfg.User+":"+cfg.Password, user+":"+password)
	}
	return observability.ClientCredentials(cfg.URI, credentials...)
}

func (h *httpTransport) displayCredentials() []string {
	return remoteDisplayCredentials(RemoteEngineConfig{URI: h.baseURL, User: h.user, Password: h.password, AuthToken: h.authToken})
}
