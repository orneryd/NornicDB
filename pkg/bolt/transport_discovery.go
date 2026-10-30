package bolt

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// startDiscoveryRefresher pre-encodes the discovery response and starts a
// 1s ticker that refreshes the Date header. The pre-encoded bytes are
// stored under atomic.Pointer for lock-free reads on the hot path.
//
// Returns the initial encoding error if the OAuth config fails URL
// validation; in that case the server should fail startup.
func (s *Server) startDiscoveryRefresher(parents ...context.Context) error {
	initial, err := buildDiscoveryResponse(s.config.OAuthConfig, time.Now())
	if err != nil {
		return err
	}
	s.discoveryResponse.Store(&initial)
	w := s.lifetime()
	parent := w.Context()
	if len(parents) > 0 {
		parent = parents[0]
	}
	ctx, release, accepted := w.Acquire(parent)
	if !accepted {
		return fmt.Errorf("bolt server is closed")
	}
	s.lifecycleMu.Lock()
	if s.closed.Load() || s.discoveryStarted {
		s.lifecycleMu.Unlock()
		release()
		return nil
	}
	s.discoveryStarted = true
	s.lifecycleMu.Unlock()
	go func() {
		defer release()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				bytes, err := buildDiscoveryResponse(s.config.OAuthConfig, now)
				if err != nil {
					// Validation errors are surfaced at startup; a
					// runtime failure here means OAuthConfig was mutated.
					// Log but keep the previous bytes.
					w.RecordError("Bolt discovery refresh", err)
					s.logEvent(context.Background(), slog.LevelWarn, localization.BoltDiscoveryRefreshFailedEvent(err))
					continue
				}
				s.discoveryResponse.Store(&bytes)
			}
		}
	}()
	return nil
}

// serveDiscovery writes the cached pre-encoded discovery response to conn
// and closes (the caller closes conn via the outer defer). If the cache
// is empty (refresher never started — happens in tests that bypass
// ListenAndServe) it builds and writes a one-shot response.
func (s *Server) serveDiscovery(conn net.Conn) {
	cached := s.discoveryResponse.Load()
	if cached == nil {
		bytes, err := buildDiscoveryResponse(s.config.OAuthConfig, time.Now())
		if err != nil {
			// Should be impossible at this point because startup
			// already validated; defensive write of a minimal 200.
			_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
			return
		}
		_, _ = conn.Write(bytes)
		return
	}
	_, _ = conn.Write(*cached)
}
