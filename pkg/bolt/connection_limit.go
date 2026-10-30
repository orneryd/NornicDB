package bolt

import (
	"context"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// connectionLimitLogInterval is the least time between two reports of
// connections turned away at MaxConnections, so a client retrying in a loop
// can't flood the log.
const connectionLimitLogInterval = 30 * time.Second

// connectionLimitLog counts the connections MaxConnections turns away since
// the last report, and when that report was made (Unix nanoseconds).
type connectionLimitLog struct {
	rejected   atomic.Int64
	lastLogged atomic.Int64
}

// logConnectionLimit records a connection closed at the connection limit.
// The first one is logged at once; after that, one warning per interval
// reports how many were turned away since the previous one.
func (s *Server) logConnectionLimit(conn net.Conn, maxConnections int, now time.Time) {
	limit := &s.connectionLimit
	limit.rejected.Add(1)
	for {
		last := limit.lastLogged.Load()
		if last != 0 && now.UnixNano()-last < int64(connectionLimitLogInterval) {
			return // reported this interval; the next report counts it
		}
		if limit.lastLogged.CompareAndSwap(last, now.UnixNano()) {
			break
		}
	}
	remote := ""
	if addr := conn.RemoteAddr(); addr != nil {
		remote = addr.String()
	}
	s.logEvent(context.Background(), slog.LevelWarn,
		localization.BoltConnectionLimitReachedEvent(remote, maxConnections, limit.rejected.Swap(0)))
}
