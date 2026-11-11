package server

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A closed journal refuses a node's reset record. The HTTP clear then
// answers 500 and the node keeps its vectors. The regenerate route, which has
// already answered 202, logs one of its two failure outcomes and no
// successful clear, and skips its wake of the worker after the clear (no
// "worker triggered" event); the node keeps its vectors. The route restarts
// the worker before it clears, and that worker keeps running: these tests do
// not check the worker's state.

func closeJournal(t *testing.T, s *Server) {
	t.Helper()
	walEngine, ok := s.db.GetBaseStorageForManager().(*storage.WALEngine)
	require.True(t, ok, "the server's storage is journaled")
	require.NoError(t, walEngine.GetWAL().Close())
}

// eventCapture records the event IDs the server logs.
type eventCapture struct {
	mu  sync.Mutex
	ids []string
}

func (c *eventCapture) Enabled(context.Context, slog.Level) bool { return true }
func (c *eventCapture) WithAttrs([]slog.Attr) slog.Handler       { return c }
func (c *eventCapture) WithGroup(string) slog.Handler            { return c }
func (c *eventCapture) Handle(_ context.Context, record slog.Record) error {
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "event_id" {
			c.mu.Lock()
			c.ids = append(c.ids, attr.Value.String())
			c.mu.Unlock()
		}
		return true
	})
	return nil
}

func (c *eventCapture) has(id localization.EventID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, logged := range c.ids {
		if logged == string(id) {
			return true
		}
	}
	return false
}

func TestEmbedClearReportsARefusedJournalRecord(t *testing.T) {
	s, _ := setupTestServer(t)
	engine := seedEmbeddedNode(t, s, s.dbManager.DefaultDatabaseName(), "doc-journal-refused")
	closeJournal(t, s)

	rec := httptest.NewRecorder()
	s.handleEmbedClear(rec, as(httptest.NewRequest(http.MethodPost, "/nornicdb/embed/clear", nil), "admin"))
	require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	require.True(t, hasEmbedding(t, engine, "doc-journal-refused"), "the node keeps its vectors")
}

func TestEmbedRegenerateLogsNoSuccessAfterARefusedJournalRecord(t *testing.T) {
	capture := &eventCapture{}
	s, _ := setupTestServerWithConfig(t, func(c *Config) { c.Logger = slog.New(capture) })
	s.db.SetEmbedder(&countingEmbedder{dims: 1024})
	engine := seedEmbeddedNode(t, s, s.dbManager.DefaultDatabaseName(), "doc-journal-refused")
	closeJournal(t, s)

	rec := httptest.NewRecorder()
	s.handleEmbedTrigger(rec, as(httptest.NewRequest(http.MethodPost, "/nornicdb/embed/trigger?regenerate=true", nil), "admin"))
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())

	// The background clear ends with one of the route's outcome events. The
	// route logs an error whose text contains "closed", as a closed journal's
	// does, as "skipped: database closing", and any other error as "clear
	// failed". Either is accepted here.
	require.Eventually(t, func() bool {
		return anyLogged(capture, localization.EventServerEmbedClearFailed, localization.EventServerEmbedRegenerationSkippedDBClosing, localization.EventServerEmbedCleared)
	}, 30*time.Second, 20*time.Millisecond)
	require.True(t, anyLogged(capture, localization.EventServerEmbedClearFailed, localization.EventServerEmbedRegenerationSkippedDBClosing), "the clear's failure outcome is logged (clear failed, or skipped: database closing)")
	require.False(t, capture.has(localization.EventServerEmbedCleared), "no successful clear is logged")
	require.False(t, capture.has(localization.EventServerEmbedWorkerTriggered), "the wake after the clear is skipped: no worker-triggered event is logged")
	require.True(t, hasEmbedding(t, engine, "doc-journal-refused"), "the node keeps its vectors")
}

func anyLogged(capture *eventCapture, ids ...localization.EventID) bool {
	for _, id := range ids {
		if capture.has(id) {
			return true
		}
	}
	return false
}
