package storage

import (
	"encoding/json"
	"log"
)

// WALLogger receives structured diagnostics emitted by WAL recovery / corruption handlers.
//
// This is intentionally minimal to avoid coupling storage to a specific logging library.
// Implementations should treat fields as a stable machine-readable contract.
type WALLogger interface {
	Log(level string, msg string, fields map[string]any)
}

type defaultWALLogger struct{}

func (defaultWALLogger) Log(level string, msg string, fields map[string]any) {
	// Best-effort structured printing using stdlib log.
	payload := map[string]any{
		"level": level,
		"msg":   msg,
	}
	for k, v := range fields {
		payload[k] = v
	}
	b, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[wal] level=%s msg=%s fields=%v", level, msg, fields)
		return
	}
	log.Printf("[wal] %s", string(b))
}

