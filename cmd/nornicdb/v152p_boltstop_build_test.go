package main

import (
	"context"
	"time"

	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// boltStopServer stops Bolt as this build's binary does (adapter_bolt.go
// Shutdown: srv.Shutdown(ctx)), with the 30 s budget of a clean stop.
func boltStopServer(server *bolt.Server) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return server.Shutdown(ctx)
}

func boltStopReadJournal(dir string) ([]storage.WALEntry, error) {
	return storage.ReadWALEntriesFromDir(context.Background(), dir)
}
