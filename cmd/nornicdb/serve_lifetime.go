package main

import (
	"context"
	"fmt"
	"os"

	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/errors/compose"
	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/server"
)

// serveLifetime is the command's existing return-frame ownership. Construction
// enrolls each resource immediately; the component slice transfers to Run only
// after setup succeeds. Every return uses this same retirement path.
type serveLifetime struct {
	db            *nornicdb.DB
	http          *server.Server
	bolt          *bolt.Server
	boltLifetime  *lifecycle.WorkLifetime
	writer        *os.File
	stopCompactor func() error
	components    []lifecycle.Component
	supervised    bool
}

func (s *serveLifetime) close() error {
	var result error
	if s.bolt != nil {
		s.bolt.BeginShutdown()
	} else if s.boltLifetime != nil {
		s.boltLifetime.CloseAdmission()
		s.boltLifetime.Cancel()
	}
	if s.http != nil {
		budget, cancel := context.WithTimeout(context.Background(), lifecycle.ShutdownTimeout)
		result = compose.Outcomes(result, s.http.Stop(budget))
		cancel()
		result = compose.Outcomes(result, s.http.WaitStopped(context.Background()))
	}
	if s.bolt != nil {
		result = compose.Outcomes(result, s.bolt.Close())
	} else if s.boltLifetime != nil {
		result = compose.Outcomes(result, s.boltLifetime.Close())
	}
	// Before Run, this is the same component slice's construction phase. Its
	// entries own already-acquired native resources; non-started sentinels are
	// appended only by the final assembly, immediately before supervision.
	if !s.supervised {
		result = compose.Outcomes(result, lifecycle.DrainReverse(context.Background(), s.components))
	}
	if s.db != nil {
		result = compose.Outcomes(result, s.db.Close())
	}
	// NewLogger returns either its newly opened file or a borrowed process
	// stream. Keep the owned file alive through the last DB disposal log.
	if s.writer != nil {
		if err := s.writer.Close(); err != nil {
			result = compose.Outcomes(result, fmt.Errorf("command logger close: %w", err))
		}
	}
	if s.stopCompactor != nil {
		result = compose.Outcomes(result, s.stopCompactor())
	}
	return result
}

// providerConstructionComponent occupies telemetry's registration slot until
// the listener adopts the same provider. It is never started: failed setup
// drains it directly, and successful setup replaces it with the listener.
type providerConstructionComponent struct{ *observability.Provider }

func (*providerConstructionComponent) Name() string                { return "telemetry provider" }
func (*providerConstructionComponent) Start(context.Context) error { return nil }
