package kms

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type AuditArchiverConfig struct {
	LocalPath  string
	SignEvents bool
	SignKey    []byte
}

type AuditArchiver struct {
	mu     sync.Mutex
	path   string
	signer *AuditSigner
}

func NewAuditArchiver(cfg AuditArchiverConfig) (*AuditArchiver, error) {
	if cfg.LocalPath == "" {
		return nil, fmt.Errorf("audit archiver path is required")
	}
	var signer *AuditSigner
	if cfg.SignEvents {
		if len(cfg.SignKey) == 0 {
			return nil, fmt.Errorf("audit signing key is required when sign_events is enabled")
		}
		signer = NewAuditSigner(cfg.SignKey)
	}
	return &AuditArchiver{
		path:   cfg.LocalPath,
		signer: signer,
	}, nil
}

func (a *AuditArchiver) Archive(ctx context.Context, event AuditEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	if a.signer != nil {
		signed, err := a.signer.Sign(event)
		if err != nil {
			return err
		}
		event = signed
	}
	line, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(a.path), 0700); err != nil {
		return err
	}
	f, err := os.OpenFile(a.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(append(line, '\n')); err != nil {
		return err
	}
	return nil
}
