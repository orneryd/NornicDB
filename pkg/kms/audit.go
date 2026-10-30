package kms

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

type AuditEvent struct {
	EventType  string                 `json:"event_type"`
	KeyURI     string                 `json:"key_uri,omitempty"`
	Principal  string                 `json:"principal,omitempty"`
	Timestamp  time.Time              `json:"timestamp"`
	Status     string                 `json:"status"`
	ErrorCode  string                 `json:"error_code,omitempty"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
	Signature  string                 `json:"signature,omitempty"`
	ProviderID string                 `json:"provider_id,omitempty"`
}

// AuditSigner signs and verifies immutable KMS audit events.
type AuditSigner struct {
	key []byte
}

func NewAuditSigner(key []byte) *AuditSigner {
	k := make([]byte, len(key))
	copy(k, key)
	return &AuditSigner{key: k}
}

func (s *AuditSigner) Sign(event AuditEvent) (AuditEvent, error) {
	event.Signature = ""
	payload, err := json.Marshal(event)
	if err != nil {
		return AuditEvent{}, err
	}
	mac := hmac.New(sha256.New, s.key)
	mac.Write(payload)
	event.Signature = hex.EncodeToString(mac.Sum(nil))
	return event, nil
}

func (s *AuditSigner) Verify(event AuditEvent) bool {
	sig := event.Signature
	if sig == "" {
		return false
	}
	event.Signature = ""
	payload, err := json.Marshal(event)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, s.key)
	mac.Write(payload)
	expected := mac.Sum(nil)
	decoded, err := hex.DecodeString(sig)
	if err != nil {
		return false
	}
	return hmac.Equal(expected, decoded)
}

// NopAuditProvider can be embedded by providers that don't persist audit events.
type NopAuditProvider struct{}

func (NopAuditProvider) SignAuditEvent(context.Context, AuditEvent) error {
	return nil
}
