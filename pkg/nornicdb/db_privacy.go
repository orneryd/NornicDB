package nornicdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// =============================================================================
// GDPR Consent Management
// =============================================================================

// Consent represents a user's consent for a specific purpose.
// Implements GDPR Article 7 requirements for consent management.
type Consent struct {
	UserID    string    `json:"user_id"`
	Purpose   string    `json:"purpose"`
	Given     bool      `json:"given"`
	Timestamp time.Time `json:"timestamp"`
	Source    string    `json:"source"` // e.g., "web_form", "api", "email"
}

// RecordConsent records a user's consent for a specific purpose (GDPR compliance).
// Creates or updates a consent record in the database.
//
// Example:
//
//	err := db.RecordConsent(ctx, &Consent{
//	    UserID:  "user-123",
//	    Purpose: "marketing",
//	    Given:   true,
//	    Source:  "web_form",
//	})
func (db *DB) RecordConsent(ctx context.Context, consent *Consent) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	if consent.UserID == "" {
		return localizedError(localization.NornicDBCoreConsentUserIDRequired(), nil)
	}
	if consent.Purpose == "" {
		return localizedError(localization.NornicDBCoreConsentPurposeRequired(), nil)
	}

	// Set timestamp if not provided
	if consent.Timestamp.IsZero() {
		consent.Timestamp = time.Now()
	}

	// Create consent node ID
	consentID := storage.NodeID(fmt.Sprintf("consent:%s:%s", consent.UserID, consent.Purpose))

	// Check if consent already exists
	existingNode, err := db.storage.GetNode(consentID)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return localizedError(localization.NornicDBCoreConsentExistingCheckFailed(err), err)
	}

	props := map[string]interface{}{
		"user_id":   consent.UserID,
		"purpose":   consent.Purpose,
		"given":     consent.Given,
		"timestamp": consent.Timestamp.Format(time.RFC3339),
		"source":    consent.Source,
	}

	if existingNode != nil {
		// Update existing consent
		existingNode.Properties = props
		return db.storage.UpdateNode(existingNode)
	}

	// Create new consent node
	node := &storage.Node{
		ID:         consentID,
		Labels:     []string{"Consent"},
		Properties: props,
	}

	_, err = db.storage.CreateNode(node)
	return err
}

// HasConsent checks if a user has given consent for a specific purpose.
// Returns true if consent was given, false otherwise.
//
// Example:
//
//	hasConsent, err := db.HasConsent(ctx, "user-123", "marketing")
//	if err != nil {
//	    return err
//	}
//	if !hasConsent {
//	    return ErrNoConsent
//	}
func (db *DB) HasConsent(ctx context.Context, userID, purpose string) (bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return false, ErrClosed
	}

	consentID := storage.NodeID(fmt.Sprintf("consent:%s:%s", userID, purpose))
	node, err := db.storage.GetNode(consentID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return false, nil // No consent record = no consent given
		}
		return false, localizedError(localization.NornicDBCoreConsentCheckFailed(err), err)
	}

	given, ok := node.Properties["given"].(bool)
	if !ok {
		return false, nil
	}

	return given, nil
}

// RevokeConsent revokes a user's consent for a specific purpose.
// Sets the consent to false and updates the timestamp.
//
// Example:
//
//	err := db.RevokeConsent(ctx, "user-123", "marketing")
func (db *DB) RevokeConsent(ctx context.Context, userID, purpose string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	consentID := storage.NodeID(fmt.Sprintf("consent:%s:%s", userID, purpose))
	node, err := db.storage.GetNode(consentID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			// Create a revoked consent record for audit trail
			node = &storage.Node{
				ID:     consentID,
				Labels: []string{"Consent"},
				Properties: map[string]interface{}{
					"user_id":   userID,
					"purpose":   purpose,
					"given":     false,
					"timestamp": time.Now().Format(time.RFC3339),
					"source":    "revocation",
				},
			}
			_, err := db.storage.CreateNode(node)
			return err
		}
		return localizedError(localization.NornicDBCoreConsentGetFailed(err), err)
	}

	// Update existing consent to revoked
	node.Properties["given"] = false
	node.Properties["timestamp"] = time.Now().Format(time.RFC3339)
	node.Properties["source"] = "revocation"

	return db.storage.UpdateNode(node)
}

// GetUserConsents returns all consent records for a user.
// Useful for GDPR data access requests.
func (db *DB) GetUserConsents(ctx context.Context, userID string) ([]Consent, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	var consents []Consent
	prefix := fmt.Sprintf("consent:%s:", userID)

	err := storage.StreamNodesWithFallback(ctx, db.storage, 1000, func(n *storage.Node) error {
		if strings.HasPrefix(string(n.ID), prefix) {
			consent := Consent{
				UserID:  userID,
				Purpose: n.Properties["purpose"].(string),
			}
			if given, ok := n.Properties["given"].(bool); ok {
				consent.Given = given
			}
			if ts, ok := n.Properties["timestamp"].(string); ok {
				consent.Timestamp, _ = time.Parse(time.RFC3339, ts)
			}
			if source, ok := n.Properties["source"].(string); ok {
				consent.Source = source
			}
			consents = append(consents, consent)
		}
		return nil
	})
	if err != nil {
		return nil, localizedError(localization.NornicDBCoreConsentStreamFailed(err), err)
	}

	return consents, nil
}

// encryptProperties is a no-op - encryption is handled at the BadgerDB storage level.
// All data is encrypted transparently when encryption is enabled.
// This method is kept for API compatibility but simply returns the input unchanged.
func (db *DB) encryptProperties(props map[string]interface{}) map[string]interface{} {
	return props
}

// decryptProperties is a no-op - decryption is handled at the BadgerDB storage level.
// All data is decrypted transparently when read from an encrypted database.
// This method is kept for API compatibility but simply returns the input unchanged.
func (db *DB) decryptProperties(props map[string]interface{}) map[string]interface{} {
	return props
}

// IsEncryptionEnabled returns whether data-at-rest encryption is enabled.
// When enabled, ALL data is encrypted at the BadgerDB storage level using AES-256.
func (db *DB) IsEncryptionEnabled() bool {
	return db.encryptionEnabled
}

// EncryptionStats returns encryption statistics for monitoring.
func (db *DB) EncryptionStats() map[string]interface{} {
	return map[string]interface{}{
		"enabled":        db.encryptionEnabled,
		"algorithm":      "AES-256 (BadgerDB)",
		"key_derivation": "PBKDF2-SHA256 (600k iterations)",
		"scope":          "full-database",
	}
}
