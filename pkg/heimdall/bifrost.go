// Package heimdall provides Heimdall - the cognitive guardian for NornicDB.
package heimdall

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/errors/compose"
	"github.com/orneryd/nornicdb/pkg/lifecycle"
)

// Bifrost implements BifrostBridge for real-time communication with clients.
// Named after the rainbow bridge that connects Asgard to other realms.
// Bifrost is the communication layer between Heimdall and connected UI clients.
type Bifrost struct {
	mu      sync.RWMutex
	clients map[string]*BifrostClient
	config  Config
}

// BifrostClient represents a connected client.
type BifrostClient struct {
	ID          string
	Flusher     http.Flusher
	Writer      http.ResponseWriter
	ConnectedAt time.Time
	LastPing    time.Time

	mu sync.Mutex
	// users admits writers until the client is retired, then joins them.
	users   lifecycle.WorkLifetime
	canRead func() bool
}

// BifrostMessage is a message sent through Bifrost.
type BifrostMessage struct {
	Type      string                 `json:"type"`      // "message", "notification", "confirmation"
	Timestamp int64                  `json:"timestamp"` // Unix timestamp
	Content   string                 `json:"content,omitempty"`
	Title     string                 `json:"title,omitempty"`
	Level     string                 `json:"level,omitempty"` // "info", "warning", "error", "success"
	Data      map[string]interface{} `json:"data,omitempty"`
}

// NewBifrost creates a new Bifrost bridge.
// Returns nil if Bifrost is not enabled in config.
func NewBifrost(cfg Config) *Bifrost {
	if !cfg.BifrostEnabled {
		return nil
	}
	return &Bifrost{
		clients: make(map[string]*BifrostClient),
		config:  cfg,
	}
}

// RegisterClient adds a new connected client.
func (b *Bifrost) RegisterClient(id string, w http.ResponseWriter, f http.Flusher) {
	GetSubsystemManager().recordOutcome("register Bifrost client", b.registerClient(id, w, f, nil, nil))
}

// registerClient publishes the connection only after its initial message has
// been flushed. All later writers share the client's serialization lock.
func (b *Bifrost) registerClient(id string, w http.ResponseWriter, f http.Flusher, canRead func() bool, initial *BifrostMessage) error {
	client := &BifrostClient{
		ID:          id,
		Writer:      w,
		Flusher:     f,
		ConnectedAt: time.Now(),
		LastPing:    time.Now(),
		canRead:     canRead,
	}
	if initial != nil {
		data, err := encodeBifrostMessage(*initial)
		if err != nil {
			return err
		}
		if err := client.write(data); err != nil {
			return err
		}
	}
	b.mu.Lock()
	previous := b.clients[id]
	b.clients[id] = client
	b.mu.Unlock()
	if previous != nil {
		previous.retire()
	}
	return nil
}

func (c *BifrostClient) acquire() (func(), bool) {
	_, release, accepted := c.users.Acquire(context.Background())
	return release, accepted
}
func (c *BifrostClient) retire() {
	c.users.CloseAdmission()
	<-c.users.Quiescent()
}

func (c *BifrostClient) write(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.Writer.Write(data); err != nil {
		return err
	}
	c.Flusher.Flush()
	return nil
}

// UnregisterClient removes a disconnected client.
func (b *Bifrost) UnregisterClient(id string) {
	b.mu.Lock()
	client := b.clients[id]
	delete(b.clients, id)
	b.mu.Unlock()
	if client != nil {
		client.retire()
	}
}

// === BifrostBridge Interface Implementation ===

// SendMessage sends a message to all connected Bifrost clients.
// The message appears as a system message in the chat.
func (b *Bifrost) SendMessage(msg string) error {
	return b.broadcast(BifrostMessage{
		Type:      "message",
		Timestamp: time.Now().Unix(),
		Content:   msg,
	})
}

// SendNotification sends a notification with a specific type.
// Types: "info", "warning", "error", "success"
func (b *Bifrost) SendNotification(notifType, title, message string) error {
	return b.broadcast(BifrostMessage{
		Type:      "notification",
		Timestamp: time.Now().Unix(),
		Level:     notifType,
		Title:     title,
		Content:   message,
	})
}

// Broadcast sends a message to all connected clients.
// Useful for system-wide announcements.
func (b *Bifrost) Broadcast(msg string) error {
	return b.broadcast(BifrostMessage{
		Type:      "broadcast",
		Timestamp: time.Now().Unix(),
		Content:   msg,
	})
}

// RequestConfirmation asks the user to confirm an action.
// Returns true if user confirms, false if they decline or timeout.
// Note: This is a simplified implementation - real implementation would
// need WebSocket for bidirectional communication.
func (b *Bifrost) RequestConfirmation(action string) (bool, error) {
	// For SSE (unidirectional), we can't wait for response
	// Send notification and return false (require explicit confirmation via API)
	err := b.broadcast(BifrostMessage{
		Type:      "confirmation_request",
		Timestamp: time.Now().Unix(),
		Content:   action,
		Data: map[string]interface{}{
			"action":  action,
			"timeout": 30, // seconds
		},
	})
	if err != nil {
		return false, err
	}
	// SSE is unidirectional - confirmation must come via separate API call
	// Return false to indicate confirmation pending
	return false, nil
}

// IsConnected returns true if there are active Bifrost connections.
func (b *Bifrost) IsConnected() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.clients) > 0
}

// ConnectionCount returns the number of active Bifrost connections.
func (b *Bifrost) ConnectionCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.clients)
}

// === Internal Methods ===

// broadcast sends a message to all connected clients via SSE.
func (b *Bifrost) broadcast(msg BifrostMessage) error {
	b.mu.RLock()
	clients := make([]*BifrostClient, 0, len(b.clients))
	for _, client := range b.clients {
		clients = append(clients, client)
	}
	b.mu.RUnlock()
	if len(clients) == 0 {
		// No clients connected - not an error, just no-op
		return nil
	}

	sseData, err := encodeBifrostMessage(msg)
	if err != nil {
		return err
	}

	var outcomes error
	for _, client := range clients {
		release, accepted := client.acquire()
		if !accepted {
			continue
		}
		func() {
			defer release()
			if client.canRead != nil && !client.canRead() {
				return
			}
			if err := client.write(sseData); err != nil {
				outcomes = compose.Outcomes(outcomes, fmt.Errorf("Bifrost client %s: %w", client.ID, err))
			}
		}()
	}

	return outcomes
}

func encodeBifrostMessage(msg BifrostMessage) ([]byte, error) {
	data, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}
	return []byte(fmt.Sprintf("data: %s\n\n", string(data))), nil
}

// Stats returns current Bifrost statistics.
func (b *Bifrost) Stats() map[string]interface{} {
	b.mu.RLock()
	defer b.mu.RUnlock()

	clientInfo := make([]map[string]interface{}, 0, len(b.clients))
	for _, c := range b.clients {
		clientInfo = append(clientInfo, map[string]interface{}{
			"id":           c.ID,
			"connected_at": c.ConnectedAt.Unix(),
			"last_ping":    c.LastPing.Unix(),
		})
	}

	return map[string]interface{}{
		"enabled":          b.config.BifrostEnabled,
		"connection_count": len(b.clients),
		"clients":          clientInfo,
	}
}

// Ensure Bifrost implements BifrostBridge
var _ BifrostBridge = (*Bifrost)(nil)
