// Package temporal - Session detection via velocity changes.
//
// SessionDetector identifies user context switches by monitoring:
//   - Time gaps between accesses
//   - Sudden changes in access rate (velocity)
//   - Pattern breaks (different time-of-day)
//
// Sessions are important for:
//   - Co-access inference (nodes accessed in same session are related)
//   - Context-aware search (prioritize current session nodes)
//   - Memory consolidation (sessions = semantic boundaries)
//
// # ELI12 (Explain Like I'm 12)
//
// Think about how you use your phone. You might:
//
//	📱 Morning: Check weather → Check email → Check news (WORK SESSION)
//	☕ Coffee break
//	📱 Later: Instagram → TikTok → YouTube (FUN SESSION)
//
// The SessionDetector notices when you switch between "modes":
//
// How it detects session changes:
//
//  1. TIME GAP: If you stop for 5+ minutes, that's probably a new session
//     "You were looking at work stuff, went to lunch, now you're on games"
//
//  2. VELOCITY CHANGE (the Kalman filter magic!):
//     The filter tracks HOW FAST you're accessing things.
//     If velocity suddenly changes, your "mode" changed!
//
//     Before: accessing every 2 seconds (fast browsing)
//     After:  accessing every 30 seconds (reading something)
//     → "Whoa, you slowed WAY down - new session!"
//
// Why does this matter for memory?
//
//	Session 1: [Weather, Email, News] → These are probably related (work stuff)
//	Session 2: [Instagram, TikTok]    → These are probably related (fun stuff)
//
// So if you search for "weather", we boost Email and News because they were
// accessed in the SAME SESSION. That's co-access inference!
package temporal

import "github.com/orneryd/nornicdb/pkg/util"

import (
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/filter"
)

// Session represents a detected user session.
type Session struct {
	ID        string
	StartTime time.Time
	EndTime   time.Time
	NodeIDs   []string // Nodes accessed in this session
	Duration  time.Duration
	IsCurrent bool
}

// SessionEvent represents a session boundary event.
type SessionEvent struct {
	Type      SessionEventType
	Timestamp time.Time
	NodeID    string
	OldRate   float64
	NewRate   float64
	Reason    string
}

// SessionEventType represents types of session events.
type SessionEventType string

const (
	SessionStart    SessionEventType = "start"
	SessionEnd      SessionEventType = "end"
	SessionContinue SessionEventType = "continue"
)

// SessionDetectorConfig holds configuration for session detection.
type SessionDetectorConfig struct {
	// TimeGapThresholdSeconds - gap that triggers new session
	TimeGapThresholdSeconds float64

	// VelocityChangeThreshold - relative change that triggers session
	VelocityChangeThreshold float64

	// MinSessionDurationSeconds - minimum duration to be a valid session
	MinSessionDurationSeconds float64

	// MaxSessionDurationSeconds - maximum session duration (force break)
	MaxSessionDurationSeconds float64

	// FilterConfig for velocity tracking
	FilterConfig filter.VelocityConfig
}

// DefaultSessionDetectorConfig returns sensible defaults.
func DefaultSessionDetectorConfig() SessionDetectorConfig {
	return SessionDetectorConfig{
		TimeGapThresholdSeconds:   300,  // 5 minutes
		VelocityChangeThreshold:   0.5,  // 50% change
		MinSessionDurationSeconds: 10,   // 10 seconds
		MaxSessionDurationSeconds: 7200, // 2 hours
		FilterConfig:              filter.TemporalTrackingConfig(),
	}
}

// SessionDetector detects session boundaries from access patterns.
type SessionDetector struct {
	mu     sync.RWMutex
	config SessionDetectorConfig

	// Per-node session tracking
	nodeSessions map[string]*nodeSessionData

	// Active sessions
	activeSessions map[string]*Session

	// Session history (last N sessions per node)
	sessionHistory map[string][]*Session

	// Event listeners
	listeners []func(SessionEvent)

	// Session ID counter
	sessionCounter int64
}

// nodeSessionData tracks session state for a single node.
type nodeSessionData struct {
	// Velocity filter for rate tracking
	velocityFilter *filter.KalmanVelocity

	// Last access time
	lastAccess time.Time

	// Current session
	currentSession *Session

	// Last velocity (for change detection)
	lastVelocity float64

	// Total accesses
	totalAccesses int64
}

// NewSessionDetector creates a new session detector.
func NewSessionDetector(cfg SessionDetectorConfig) *SessionDetector {
	return &SessionDetector{
		config:         cfg,
		nodeSessions:   make(map[string]*nodeSessionData),
		activeSessions: make(map[string]*Session),
		sessionHistory: make(map[string][]*Session),
		listeners:      make([]func(SessionEvent), 0),
	}
}

// RecordAccess records an access and checks for session boundaries.
func (sd *SessionDetector) RecordAccess(nodeID string, timestamp time.Time) SessionEvent {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	data, exists := sd.nodeSessions[nodeID]
	if !exists {
		data = sd.createNodeSession(nodeID)
		sd.nodeSessions[nodeID] = data
	}

	event := sd.processAccess(nodeID, data, timestamp)

	// Notify listeners
	for _, listener := range sd.listeners {
		listener(event)
	}

	return event
}

// createNodeSession creates session tracking for a new node.
func (sd *SessionDetector) createNodeSession(nodeID string) *nodeSessionData {
	return &nodeSessionData{
		velocityFilter: filter.NewKalmanVelocity(sd.config.FilterConfig),
	}
}

// processAccess handles an access event for a node.
func (sd *SessionDetector) processAccess(nodeID string, data *nodeSessionData, timestamp time.Time) SessionEvent {
	event := SessionEvent{
		Timestamp: timestamp,
		NodeID:    nodeID,
	}

	// First access for this node
	if data.totalAccesses == 0 {
		event.Type = SessionStart
		event.Reason = "first_access"
		data.currentSession = sd.createSession(nodeID, timestamp)
		data.lastAccess = timestamp
		data.totalAccesses = 1
		return event
	}

	// Calculate time gap
	gap := timestamp.Sub(data.lastAccess).Seconds()

	// Update velocity filter
	accessRate := 1.0 / gap
	if gap < 0.001 {
		accessRate = 1000.0
	}
	data.velocityFilter.Process(accessRate)
	currentVelocity := data.velocityFilter.Velocity()

	// Check for session boundary
	isNewSession := false
	reason := ""

	// Check 1: Time gap too large
	if gap > sd.config.TimeGapThresholdSeconds {
		isNewSession = true
		reason = "time_gap"
	}

	// Check 2: Velocity change too large
	if data.lastVelocity != 0 {
		velChange := (currentVelocity - data.lastVelocity) / data.lastVelocity
		if velChange > sd.config.VelocityChangeThreshold || velChange < -sd.config.VelocityChangeThreshold {
			isNewSession = true
			reason = "velocity_change"
		}
	}

	// Check 3: Session too long (force break)
	if data.currentSession != nil {
		sessionDuration := timestamp.Sub(data.currentSession.StartTime).Seconds()
		if sessionDuration > sd.config.MaxSessionDurationSeconds {
			isNewSession = true
			reason = "max_duration"
		}
	}

	// Handle session transition
	if isNewSession {
		// End current session
		if data.currentSession != nil {
			sd.endSession(nodeID, data, timestamp)
		}

		// Start new session
		event.Type = SessionStart
		event.Reason = reason
		event.OldRate = data.lastVelocity
		event.NewRate = currentVelocity
		data.currentSession = sd.createSession(nodeID, timestamp)
	} else {
		event.Type = SessionContinue
		event.Reason = "same_session"

		// Add node to current session if not already present
		if data.currentSession != nil {
			data.currentSession.EndTime = timestamp
			// Check if node is already in session
			found := false
			for _, id := range data.currentSession.NodeIDs {
				if id == nodeID {
					found = true
					break
				}
			}
			if !found {
				data.currentSession.NodeIDs = append(data.currentSession.NodeIDs, nodeID)
			}
		}
	}

	// Update state
	data.lastAccess = timestamp
	data.lastVelocity = currentVelocity
	data.totalAccesses++

	return event
}

// createSession creates a new session.
func (sd *SessionDetector) createSession(nodeID string, startTime time.Time) *Session {
	sd.sessionCounter++
	session := &Session{
		ID:        nodeID + "-" + startTime.Format("20060102-150405"),
		StartTime: startTime,
		EndTime:   startTime,
		NodeIDs:   []string{nodeID},
		IsCurrent: true,
	}
	sd.activeSessions[nodeID] = session
	return session
}

// endSession ends the current session for a node.
func (sd *SessionDetector) endSession(nodeID string, data *nodeSessionData, endTime time.Time) {
	if data.currentSession == nil {
		return
	}

	data.currentSession.EndTime = endTime
	data.currentSession.Duration = endTime.Sub(data.currentSession.StartTime)
	data.currentSession.IsCurrent = false

	// Add to history
	history := sd.sessionHistory[nodeID]
	history = append(history, data.currentSession)
	// Keep last 50 sessions
	if len(history) > 50 {
		history = history[1:]
	}
	sd.sessionHistory[nodeID] = history

	// Remove from active
	delete(sd.activeSessions, nodeID)
}

// GetCurrentSession returns the current session for a node.
func (sd *SessionDetector) GetCurrentSession(nodeID string) *Session {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	data, exists := sd.nodeSessions[nodeID]
	if !exists || data.currentSession == nil {
		return nil
	}

	// Create a copy
	session := *data.currentSession
	session.NodeIDs = make([]string, len(data.currentSession.NodeIDs))
	copy(session.NodeIDs, data.currentSession.NodeIDs)

	return &session
}

// GetSessionHistory returns session history for a node.
func (sd *SessionDetector) GetSessionHistory(nodeID string, limit int) []*Session {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	history := sd.sessionHistory[nodeID]
	if history == nil {
		return nil
	}

	// Return most recent first
	result := make([]*Session, 0, util.SafePreallocCap(limit, len(history)))
	for i := len(history) - 1; i >= 0 && len(result) < limit; i-- {
		session := *history[i]
		session.NodeIDs = make([]string, len(history[i].NodeIDs))
		copy(session.NodeIDs, history[i].NodeIDs)
		result = append(result, &session)
	}

	return result
}

// GetActiveSessions returns all currently active sessions.
func (sd *SessionDetector) GetActiveSessions() []*Session {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	result := make([]*Session, 0, len(sd.activeSessions))
	for _, session := range sd.activeSessions {
		s := *session
		s.NodeIDs = make([]string, len(session.NodeIDs))
		copy(s.NodeIDs, session.NodeIDs)
		result = append(result, &s)
	}

	return result
}

// GetCoAccessedNodes returns nodes accessed in the same session.
func (sd *SessionDetector) GetCoAccessedNodes(nodeID string) []string {
	session := sd.GetCurrentSession(nodeID)
	if session == nil {
		return nil
	}

	// Return all nodes except the query node
	result := make([]string, 0, len(session.NodeIDs)-1)
	for _, id := range session.NodeIDs {
		if id != nodeID {
			result = append(result, id)
		}
	}

	return result
}

// IsSessionBoundary checks if the last access was a session boundary.
func (sd *SessionDetector) IsSessionBoundary(nodeID string) bool {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	data, exists := sd.nodeSessions[nodeID]
	if !exists {
		return false
	}

	// Check if current session just started (within last second)
	if data.currentSession != nil {
		age := time.Since(data.currentSession.StartTime)
		return age < time.Second
	}

	return false
}

// GetVelocity returns the current access rate velocity for a node.
func (sd *SessionDetector) GetVelocity(nodeID string) float64 {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	data, exists := sd.nodeSessions[nodeID]
	if !exists {
		return 0
	}

	return data.velocityFilter.Velocity()
}

// AddListener adds a listener for session events.
func (sd *SessionDetector) AddListener(listener func(SessionEvent)) {
	sd.mu.Lock()
	defer sd.mu.Unlock()
	sd.listeners = append(sd.listeners, listener)
}

// Reset clears all session data.
func (sd *SessionDetector) Reset() {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	sd.nodeSessions = make(map[string]*nodeSessionData)
	sd.activeSessions = make(map[string]*Session)
	sd.sessionHistory = make(map[string][]*Session)
	sd.sessionCounter = 0
}
