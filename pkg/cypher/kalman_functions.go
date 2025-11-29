// Kalman filter Cypher functions for NornicDB.
//
// These functions expose the Kalman filter implementations as database-callable
// functions, enabling users to apply signal filtering and prediction directly
// in their Cypher queries. Perfect for real-time time series analysis.
//
// # Overview
//
// Kalman filters are optimal state estimators that combine noisy measurements
// with predictions to produce smooth, accurate estimates. They're widely used
// in aerospace, robotics, finance, and signal processing.
//
// # Available Filters
//
// Three filter types are available, each suited for different use cases:
//
//   - kalman.*          - Basic scalar filter for noise smoothing
//   - kalman.velocity.* - 2-state filter tracking position AND velocity (trends)
//   - kalman.adaptive.* - Auto-switching filter that picks the best mode
//
// # State Management
//
// Users store the filter state as a JSON string in a node property. The state
// is passed to each function call and an updated state is returned. This allows
// the database to remain stateless while users maintain their own filter state.
//
// # Real-World Example: News Sentiment → Stock Prediction
//
// An LLM watches the Associated Press news feed in real-time, scoring each
// headline's market sentiment (-1.0 to +1.0). The Kalman filter smooths these
// noisy signals to predict stock movements:
//
//	// Step 1: Create a stock tracker with Kalman filtering
//	CREATE (s:Stock {
//	    symbol: "AAPL",
//	    kalmanState: kalman.velocity.init()  // Track trends
//	})
//
//	// Step 2: LLM processes AP news headline and scores sentiment
//	// (This happens in your application code, score passed as parameter)
//	// headline: "Apple announces record iPhone sales in China"
//	// $sentimentScore = 0.72  (positive)
//
//	// Step 3: Process the sentiment score through Kalman filter
//	MATCH (s:Stock {symbol: "AAPL"})
//	WITH s, kalman.velocity.process($sentimentScore, s.kalmanState) AS result
//	SET s.kalmanState = result.state,
//	    s.sentiment = result.value,
//	    s.momentum = result.velocity,
//	    s.lastUpdate = timestamp()
//	RETURN result.value AS smoothedSentiment,
//	       result.velocity AS sentimentTrend
//
//	// Step 4: Predict sentiment 5 time-steps ahead
//	MATCH (s:Stock {symbol: "AAPL"})
//	RETURN s.symbol,
//	       s.sentiment AS currentSentiment,
//	       s.momentum AS trend,
//	       kalman.velocity.predict(s.kalmanState, 5) AS predictedSentiment,
//	       CASE
//	           WHEN s.momentum > 0.1 THEN "BULLISH"
//	           WHEN s.momentum < -0.1 THEN "BEARISH"
//	           ELSE "NEUTRAL"
//	       END AS signal
//
//	// Step 5: Find stocks with strongest momentum
//	MATCH (s:Stock)
//	WHERE s.kalmanState IS NOT NULL
//	RETURN s.symbol, s.sentiment, s.momentum
//	ORDER BY abs(s.momentum) DESC
//	LIMIT 10
//
// # Why Kalman Filtering Works for This
//
// Individual news headlines are noisy - one bad headline doesn't mean the stock
// will crash. The Kalman filter:
//   - Smooths out noise from individual headlines
//   - Tracks the TREND (velocity) of sentiment over time
//   - Predicts where sentiment is heading
//   - Adapts to changing conditions automatically
//
// # ELI12 (Explain Like I'm 12)
//
// Imagine you're trying to guess tomorrow's weather by asking 10 friends.
// Some say "sunny", some say "rainy" - it's confusing! The Kalman filter is
// like having a really smart friend who:
//   - Remembers what everyone said yesterday
//   - Notices if opinions are trending toward "sunny" or "rainy"
//   - Doesn't freak out when one person gives a weird answer
//   - Uses all this to make a better prediction than any single friend
//
// For stocks: news headlines are like those friends - some are right, some
// are wrong, some are noise. Kalman helps you see the real trend!
//
// # Other Use Cases
//
//   - IoT sensor smoothing (temperature, pressure, GPS)
//   - User behavior prediction (session length, click rates)
//   - Memory decay tracking (see NornicDB's memory tier system)
//   - Query latency monitoring
//   - Any noisy time series data!
package cypher

import (
	"encoding/json"
	"math"
)

// ========================================
// Kalman State Structures (JSON-serializable)
// ========================================

// KalmanState represents the serializable state of a basic Kalman filter.
// This is stored as JSON in a node property and passed to kalman.* functions.
type KalmanState struct {
	// Current state estimate
	X float64 `json:"x"`
	// Previous state (for velocity calculation)
	LastX float64 `json:"lx"`
	// Estimate covariance (uncertainty)
	P float64 `json:"p"`
	// Kalman gain
	K float64 `json:"k"`
	// Setpoint error factor
	E float64 `json:"e"`
	// Process noise (scaled)
	Q float64 `json:"q"`
	// Measurement noise
	R float64 `json:"r"`
	// Variance scale for adaptive R
	VarianceScale float64 `json:"vs"`
	// Number of observations processed
	Observations int `json:"n"`
}

// KalmanVelocityState represents the serializable state of a 2-state Kalman filter.
// Tracks both position and velocity for trend prediction.
type KalmanVelocityState struct {
	// Position estimate
	Pos float64 `json:"pos"`
	// Velocity estimate
	Vel float64 `json:"vel"`
	// 2x2 Covariance matrix [p00, p01, p10, p11]
	P [4]float64 `json:"p"`
	// Position process noise
	QPos float64 `json:"qp"`
	// Velocity process noise
	QVel float64 `json:"qv"`
	// Measurement noise
	R float64 `json:"r"`
	// Time step
	Dt float64 `json:"dt"`
	// Number of observations
	Observations int `json:"n"`
}

// KalmanAdaptiveState represents the serializable state of an adaptive filter.
type KalmanAdaptiveState struct {
	// Underlying basic filter state
	Basic KalmanState `json:"basic"`
	// Underlying velocity filter state
	Velocity KalmanVelocityState `json:"velocity"`
	// Current mode: "basic" or "velocity"
	Mode string `json:"mode"`
	// Observations since last switch
	SinceSwitch int `json:"ss"`
	// Trend detection threshold
	TrendThreshold float64 `json:"tt"`
	// Stability threshold
	StabilityThreshold float64 `json:"st"`
	// Switch hysteresis count
	Hysteresis int `json:"hy"`
	// Total observations
	Observations int `json:"n"`
	// Last filtered value
	LastFiltered float64 `json:"lf"`
	// Trend score (running velocity estimate)
	TrendScore float64 `json:"ts"`
}

// KalmanProcessResult is the result of kalman.process()
type KalmanProcessResult struct {
	Value float64 `json:"value"`
	State string  `json:"state"`
}

// KalmanVelocityProcessResult is the result of kalman.velocity.process()
type KalmanVelocityProcessResult struct {
	Value    float64 `json:"value"`
	Velocity float64 `json:"velocity"`
	State    string  `json:"state"`
}

// KalmanAdaptiveProcessResult is the result of kalman.adaptive.process()
type KalmanAdaptiveProcessResult struct {
	Value float64 `json:"value"`
	Mode  string  `json:"mode"`
	State string  `json:"state"`
}

// ========================================
// Default Configurations
// ========================================

// defaultKalmanState returns a new basic Kalman state with default config.
func defaultKalmanState() *KalmanState {
	return &KalmanState{
		X:             0,
		LastX:         0,
		P:             30.0, // Initial covariance
		K:             0,
		E:             1.0,
		Q:             0.0001, // ProcessNoise * 0.001
		R:             88.0,   // Measurement noise seed
		VarianceScale: 10.0,
		Observations:  0,
	}
}

// defaultKalmanVelocityState returns a new velocity Kalman state with default config.
func defaultKalmanVelocityState() *KalmanVelocityState {
	return &KalmanVelocityState{
		Pos:          0,
		Vel:          0,
		P:            [4]float64{100.0, 0, 0, 10.0}, // [p00, p01, p10, p11]
		QPos:         0.1,
		QVel:         0.01,
		R:            1.0,
		Dt:           1.0,
		Observations: 0,
	}
}

// defaultKalmanAdaptiveState returns a new adaptive Kalman state.
func defaultKalmanAdaptiveState() *KalmanAdaptiveState {
	return &KalmanAdaptiveState{
		Basic:              *defaultKalmanState(),
		Velocity:           *defaultKalmanVelocityState(),
		Mode:               "basic",
		SinceSwitch:        0,
		TrendThreshold:     0.1,
		StabilityThreshold: 0.02,
		Hysteresis:         10,
		Observations:       0,
		LastFiltered:       0,
		TrendScore:         0,
	}
}

// ========================================
// Basic Kalman Functions
// ========================================

// kalmanInit creates a new Kalman filter state with optional configuration.
//
// This is the entry point for using Kalman filtering in Cypher queries.
// The returned JSON string should be stored in a node property for later use.
//
// # Cypher Syntax
//
//	kalman.init() → STRING
//	kalman.init(config :: MAP) → STRING
//
// # Configuration Options
//
//	{
//	    processNoise: 0.1,       // How much state changes between measurements (default: 0.1)
//	    measurementNoise: 88.0,  // How noisy measurements are (default: 88.0)
//	    initialCovariance: 30.0, // Initial uncertainty (default: 30.0)
//	    varianceScale: 10.0      // Adaptive noise scaling (default: 10.0)
//	}
//
// # Example 1: Default Configuration
//
//	CREATE (s:Sensor {id: "temp-1", kalmanState: kalman.init()})
//
// # Example 2: Custom Configuration for Noisy Data
//
//	CREATE (s:Sensor {
//	    id: "gps-tracker",
//	    kalmanState: kalman.init({measurementNoise: 150.0})
//	})
//
// # Example 3: News Sentiment Tracker
//
//	CREATE (s:Stock {
//	    symbol: "AAPL",
//	    kalmanState: kalman.init({processNoise: 0.05, measurementNoise: 50.0})
//	})
func kalmanInit(configMap map[string]interface{}) string {
	state := defaultKalmanState()

	// Apply custom config if provided
	if configMap != nil {
		if pn, ok := configMap["processNoise"].(float64); ok {
			state.Q = pn * 0.001
		}
		if mn, ok := configMap["measurementNoise"].(float64); ok {
			state.R = mn
		}
		if ic, ok := configMap["initialCovariance"].(float64); ok {
			state.P = ic
		}
		if vs, ok := configMap["varianceScale"].(float64); ok {
			state.VarianceScale = vs
		}
	}

	bytes, _ := json.Marshal(state)
	return string(bytes)
}

// kalmanProcess updates the filter with a new measurement and returns filtered value.
//
// This is the core function for processing time-series data. It takes a noisy
// measurement, updates the internal state, and returns a smoothed estimate.
//
// # Cypher Syntax
//
//	kalman.process(measurement :: FLOAT, state :: STRING) → MAP
//	kalman.process(measurement :: FLOAT, state :: STRING, target :: FLOAT) → MAP
//
// # Return Value
//
//	{
//	    value: FLOAT,  // The filtered/smoothed value
//	    state: STRING  // Updated state JSON (store this for next call)
//	}
//
// # Parameters
//
//   - measurement: The raw observed value
//   - state: The JSON state from kalman.init() or previous process() call
//   - target: Optional setpoint for error boosting (helps converge faster)
//
// # Example 1: Process Temperature Reading
//
//	MATCH (s:Sensor {id: "temp-1"})
//	WITH s, kalman.process(23.5, s.kalmanState) AS result
//	SET s.kalmanState = result.state,
//	    s.temperature = result.value
//	RETURN result.value AS smoothedTemp
//
// # Example 2: Process News Sentiment Score
//
//	// LLM scored headline: "Fed raises rates unexpectedly" → -0.65
//	MATCH (s:Stock {symbol: "SPY"})
//	WITH s, kalman.process($sentimentScore, s.kalmanState) AS result
//	SET s.kalmanState = result.state,
//	    s.sentiment = result.value
//	RETURN s.symbol, result.value AS filteredSentiment
//
// # Example 3: With Target Setpoint
//
//	// Try to converge toward neutral (0.0) sentiment baseline
//	MATCH (s:Stock {symbol: "AAPL"})
//	WITH s, kalman.process($score, s.kalmanState, 0.0) AS result
//	SET s.kalmanState = result.state
//	RETURN result.value
func kalmanProcess(measurement float64, stateJSON string, target float64) map[string]interface{} {
	var state KalmanState
	if err := json.Unmarshal([]byte(stateJSON), &state); err != nil {
		// Return original measurement if state is invalid
		return map[string]interface{}{
			"value": measurement,
			"state": stateJSON,
			"error": "invalid state",
		}
	}

	// === Kalman Filter Algorithm ===
	// Project state ahead using velocity
	velocity := state.X - state.LastX
	state.X += velocity

	// Save for next velocity calculation
	state.LastX = state.X

	// Setpoint-based error boosting
	if target != 0.0 && state.LastX != 0.0 {
		state.E = math.Abs(1.0 - (target / state.LastX))
	} else {
		state.E = 1.0
	}

	// Prediction update: increase uncertainty
	state.P = state.P + (state.Q * state.E)

	// Measurement update
	state.K = state.P / (state.P + state.R)

	// Innovation
	innovation := measurement - state.X
	state.X += state.K * innovation

	// Update covariance
	state.P = (1.0 - state.K) * state.P

	state.Observations++

	// Serialize updated state
	bytes, _ := json.Marshal(state)

	return map[string]interface{}{
		"value": state.X,
		"state": string(bytes),
	}
}

// kalmanPredict estimates the state n steps into the future.
//
// Uses the current velocity (rate of change) to project forward without
// updating the filter state. Great for "what if" predictions.
//
// # Cypher Syntax
//
//	kalman.predict(state :: STRING, steps :: INTEGER) → FLOAT
//
// # Example 1: Predict Temperature in 5 Steps
//
//	MATCH (s:Sensor {id: "temp-1"})
//	RETURN kalman.predict(s.kalmanState, 5) AS predictedTemp
//
// # Example 2: News Sentiment Prediction for Trading Signal
//
//	MATCH (s:Stock)
//	WHERE s.kalmanState IS NOT NULL
//	WITH s,
//	     kalman.state(s.kalmanState) AS current,
//	     kalman.predict(s.kalmanState, 3) AS predicted
//	RETURN s.symbol,
//	       current AS nowSentiment,
//	       predicted AS predictedSentiment,
//	       CASE
//	           WHEN predicted > current + 0.1 THEN "BUY SIGNAL"
//	           WHEN predicted < current - 0.1 THEN "SELL SIGNAL"
//	           ELSE "HOLD"
//	       END AS recommendation
//	ORDER BY abs(predicted - current) DESC
func kalmanPredict(stateJSON string, steps int) float64 {
	var state KalmanState
	if err := json.Unmarshal([]byte(stateJSON), &state); err != nil {
		return 0
	}

	velocity := state.X - state.LastX
	return state.X + (float64(steps) * velocity)
}

// kalmanStateValue extracts the current state estimate from the state JSON.
//
// Useful when you need just the current value without processing new data.
//
// # Cypher Syntax
//
//	kalman.state(state :: STRING) → FLOAT
//
// # Example
//
//	MATCH (s:Sensor)
//	RETURN s.id, kalman.state(s.kalmanState) AS currentEstimate
func kalmanStateValue(stateJSON string) float64 {
	var state KalmanState
	if err := json.Unmarshal([]byte(stateJSON), &state); err != nil {
		return 0
	}
	return state.X
}

// kalmanVelocityValue returns the current velocity (rate of change) from basic filter.
//
// Note: For proper velocity tracking, use kalman.velocity.* functions instead.
//
// # Cypher Syntax
//
//	kalman.rate(state :: STRING) → FLOAT
func kalmanVelocityValue(stateJSON string) float64 {
	var state KalmanState
	if err := json.Unmarshal([]byte(stateJSON), &state); err != nil {
		return 0
	}
	return state.X - state.LastX
}

// ========================================
// Velocity Kalman Functions (2-state)
// ========================================

// kalmanVelocityInit creates a 2-state Kalman filter that tracks position AND velocity.
//
// This is the recommended filter for trending data like stock prices, sentiment
// scores, or any time series with momentum. It explicitly estimates both WHERE
// something is AND HOW FAST it's moving.
//
// # Cypher Syntax
//
//	kalman.velocity.init() → STRING
//	kalman.velocity.init(initialPos :: FLOAT, initialVel :: FLOAT) → STRING
//
// # When to Use Velocity Filter
//
//   - Data has trends or momentum (news sentiment, stock prices)
//   - You need to predict future values accurately
//   - The signal drifts over time rather than oscillating around a fixed point
//
// # Example 1: Initialize Stock Sentiment Tracker
//
//	// Track market sentiment from AP news feed
//	CREATE (s:Stock {
//	    symbol: "TSLA",
//	    kalmanState: kalman.velocity.init()
//	})
//
// # Example 2: Initialize with Known Starting Point
//
//	// We know sentiment was 0.5 and trending up at 0.02 per tick
//	CREATE (s:Stock {
//	    symbol: "NVDA",
//	    kalmanState: kalman.velocity.init(0.5, 0.02)
//	})
//
// # Example 3: Real-time News → Stock Signal Pipeline
//
//	// Step 1: Create trackers for S&P 500 stocks
//	UNWIND $symbols AS sym
//	CREATE (s:Stock {symbol: sym, kalmanState: kalman.velocity.init()})
//
//	// Step 2: LLM processes headline, returns {symbol, score}
//	// "Apple beats earnings" → {symbol: "AAPL", score: 0.82}
//
//	// Step 3: Update tracker with new sentiment
//	MATCH (s:Stock {symbol: $symbol})
//	WITH s, kalman.velocity.process($score, s.kalmanState) AS result
//	SET s.kalmanState = result.state,
//	    s.sentiment = result.value,
//	    s.momentum = result.velocity
//	RETURN result
func kalmanVelocityInit(initialPos, initialVel float64, hasInitial bool) string {
	state := defaultKalmanVelocityState()
	if hasInitial {
		state.Pos = initialPos
		state.Vel = initialVel
	}
	bytes, _ := json.Marshal(state)
	return string(bytes)
}

// kalmanVelocityProcess updates the 2-state filter with a new measurement.
//
// Returns both the filtered position AND the estimated velocity (trend).
// The velocity tells you if the signal is trending up or down and how fast.
//
// # Cypher Syntax
//
//	kalman.velocity.process(measurement :: FLOAT, state :: STRING) → MAP
//
// # Return Value
//
//	{
//	    value: FLOAT,    // Filtered position estimate
//	    velocity: FLOAT, // Trend direction and magnitude
//	    state: STRING    // Updated state JSON
//	}
//
// # Example 1: Process News Sentiment and Get Trend
//
//	MATCH (s:Stock {symbol: "AAPL"})
//	WITH s, kalman.velocity.process($sentimentScore, s.kalmanState) AS result
//	SET s.kalmanState = result.state,
//	    s.sentiment = result.value,
//	    s.momentum = result.velocity,
//	    s.trend = CASE
//	        WHEN result.velocity > 0.05 THEN "BULLISH"
//	        WHEN result.velocity < -0.05 THEN "BEARISH"
//	        ELSE "NEUTRAL"
//	    END
//	RETURN s.symbol, s.sentiment, s.momentum, s.trend
//
// # Example 2: Batch Update from News Feed
//
//	// Process multiple headlines at once
//	UNWIND $newsItems AS news
//	MATCH (s:Stock {symbol: news.symbol})
//	WITH s, news, kalman.velocity.process(news.score, s.kalmanState) AS result
//	SET s.kalmanState = result.state,
//	    s.sentiment = result.value,
//	    s.momentum = result.velocity
//	RETURN s.symbol, result.value, result.velocity
//
// # Example 3: Alert on Momentum Shift
//
//	MATCH (s:Stock)
//	WITH s, kalman.velocity.process($score, s.kalmanState) AS result
//	WHERE abs(result.velocity) > 0.15  // Strong momentum
//	SET s.kalmanState = result.state
//	RETURN s.symbol,
//	       result.velocity AS momentum,
//	       "MOMENTUM ALERT" AS signal
func kalmanVelocityProcess(measurement float64, stateJSON string) map[string]interface{} {
	var state KalmanVelocityState
	if err := json.Unmarshal([]byte(stateJSON), &state); err != nil {
		return map[string]interface{}{
			"value":    measurement,
			"velocity": 0.0,
			"state":    stateJSON,
			"error":    "invalid state",
		}
	}

	dt := state.Dt
	if dt <= 0 {
		dt = 1.0
	}

	// === PREDICT STEP ===
	// State prediction: x(k|k-1) = F * x(k-1|k-1)
	predPos := state.Pos + state.Vel*dt
	predVel := state.Vel

	// Covariance prediction
	p00, p01, p10, p11 := state.P[0], state.P[1], state.P[2], state.P[3]
	predP00 := p00 + dt*p10 + dt*p01 + dt*dt*p11 + state.QPos
	predP01 := p01 + dt*p11
	predP10 := p10 + dt*p11
	predP11 := p11 + state.QVel

	// === UPDATE STEP ===
	// Innovation
	innovation := measurement - predPos

	// Innovation covariance
	s := predP00 + state.R

	// Kalman gain
	k0 := predP00 / s
	k1 := predP10 / s

	// State update
	state.Pos = predPos + k0*innovation
	state.Vel = predVel + k1*innovation

	// Covariance update
	state.P[0] = (1 - k0) * predP00
	state.P[1] = (1 - k0) * predP01
	state.P[2] = predP10 - k1*predP00
	state.P[3] = predP11 - k1*predP01

	state.Observations++

	bytes, _ := json.Marshal(state)

	return map[string]interface{}{
		"value":    state.Pos,
		"velocity": state.Vel,
		"state":    string(bytes),
	}
}

// kalmanVelocityPredict predicts position n steps into the future using velocity.
//
// This is more accurate than basic predict() because it uses the explicitly
// tracked velocity rather than inferring it from position changes.
//
// # Cypher Syntax
//
//	kalman.velocity.predict(state :: STRING, steps :: INTEGER) → FLOAT
//
// # Example 1: Predict Sentiment in 5 Time Steps
//
//	MATCH (s:Stock {symbol: "AAPL"})
//	RETURN s.symbol,
//	       kalman.state(s.kalmanState) AS now,
//	       kalman.velocity.predict(s.kalmanState, 5) AS in5Steps,
//	       kalman.velocity.predict(s.kalmanState, 10) AS in10Steps
//
// # Example 2: Find Stocks About to Cross Threshold
//
//	MATCH (s:Stock)
//	WITH s,
//	     kalman.state(s.kalmanState) AS current,
//	     kalman.velocity.predict(s.kalmanState, 3) AS predicted
//	WHERE current < 0.7 AND predicted >= 0.7  // About to go bullish
//	RETURN s.symbol, current, predicted, "BREAKOUT CANDIDATE" AS signal
//
// # Example 3: Risk Assessment - Predict Downturn
//
//	MATCH (s:Stock)
//	WITH s,
//	     kalman.velocity.predict(s.kalmanState, 5) AS predicted
//	WHERE predicted < -0.5  // Predicted strong negative sentiment
//	RETURN s.symbol, predicted, "HIGH RISK" AS warning
//	ORDER BY predicted ASC
func kalmanVelocityPredict(stateJSON string, steps int) float64 {
	var state KalmanVelocityState
	if err := json.Unmarshal([]byte(stateJSON), &state); err != nil {
		return 0
	}

	dt := state.Dt
	if dt <= 0 {
		dt = 1.0
	}

	return state.Pos + state.Vel*float64(steps)*dt
}

// ========================================
// Adaptive Kalman Functions
// ========================================

// kalmanAdaptiveInit creates an adaptive filter that auto-switches between modes.
//
// The adaptive filter monitors the signal and automatically switches between:
//   - Basic mode: Best for stable signals with noise
//   - Velocity mode: Best for trending signals with momentum
//
// This is "set and forget" - it adapts to changing signal characteristics.
//
// # Cypher Syntax
//
//	kalman.adaptive.init() → STRING
//	kalman.adaptive.init(config :: MAP) → STRING
//
// # Configuration Options
//
//	{
//	    trendThreshold: 0.1,      // Velocity above this → switch to velocity mode
//	    stabilityThreshold: 0.02, // Velocity below this → switch to basic mode
//	    hysteresis: 10,           // Min observations before switching
//	    initialMode: "basic"      // Start in "basic" or "velocity" mode
//	}
//
// # Example 1: Default Adaptive Filter
//
//	CREATE (s:Stock {
//	    symbol: "SPY",
//	    kalmanState: kalman.adaptive.init()
//	})
//
// # Example 2: Sensitive to Trends (Quick Switch to Velocity Mode)
//
//	CREATE (s:Stock {
//	    symbol: "TSLA",  // Volatile stock
//	    kalmanState: kalman.adaptive.init({
//	        trendThreshold: 0.05,   // Switch on smaller trends
//	        hysteresis: 5           // Switch faster
//	    })
//	})
//
// # Example 3: Prefer Stability (Slow to Switch)
//
//	CREATE (s:Stock {
//	    symbol: "JNJ",  // Stable stock
//	    kalmanState: kalman.adaptive.init({
//	        trendThreshold: 0.2,    // Only switch on strong trends
//	        hysteresis: 20          // Wait longer before switching
//	    })
//	})
func kalmanAdaptiveInit(configMap map[string]interface{}) string {
	state := defaultKalmanAdaptiveState()

	// Apply custom config if provided
	if configMap != nil {
		if tt, ok := configMap["trendThreshold"].(float64); ok {
			state.TrendThreshold = tt
		}
		if st, ok := configMap["stabilityThreshold"].(float64); ok {
			state.StabilityThreshold = st
		}
		if hy, ok := configMap["hysteresis"].(float64); ok {
			state.Hysteresis = int(hy)
		}
		if mode, ok := configMap["initialMode"].(string); ok {
			if mode == "velocity" {
				state.Mode = "velocity"
			}
		}
	}

	bytes, _ := json.Marshal(state)
	return string(bytes)
}

// kalmanAdaptiveProcess processes a measurement with automatic mode switching.
//
// The filter monitors the signal and switches modes when appropriate:
//   - Detects trend → switches to velocity mode for better tracking
//   - Detects stability → switches to basic mode for better smoothing
//
// # Cypher Syntax
//
//	kalman.adaptive.process(measurement :: FLOAT, state :: STRING) → MAP
//
// # Return Value
//
//	{
//	    value: FLOAT,  // Filtered value
//	    mode: STRING,  // Current mode: "basic" or "velocity"
//	    state: STRING  // Updated state JSON
//	}
//
// # Example 1: Process with Mode Visibility
//
//	MATCH (s:Stock {symbol: "AAPL"})
//	WITH s, kalman.adaptive.process($score, s.kalmanState) AS result
//	SET s.kalmanState = result.state,
//	    s.sentiment = result.value,
//	    s.filterMode = result.mode
//	RETURN s.symbol, result.value, result.mode
//
// # Example 2: Log Mode Switches
//
//	MATCH (s:Stock {symbol: "TSLA"})
//	WITH s, s.filterMode AS oldMode
//	WITH s, oldMode, kalman.adaptive.process($score, s.kalmanState) AS result
//	SET s.kalmanState = result.state, s.filterMode = result.mode
//	WITH s, oldMode, result
//	WHERE oldMode <> result.mode  // Mode changed!
//	CREATE (log:ModeSwitch {
//	    symbol: s.symbol,
//	    from: oldMode,
//	    to: result.mode,
//	    timestamp: timestamp()
//	})
//	RETURN "Mode switch detected" AS alert
//
// # Example 3: Full News Pipeline with Adaptive Filtering
//
//	// LLM watches AP feed, scores headlines in real-time
//	// $headlines = [{symbol: "AAPL", score: 0.72}, {symbol: "MSFT", score: -0.15}, ...]
//
//	UNWIND $headlines AS headline
//	MATCH (s:Stock {symbol: headline.symbol})
//	WITH s, headline, kalman.adaptive.process(headline.score, s.kalmanState) AS result
//	SET s.kalmanState = result.state,
//	    s.sentiment = result.value,
//	    s.filterMode = result.mode,
//	    s.lastUpdate = timestamp()
//	RETURN s.symbol, result.value, result.mode
//	ORDER BY abs(result.value) DESC
func kalmanAdaptiveProcess(measurement float64, stateJSON string) map[string]interface{} {
	var state KalmanAdaptiveState
	if err := json.Unmarshal([]byte(stateJSON), &state); err != nil {
		return map[string]interface{}{
			"value": measurement,
			"mode":  "error",
			"state": stateJSON,
			"error": "invalid state",
		}
	}

	var filtered float64

	// Process with current mode
	if state.Mode == "velocity" {
		// Use velocity filter
		velStateBytes, _ := json.Marshal(state.Velocity)
		result := kalmanVelocityProcess(measurement, string(velStateBytes))
		filtered = result["value"].(float64)
		// Parse updated state
		json.Unmarshal([]byte(result["state"].(string)), &state.Velocity)
		// Update trend score from velocity
		state.TrendScore = state.Velocity.Vel
	} else {
		// Use basic filter
		basicStateBytes, _ := json.Marshal(state.Basic)
		result := kalmanProcess(measurement, string(basicStateBytes), 0)
		filtered = result["value"].(float64)
		// Parse updated state
		json.Unmarshal([]byte(result["state"].(string)), &state.Basic)
		// Update trend score from basic velocity
		state.TrendScore = state.Basic.X - state.Basic.LastX
	}

	state.Observations++
	state.SinceSwitch++

	// Check for mode switch (only after hysteresis period)
	if state.SinceSwitch >= state.Hysteresis {
		trendMagnitude := math.Abs(state.TrendScore)

		if state.Mode == "basic" && trendMagnitude > state.TrendThreshold {
			// Switch to velocity mode
			state.Mode = "velocity"
			state.SinceSwitch = 0
			// Sync state
			state.Velocity.Pos = state.Basic.X
			state.Velocity.Vel = state.TrendScore
		} else if state.Mode == "velocity" && trendMagnitude < state.StabilityThreshold {
			// Switch to basic mode
			state.Mode = "basic"
			state.SinceSwitch = 0
			// Sync state
			state.Basic.X = state.Velocity.Pos
			state.Basic.LastX = state.Velocity.Pos - state.Velocity.Vel
		}
	}

	state.LastFiltered = filtered

	bytes, _ := json.Marshal(state)

	return map[string]interface{}{
		"value": filtered,
		"mode":  state.Mode,
		"state": string(bytes),
	}
}

// kalmanReset resets a Kalman state to initial values while preserving config.
//
// Useful when you want to start fresh without losing the filter type.
// Automatically detects which filter type (basic, velocity, or adaptive) and
// returns an appropriate fresh state.
//
// # Cypher Syntax
//
//	kalman.reset(state :: STRING) → STRING
//
// # Example 1: Reset After Anomaly
//
//	MATCH (s:Stock {symbol: "AAPL"})
//	WHERE s.sentiment > 0.99 OR s.sentiment < -0.99  // Outlier detected
//	SET s.kalmanState = kalman.reset(s.kalmanState)
//	RETURN s.symbol, "Filter reset" AS action
//
// # Example 2: Weekly Reset for All Trackers
//
//	MATCH (s:Stock)
//	SET s.kalmanState = kalman.reset(s.kalmanState),
//	    s.lastReset = timestamp()
//	RETURN count(s) AS resetCount
func kalmanReset(stateJSON string) string {
	// Try to detect type and reset appropriately
	var testMap map[string]interface{}
	if err := json.Unmarshal([]byte(stateJSON), &testMap); err != nil {
		return kalmanInit(nil)
	}

	// Check for velocity state (has "pos" field)
	if _, hasPos := testMap["pos"]; hasPos {
		return kalmanVelocityInit(0, 0, false)
	}

	// Check for adaptive state (has "mode" field)
	if _, hasMode := testMap["mode"]; hasMode {
		return kalmanAdaptiveInit(nil)
	}

	// Default to basic
	return kalmanInit(nil)
}
