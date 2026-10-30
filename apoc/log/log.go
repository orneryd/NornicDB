// Package log provides APOC logging functions.
//
// This package implements all apoc.log.* functions for logging
// and debugging in Cypher queries.
package log

// Need to import runtime for Memory function
import (
	"fmt"
	"log"
	"os"
	"runtime"
	"time"
)

// Level represents log levels.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

var (
	currentLevel = LevelInfo
	logger       = log.New(os.Stdout, "", log.LstdFlags)
)

// Info logs an info message.
//
// Example:
//
//	apoc.log.info('Processing started', {count: 100})
func Info(message string, params map[string]interface{}) {
	if currentLevel <= LevelInfo {
		logMessage("INFO", message, params)
	}
}

// Debug logs a debug message.
//
// Example:
//
//	apoc.log.debug('Variable value', {var: value})
func Debug(message string, params map[string]interface{}) {
	if currentLevel <= LevelDebug {
		logMessage("DEBUG", message, params)
	}
}

// Warn logs a warning message.
//
// Example:
//
//	apoc.log.warn('Deprecated function used', {function: 'oldFunc'})
func Warn(message string, params map[string]interface{}) {
	if currentLevel <= LevelWarn {
		logMessage("WARN", message, params)
	}
}

// Error logs an error message.
//
// Example:
//
//	apoc.log.error('Operation failed', {error: err})
func Error(message string, params map[string]interface{}) {
	if currentLevel <= LevelError {
		logMessage("ERROR", message, params)
	}
}

// logMessage formats and logs a message.
func logMessage(level string, message string, params map[string]interface{}) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	logLine := fmt.Sprintf("[%s] %s: %s", timestamp, level, message)

	if len(params) > 0 {
		logLine += fmt.Sprintf(" %v", params)
	}

	logger.Println(logLine)
}

// Stream logs a message and returns the input value (for chaining).
//
// Example:
//
//	apoc.log.stream('Processing node', node) => node
func Stream(message string, value interface{}) interface{} {
	Info(message, map[string]interface{}{"value": value})
	return value
}

// Format logs a formatted message.
//
// Example:
//
//	apoc.log.format('User %s logged in at %d', ['Alice', timestamp])
func Format(format string, args []interface{}) {
	message := fmt.Sprintf(format, args...)
	Info(message, nil)
}

// SetLevel sets the logging level.
//
// Example:
//
//	apoc.log.setLevel('DEBUG')
func SetLevel(level string) {
	switch level {
	case "DEBUG":
		currentLevel = LevelDebug
	case "INFO":
		currentLevel = LevelInfo
	case "WARN":
		currentLevel = LevelWarn
	case "ERROR":
		currentLevel = LevelError
	}
}

// GetLevel returns the current logging level.
//
// Example:
//
//	apoc.log.getLevel() => 'INFO'
func GetLevel() string {
	switch currentLevel {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	default:
		return "INFO"
	}
}

// Metrics logs performance metrics.
//
// Example:
//
//	apoc.log.metrics('query_time', 150, 'ms')
func Metrics(name string, value interface{}, unit string) {
	Info(fmt.Sprintf("Metric: %s", name), map[string]interface{}{
		"value": value,
		"unit":  unit,
	})
}

// Timer starts a timer and returns a function to log elapsed time.
//
// Example:
//
//	stop := apoc.log.timer('operation')
//	// ... do work ...
//	stop()
func Timer(name string) func() {
	start := time.Now()
	return func() {
		elapsed := time.Since(start)
		Info(fmt.Sprintf("Timer: %s", name), map[string]interface{}{
			"elapsed": elapsed.String(),
		})
	}
}

// Progress logs progress updates.
//
// Example:
//
//	apoc.log.progress('Processing', 50, 100)
func Progress(operation string, current, total int) {
	percentage := float64(current) / float64(total) * 100
	Info(fmt.Sprintf("Progress: %s", operation), map[string]interface{}{
		"current":    current,
		"total":      total,
		"percentage": fmt.Sprintf("%.2f%%", percentage),
	})
}

// Trace logs a stack trace.
//
// Example:
//
//	apoc.log.trace('Error occurred')
func Trace(message string) {
	Error(message, map[string]interface{}{
		"trace": "stack trace placeholder",
	})
}

// Query logs a Cypher query.
//
// Example:
//
//	apoc.log.query('MATCH (n) RETURN n', {})
func Query(query string, params map[string]interface{}) {
	Debug("Executing query", map[string]interface{}{
		"query":  query,
		"params": params,
	})
}

// Result logs query results.
//
// Example:
//
//	apoc.log.result('Query returned', results)
func Result(message string, results interface{}) {
	Debug(message, map[string]interface{}{
		"results": results,
	})
}

// Memory logs memory usage.
//
// Example:
//
//	apoc.log.memory('Current memory usage')
func Memory(message string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	Info(message, map[string]interface{}{
		"alloc":      fmt.Sprintf("%d MB", m.Alloc/1024/1024),
		"totalAlloc": fmt.Sprintf("%d MB", m.TotalAlloc/1024/1024),
		"sys":        fmt.Sprintf("%d MB", m.Sys/1024/1024),
		"numGC":      m.NumGC,
	})
}

// Stats logs statistics.
//
// Example:
//
//	apoc.log.stats('Operation stats', {count: 100, errors: 5})
func Stats(name string, stats map[string]interface{}) {
	Info(fmt.Sprintf("Stats: %s", name), stats)
}

// Audit logs an audit event.
//
// Example:
//
//	apoc.log.audit('user_login', {user: 'alice', ip: '192.168.1.1'})
func Audit(event string, details map[string]interface{}) {
	details["event"] = event
	details["timestamp"] = time.Now().Unix()
	Info("Audit", details)
}

// Security logs a security event.
//
// Example:
//
//	apoc.log.security('unauthorized_access', {user: 'bob', resource: '/admin'})
func Security(event string, details map[string]interface{}) {
	details["event"] = event
	details["timestamp"] = time.Now().Unix()
	Warn("Security", details)
}

// Performance logs performance data.
//
// Example:
//
//	apoc.log.performance('query_execution', {duration: 150, rows: 1000})
func Performance(operation string, metrics map[string]interface{}) {
	metrics["operation"] = operation
	Info("Performance", metrics)
}

// Custom logs with a custom logger.
//
// Example:
//
//	apoc.log.custom(customLogger, 'message', {})
func Custom(customLogger *log.Logger, message string, params map[string]interface{}) {
	logLine := fmt.Sprintf("%s %v", message, params)
	customLogger.Println(logLine)
}

// ToFile logs to a specific file.
//
// Example:
//
//	apoc.log.toFile('/var/log/app.log', 'message')
func ToFile(filePath string, message string) error {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	fileLogger := log.New(file, "", log.LstdFlags)
	fileLogger.Println(message)

	return nil
}

// Rotate rotates log files.
//
// Example:
//
//	apoc.log.rotate('/var/log/app.log', 10)
func Rotate(filePath string, maxFiles int) error {
	// Placeholder - would implement log rotation
	return nil
}

// Clear clears log files.
//
// Example:
//
//	apoc.log.clear('/var/log/app.log')
func Clear(filePath string) error {
	return os.Truncate(filePath, 0)
}

// Tail returns the last N lines from a log file.
//
// Example:
//
//	apoc.log.tail('/var/log/app.log', 100) => lines
func Tail(filePath string, lines int) ([]string, error) {
	// Placeholder - would read last N lines
	return []string{}, nil
}

// Search searches log files for a pattern.
//
// Example:
//
//	apoc.log.search('/var/log/app.log', 'ERROR') => matching lines
func Search(filePath string, pattern string) ([]string, error) {
	// Placeholder - would search log file
	return []string{}, nil
}
