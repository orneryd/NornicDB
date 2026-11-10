# Testing Guide

**How to write and run tests for NornicDB.**

## Test Structure

Tests live alongside the code they test:

- `db.go` → `db_test.go`
- `server.go` → `server_test.go`

## Running Tests

Use the Go toolchain in `go.mod` (currently **1.27.3**). Build a Go plugin and its host with the same exact toolchain, build tags, and race setting; rebuild both after upgrades. Keep real plugin-loading checks enabled on supported platforms.

`make test` owns the complete package/lint gate. Direct package commands below are useful focused checks, not substitutes for a complete release result. See the [contribution guide](contributing.md) for branch policy, verifier selection, resource-limited validation, and how to record skips.

```bash
# All tests
go test ./...

# Verbose output
go test -v ./...

# Specific package
go test ./pkg/nornicdb/...

# Single test
go test -run TestStore ./pkg/nornicdb/...

# With coverage
go test -cover ./...

# Generate coverage report
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

## Writing Tests

### Basic Test

```go
func TestMyFunction(t *testing.T) {
    result := MyFunction()
    if result != expected {
        t.Errorf("expected %v, got %v", expected, result)
    }
}
```

### Table-Driven Tests

```go
func TestMyFunction(t *testing.T) {
    tests := []struct {
        name     string
        input    string
        expected int
    }{
        {"empty", "", 0},
        {"simple", "hello", 5},
        {"unicode", "日本", 2},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := MyFunction(tt.input)
            if result != tt.expected {
                t.Errorf("expected %d, got %d", tt.expected, result)
            }
        })
    }
}
```

### Database Tests

```go
func TestDatabaseOperation(t *testing.T) {
    // Use t.TempDir() for automatic cleanup
    db, err := Open(t.TempDir(), nil)
    require.NoError(t, err)
    defer db.Close()

    // Test operations...
}
```

### Server Tests

```go
func TestEndpoint(t *testing.T) {
    server := setupTestServer(t)
    defer server.Stop()

    req := httptest.NewRequest("GET", "/health", nil)
    w := httptest.NewRecorder()
    server.ServeHTTP(w, req)

    assert.Equal(t, http.StatusOK, w.Code)
}
```

## Test Helpers

### Using testify

```go
import (
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestWithTestify(t *testing.T) {
    // assert continues on failure
    assert.Equal(t, expected, actual)

    // require stops on failure
    require.NoError(t, err)
}
```

### Common Patterns

```go
// Skip long tests
func TestLongOperation(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping in short mode")
    }
    // ...
}

// Parallel tests
func TestParallel(t *testing.T) {
    t.Parallel()
    // ...
}
```

## Coverage Requirements

- New code should have >80% coverage
- Critical paths should have >90% coverage
- Run `go test -cover` before submitting

## CI/CD

Tests run automatically on:

- Pull request creation
- Push to main branch
- Release tags

## See Also

- **[Setup](setup.md)** - Development setup
- **[Code Style](code-style.md)** - Code conventions
- **[Contributing](../contributing.md)** - Contribution process
