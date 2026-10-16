# Testing Guide

**How to write and run tests for NornicDB.**

## Test Structure

Tests live alongside the code they test:

- `db.go` → `db_test.go`
- `server.go` → `server_test.go`

## Running Tests

Use the repository-pinned Go **1.26.9** toolchain for local tests and CI. This
patch includes the [macOS 27 plugin-linker fix](https://github.com/golang/go/issues/81889):
earlier Go 1.26 patches can build `.so` files whose Mach-O chained-fixup segment
count disagrees with the debug segments, causing `plugin.Open` to fail with
`seg_count does not match number of segments`. Go 1.27 users need **1.27.2** or
later for the corresponding fix. Build the host and its plugins with the same
exact Go toolchain, build tags and race setting; rebuild both after upgrading.
Keep the real plugin-loading assertions and debug information enabled.

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
