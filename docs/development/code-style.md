# Code Style

**Coding conventions for NornicDB.**

## Go Formatting

Use `gofmt -s` or `goimports`:

```bash
gofmt -w -s .
goimports -w .
```

The repository also includes a tracked pre-commit hook at `.githooks/pre-commit`
that automatically runs `gofmt -w -s` on staged Go files before the commit is
created. Install it once per clone:

```bash
make install-hooks
```

## Naming Conventions

### Files

- Lowercase with underscores: `db_test.go`
- Test files: `*_test.go`
- Platform-specific: `*_linux.go`, `*_darwin.go`

### Variables

```go
// Local variables: camelCase
userName := "alice"

// Package-level variables: camelCase or CamelCase
var defaultTimeout = 30 * time.Second

// Constants: CamelCase
const MaxRetries = 3
```

### Functions

```go
// Exported: CamelCase
func CreateUser() {}

// Unexported: camelCase
func validateInput() {}
```

### Interfaces

```go
// Use -er suffix for single-method interfaces
type Reader interface {
    Read([]byte) (int, error)
}

// Use descriptive names for multi-method
type Engine interface {
    Get(key []byte) ([]byte, error)
    Set(key, value []byte) error
}
```

## Error Handling

### Always handle errors

```go
// Good
result, err := doSomething()
if err != nil {
    return fmt.Errorf("doing something: %w", err)
}

// Bad - ignoring error
result, _ := doSomething()
```

### Error wrapping

```go
// Wrap with context
if err != nil {
    return fmt.Errorf("creating user %s: %w", username, err)
}
```

### Error types

```go
// Define package errors
var (
    ErrNotFound = errors.New("not found")
    ErrInvalid  = errors.New("invalid input")
)
```

## Comments

### Package comments

```go
// Package nornicdb provides a graph database with memory decay.
//
// NornicDB implements a Neo4j-compatible graph database with
// vector search and automatic memory management.
package nornicdb
```

### Function comments

```go
// CreateUser creates a new user with the given credentials.
// Returns ErrUserExists if the username is already taken.
func CreateUser(username, password string) (*User, error) {
```

### Inline comments

```go
// Use sparingly, only when needed
func complex() {
    // Step 1: Validate input
    if err := validate(); err != nil {
        return err
    }

    // Step 2: Process data
    // This algorithm is O(n log n) due to...
    process()
}
```

## Code Organization

### File structure

1. Package declaration
2. Imports (stdlib, then external, then internal)
3. Constants
4. Package variables
5. Types
6. Constructor functions
7. Methods
8. Helper functions

### Import grouping

```go
import (
    // Standard library
    "context"
    "fmt"
    "time"

    // External packages
    "github.com/stretchr/testify/assert"

    // Internal packages
    "github.com/orneryd/nornicdb/pkg/storage"
)
```

## Best Practices

### Use context

```go
func DoSomething(ctx context.Context) error {
    select {
    case <-ctx.Done():
        return ctx.Err()
    default:
        // continue
    }
}
```

### Avoid global state

```go
// Bad
var globalDB *Database

// Good - pass dependencies
func NewService(db *Database) *Service {
    return &Service{db: db}
}
```

### Prefer composition

```go
// Good
type Server struct {
    db   *Database
    auth *Authenticator
}

// Avoid deep inheritance
```

## Linting

Run before committing:

```bash
golangci-lint run
```

## See Also

- **[Testing](testing.md)** - Testing guidelines
- **[Setup](setup.md)** - Development setup
- **[Contributing](../contributing.md)** - Contribution process
