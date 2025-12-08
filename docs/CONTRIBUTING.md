# Contributing to NornicDB

Thank you for your interest in contributing to NornicDB! This guide will help you get started.

## 📋 Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Coding Standards](#coding-standards)
- [Testing Guidelines](#testing-guidelines)
- [Documentation](#documentation)
- [Submitting Changes](#submitting-changes)
- [Community](#community)

## Code of Conduct

We are committed to providing a welcoming and inclusive environment. Please be respectful and professional in all interactions.

### Our Standards

- **Be respectful** - Treat everyone with respect and kindness
- **Be collaborative** - Work together to improve the project
- **Be constructive** - Provide helpful feedback and suggestions
- **Be patient** - Remember that everyone is learning

## Getting Started

### Prerequisites

- **Go 1.21+** - [Install Go](https://golang.org/doc/install)
- **Git** - [Install Git](https://git-scm.com/downloads)
- **Docker** (optional) - [Install Docker](https://docs.docker.com/get-docker/)

### Setup Development Environment

1. **Fork the repository**

   Visit [github.com/orneryd/nornicdb](https://github.com/orneryd/nornicdb) and click "Fork"

2. **Clone your fork**

   ```bash
   git clone https://github.com/YOUR_USERNAME/nornicdb.git
   cd nornicdb
   ```

3. **Add upstream remote**

   ```bash
   git remote add upstream https://github.com/orneryd/nornicdb.git
   ```

4. **Install dependencies**

   ```bash
   go mod download
   ```

5. **Run tests**

   ```bash
   go test ./...
   ```

6. **Build the project**

   ```bash
   go build -o nornicdb ./cmd/nornicdb
   ```

See [Development Setup](development/setup.md) for detailed instructions.

## Development Workflow

### 1. Create a Branch

```bash
# Update your fork
git fetch upstream
git checkout main
git merge upstream/main

# Create feature branch
git checkout -b feature/your-feature-name
```

### 2. Make Changes

- Write code following our [coding standards](#coding-standards)
- Add tests for new functionality
- Update documentation as needed
- Commit changes with clear messages

### 3. Test Your Changes

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...

# Run specific package
go test ./pkg/storage/...

# Run benchmarks
go test -bench=. ./pkg/...
```

### 4. Submit Pull Request

```bash
# Push to your fork
git push origin feature/your-feature-name

# Create pull request on GitHub
```

## Coding Standards

### Go Style Guide

We follow the official [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments).

#### Key Points

1. **Use `gofmt`** - Format all code with `gofmt`
2. **Use `golint`** - Check code with `golint`
3. **Follow conventions** - Use Go naming conventions
4. **Write godoc comments** - Document all public APIs
5. **Keep it simple** - Prefer simple, readable code

### Code Organization

```
pkg/
├── storage/      # Storage engine
├── cypher/       # Query parser and executor
├── index/        # Indexing system
├── gpu/          # GPU acceleration
├── auth/         # Authentication
└── ...
```

### Naming Conventions

- **Packages:** lowercase, single word (e.g., `storage`, `cypher`)
- **Files:** lowercase with underscores (e.g., `query_cache.go`)
- **Types:** PascalCase (e.g., `QueryCache`, `StorageEngine`)
- **Functions:** PascalCase for exported, camelCase for private
- **Variables:** camelCase (e.g., `queryCache`, `nodeID`)
- **Constants:** PascalCase or UPPER_CASE (e.g., `DefaultPort`, `MAX_RETRIES`)

### Documentation Standards

Every public function must have godoc comments:

```go
// NewQueryCache creates a new LRU cache for query plans with TTL expiration.
//
// The cache uses a least-recently-used (LRU) eviction policy and supports
// time-to-live (TTL) expiration for cached entries.
//
// Parameters:
//   - size: Maximum number of entries (must be > 0)
//   - ttl: Time-to-live for entries (0 = no expiration)
//
// Returns:
//   - *QueryCache: Configured cache instance
//
// Example:
//
//	cache := NewQueryCache(1000, 5*time.Minute)
//	cache.Put("query1", plan)
//	if plan, ok := cache.Get("query1"); ok {
//		// Use cached plan
//	}
//
// Performance:
//   - Get: O(1) average, ~100ns
//   - Put: O(1) average, ~200ns
//   - Thread-safe with read-write locks
func NewQueryCache(size int, ttl time.Duration) *QueryCache {
    // Implementation
}
```

See [Documentation Standards](development/documentation.md) for details.

## Testing Guidelines

### Test Coverage

- **Minimum 80% coverage** for new code
- **100% coverage** for critical paths (storage, transactions)
- **Integration tests** for end-to-end workflows
- **Benchmarks** for performance-critical code

### Writing Tests

```go
func TestQueryCache_GetPut(t *testing.T) {
    cache := NewQueryCache(10, 0)
    
    // Test Put
    cache.Put("key1", "value1")
    
    // Test Get
    value, ok := cache.Get("key1")
    if !ok {
        t.Fatal("expected key to exist")
    }
    if value != "value1" {
        t.Errorf("expected value1, got %v", value)
    }
    
    // Test non-existent key
    _, ok = cache.Get("key2")
    if ok {
        t.Error("expected key to not exist")
    }
}
```

### Running Tests

```bash
# All tests
go test ./...

# Specific package
go test ./pkg/storage

# With coverage
go test -cover ./...

# With race detector
go test -race ./...

# Verbose output
go test -v ./...

# Benchmarks
go test -bench=. ./pkg/cache
```

See [Testing Guide](development/testing.md) for details.

## Documentation

### Types of Documentation

1. **Code Comments** - Godoc for all public APIs
2. **User Guides** - How to use features
3. **API Reference** - Complete function reference
4. **Architecture Docs** - System design
5. **Examples** - Real-world usage

### Documentation Structure

```
docs/
├── getting-started/     # New user guides
├── user-guides/         # Feature guides
├── api-reference/       # API documentation
├── features/            # Feature-specific docs
├── architecture/        # System design
├── performance/         # Benchmarks
├── advanced/            # Advanced topics
├── compliance/          # Security & compliance
├── ai-agents/           # AI integration
├── neo4j-migration/     # Migration guide
├── operations/          # DevOps
└── development/         # Contributing
```

### Writing Documentation

- **Use Markdown** - All docs in Markdown format
- **Include examples** - Show real-world usage
- **Add ELI12** - Explain complex concepts simply
- **Link related docs** - Cross-reference related content
- **Keep it current** - Update docs with code changes

See [Documentation Standards](development/documentation.md) for details.

## Submitting Changes

### Pull Request Process

1. **Create PR** - Submit pull request to `main` branch
2. **Describe changes** - Explain what and why
3. **Link issues** - Reference related issues
4. **Wait for review** - Maintainers will review
5. **Address feedback** - Make requested changes
6. **Merge** - PR will be merged when approved

### PR Checklist

- [ ] Code follows style guide
- [ ] Tests added/updated
- [ ] Tests pass locally
- [ ] Documentation updated
- [ ] No breaking changes (or documented)
- [ ] Commit messages are clear
- [ ] Branch is up to date with main

### Commit Messages

Use clear, descriptive commit messages:

```
feat: add GPU acceleration for vector search

- Implement Metal backend for Apple Silicon
- Add CUDA backend for NVIDIA GPUs
- Add OpenCL backend for AMD GPUs
- Automatic CPU fallback when GPU unavailable

Closes #123
```

**Format:**
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `test:` - Test changes
- `refactor:` - Code refactoring
- `perf:` - Performance improvements
- `chore:` - Maintenance tasks

## Community

### Getting Help

- **GitHub Issues** - Bug reports and feature requests
- **GitHub Issues** - Questions and ideas
- **Discord** - Real-time chat (coming soon)

### Reporting Bugs

When reporting bugs, please include:

1. **Description** - What happened vs what you expected
2. **Steps to reproduce** - How to trigger the bug
3. **Environment** - OS, Go version, NornicDB version
4. **Logs** - Relevant error messages
5. **Code sample** - Minimal reproduction case

### Feature Requests

When requesting features, please include:

1. **Use case** - Why you need this feature
2. **Proposed solution** - How it should work
3. **Alternatives** - Other approaches you considered
4. **Examples** - Similar features in other projects

## Recognition

Contributors will be recognized in:

- **CHANGELOG.md** - Listed in release notes
- **README.md** - Contributors section
- **GitHub** - Contributor badge

Thank you for contributing to NornicDB! 🎉

---

**Questions?** Open a [GitHub Issue](https://github.com/orneryd/NornicDB/issues)  
**Found a bug?** Open a [GitHub Issue](https://github.com/orneryd/nornicdb/issues)  
**Want to chat?** Join our Discord (coming soon)
