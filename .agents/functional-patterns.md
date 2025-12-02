# Functional Go Patterns

**Purpose**: Advanced functional programming patterns in Go  
**Audience**: AI coding agents  
**Focus**: Dependency injection, higher-order functions, composition

---

## Core Concept

Go supports **first-class functions** - functions can be:
- Assigned to variables
- Passed as arguments
- Returned from functions
- Stored in data structures

This enables powerful patterns for **dependency injection** and **runtime behavior modification**.

---

## Pattern 1: Function Types for Dependency Injection

### Basic Pattern

**Instead of interfaces, use function types:**

```go
// Traditional interface approach
type EmbeddingGenerator interface {
    Generate(text string) ([]float32, error)
}

// Functional approach - simpler!
type EmbeddingFunc func(text string) ([]float32, error)
```

### Real-World Example

```go
// pkg/search/search.go

// EmbeddingFunc generates vector embeddings for text
type EmbeddingFunc func(text string) ([]float32, error)

// SearchEngine uses injected embedding function
type SearchEngine struct {
    embedder EmbeddingFunc  // ← Function as dependency
    index    *HNSWIndex
}

// NewSearchEngine creates engine with custom embedder
func NewSearchEngine(embedder EmbeddingFunc) *SearchEngine {
    return &SearchEngine{
        embedder: embedder,
        index:    NewHNSWIndex(),
    }
}

// Search uses the injected embedder
func (s *SearchEngine) Search(query string, limit int) ([]Result, error) {
    // Generate embedding using injected function
    embedding, err := s.embedder(query)
    if err != nil {
        return nil, err
    }
    
    // Search index
    return s.index.Search(embedding, limit)
}
```

### Usage Examples

```go
// Production: Use GPU-accelerated embeddings
gpuEmbedder := func(text string) ([]float32, error) {
    return gpu.GenerateEmbedding(text)
}
engine := NewSearchEngine(gpuEmbedder)

// Testing: Use mock embeddings
mockEmbedder := func(text string) ([]float32, error) {
    return []float32{0.1, 0.2, 0.3}, nil
}
testEngine := NewSearchEngine(mockEmbedder)

// Development: Use cached embeddings
cachedEmbedder := func(text string) ([]float32, error) {
    if cached, ok := cache.Get(text); ok {
        return cached.([]float32), nil
    }
    embedding, err := gpu.GenerateEmbedding(text)
    if err == nil {
        cache.Set(text, embedding)
    }
    return embedding, err
}
devEngine := NewSearchEngine(cachedEmbedder)

// Offline: Use pre-computed embeddings
offlineEmbedder := func(text string) ([]float32, error) {
    return loadPrecomputed(text)
}
offlineEngine := NewSearchEngine(offlineEmbedder)
```

### Benefits

✅ **Simple** - No interface boilerplate  
✅ **Flexible** - Easy to swap implementations  
✅ **Testable** - Trivial to inject mocks  
✅ **Composable** - Can wrap functions  

---

## Pattern 2: Higher-Order Functions

### Function Decorators

**Wrap functions to add behavior:**

```go
// Logging decorator
func WithLogging(fn EmbeddingFunc, logger *log.Logger) EmbeddingFunc {
    return func(text string) ([]float32, error) {
        logger.Printf("Generating embedding for: %s", text)
        start := time.Now()
        
        result, err := fn(text)
        
        duration := time.Since(start)
        if err != nil {
            logger.Printf("Error after %v: %v", duration, err)
        } else {
            logger.Printf("Success in %v, dims: %d", duration, len(result))
        }
        
        return result, err
    }
}

// Caching decorator
func WithCache(fn EmbeddingFunc, cache Cache) EmbeddingFunc {
    return func(text string) ([]float32, error) {
        // Check cache first
        if cached, ok := cache.Get(text); ok {
            return cached.([]float32), nil
        }
        
        // Generate and cache
        result, err := fn(text)
        if err == nil {
            cache.Set(text, result)
        }
        
        return result, err
    }
}

// Retry decorator
func WithRetry(fn EmbeddingFunc, maxRetries int) EmbeddingFunc {
    return func(text string) ([]float32, error) {
        var lastErr error
        
        for i := 0; i < maxRetries; i++ {
            result, err := fn(text)
            if err == nil {
                return result, nil
            }
            
            lastErr = err
            time.Sleep(time.Duration(i+1) * time.Second)
        }
        
        return nil, fmt.Errorf("failed after %d retries: %w", 
            maxRetries, lastErr)
    }
}

// Timeout decorator
func WithTimeout(fn EmbeddingFunc, timeout time.Duration) EmbeddingFunc {
    return func(text string) ([]float32, error) {
        ctx, cancel := context.WithTimeout(context.Background(), timeout)
        defer cancel()
        
        resultChan := make(chan []float32)
        errChan := make(chan error)
        
        go func() {
            result, err := fn(text)
            if err != nil {
                errChan <- err
            } else {
                resultChan <- result
            }
        }()
        
        select {
        case result := <-resultChan:
            return result, nil
        case err := <-errChan:
            return nil, err
        case <-ctx.Done():
            return nil, fmt.Errorf("timeout after %v", timeout)
        }
    }
}
```

### Composing Decorators

**Stack multiple behaviors:**

```go
// Start with base embedder
baseEmbedder := gpu.GenerateEmbedding

// Add caching
cachedEmbedder := WithCache(baseEmbedder, cache)

// Add logging
loggedEmbedder := WithLogging(cachedEmbedder, logger)

// Add retry
resilientEmbedder := WithRetry(loggedEmbedder, 3)

// Add timeout
finalEmbedder := WithTimeout(resilientEmbedder, 30*time.Second)

// Use composed function
engine := NewSearchEngine(finalEmbedder)

// Execution flow:
// 1. Check timeout
// 2. Try up to 3 times
// 3. Log each attempt
// 4. Check cache
// 5. Call GPU embedder
```

---

## Pattern 3: Function Factories

### Parameterized Function Creation

```go
// Factory creates configured embedding functions
func NewOllamaEmbedder(baseURL string, model string) EmbeddingFunc {
    client := &http.Client{Timeout: 30 * time.Second}
    
    return func(text string) ([]float32, error) {
        req := map[string]interface{}{
            "model":  model,
            "prompt": text,
        }
        
        body, _ := json.Marshal(req)
        resp, err := client.Post(
            baseURL+"/api/embeddings",
            "application/json",
            bytes.NewBuffer(body),
        )
        if err != nil {
            return nil, err
        }
        defer resp.Body.Close()
        
        var result struct {
            Embedding []float32 `json:"embedding"`
        }
        
        if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
            return nil, err
        }
        
        return result.Embedding, nil
    }
}

// Usage
embedder := NewOllamaEmbedder("http://localhost:11434", "mxbai-embed-large")
engine := NewSearchEngine(embedder)
```

### Configuration-Based Factories

```go
// EmbedderConfig defines embedding configuration
type EmbedderConfig struct {
    Provider   string // "ollama", "openai", "local"
    Model      string
    BaseURL    string
    APIKey     string
    Dimensions int
}

// NewEmbedderFromConfig creates embedder based on config
func NewEmbedderFromConfig(cfg EmbedderConfig) (EmbeddingFunc, error) {
    switch cfg.Provider {
    case "ollama":
        return NewOllamaEmbedder(cfg.BaseURL, cfg.Model), nil
        
    case "openai":
        return NewOpenAIEmbedder(cfg.APIKey, cfg.Model), nil
        
    case "local":
        return NewLocalGGUFEmbedder(cfg.Model, cfg.Dimensions)
        
    default:
        return nil, fmt.Errorf("unknown provider: %s", cfg.Provider)
    }
}

// Usage
config := EmbedderConfig{
    Provider: "ollama",
    Model:    "mxbai-embed-large",
    BaseURL:  "http://localhost:11434",
}

embedder, err := NewEmbedderFromConfig(config)
if err != nil {
    log.Fatal(err)
}

engine := NewSearchEngine(embedder)
```

---

## Pattern 4: Function Pipelines

### Sequential Processing

```go
// ProcessFunc transforms data
type ProcessFunc func([]float32) ([]float32, error)

// Pipeline chains multiple processors
type Pipeline struct {
    steps []ProcessFunc
}

// NewPipeline creates a processing pipeline
func NewPipeline(steps ...ProcessFunc) *Pipeline {
    return &Pipeline{steps: steps}
}

// Execute runs all steps in sequence
func (p *Pipeline) Execute(input []float32) ([]float32, error) {
    result := input
    
    for i, step := range p.steps {
        var err error
        result, err = step(result)
        if err != nil {
            return nil, fmt.Errorf("step %d failed: %w", i, err)
        }
    }
    
    return result, nil
}

// Common processing steps
func Normalize() ProcessFunc {
    return func(v []float32) ([]float32, error) {
        var sum float32
        for _, val := range v {
            sum += val * val
        }
        
        magnitude := float32(math.Sqrt(float64(sum)))
        if magnitude == 0 {
            return v, nil
        }
        
        result := make([]float32, len(v))
        for i, val := range v {
            result[i] = val / magnitude
        }
        
        return result, nil
    }
}

func Quantize(bits int) ProcessFunc {
    return func(v []float32) ([]float32, error) {
        levels := float32(1 << bits)
        result := make([]float32, len(v))
        
        for i, val := range v {
            quantized := math.Round(float64(val * levels))
            result[i] = float32(quantized) / levels
        }
        
        return result, nil
    }
}

func Truncate(dims int) ProcessFunc {
    return func(v []float32) ([]float32, error) {
        if len(v) <= dims {
            return v, nil
        }
        return v[:dims], nil
    }
}

// Usage
pipeline := NewPipeline(
    Normalize(),
    Quantize(8),
    Truncate(512),
)

processed, err := pipeline.Execute(embedding)
```

---

## Pattern 5: Functional Options

### Flexible Configuration

```go
// Option configures SearchEngine
type Option func(*SearchEngine)

// WithCache adds caching
func WithCache(cache Cache) Option {
    return func(s *SearchEngine) {
        s.cache = cache
    }
}

// WithLogger adds logging
func WithLogger(logger *log.Logger) Option {
    return func(s *SearchEngine) {
        s.logger = logger
    }
}

// WithMaxResults sets result limit
func WithMaxResults(max int) Option {
    return func(s *SearchEngine) {
        s.maxResults = max
    }
}

// NewSearchEngine creates engine with options
func NewSearchEngine(embedder EmbeddingFunc, opts ...Option) *SearchEngine {
    engine := &SearchEngine{
        embedder:   embedder,
        maxResults: 10,  // default
    }
    
    // Apply options
    for _, opt := range opts {
        opt(engine)
    }
    
    return engine
}

// Usage
engine := NewSearchEngine(
    embedder,
    WithCache(cache),
    WithLogger(logger),
    WithMaxResults(50),
)
```

---

## Pattern 6: Middleware Pattern

### Request/Response Wrapping

```go
// Middleware wraps execution with additional behavior
type Middleware func(ExecuteFunc) ExecuteFunc

// ExecuteFunc executes a query
type ExecuteFunc func(ctx context.Context, query string) (*Result, error)

// LoggingMiddleware logs execution
func LoggingMiddleware(logger *log.Logger) Middleware {
    return func(next ExecuteFunc) ExecuteFunc {
        return func(ctx context.Context, query string) (*Result, error) {
            logger.Printf("Executing: %s", query)
            start := time.Now()
            
            result, err := next(ctx, query)
            
            duration := time.Since(start)
            if err != nil {
                logger.Printf("Error after %v: %v", duration, err)
            } else {
                logger.Printf("Success in %v, rows: %d", duration, len(result.Rows))
            }
            
            return result, err
        }
    }
}

// MetricsMiddleware tracks metrics
func MetricsMiddleware(metrics *Metrics) Middleware {
    return func(next ExecuteFunc) ExecuteFunc {
        return func(ctx context.Context, query string) (*Result, error) {
            start := time.Now()
            
            result, err := next(ctx, query)
            
            duration := time.Since(start)
            metrics.RecordQuery(duration, err == nil)
            
            return result, err
        }
    }
}

// CachingMiddleware adds caching
func CachingMiddleware(cache Cache) Middleware {
    return func(next ExecuteFunc) ExecuteFunc {
        return func(ctx context.Context, query string) (*Result, error) {
            // Check cache
            if cached, ok := cache.Get(query); ok {
                return cached.(*Result), nil
            }
            
            // Execute
            result, err := next(ctx, query)
            if err == nil {
                cache.Set(query, result)
            }
            
            return result, err
        }
    }
}

// Chain applies middleware in order
func Chain(execute ExecuteFunc, middleware ...Middleware) ExecuteFunc {
    // Apply in reverse order so first middleware is outermost
    for i := len(middleware) - 1; i >= 0; i-- {
        execute = middleware[i](execute)
    }
    return execute
}

// Usage
baseExecute := func(ctx context.Context, query string) (*Result, error) {
    // Core execution logic
    return executor.Execute(ctx, query, nil)
}

// Wrap with middleware
execute := Chain(
    baseExecute,
    LoggingMiddleware(logger),
    MetricsMiddleware(metrics),
    CachingMiddleware(cache),
)

// Execute with all middleware
result, err := execute(ctx, "MATCH (n) RETURN n")
```

---

## Real-World Examples from Codebase

### Example 1: Storage Interface

```go
// pkg/storage/types.go

// Engine defines storage operations
type Engine interface {
    CreateNode(node *Node) error
    GetNode(id NodeID) (*Node, error)
    UpdateNode(node *Node) error
    DeleteNode(id NodeID) error
    // ... more methods
}

// Multiple implementations:
// - MemoryEngine (testing)
// - BadgerEngine (production)
// - CachedEngine (with caching layer)

// Usage with dependency injection
type Executor struct {
    storage Engine  // ← Interface, not concrete type
}

func NewExecutor(storage Engine) *Executor {
    return &Executor{storage: storage}
}

// Testing with mock
func TestExecutor(t *testing.T) {
    mockStorage := &MockEngine{
        nodes: make(map[NodeID]*Node),
    }
    
    executor := NewExecutor(mockStorage)
    
    // Test with mock storage
    result, err := executor.Execute(ctx, "CREATE (n:Test)", nil)
    assert.NoError(t, err)
}
```

### Example 2: Embedding Provider

```go
// pkg/embed/embed.go

// EmbedFunc generates embeddings
type EmbedFunc func(text string) ([]float32, error)

// Provider wraps embedding function with metadata
type Provider struct {
    Name       string
    Dimensions int
    Embed      EmbedFunc
}

// NewOllamaProvider creates Ollama-based embedder
func NewOllamaProvider(baseURL, model string, dims int) *Provider {
    embedFunc := func(text string) ([]float32, error) {
        // Call Ollama API
        return callOllamaAPI(baseURL, model, text)
    }
    
    return &Provider{
        Name:       "ollama/" + model,
        Dimensions: dims,
        Embed:      embedFunc,
    }
}

// NewLocalProvider creates local GGUF-based embedder
func NewLocalProvider(modelPath string, dims int) (*Provider, error) {
    model, err := loadGGUFModel(modelPath)
    if err != nil {
        return nil, err
    }
    
    embedFunc := func(text string) ([]float32, error) {
        return model.GenerateEmbedding(text)
    }
    
    return &Provider{
        Name:       "local/" + filepath.Base(modelPath),
        Dimensions: dims,
        Embed:      embedFunc,
    }, nil
}

// Usage - swap providers at runtime
var embedder EmbedFunc

if config.UseLocal {
    provider, _ := NewLocalProvider(config.ModelPath, 1024)
    embedder = provider.Embed
} else {
    provider := NewOllamaProvider(config.OllamaURL, config.Model, 1024)
    embedder = provider.Embed
}

// Add caching
embedder = WithCache(embedder, cache)

// Use in search engine
engine := NewSearchEngine(embedder)
```

---

## Benefits Summary

### Compared to Interfaces

**Function Types:**
✅ Less boilerplate  
✅ Easier to compose  
✅ Simpler mocking  
✅ More flexible  

**Interfaces:**
✅ Multiple methods  
✅ Stronger contracts  
✅ Better for complex APIs  

### When to Use Each

**Use function types when:**
- Single method interface
- Need runtime swapping
- Want easy composition
- Mocking is important

**Use interfaces when:**
- Multiple related methods
- Complex contracts
- Need type safety
- Building large APIs

---

## Quick Reference

### Function Type Template

```go
// Define function type
type ProcessFunc func(input Data) (output Data, err error)

// Use in struct
type Processor struct {
    process ProcessFunc
}

// Factory function
func NewProcessor(fn ProcessFunc) *Processor {
    return &Processor{process: fn}
}

// Decorator
func WithLogging(fn ProcessFunc) ProcessFunc {
    return func(input Data) (Data, error) {
        log.Println("Processing...")
        return fn(input)
    }
}

// Usage
processor := NewProcessor(
    WithLogging(myProcessFunc),
)
```

---

**Remember**: Functional patterns make code more flexible and testable. Use them when you need runtime behavior modification or easy dependency injection.
