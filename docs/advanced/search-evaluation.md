# NornicDB Eval Harness

**Search Quality Evaluation & Validation**

## Overview

The eval harness provides automated testing and validation of NornicDB's search quality. It computes standard Information Retrieval (IR) metrics and reports pass/fail based on configurable thresholds.

## Quick Start

```bash
# Run against running server with built-in tests
cd nornicdb
go run ./cmd/eval

# Run with custom test suite
go run ./cmd/eval -suite path/to/tests.json

# Output JSON for CI/CD
go run ./cmd/eval -output json -save results.json
```

## Metrics Computed

| Metric | Description | Range |
|--------|-------------|-------|
| **Precision@K** | Fraction of top-K results that are relevant | 0-1 |
| **Recall@K** | Fraction of all relevant docs in top-K | 0-1 |
| **MRR** | Mean Reciprocal Rank - where first relevant result appears | 0-1 |
| **NDCG@K** | Normalized Discounted Cumulative Gain - ranking quality | 0-1 |
| **MAP** | Mean Average Precision | 0-1 |
| **Hit Rate** | Fraction of queries with at least one relevant result | 0-1 |

## Command-Line Options

```bash
go run ./cmd/eval [flags]

Flags:
  -url string
        NornicDB server URL (default "http://localhost:7474")
  -suite string
        Path to test suite JSON file
  -output string
        Output format: summary, detailed, json, compact (default "summary")
  -save string
        Save results to JSON file
  -threshold string
        Override thresholds (format: p10=0.5,mrr=0.5,hit=0.8)
  -create-sample
        Create sample test data in the database
```

## Test Suite Format

```json
{
  "name": "my-test-suite",
  "description": "Search quality tests",
  "version": "1.0.0",
  "test_cases": [
    {
      "name": "ML Concept Search",
      "query": "machine learning neural networks",
      "expected": ["node-id-1", "node-id-2"],
      "tags": ["ml", "concepts"]
    },
    {
      "name": "Graded Relevance Test",
      "query": "database architecture",
      "expected": ["db-1", "db-2", "db-3"],
      "relevance_grades": {
        "db-1": 3,
        "db-2": 2,
        "db-3": 1
      },
      "tags": ["database"]
    }
  ]
}
```

### Test Case Fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Human-readable test name |
| `query` | string | Search query text |
| `expected` | []string | Node IDs that should be returned |
| `relevance_grades` | map[string]int | Optional graded relevance (0-3) for NDCG |
| `tags` | []string | Optional tags for filtering |

## Default Thresholds

```go
Thresholds{
    Precision10: 0.5,  // At least 50% of top-10 relevant
    Recall10:    0.3,  // At least 30% of relevant in top-10
    MRR:         0.5,  // First relevant in top 2 on average
    NDCG10:      0.5,  // Reasonable ranking quality
    HitRate:     0.8,  // 80% of queries have at least one hit
}
```

## Output Formats

### Summary (default)

```
╔════════════════════════════════════════════════════════════════╗
║           NornicDB Search Evaluation Results                   ║
╚════════════════════════════════════════════════════════════════╝

📊 Suite: my-tests
📅 Time:  2025-12-01T09:10:12-07:00
⏱️  Duration: 125ms

✅ Tests: 5/5 passed (100.0%)

┌─────────────────────────────────────────────────────────────────┐
│                     Aggregate Metrics                           │
├─────────────────────────────────────────────────────────────────┤
│ ✓ MRR            [████████████████████] 1.000 (target: 0.50)
│ ✓ Recall@10      [████████████████████] 1.000 (target: 0.30)
│ ✓ Hit Rate       [████████████████████] 1.000 (target: 0.80)
└─────────────────────────────────────────────────────────────────┘
```

### Detailed

Includes per-test breakdown:

```
✅ Test 1: ML Search
   Query: "machine learning"
   Method: http | Duration: 1.552ms
   P@10: 0.10 | R@10: 1.00 | MRR: 1.00 | NDCG@10: 1.00
   Expected: 1 | Returned: 1 | Hits: 1
```

### Compact

One-line summary for CI logs:

```
[PASS] 5/5 tests | P@10=0.10 R@10=1.00 MRR=1.00 NDCG=1.00 HitRate=1.00 | 8ms
```

### JSON

Full structured output for programmatic processing.

## CI/CD Integration

### GitHub Actions Example

```yaml
- name: Run Search Quality Tests
  run: |
    cd nornicdb
    go run ./cmd/eval \
      -suite tests/search_quality.json \
      -output json \
      -save eval-results.json \
      -threshold="hit=0.9,mrr=0.7"
    
- name: Upload Results
  uses: actions/upload-artifact@v3
  with:
    name: eval-results
    path: nornicdb/eval-results.json
```

### Exit Codes

- `0`: All tests passed
- `1`: One or more tests failed

## Programmatic Usage

```go
import (
    "github.com/orneryd/nornicdb/pkg/eval"
    "github.com/orneryd/nornicdb/pkg/search"
)

// Create harness
harness := eval.NewHarness(searchService)

// Add test cases
harness.AddTestCase(eval.TestCase{
    Name:     "ML concepts",
    Query:    "machine learning",
    Expected: []string{"node-1", "node-2"},
})

// Set custom thresholds
harness.SetThresholds(eval.Thresholds{
    MRR:     0.7,
    HitRate: 0.9,
})

// Run evaluation
result, err := harness.Run(ctx)

// Output results
reporter := eval.NewReporter(os.Stdout)
reporter.PrintSummary(result)
```

## Best Practices

### 1. Use Real Node IDs

Test cases should use actual storage node IDs, not user-defined `id` properties:

```json
// ✅ Good - uses actual storage IDs
"expected": ["n1", "node-abc123"]

// ❌ Bad - uses property values
"expected": ["my-custom-id"]
```

### 2. Use Graded Relevance for NDCG

For meaningful NDCG scores, provide relevance grades:

```json
"relevance_grades": {
    "highly-relevant-doc": 3,
    "relevant-doc": 2,
    "marginal-doc": 1,
    "irrelevant-doc": 0
}
```

### 3. Set Realistic Thresholds

Start with lenient thresholds and tighten as search quality improves:

```bash
# Development
-threshold="hit=0.5,mrr=0.3"

# Production
-threshold="hit=0.9,mrr=0.7,p10=0.5"
```

### 4. Tag Tests for Filtering

Use tags to organize and filter tests:

```json
"tags": ["semantic", "ml", "critical"]
```

## Troubleshooting

### Low Precision but High Recall

Search returns many results but expected docs are scattered.
- **Fix**: Improve ranking algorithm or add MMR diversification

### Zero Hit Rate

No expected docs found in any results.
- **Check**: Are expected IDs correct (storage IDs, not properties)?
- **Check**: Has the search index been rebuilt?

```bash
curl -X POST http://localhost:7474/nornicdb/search/rebuild
```

### Slow Evaluation

- Reduce `limit` in search options
- Use fewer test cases for quick iteration
- Run full suite only in CI

## Related Documentation

- [Vector Search Guide](../user-guides/vector-search.md)
- [Hybrid Search Guide](../user-guides/hybrid-search.md)
- [Hybrid Search - RRF Algorithm](../user-guides/hybrid-search.md#rrf-algorithm)

---

_Eval Harness v1.0 - December 2025_
