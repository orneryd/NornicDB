## Branch and milestone
<!-- See CONTRIBUTING.md, "Development workflow". -->
- [ ] Base is `develop`, `release/vX.Y.Z` for a release fix, or `main` for scoped maintainer operational/documentation work (explain below)
- [ ] On the release milestone, or N/A for maintainer maintenance (`make milestone VERSION=x.y.z ISSUES="<this PR>"`)
- [ ] `make verify BASE=origin/<pr-base>` passes locally (list any skipped steps below). PRs and labels do not run GitHub checks. Manual runs require admin approval.

- [ ] Through October 31, 2026 (America/Phoenix): local `make verify` result comment posted for the current head/base SHAs, with failures/skips explained; regressions fixed or linked to a shared-root fix PR pending propagation.

## Description
<!-- Provide a clear and concise description of your changes -->

## Type of Change
<!-- Mark the relevant option with an 'x' -->
- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Performance improvement
- [ ] Refactoring (no functional changes)
- [ ] Documentation update
- [ ] CI/CD or build changes

## Related Issues
<!-- Link related issues: Fixes #123, Closes #456 -->

## Changes Made
<!-- List the specific changes in this PR -->
- 
- 
- 

## Testing
<!-- Describe the tests you ran and how to reproduce them -->

### Test Coverage
- [ ] Added tests for new functionality
- [ ] Updated tests for changed functionality
- [ ] All tests pass locally (`go test ./...`)
- [ ] Coverage maintained at ≥90% (`go test -coverprofile=coverage.out ./...`)

### Manual Testing
<!-- Describe manual testing performed -->
1. 
2. 
3. 

## Performance Impact
<!-- Required for all code changes -->

### Benchmarks
<!-- Run benchmarks before and after your changes -->
```bash
# Before
go test ./pkg/cypher -bench=. -benchmem

# After
go test ./pkg/cypher -bench=. -benchmem
```

**Results:**
- Query execution: [e.g., no change, 2x faster, 10% slower]
- Memory usage: [e.g., no change, -15% reduction, +5% increase]
- Justification (if regression): [explain why the performance trade-off is acceptable]

## Neo4j Compatibility
<!-- If this changes Cypher behavior -->
- [ ] Tested against Neo4j for compatibility
- [ ] Behavior matches Neo4j exactly
- [ ] Not applicable (internal change only)

## Documentation
- [ ] Updated relevant documentation in `/docs`
- [ ] Updated CHANGELOG.md
- [ ] Added code comments for complex logic
- [ ] Updated README.md (if user-facing change)
- [ ] Not applicable

## Code Quality Checklist
<!-- All items must be checked before merging -->
- [ ] Code follows the [AGENTS.md](AGENTS.md) guidelines
- [ ] No files exceed 2500 lines (split if necessary)
- [ ] Used functional Go patterns (dependency injection via function types)
- [ ] DRY principle applied (no repeated code)
- [ ] Proper error handling
- [ ] Added/updated tests (minimum 90% coverage)
- [ ] All tests pass: `go test ./... -v`
- [ ] No race conditions: `go test ./... -race`
- [ ] Code formatted: `go fmt ./...`
- [ ] Linter passes: `golangci-lint run`
- [ ] Commit messages follow conventional format

## Proof of Value
<!-- Required for all non-trivial changes -->

### For Bug Fixes:
- [ ] Created failing test that reproduces the bug
- [ ] Test now passes with the fix
- [ ] Added regression tests

### For Performance Optimizations:
- [ ] Included before/after benchmarks showing improvement
- [ ] No significant memory regression (>10%)
- [ ] Justified any trade-offs

### For New Features:
- [ ] Documented use cases and benefits
- [ ] Included examples in documentation
- [ ] Added integration tests

## Screenshots (if applicable)
<!-- Add screenshots for UI changes or visual features -->

## Additional Notes
<!-- Any other information relevant to this PR -->

---

## Reviewer Checklist
<!-- For reviewers to complete -->
- [ ] Code quality meets standards
- [ ] Tests are comprehensive
- [ ] Documentation is clear and complete
- [ ] No security concerns
- [ ] Performance impact is acceptable
- [ ] Changes align with project goals
