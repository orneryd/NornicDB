# NornicDB AI Agent Development Guides

This directory contains detailed development guides for AI coding agents working on NornicDB.

---

## 📚 Guide Index

### Core Guides

1. **[../AGENTS.md](../AGENTS.md)** - **START HERE**
   - Enterprise-level development guide
   - Golden rules and core philosophy
   - Quick reference for all patterns
   - Pre-commit checklist

### Detailed Guides

2. **[testing-patterns.md](testing-patterns.md)** - Testing Best Practices
   - Test pyramid and structure
   - Table-driven tests
   - Bug regression tests
   - Integration and benchmark tests
   - 90% coverage requirements

3. **[bug-fix-workflow.md](bug-fix-workflow.md)** - Bug Fix Process
   - 5-step mandatory workflow
   - Test-driven bug fixes
   - Regression prevention
   - Real-world examples from codebase

4. **[refactoring-guidelines.md](refactoring-guidelines.md)** - Code Refactoring
   - File size limits (2500 lines)
   - Separation of concerns
   - Module extraction
   - Interface definition

5. **[functional-patterns.md](functional-patterns.md)** - Functional Go Patterns
   - Function types for dependency injection
   - Higher-order functions
   - Function decorators
   - Middleware patterns

6. **[documentation-standards.md](documentation-standards.md)** - Documentation Requirements
   - Package-level documentation
   - Type and function documentation
   - Real-world examples (1-3 per API)
   - ELI12 explanations

7. **[architecture-patterns.md](architecture-patterns.md)** - System Architecture
   - Layer responsibilities
   - Design patterns
   - Dependency injection
   - Package organization

8. **[performance-optimization.md](performance-optimization.md)** - Performance Tuning
   - Benchmarking process
   - Profiling techniques
   - Optimization patterns
   - Memory optimization

9. **[cypher-compatibility.md](cypher-compatibility.md)** - Neo4j Compatibility
   - Cypher feature implementation
   - Compatibility testing
   - Error handling
   - Parameter substitution

---

## 🎯 Quick Start

### For New Contributors

1. Read [../AGENTS.md](../AGENTS.md) thoroughly
2. Review [testing-patterns.md](testing-patterns.md)
3. Study [bug-fix-workflow.md](bug-fix-workflow.md)
4. Explore the codebase with guides as reference

### For Specific Tasks

**Adding a new feature:**
1. [architecture-patterns.md](architecture-patterns.md) - Design the feature
2. [testing-patterns.md](testing-patterns.md) - Write tests first
3. [documentation-standards.md](documentation-standards.md) - Document the API
4. [performance-optimization.md](performance-optimization.md) - Benchmark it

**Fixing a bug:**
1. [bug-fix-workflow.md](bug-fix-workflow.md) - Follow the 5-step process
2. [testing-patterns.md](testing-patterns.md) - Write regression tests
3. [cypher-compatibility.md](cypher-compatibility.md) - Verify Neo4j compatibility

**Refactoring code:**
1. [refactoring-guidelines.md](refactoring-guidelines.md) - Plan the refactoring
2. [architecture-patterns.md](architecture-patterns.md) - Define clean boundaries
3. [testing-patterns.md](testing-patterns.md) - Ensure tests pass

**Optimizing performance:**
1. [performance-optimization.md](performance-optimization.md) - Benchmark and profile
2. [testing-patterns.md](testing-patterns.md) - Add benchmark tests
3. [documentation-standards.md](documentation-standards.md) - Document improvements

---

## 📋 Development Checklist

### Before Starting Work

- [ ] Read relevant guides
- [ ] Understand the architecture
- [ ] Run existing tests
- [ ] Check test coverage baseline

### During Development

- [ ] Follow TDD (tests first)
- [ ] Write clear, documented code
- [ ] Keep files under 2500 lines
- [ ] Use functional patterns where appropriate
- [ ] Benchmark performance-critical code

### Before Committing

- [ ] All tests pass (`go test ./... -v`)
- [ ] Coverage ≥90% (`go tool cover`)
- [ ] No race conditions (`go test -race`)
- [ ] Code formatted (`go fmt`)
- [ ] Linter passes (`golangci-lint run`)
- [ ] Benchmarks run (if performance-critical)
- [ ] Documentation updated
- [ ] Examples tested
- [ ] CHANGELOG.md updated

---

## 🎓 Learning Path

### Week 1: Foundation
- Day 1-2: Read AGENTS.md and testing-patterns.md
- Day 3-4: Study architecture-patterns.md
- Day 5: Review bug-fix-workflow.md
- Weekend: Explore codebase, run tests

### Week 2: Practice
- Day 1-2: Fix a "good first issue"
- Day 3-4: Add tests to increase coverage
- Day 5: Refactor a large file
- Weekend: Review functional-patterns.md

### Week 3: Advanced
- Day 1-2: Optimize a performance bottleneck
- Day 3-4: Add a new Cypher feature
- Day 5: Write comprehensive documentation
- Weekend: Contribute to architecture

---

## 🔍 Guide Cross-Reference

### By Topic

**Testing:**
- [testing-patterns.md](testing-patterns.md) - Comprehensive testing guide
- [bug-fix-workflow.md](bug-fix-workflow.md) - Bug regression tests
- [cypher-compatibility.md](cypher-compatibility.md) - Compatibility testing

**Architecture:**
- [architecture-patterns.md](architecture-patterns.md) - System design
- [refactoring-guidelines.md](refactoring-guidelines.md) - Code organization
- [functional-patterns.md](functional-patterns.md) - Design patterns

**Quality:**
- [documentation-standards.md](documentation-standards.md) - Documentation
- [performance-optimization.md](performance-optimization.md) - Performance
- [cypher-compatibility.md](cypher-compatibility.md) - Compatibility

---

## 📖 Guide Summaries

### testing-patterns.md
**What:** Comprehensive testing guide  
**When:** Writing any code  
**Key Points:**
- 90% coverage minimum
- Test pyramid (80% unit, 15% integration, 5% e2e)
- Table-driven tests
- Bug regression tests mandatory

### bug-fix-workflow.md
**What:** Mandatory bug fix process  
**When:** Fixing any bug  
**Key Points:**
- 5-step process (reproduce, verify, fix, test, prevent)
- Write failing test first
- Add regression tests
- Document root cause

### refactoring-guidelines.md
**What:** How to refactor large files  
**When:** File >2000 lines  
**Key Points:**
- 2500 line hard limit
- Identify separation of concerns
- Define clear interfaces
- Maintain test coverage

### functional-patterns.md
**What:** Advanced Go patterns  
**When:** Designing flexible systems  
**Key Points:**
- Function types for DI
- Higher-order functions
- Decorators and middleware
- Runtime behavior modification

### documentation-standards.md
**What:** Documentation requirements  
**When:** Writing any public API  
**Key Points:**
- 100% of public APIs documented
- 1-3 real-world examples per API
- ELI12 explanations for complex concepts
- Package, type, and function docs

### architecture-patterns.md
**What:** System design principles  
**When:** Designing features  
**Key Points:**
- Layer responsibilities
- Dependency injection
- Design patterns (Strategy, Repository, etc.)
- Package organization

### performance-optimization.md
**What:** Performance tuning guide  
**When:** Optimizing code  
**Key Points:**
- Measure first, optimize second
- Profile to find bottlenecks
- Prove improvements with benchmarks
- No regressions allowed

### cypher-compatibility.md
**What:** Neo4j compatibility guide  
**When:** Implementing Cypher features  
**Key Points:**
- Exact Neo4j semantics
- Test against real Neo4j
- Same error codes
- Parameter substitution

---

## 🚀 Common Workflows

### Adding a New Cypher Feature

1. **Research** - Check Neo4j documentation
2. **Test** - Write compatibility tests ([cypher-compatibility.md](cypher-compatibility.md))
3. **Design** - Plan architecture ([architecture-patterns.md](architecture-patterns.md))
4. **Implement** - Write code with tests ([testing-patterns.md](testing-patterns.md))
5. **Document** - Add examples ([documentation-standards.md](documentation-standards.md))
6. **Benchmark** - Measure performance ([performance-optimization.md](performance-optimization.md))

### Fixing a Performance Issue

1. **Benchmark** - Establish baseline ([performance-optimization.md](performance-optimization.md))
2. **Profile** - Find bottleneck (CPU/memory profiling)
3. **Optimize** - Apply optimization technique
4. **Test** - Ensure correctness ([testing-patterns.md](testing-patterns.md))
5. **Benchmark** - Prove improvement
6. **Document** - Explain optimization

### Refactoring a Large File

1. **Analyze** - Identify responsibilities ([refactoring-guidelines.md](refactoring-guidelines.md))
2. **Design** - Define module boundaries ([architecture-patterns.md](architecture-patterns.md))
3. **Extract** - Move code to new files
4. **Test** - Verify all tests pass ([testing-patterns.md](testing-patterns.md))
5. **Document** - Update package docs ([documentation-standards.md](documentation-standards.md))

---

## 💡 Tips for AI Agents

### Reading Order

1. **First time:** Read AGENTS.md completely
2. **Before coding:** Skim relevant detailed guide
3. **During coding:** Reference guides as needed
4. **Before committing:** Review checklist in AGENTS.md

### Using the Guides

- **Don't memorize** - Reference when needed
- **Follow patterns** - Use examples from guides
- **Ask questions** - If unclear, check related guides
- **Update guides** - If you find better patterns

### Common Mistakes to Avoid

❌ Skipping tests  
❌ Not documenting code  
❌ Ignoring file size limits  
❌ Guessing instead of profiling  
❌ Breaking Neo4j compatibility  
❌ Not proving improvements  

✅ Write tests first  
✅ Document all public APIs  
✅ Refactor large files  
✅ Profile before optimizing  
✅ Test against Neo4j  
✅ Show benchmarks  

---

## 📞 Getting Help

### Guide Not Clear?

1. Check related guides for more context
2. Look at real examples in codebase
3. Search for similar patterns in tests
4. Review AGENTS.md for overview

### Can't Find Information?

1. Check guide index above
2. Search within guides (grep/find)
3. Look at codebase examples
4. Review test files for patterns

---

## 🔄 Guide Maintenance

### Updating Guides

When you discover better patterns:

1. Update the relevant guide
2. Add examples from codebase
3. Update cross-references
4. Test examples work

### Adding New Guides

If a topic needs detailed coverage:

1. Create new guide in .agents/
2. Follow existing guide structure
3. Add to this README index
4. Link from AGENTS.md

---

**Remember**: These guides exist to help you write high-quality, maintainable code. Use them as references, not rules to memorize. The goal is to maintain NornicDB's enterprise-grade quality and Neo4j compatibility.

**Happy coding! 🚀**
