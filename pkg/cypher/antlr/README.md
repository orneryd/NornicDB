# ANTLR Cypher Parser for NornicDB

This package provides an ANTLR-based OpenCypher parser as an alternative to NornicDB's fast inline (Nornic) parser.

> **See also:** [Cypher Parser Modes](../../docs/architecture/cypher-parser-modes.md) for full architecture documentation and benchmarks.

## Quick Start

```bash
# Run entire test suite with ANTLR parser
make antlr-test

# Regenerate parser from grammar files
make antlr-generate
```

## Makefile Targets

**From root directory:**

```bash
make antlr-test      # Run ENTIRE test suite with ANTLR parser
make antlr-generate  # Regenerate parser from .g4 grammar files
make antlr-clean     # Clean generated files and JAR
make test-parsers    # Run cypher tests with both parsers
```

**From pkg/cypher/antlr/:**

```bash
make generate  # Download ANTLR JAR + regenerate
make download  # Download ANTLR JAR only
make test      # Run tests
make clean     # Clean all
make help      # Show help
```

## When to Use

| Parser | Speed | Error Messages | Use Case |
|--------|-------|----------------|----------|
| **Nornic** (default) | ~1µs | Basic | Production |
| **ANTLR** | ~10-20µs | Detailed with line/col | Development, debugging |

Switch between parsers via environment variable:
```bash
# Use ANTLR parser (strict validation, better errors)
NORNICDB_PARSER=antlr ./nornicdb

# Use Nornic parser (default, fast)
NORNICDB_PARSER=nornic ./nornicdb
```

## Files Overview

### Grammar Files (Source of Truth)
- `CypherLexer.g4` - Lexer grammar (tokens, keywords)
- `CypherParser.g4` - Parser grammar (syntax rules)

### Generated Files (DO NOT EDIT MANUALLY)
- `cypher_lexer.go` - Generated lexer
- `cypher_parser.go` - Generated parser (~490KB)
- `cypherparser_listener.go` - Generated listener interface
- `cypherparser_base_listener.go` - Generated base listener

### Hand-Written Files
- `parse.go` - Main entry point (`Parse()` function)
- `analyzer.go` - AST analysis utilities
- `clauses.go` - Clause type definitions
- `expression.go` - Expression handling

## Regenerating the Parser

When you modify the `.g4` grammar files, you must regenerate the Go code.

### Prerequisites

1. **Java Runtime** (JRE 11+)
   ```bash
   # macOS
   brew install openjdk@11
   
   # Ubuntu/Debian
   sudo apt install openjdk-11-jre
   
   # Verify
   java -version
   ```

2. **ANTLR Tool** (automatically downloaded by Makefile)
   - Version: 4.13.1 (must match `github.com/antlr4-go/antlr/v4` Go module version)
   - JAR: `antlr-4.13.1-complete.jar`

### Regenerate Parser

```bash
# From this directory (pkg/cypher/antlr/)
make generate

# Or from NornicDB root
make antlr-generate
```

This will:
1. Download ANTLR JAR if not present
2. Run ANTLR on `CypherLexer.g4` and `CypherParser.g4`
3. Generate Go code in current directory
4. Run tests to verify

### Manual Regeneration

If you prefer not to use Make:

```bash
# Download ANTLR JAR (once)
curl -O https://www.antlr.org/download/antlr-4.13.1-complete.jar

# Generate lexer
java -jar antlr-4.13.1-complete.jar -Dlanguage=Go -no-visitor -package antlr CypherLexer.g4

# Generate parser
java -jar antlr-4.13.1-complete.jar -Dlanguage=Go -no-visitor -package antlr CypherParser.g4

# Test
go test -v ./...
```

## Modifying the Grammar

### Adding a New Keyword

1. Add to `CypherLexer.g4`:
   ```antlr
   NEWKEYWORD : 'NEWKEYWORD';
   ```

2. Use in `CypherParser.g4`:
   ```antlr
   newClause : NEWKEYWORD expression ;
   ```

3. Regenerate: `make generate`

4. Handle in `analyzer.go` or `clauses.go`

### Grammar Reference

- [ANTLR4 Documentation](https://github.com/antlr/antlr4/blob/master/doc/index.md)
- [OpenCypher Grammar Spec](https://opencypher.org/resources/)
- [ANTLR Go Target](https://github.com/antlr/antlr4/blob/master/doc/go-target.md)

## Version Compatibility

| Component | Version | Notes |
|-----------|---------|-------|
| ANTLR Tool (JAR) | 4.13.1 | Must match Go runtime |
| antlr4-go/antlr/v4 | v4.13.1 | Go module in go.mod |
| Java | 11+ | Required for ANTLR tool |

**Important:** The ANTLR JAR version must match the Go runtime version. If you upgrade one, upgrade both.

## Troubleshooting

### "no required module provides package github.com/antlr4-go/antlr/v4"
```bash
go get github.com/antlr4-go/antlr/v4
```

### Parser/Lexer version mismatch
Ensure ANTLR JAR version matches Go module version:
```bash
# Check Go module version
grep antlr go.mod

# Download matching JAR
curl -O https://www.antlr.org/download/antlr-4.13.1-complete.jar
```

### Generated code has syntax errors
- Check ANTLR version compatibility
- Ensure `-package antlr` flag is used
- Verify `.g4` files have no syntax errors

## Testing

```bash
# Run ANTLR parser tests
go test -v ./pkg/cypher/antlr/

# Run parser comparison (A/B test)
go test -v -run TestParserComparison ./pkg/cypher/

# Benchmark both parsers
go test -bench=BenchmarkParserComparison ./pkg/cypher/
```
