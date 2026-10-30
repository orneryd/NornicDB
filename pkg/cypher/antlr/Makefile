# ANTLR Parser Makefile for NornicDB
#
# Usage:
#   make generate    - Download ANTLR JAR (if needed) and regenerate parser
#   make clean       - Remove generated files
#   make test        - Run parser tests
#   make download    - Download ANTLR JAR only

# ANTLR version - MUST match github.com/antlr4-go/antlr/v4 in go.mod
ANTLR_VERSION := 4.13.1
ANTLR_JAR := antlr-$(ANTLR_VERSION)-complete.jar
ANTLR_URL := https://www.antlr.org/download/$(ANTLR_JAR)

# Java command
JAVA := java

# Generated files
GENERATED_FILES := cypher_lexer.go cypher_parser.go \
                   cypherparser_listener.go cypherparser_base_listener.go \
                   CypherLexer.interp CypherLexer.tokens \
                   CypherParser.interp CypherParser.tokens

.PHONY: all generate clean test download check-java help

all: generate

help:
	@echo "ANTLR Parser Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  generate  - Download ANTLR JAR (if needed) and regenerate parser from .g4 files"
	@echo "  download  - Download ANTLR JAR only"
	@echo "  clean     - Remove generated files and ANTLR JAR"
	@echo "  test      - Run parser tests"
	@echo "  help      - Show this help"
	@echo ""
	@echo "ANTLR Version: $(ANTLR_VERSION)"

# Check Java is available
check-java:
	@which $(JAVA) > /dev/null 2>&1 || (echo "Error: Java not found. Install JRE 11+." && exit 1)
	@echo "Java: $$($(JAVA) -version 2>&1 | head -1)"

# Download ANTLR JAR if not present
download: $(ANTLR_JAR)

$(ANTLR_JAR):
	@echo "Downloading ANTLR $(ANTLR_VERSION)..."
	curl -fsSL -o $(ANTLR_JAR) $(ANTLR_URL)
	@echo "Downloaded $(ANTLR_JAR)"

# Generate parser from grammar files
generate: check-java $(ANTLR_JAR)
	@echo ""
	@echo "=== Generating Cypher Lexer ==="
	$(JAVA) -jar $(ANTLR_JAR) -Dlanguage=Go -no-visitor -package antlr CypherLexer.g4
	@echo ""
	@echo "=== Generating Cypher Parser ==="
	$(JAVA) -jar $(ANTLR_JAR) -Dlanguage=Go -no-visitor -package antlr CypherParser.g4
	perl -0pi -e 's/\nerrorExit:/\n\tgoto errorExit\n\nerrorExit:/g; s/\n\tgoto errorExit \/\/ Trick[^\n]*//g' cypher_parser.go
	gofmt -w cypher_lexer.go cypher_parser.go cypherparser_listener.go cypherparser_base_listener.go
	@echo ""
	@echo "=== Generation Complete ==="
	@echo "Generated files:"
	@ls -la $(GENERATED_FILES) 2>/dev/null || true
	@echo ""
	@echo "Run 'make test' to verify the generated code."

# Run tests
test:
	@echo "Running ANTLR parser tests..."
	cd ../../.. && go test -v ./pkg/cypher/antlr/...

# Clean generated files
clean:
	@echo "Removing generated files..."
	rm -f $(GENERATED_FILES)
	@echo "Removing ANTLR JAR..."
	rm -f $(ANTLR_JAR)
	@echo "Clean complete."

# Verify generated code compiles
verify: generate
	@echo "Verifying generated code compiles..."
	cd ../../.. && go build ./pkg/cypher/antlr/...
	@echo "Verification complete."
