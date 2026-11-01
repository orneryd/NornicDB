package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// Statement text admission shares mode, gap and comment facts across normalization,
// framing and execution. Cold malformed-input handling stays with this owner.
// readExecutionModes is the shared header reader for routing, framing and
// mode execution. Conflict reporting waits until a statement body exists.
func readExecutionModes(query string, mode ExecutionMode) (ExecutionMode, string, bool) {
	mode, span, conflict := readExecutionModeExtent(query, mode)
	return mode, query[span.Start:span.End], conflict
}

// readExecutionModeExtent is the header owner's source product. Routing,
// framing and execution use the same reader and retain its exact body window.
func readExecutionModeExtent(query string, mode ExecutionMode) (ExecutionMode, cyphertext.Span, bool) {
	start, end := sourceWhitespaceExtent(query)
	conflict := false
	for {
		next, tail := executionModePrefixExtent(query[start:end])
		base := start
		start, end = base+tail.Start, base+tail.End
		if next == ModeNormal {
			if mode != ModeNormal {
				start += queryGapEnd(query[start:end], 0)
				left, right := sourceWhitespaceExtent(query[start:end])
				base = start
				start, end = base+left, base+right
			}
			return mode, cyphertext.Span{Start: start, End: end}, conflict
		}
		if mode == ModeNormal {
			mode = next
		} else if next != mode {
			conflict = true
		}
	}
}

func executionModePrefix(query string) (ExecutionMode, string) {
	mode, span := executionModePrefixExtent(query)
	return mode, query[span.Start:span.End]
}

func executionModePrefixExtent(query string) (ExecutionMode, cyphertext.Span) {
	left, right := sourceWhitespaceExtent(query)
	trimmed := query[left:right]
	if len(trimmed) == 0 {
		return ModeNormal, cyphertext.Span{Start: left, End: right}
	}
	// Only a mode keyword or a leading comment can introduce a mode.
	// Keep ordinary statements on the prefix reader's allocation-free exit.
	switch trimmed[0] {
	case 'e', 'E', 'p', 'P', '/':
	default:
		return ModeNormal, cyphertext.Span{Start: left, End: right}
	}
	start := queryGapEnd(trimmed, 0)
	mode, keywordEnd := ModeNormal, 0
	if matchKeywordAt(trimmed, start, "EXPLAIN") {
		mode, keywordEnd = ModeExplain, start+7
	}
	if matchKeywordAt(trimmed, start, "PROFILE") {
		mode, keywordEnd = ModeProfile, start+7
	}
	if mode == ModeNormal {
		return mode, cyphertext.Span{Start: left, End: right}
	}
	tail, unclosed := readQueryGap(trimmed, keywordEnd)
	if unclosed >= 0 {
		tail = keywordEnd
	}
	tailLeft, tailRight := sourceWhitespaceExtent(trimmed[tail:])
	return mode, cyphertext.Span{Start: left + tail + tailLeft, End: left + tail + tailRight}
}

func executionModeSource(ctx context.Context, text string) (context.Context, ExecutionMode, string) {
	mode, span, _ := readExecutionModeExtent(text, ModeNormal)
	return statementSourceWindow(ctx, text, span), mode, text[span.Start:span.End]
}

// executionModeStatement consumes repeated matching prefixes and admits the
// body before planning or re-entering execution. Every accepted prefix makes
// progress, including bare keywords, comments and non-space separators.
func executionModeStatement(mode ExecutionMode, query string) (string, error) {
	if _, unclosed := inspectOutsideStrings(query, ";"); unclosed >= 0 {
		return "", unclosedCommentError(query, unclosed, mode)
	}
	_, statement, conflict := readExecutionModes(query, mode)
	if queryGapEnd(statement, 0) == len(statement) {
		return "", emptyStatementError()
	}
	if conflict {
		return "", executionModeConflictError()
	}
	return statement, nil
}

// normalizeQueryText admits comments in the existing canonicalization scan.
// Canonical ordinary statements use exactly the same quick exit as classification.
func normalizeQueryText(ctx context.Context, query string) (string, *queryRewrite, error) {
	if err := ctx.Err(); err != nil {
		return query, nil, err
	}
	if !queryMayNeedCanonicalRewrite(query) {
		return query, nil, nil
	}
	canonical, rewrite, err := readCanonicalQueryText(query, true)
	if err == nil && rewrite != nil && strings.TrimSpace(canonical) == "" {
		err = emptyStatementError()
	}
	if err == nil {
		canonical, rewrite = preserveStatementPresentation(ctx, query, canonical, rewrite)
	}
	if err := ctx.Err(); err != nil {
		return query, nil, err
	}
	return canonical, rewrite, err
}

func emptyStatementError() error {
	return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherCoreEmptyQuery())
}

// Only a failing lexical token needs prefix interpretation. Accepted ordinary
// statements never run a second comment or mode reader here.
func unclosedCommentError(query string, index int, mode ExecutionMode) error {
	prefix := query[:index]
	prefix = strings.TrimSpace(prefix[queryGapEnd(prefix, 0):])
	prefix, _ = stripCypherPreamble(prefix)
	_, prefix, conflict := readExecutionModes(prefix, mode)
	code := "Neo.ClientError.Statement.SyntaxError"
	if queryGapEnd(prefix, 0) == len(prefix) {
		code = "Neo.DatabaseError.Statement.ExecutionFailed"
	} else if conflict {
		return executionModeConflictError()
	}
	return localizedStatusError(code, "UnexpectedSyntax", localization.CypherANTLRParseSyntaxError("Unclosed block comment"))
}

func executionModeConflictError() error {
	return nornicerrors.MarkCompileTime(localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgument",
		localization.CypherANTLRParseSyntaxError("EXPLAIN and PROFILE cannot be combined")))
}

// statementExecutionModes admits only eligible leading mode/preamble text.
// Missing bodies are syntax errors, including when both modes are present.
func statementExecutionModes(cypher string) (ExecutionMode, string, bool) {
	query := strings.TrimSpace(cypher)
	query = strings.TrimSpace(query[queryGapEnd(query, 0):])
	if len(query) == 0 {
		return ModeNormal, query, false
	}
	switch asciiUpper(query[0]) {
	case 'E', 'P', 'C':
	default:
		return ModeNormal, query, false
	}
	query, _ = stripCypherPreamble(query)
	return readExecutionModes(query, ModeNormal)
}

// readQueryGap keeps ordinary tokens on an inlineable exit. The slow reader is
// shared by strict admission and permissive gap consumers; only comments need
// termination work, and ordinary whitespace never calls the comment reader.
func readQueryGap(query string, index int) (int, int) { return cyphertext.GapEnd(query, index) }

// ValidateStatementText admits raw transport text before any adapter can
// discard malformed comments. For example, CREATE (:N) /* fails before writes.
func ValidateStatementText(query string) error {
	if _, unclosed := inspectOutsideStrings(query, ";"); unclosed >= 0 {
		return unclosedCommentError(query, unclosed, ModeNormal)
	}
	if queryGapEnd(query, 0) == len(query) {
		return emptyStatementError()
	}
	return nil
}
