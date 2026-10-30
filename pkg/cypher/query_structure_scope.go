package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"maps"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
)

// Rewriting transports the enclosing frame into the existing statement owner.
// A lowered branch is not a standalone statement with an empty input schema.
func (r *structureRewriter) fragmentColumns(query string) ([]string, error) {
	executor, ok := r.columns.(*StorageExecutor)
	if !ok {
		return r.columns.StatementResultColumnsContext(r.ctx, query, AcceptedSource)
	}
	program, err := r.fragmentProgram(executor, query)
	if err != nil {
		return nil, err
	}
	return knownResultColumns(program.outputs), nil
}

func (r *structureRewriter) fragmentProgram(executor *StorageExecutor, query string) (*projectedScopeFacts, error) {
	program := executor.projectedStatementScope(r.ctx, query, projectedMapNames(bindingNames(r.incoming)), resultmetadata.Known(nil), cypherGrammarVersion(r.query) == "25", r.bindings)
	if !program.complete {
		if program.err != nil {
			return nil, program.err
		}
		return nil, unresolvedStatementError()
	}
	return program, nil
}

// Composition bodies have not been lowered yet. Preserve their byte windows
// with a literal body while the canonical grammar identifies the real clause.
// CASE/WHEN and keyword aliases remain expressions; OPTIONAL belongs to CALL.
// This masking supplies syntax only: binding rules stay in the scope owner.
func (r *structureRewriter) scopeSyntax(start, end int) (cypherantlr.StatementSyntax, error) {
	text := []byte(r.query[start:end])
	opener := &labelExpressionRewriter{query: r.query}
	for i := start; i < end; i++ {
		if isCypherQuoteByte(r.query[i]) {
			i = skipCypherQuotedText(r.query[:end], i, r.query[i]) - 1
			continue
		}
		if r.query[i] == '/' {
			if close := queryCommentEnd(r.query[:end], i); close >= 0 {
				i = close - 1
				continue
			}
		}
		if r.query[i] != '{' || !opener.opensSubquery(i) {
			continue
		}
		close := cyphertext.FindMatchingDelimiter(r.query[:end], i, '{', '}', cyphertext.OpeningDelimiter)
		if close < 0 {
			break
		}
		if close-i-1 >= len("RETURN 0") {
			for at := i + 1; at < close; at++ {
				text[at-start] = ' '
			}
			copy(text[i+1-start:], "RETURN 0")
		}
		i = close
	}
	syntax, err := cypherantlr.ParseSyntax(r.ctx, string(text), cypherantlr.FragmentEntry, cypherGrammarVersion(r.query) == "25", cypherantlr.AcceptedSource)
	if err != nil {
		return syntax, syntaxAdmissionError(r.ctx, r.query[start:end], AcceptedSource, err, syntax)
	}
	return syntax, nil
}

func (r *structureRewriter) rewrittenPrefix(start, end int) string {
	var edits []labelRewriteEdit
	for _, edit := range r.edits {
		if edit.start >= start && edit.end <= end {
			edit.start -= start
			edit.end -= start
			edits = append(edits, edit)
		}
	}
	text, _ := applyLabelRewriteEdits(r.query[start:end], edits, true)
	return text
}

func (r *structureRewriter) enterSubquery(start, end, open int) error {
	executor, ok := r.columns.(*StorageExecutor)
	if !ok {
		return nil
	}
	syntax, err := r.scopeSyntax(start, end)
	if err != nil {
		return err
	}
	shape := queryBodyFromSyntax(statementSyntaxQuery(syntax))
	for _, branch := range shape.branches {
		for _, clause := range branch.clauses {
			if open-start < clause.start || open-start >= clause.end {
				continue
			}
			prefix := r.rewrittenPrefix(start, start+clause.start)
			probe := prefix + syntax.Source()[clause.start:clause.end]
			if clause.kind != pipelineClauseReturn {
				probe += " RETURN 0"
			}
			program, err := r.fragmentProgram(executor, probe)
			if err != nil {
				return err
			}
			frame := program.bound.frames[len(prefix)+open-start-clause.start+1]
			if frame == nil {
				return unresolvedStatementError()
			}
			r.bindings, r.incoming = frame, maps.Clone(frame.entry)
			return nil
		}
	}
	return unresolvedStatementError()
}
