package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

type conditionalFramesKey struct{}

// conditionalFrames belongs to this execution's retained source, not the
// globally cached grammar tree. Identical user text cannot manufacture marks.
type conditionalFrames struct {
	source *cyphertext.Source
	calls  []cyphertext.Span
	parent *conditionalFrames
}

func hasConditionalFrames(ctx context.Context) bool {
	frames, _ := ctx.Value(conditionalFramesKey{}).(*conditionalFrames)
	return frames != nil
}

func generatedConditionalCall(ctx context.Context, clause pipelineClause) bool {
	frames, _ := ctx.Value(conditionalFramesKey{}).(*conditionalFrames)
	if frames == nil || !clause.syntax.Valid() {
		return false
	}
	call, found := clause.syntax.DirectToken(cypherantlr.CypherLexerCALL)
	if !found {
		return false
	}
	source, retained := cyphertext.RetainedSource(ctx, clause.syntax.Source(), cyphertext.AcceptedSource)
	if !retained {
		return false
	}
	start, end := call.Span()
	for ; frames != nil; frames = frames.parent {
		span, ok := source.SpanInAncestor(frames.source, cyphertext.Span{Start: start, End: end})
		if !ok {
			continue
		}
		for _, generated := range frames.calls {
			if span == generated {
				return true
			}
		}
	}
	return false
}
