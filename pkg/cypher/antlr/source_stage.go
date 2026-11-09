package antlr

import "github.com/orneryd/nornicdb/internal/cyphertext"

// SourceStage identifies client source or already projected compiler source.
// It aliases the canonical source owner's enum for external Go callers.
type SourceStage = cyphertext.SourceStage

const (
	RawSource      = cyphertext.RawSource
	AcceptedSource = cyphertext.AcceptedSource
)
