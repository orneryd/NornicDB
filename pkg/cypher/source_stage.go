package cypher

import "github.com/orneryd/nornicdb/internal/cyphertext"

// SourceStage distinguishes client source from already projected compiler text.
// It aliases the canonical source owner; callers must select their ingress.
type SourceStage = cyphertext.SourceStage

const (
	RawSource      = cyphertext.RawSource
	AcceptedSource = cyphertext.AcceptedSource
)
