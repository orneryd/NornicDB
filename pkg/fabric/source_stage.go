package fabric

import "github.com/orneryd/nornicdb/internal/cyphertext"

// SourceStage identifies whether text is client source or an already projected
// compiler product. It aliases the canonical lexical stage, without a second
// projection policy. Public callers normally choose RawSource.
type SourceStage = cyphertext.SourceStage

const (
	// RawSource admits client text and projects its Unicode escapes exactly once.
	RawSource = cyphertext.RawSource
	// AcceptedSource consumes a compiler product without projecting escapes again.
	AcceptedSource = cyphertext.AcceptedSource
)
