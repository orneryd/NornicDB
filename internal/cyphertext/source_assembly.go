package cyphertext

import (
	"fmt"
	"strings"
)

// SourcePiece is either an actual descendant view or explicitly generated text.
// An empty generated piece is valid. It never associates text by equality.
// Author: #1663 implementation lane.
type SourcePiece struct {
	View      *Source
	Generated string
}

// Assemble composes producer-owned pieces through this Source's existing copy
// relations. Every view must descend from source. Generated and legacy bytes
// retain their origins through RewriteWithCopies; no new mapper is introduced.
func (source *Source) Assemble(pieces []SourcePiece) (*Source, error) {
	var output strings.Builder
	var copies []SourceCopy
	for _, piece := range pieces {
		if piece.View == nil {
			output.WriteString(piece.Generated)
			continue
		}
		if piece.Generated != "" {
			return nil, fmt.Errorf("cyphertext: assembly piece has both copied and generated input")
		}
		relations, err := piece.View.CopyRelationsFrom(source)
		if err != nil {
			return nil, err
		}
		base := output.Len()
		for _, relation := range relations {
			relation.Accepted.Start += base
			relation.Accepted.End += base
			copies = append(copies, relation)
		}
		output.WriteString(piece.View.Text)
	}
	text := output.String()
	return source.RewriteWithCopies(text, []SourceEdit{{Original: Span{End: len(source.Text)}, Accepted: Span{End: len(text)}}}, copies), nil
}
