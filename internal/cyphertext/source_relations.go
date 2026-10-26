package cyphertext

import (
	"fmt"
	"sort"
)

// CopyRelationsFrom composes exact copies along this Source's actual ancestry.
// Returned Original spans are in ancestor.Text, Accepted spans in source.Text.
// The complement consists of generated or legacy-inexact bytes, never copies.
// Producers composing output use these relations in RewriteWithCopies over the
// complete replacement extent, which explicitly retains that generated origin.
// An equal-text Source that is not an ancestor is an error.
func (source *Source) CopyRelationsFrom(ancestor *Source) ([]SourceCopy, error) {
	if source == ancestor {
		if len(source.Text) == 0 {
			return nil, nil
		}
		return []SourceCopy{{Original: Span{0, len(source.Text)}, Accepted: Span{0, len(source.Text)}}}, nil
	}
	if source.parent == nil {
		return nil, fmt.Errorf("cyphertext: copy-relation ancestor is not in this lineage")
	}
	parent, err := source.parent.CopyRelationsFrom(ancestor)
	if err != nil {
		return nil, err
	}
	immediate, err := source.exactParentCopies()
	if err != nil {
		return nil, err
	}
	var result []SourceCopy
	for _, child := range immediate {
		for _, copy := range parent {
			start, end := max(child.Original.Start, copy.Accepted.Start), min(child.Original.End, copy.Accepted.End)
			if start >= end {
				continue
			}
			result = append(result, SourceCopy{Original: Span{copy.Original.Start + start - copy.Accepted.Start, copy.Original.Start + end - copy.Accepted.Start}, Accepted: Span{child.Accepted.Start + start - child.Original.Start, child.Accepted.Start + end - child.Original.Start}})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Accepted.Start < result[j].Accepted.Start })
	return result, nil
}

func (source *Source) exactParentCopies() ([]SourceCopy, error) {
	if source.window {
		if len(source.Text) == 0 {
			return nil, nil
		}
		return []SourceCopy{{Original: Span{source.base, source.base + len(source.Text)}, Accepted: Span{0, len(source.Text)}}}, nil
	}
	result := append([]SourceCopy(nil), source.copies...)
	unchanged := func(original, accepted Span) error {
		if original.End-original.Start != accepted.End-accepted.Start {
			return fmt.Errorf("cyphertext: inconsistent unchanged source extent")
		}
		cursor := accepted.Start
		for _, copy := range source.copies {
			if copy.Accepted.End <= cursor {
				continue
			}
			if copy.Accepted.Start >= accepted.End {
				break
			}
			if cursor < copy.Accepted.Start {
				end := min(copy.Accepted.Start, accepted.End)
				result = append(result, SourceCopy{Original: Span{original.Start + cursor - accepted.Start, original.Start + end - accepted.Start}, Accepted: Span{cursor, end}})
			}
			cursor = max(cursor, copy.Accepted.End)
		}
		if cursor < accepted.End {
			result = append(result, SourceCopy{Original: Span{original.Start + cursor - accepted.Start, original.End}, Accepted: Span{cursor, accepted.End}})
		}
		return nil
	}
	original, accepted := 0, 0
	for _, edit := range source.Edits {
		if err := unchanged(Span{original, edit.Original.Start}, Span{accepted, edit.Accepted.Start}); err != nil {
			return nil, err
		}
		original, accepted = edit.Original.End, edit.Accepted.End
	}
	if err := unchanged(Span{original, len(source.parent.Text)}, Span{accepted, len(source.Text)}); err != nil {
		return nil, err
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Accepted.Start < result[j].Accepted.Start })
	return result, nil
}
