// Author: Codex R17 implementation lane, 1 November 2026.
package cyphertext

// ViewFrom presents existing input coordinates in this source's current text.
// Every association comes from retained edits/windows. It neither searches for
// an equal substring nor normalizes, tokenizes or parses another input.
func (source *Source) ViewFrom(input string) (*Source, Span, bool) {
	root := source
	for root.parent != nil {
		root = root.parent
	}
	if root.Text != input && root.Original == input {
		anchored, _ := source.Reframe(&Source{Original: input, Text: input})
		return anchored.ViewFrom(input)
	}
	var steps []*Source
	extent := Span{0, len(source.Text)}
	for current := source; current != nil; current = current.parent {
		if current.Text == input {
			view := &Source{Original: source.Text, Text: source.Text}
			for _, step := range steps {
				parent := step.parent
				if parent == nil {
					return nil, Span{}, false
				}
				var inverse []SourceEdit
				if step.base != 0 || step.base+len(step.Text) != len(parent.Text) && len(step.Edits) == 0 {
					if step.base > 0 {
						inverse = append(inverse, SourceEdit{Original: Span{0, 0}, Accepted: Span{0, step.base}})
					}
					end := step.base + len(step.Text)
					if end < len(parent.Text) {
						inverse = append(inverse, SourceEdit{Original: Span{len(step.Text), len(step.Text)}, Accepted: Span{end, len(parent.Text)}})
					}
				} else {
					for _, edit := range step.Edits {
						inverse = append(inverse, SourceEdit{Original: edit.Accepted, Accepted: edit.Original})
					}
				}
				view = view.Rewrite(parent.Text, inverse)
			}
			return view, extent, true
		}
		steps = append(steps, current)
		local := *current
		local.parent = nil
		extent.Start, _ = local.OriginalOffset(extent.Start, true, SourceExtent)
		extent.End, _ = local.OriginalOffset(extent.End, false, SourceExtent)
	}
	return nil, Span{}, false
}

// Reframe composes an existing input-to-original association with a new
// original-to-presentation association. The input and its edits stay owned;
// only the final coordinate frame changes. No input is searched or re-read.
func (source *Source) Reframe(presentation *Source) (*Source, bool) {
	if source == nil || presentation == nil || presentation.Text != source.Original {
		return nil, false
	}
	var compose func(*Source) *Source
	compose = func(current *Source) *Source {
		copy := *current
		copy.Original = presentation.Original
		if current.parent == nil {
			copy.parent = presentation
		} else {
			copy.parent = compose(current.parent)
		}
		return &copy
	}
	return compose(source), true
}
