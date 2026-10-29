package cyphertext

// spaceEnd retains the declaration protocol's whitespace-only policy while
// the inherited source primitive owns character widths.
func spaceEnd(text string, at int) int {
	for at < len(text) {
		width := WhitespaceAt(text, at)
		if width == 0 {
			break
		}
		at += width
	}
	return at
}

func gapEnd(text string, at int) int {
	end, _ := GapEnd(text, at)
	return end
}
