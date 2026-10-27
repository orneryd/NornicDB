package cyphertext

import "strings"

// nextNameUnit locates an admitted lone UTF16 unit in decoded name bytes.
// The byte prefilter is not a decoder: ReadCodepoint alone classifies the unit.
// Ordinary runs retain the standard library's valid/invalid UTF8 case semantics.
func nextNameUnit(name string, from int) (int, int) {
	for from < len(name) {
		relative := strings.IndexByte(name[from:], 0xed)
		if relative < 0 {
			break
		}
		start := from + relative
		value, width := ReadCodepoint(name, start)
		if value >= 0xd800 && value <= 0xdfff {
			return start, start + width
		}
		from = start + width
	}
	return len(name), len(name)
}

// NameLower returns the existing lowercase-key identity of a decoded name,
// preserving admitted lone UTF16 units. It never interprets source escapes.
// Ordinary Unicode and invalid non-unit bytes keep strings.ToLower semantics.
func NameLower(name string) string {
	// Ordinary UTF-8 names need no code-unit walk. Keep this admission path
	// small; ReadCodepoint remains the authority when a possible unit occurs.
	if strings.IndexByte(name, 0xed) < 0 {
		return strings.ToLower(name)
	}
	return lowerNameUnits(name)
}

func lowerNameUnits(name string) string {
	unit, after := nextNameUnit(name, 0)
	if unit == len(name) {
		return strings.ToLower(name)
	}
	var out strings.Builder
	cursor, copied := 0, 0
	for {
		segment := name[cursor:unit]
		lower := strings.ToLower(segment)
		if lower != segment {
			if out.Len() == 0 {
				out.Grow(len(name))
			}
			out.WriteString(name[copied:cursor])
			out.WriteString(lower)
			copied = unit
		}
		if unit == len(name) {
			break
		}
		cursor = after
		unit, after = nextNameUnit(name, cursor)
	}
	if out.Len() == 0 {
		return name
	}
	out.WriteString(name[copied:])
	return out.String()
}

// NameEqualFold compares decoded names with the existing simple-fold policy.
// Unlike lowercase-key equality it retains strings.EqualFold's scalar rules;
// admitted lone UTF16 units compare only with the exact same unit.
func NameEqualFold(left, right string) bool {
	if left == right {
		return true
	}
	leftAt, rightAt := 0, 0
	for {
		leftUnit, leftAfter := nextNameUnit(left, leftAt)
		rightUnit, rightAfter := nextNameUnit(right, rightAt)
		if !strings.EqualFold(left[leftAt:leftUnit], right[rightAt:rightUnit]) {
			return false
		}
		leftDone, rightDone := leftUnit == len(left), rightUnit == len(right)
		if leftDone || rightDone {
			return leftDone && rightDone
		}
		if left[leftUnit:leftAfter] != right[rightUnit:rightAfter] {
			return false
		}
		leftAt, rightAt = leftAfter, rightAfter
	}
}
