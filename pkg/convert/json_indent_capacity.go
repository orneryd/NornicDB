package convert

// maxJSONCapacity is the largest length a byte slice can hold on this platform.
const maxJSONCapacity = int(^uint(0) >> 1)

// indentedJSONCapacity reports the capacity a caller-owned buffer needs so that
// a single json.Indent call over already-valid compact src neither reallocates
// nor copies. It is max(exact indented length, 2*len(src)) because encoding/json
// pre-grows the destination by indentGrowthFactor (2) times the compact length
// before appending: a smaller buffer would make that pre-growth allocate again.
//
// The exact length follows the same rule Indent applies to a compact document:
// every input byte is copied once, every ':' gains one space, and every line
// break it emits gains '\n', the prefix and one indent per open container. A
// container that stays empty suppresses its closing line break. Quoted strings
// are opaque, so punctuation and escaped quotes inside them are never
// structural, and no value is decoded, copied or reformatted here.
//
// ok is false when the capacity cannot be represented in an int, and the caller
// must then leave growth to json.Indent. src must be compact valid JSON, which
// is what the domain writer's state holds before indenting.
func indentedJSONCapacity(src []byte, prefix, indent string) (capacity int, ok bool) {
	total, depth, needIndent := len(src), 0, false
	for i := 0; i < len(src); i++ {
		c := src[i]
		// Indent defers the line break of a non-empty container until its first
		// value, so the break belongs to the value that starts here, not to the
		// bracket that opened the container.
		if needIndent && c != '}' && c != ']' {
			depth++
			line := indentedJSONLineLength(prefix, indent, depth)
			if line < 0 || line > maxJSONCapacity-total {
				return 0, false
			}
			total += line
			needIndent = false
		}
		switch c {
		case '"':
			for i++; i < len(src); i++ {
				if src[i] == '\\' {
					i++
				} else if src[i] == '"' {
					break
				}
			}
		case '{', '[':
			needIndent = true
		case ',':
			line := indentedJSONLineLength(prefix, indent, depth)
			if line < 0 || line > maxJSONCapacity-total {
				return 0, false
			}
			total += line
		case ':':
			if total == maxJSONCapacity {
				return 0, false
			}
			total++
		case '}', ']':
			if needIndent {
				needIndent = false
			} else {
				depth--
				line := indentedJSONLineLength(prefix, indent, depth)
				if line < 0 || line > maxJSONCapacity-total {
					return 0, false
				}
				total += line
			}
		}
	}
	if len(src) > maxJSONCapacity/2 {
		return 0, false
	}
	if preGrow := 2 * len(src); preGrow > total {
		total = preGrow
	}
	return total, true
}

// indentedJSONLineLength is the '\n', prefix and per-depth indent bytes Indent
// writes at depth, or -1 when that length overflows an int.
func indentedJSONLineLength(prefix, indent string, depth int) int {
	if indent != "" && depth > (maxJSONCapacity-1-len(prefix))/len(indent) {
		return -1
	}
	return 1 + len(prefix) + depth*len(indent)
}
