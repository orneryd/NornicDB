package cyphertext

// Number is a numeric source token, without a value or magnitude policy. End
// includes its radix, grouping, fraction, exponent and suffix. TailEnd also
// includes an invalid name tail; Invalid identifies malformed lexical syntax.
// Leading-zero, suffix and supported-radix admission remain caller policies.
type Number struct {
	Start, End, TailEnd                        int
	DigitsStart, IntegerEnd                    int
	Base                                       int
	Fraction, Exponent, Grouped, GroupingValid bool
	Suffix                                     byte
	Invalid                                    bool
}

// IsDigit is the ASCII digit class in Cypher numeric tokens.
func IsDigit(character byte) bool { return character >= '0' && character <= '9' }

// DigitForBase reports a digit in a numeric token, not a character in a name.
func DigitForBase(character byte, base int) bool {
	if IsDigit(character) {
		return int(character-'0') < base
	}
	return base == 16 && (character >= 'a' && character <= 'f' || character >= 'A' && character <= 'F')
}

// DigitRun reads digits and grouping underscores, retaining malformed grouping.
func DigitRun(text string, start, base int, leadingUnderscore bool) (end int, valid, grouped bool) {
	end, valid = start, true
	previousUnderscore := false
	digits := 0
	for end < len(text) {
		character := text[end]
		if character == '_' {
			grouped = true
			if previousUnderscore || end == start && !leadingUnderscore {
				valid = false
			}
			previousUnderscore = true
		} else if DigitForBase(character, base) {
			digits++
			previousUnderscore = false
		} else {
			break
		}
		end++
	}
	return end, valid && digits > 0 && !previousUnderscore, grouped
}

// NumberStartsAt excludes name/parameter continuations and the second range dot.
func NumberStartsAt(text string, start int) bool {
	if start < 0 || start >= len(text) {
		return false
	}
	character := text[start]
	if !IsDigit(character) && !(character == '.' && start+1 < len(text) && IsDigit(text[start+1])) {
		return false
	}
	return start == 0 || !NamePartAt(text, start-1) && !(character == '.' && text[start-1] == '.')
}

// ScanNumber reads the complete numeric token beginning at start. Signs outside
// exponents are separate operators. In 1..2 the number ends before the dots.
func ScanNumber(text string, start int) (Number, bool) {
	number := Number{Start: start, End: start, TailEnd: start, Base: 10, DigitsStart: start, GroupingValid: true}
	if start < 0 || start >= len(text) || !IsDigit(text[start]) && !(text[start] == '.' && start+1 < len(text) && IsDigit(text[start+1])) {
		return number, false
	}
	if text[start] == '0' && start+1 < len(text) {
		switch text[start+1] {
		case 'x', 'X':
			number.Base = 16
		case 'o', 'O':
			number.Base = 8
		}
	}
	end := start
	if number.Base != 10 {
		number.DigitsStart = start + 2
		end, number.GroupingValid, number.Grouped = DigitRun(text, start+2, number.Base, true)
		number.IntegerEnd = end
	} else {
		if text[start] != '.' {
			end, number.GroupingValid, number.Grouped = DigitRun(text, start, 10, false)
		}
		number.IntegerEnd = end
		if end < len(text) && text[end] == '.' && end+1 < len(text) && IsDigit(text[end+1]) {
			var valid, grouped bool
			end, valid, grouped = DigitRun(text, end+1, 10, false)
			number.Fraction, number.GroupingValid, number.Grouped = true, number.GroupingValid && valid, number.Grouped || grouped
		} else if end < len(text) && text[end] == '.' && (end+1 == len(text) || text[end+1] != '.') {
			number.Invalid = true
		}
		if end < len(text) && (text[end] == 'e' || text[end] == 'E') {
			number.Exponent = true
			end++
			if end < len(text) && (text[end] == '+' || text[end] == '-') {
				end++
			}
			var valid, grouped bool
			end, valid, grouped = DigitRun(text, end, 10, false)
			number.GroupingValid, number.Grouped = number.GroupingValid && valid, number.Grouped || grouped
		}
		if end < len(text) {
			switch text[end] {
			case 'f', 'F', 'd', 'D':
				number.Suffix = text[end]
				end++
			}
		}
	}
	number.End, number.TailEnd = end, NamePartEnd(text, end)
	number.Invalid = number.Invalid || !number.GroupingValid || number.TailEnd > end
	return number, true
}
