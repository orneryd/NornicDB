package fn

import "unicode"

const (
	sigmaOther byte = iota
	sigmaLetter
	sigmaNumber
	sigmaEnclosing
	sigmaIgnore
	sigmaDanda
	sigmaBothPunctuation
	sigmaWordPunctuation
	sigmaNumberPunctuation
	sigmaNumberSuffix
	sigmaKindMask byte = 15
	sigmaCased    byte = 128
)

// sigmaContext combines the existing Unicode category tables with the small
// generated delta for the pinned Java word/cased profile. The delta describes
// context only, never a general lowercase mapping.
func sigmaContext(r rune) byte {
	if r < 128 {
		switch {
		case r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z':
			return sigmaLetter | sigmaCased
		case r >= '0' && r <= '9':
			return sigmaNumber
		case r == '\'' || r == '"' || r == '.':
			return sigmaBothPunctuation
		case r == '-' || r == '_':
			return sigmaWordPunctuation
		case r == ',':
			return sigmaNumberPunctuation
		case r == '%' || r == '&':
			return sigmaNumberSuffix
		default:
			return sigmaOther
		}
	}
	lo, hi := 0, len(sigmaContextDeltas)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		entry := sigmaContextDeltas[mid]
		if r < entry.lo {
			hi = mid
		} else if r > entry.hi {
			lo = mid + 1
		} else {
			return entry.value
		}
	}
	var kind byte
	switch {
	case unicode.Is(unicode.Cf, r):
		kind = sigmaIgnore
	case unicode.Is(unicode.Mn, r) || unicode.Is(unicode.Me, r):
		kind = sigmaEnclosing
	case r == 0x964 || r == 0x965:
		kind = sigmaDanda

	case unicode.IsLetter(r) || unicode.Is(unicode.Mc, r):
		kind = sigmaLetter
	case unicode.IsNumber(r):
		kind = sigmaNumber
	case unicode.Is(unicode.Pd, r) || unicode.Is(unicode.Pc, r) || r == 0x2027:
		kind = sigmaWordPunctuation
	case r == 0x66b:
		kind = sigmaNumberPunctuation
	case r == 0xa2 || r == 0x66a || r == 0x2030 || r == 0x2031:
		kind = sigmaNumberSuffix
	}
	if unicode.Is(unicode.Lu, r) || unicode.Is(unicode.Ll, r) || unicode.Is(unicode.Lt, r) {
		kind |= sigmaCased
	}
	return kind
}
