package text

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	xunicode "golang.org/x/text/encoding/unicode"
)

// encodedUTF8 supplies Java's UTF-8 encoder replacement for lone UTF-16 units.
func encodedUTF8(text string) string {
	var out strings.Builder
	last := 0
	for at := 0; at < len(text); {
		r, width := cyphertext.ReadCodepoint(text, at)
		if r >= 0xd800 && r <= 0xdfff {
			out.WriteString(text[last:at])
			out.WriteByte('?')
			last = at + width
		}
		at += width
	}
	if last == 0 {
		return text
	}
	out.WriteString(text[last:])
	return out.String()
}

// decodedUTF8 reuses the Unicode decoder's maximal-subpart replacement. Java
// additionally consumes an ED A0..BF surrogate prefix as one malformed group,
// including its third byte only when that byte is a continuation.
func decodedUTF8(text string) string {
	var out strings.Builder
	last := 0
	for at := 0; at+1 < len(text); at++ {
		if text[at] == 0xed && text[at+1] >= 0xa0 && text[at+1] <= 0xbf {
			out.WriteString(text[last:at])
			out.WriteRune('\ufffd')
			at += 2
			if at < len(text) && text[at] >= 0x80 && text[at] <= 0xbf {
				at++
			}
			last = at
			at--
		}
	}
	if last > 0 {
		out.WriteString(text[last:])
		text = out.String()
	}
	// UTF8's replacement decoder accepts every byte sequence.
	result, _ := xunicode.UTF8.NewDecoder().String(text)
	return result
}

// UrldecodeChecked decodes Java form text, reporting malformed percent escapes.
// Only contiguous escaped-byte runs are UTF-8 decoded: surrounding admitted
// UTF-16 units belong to the original text and remain unchanged.
func UrldecodeChecked(text string) (string, error) {
	if !strings.ContainsAny(text, "%+") {
		return text, nil
	}
	var out strings.Builder
	out.Grow(len(text))
	for at := 0; at < len(text); {
		switch text[at] {
		case '+':
			out.WriteByte(' ')
			at++
		case '%':
			end := at
			for end < len(text) && text[end] == '%' {
				if end+3 > len(text) {
					return "", fmt.Errorf("apoc.text.urldecode: incomplete percent escape")
				}
				end += 3
			}
			decoded, err := url.PathUnescape(text[at:end])
			if err != nil {
				return "", fmt.Errorf("apoc.text.urldecode: %w", err)
			}
			out.WriteString(decodedUTF8(decoded))
			at = end
		default:
			out.WriteByte(text[at])
			at++
		}
	}
	return out.String(), nil
}

// Base64DecodeChecked accepts Java's standard alphabet, padded or completely
// unpadded final groups, and reports malformed input without partial output.
func Base64DecodeChecked(text string) (string, error) {
	if strings.ContainsAny(text, "\r\n") {
		return "", fmt.Errorf("apoc.text.base64Decode: invalid base64 line break")
	}
	encoding := base64.StdEncoding
	if !strings.Contains(text, "=") {
		encoding = base64.RawStdEncoding
	}
	decoded, err := encoding.DecodeString(text)
	if err != nil {
		return "", fmt.Errorf("apoc.text.base64Decode: %w", err)
	}
	return decodedUTF8(string(decoded)), nil
}

// Urlencode encodes a string for use in URLs.
//
// Example:
//
//	apoc.text.urlencode('hello world') => 'hello+world'
func Urlencode(text string) string {
	return strings.ReplaceAll(strings.ReplaceAll(url.QueryEscape(encodedUTF8(text)), "%2A", "*"), "~", "%7E")
}

// Urldecode decodes form text, returning an empty string on malformed input.
// Registered calls use UrldecodeChecked to preserve the error.
//
// Example:
//
//	apoc.text.urldecode('hello+world') => 'hello world'
func Urldecode(text string) string {
	value, _ := UrldecodeChecked(text)
	return value
}

// Base64Encode encodes a string to base64.
//
// Example:
//
//	apoc.text.base64Encode('hello') => 'aGVsbG8='
func Base64Encode(text string) string {
	return base64.StdEncoding.EncodeToString([]byte(encodedUTF8(text)))
}

// Base64Decode decodes base64, returning an empty string on malformed input.
// Registered calls use Base64DecodeChecked to preserve the error.
//
// Example:
//
//	apoc.text.base64Decode('aGVsbG8=') => 'hello'
func Base64Decode(text string) string {
	value, _ := Base64DecodeChecked(text)
	return value
}
