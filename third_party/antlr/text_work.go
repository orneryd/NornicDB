// Copyright (c) 2026 NornicDB contributors.
// Use of this file is governed by the BSD 3-clause license in LICENSE.

package antlr

import "strings"

// ConcatTextWithWork supports query-owned adapters without retaining work on
// shared runtime data. Copies use the same canonical bounded text owner.
func ConcatTextWithWork(check func(), parts ...string) string {
	return concatTextWithWork(check, parts...)
}

func concatTextWithWork(check func(), parts ...string) string {
	if check == nil {
		return strings.Join(parts, "")
	}
	length := 0
	for _, part := range parts {
		chargeWork(check)
		length += len(part)
	}
	var result strings.Builder
	chargeWork(check)
	result.Grow(length)
	for _, part := range parts {
		for len(part) > 0 {
			chargeWork(check)
			size := intMin(len(part), 4096)
			result.WriteString(part[:size])
			part = part[size:]
		}
	}
	return result.String()
}
func escapeTextWithWork(text string, check func()) string {
	if check == nil {
		text = strings.ReplaceAll(text, "\n", "\\n")
		text = strings.ReplaceAll(text, "\r", "\\r")
		return strings.ReplaceAll(text, "\t", "\\t")
	}
	length := len(text)
	for i := 0; i < len(text); i++ {
		chargeWork(check)
		if text[i] == '\n' || text[i] == '\r' || text[i] == '\t' {
			length++
		}
	}
	var result strings.Builder
	chargeWork(check)
	result.Grow(length)
	start := 0
	for i := 0; i < len(text); i++ {
		chargeWork(check)
		switch text[i] {
		case '\n', '\r', '\t':
			if start < i {
				result.WriteString(text[start:i])
			}
			result.WriteByte('\\')
			switch text[i] {
			case '\n':
				result.WriteByte('n')
			case '\r':
				result.WriteByte('r')
			case '\t':
				result.WriteByte('t')
			}
			start = i + 1
		}
		if i-start >= 4096 {
			result.WriteString(text[start:i])
			start = i
		}
	}
	result.WriteString(text[start:])
	return result.String()
}
func recognizerWork(recognizer Recognizer) func() {
	if parser, ok := recognizer.(Parser); ok && parser.GetInterpreter() != nil {
		return parser.GetInterpreter().cancellationCheck
	}
	if lexer, ok := recognizer.(interface{ GetInterpreter() ILexerATNSimulator }); ok {
		if simulator, ok := lexer.GetInterpreter().(*LexerATNSimulator); ok {
			return simulator.cancellationCheck
		}
	}
	return nil
}
