package main

import (
	"go/token"
	"strings"
)

// scannerErrors mirrors go/scanner.ErrorList, which go/parser returns by value.
type scannerErrors []struct {
	Pos token.Position
	Msg string
}

func (s scannerErrors) Error() string {
	parts := make([]string, 0, len(s))
	for _, e := range s {
		parts = append(parts, e.Pos.String()+": "+e.Msg)
	}
	return strings.Join(parts, "\n")
}
