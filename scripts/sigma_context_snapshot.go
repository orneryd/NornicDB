//go:build ignore

// Capture the standard-library category bits used by the sigma delta generator.
// Usage: go run scripts/sigma_context_snapshot.go /external/go-context.bin
// Compress the raw output with Python gzip.compress(data, mtime=0).
package main

import (
	"fmt"
	"os"
	"unicode"
)

func main() {
	if len(os.Args) != 2 {
		panic("usage: sigma_context_snapshot /external/go-context.bin")
	}
	data := make([]byte, unicode.MaxRune+1)
	for r := rune(0); r <= unicode.MaxRune; r++ {
		var bits byte
		if unicode.IsLetter(r) {
			bits |= 1
		}
		if unicode.Is(unicode.Mc, r) {
			bits |= 2
		}
		if unicode.IsNumber(r) {
			bits |= 4
		}
		if unicode.Is(unicode.Mn, r) || unicode.Is(unicode.Me, r) {
			bits |= 8
		}
		if unicode.Is(unicode.Cf, r) {
			bits |= 16
		}
		if unicode.Is(unicode.Pd, r) || unicode.Is(unicode.Pc, r) {
			bits |= 32
		}
		if unicode.Is(unicode.Lu, r) || unicode.Is(unicode.Ll, r) || unicode.Is(unicode.Lt, r) {
			bits |= 64
		}
		data[r] = bits
	}
	if err := os.WriteFile(os.Args[1], data, 0600); err != nil {
		panic(err)
	}
	fmt.Println("Unicode", unicode.Version)
}
