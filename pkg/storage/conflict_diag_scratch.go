package storage

// SCRATCH DIAGNOSTIC (not for merge).

import (
	"log"
	"runtime/debug"
	"strings"
)

func scratchConflictDiag(site, key string) {
	stack := string(debug.Stack())
	if len(stack) > 3000 {
		stack = stack[:3000]
	}
	log.Printf("CONFLICTDIAG site=%s key=%s stack=%s", site, key, strings.ReplaceAll(stack, "\n", " | "))
}
