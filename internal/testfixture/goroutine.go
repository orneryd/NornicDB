package testfixture

import (
	"bytes"
	"runtime"
	"strconv"
)

// CurrentGoroutineID identifies the calling test goroutine for lock diagnostics.
func CurrentGoroutineID() uint64 {
	var header [64]byte
	fields := bytes.Fields(header[:runtime.Stack(header[:], false)])
	id, err := strconv.ParseUint(string(fields[1]), 10, 64)
	if err != nil {
		panic("testfixture: unexpected goroutine stack header: " + err.Error())
	}
	return id
}
