package text

import (
	"strings"
	"unicode/utf8"
	"unsafe"
)

func bufferCapacity(capacity, needed int) int {
	if capacity > int(^uint(0)>>1)/2 {
		return needed
	}
	return max(needed, max(128, capacity*2))
}

// ReserveString reserves checked output capacity. Allocation/zeroing itself
// is native and not preemptible. Every application-controlled old-prefix copy
// is at most 4 KiB and consumes the caller's existing work budget. Reset keeps
// the builder at its original address; a nonempty Builder is never copied or
// grown. The old string keeps the prior buffer alive until copying completes.
func ReserveString(out *strings.Builder, additional int, work Work) bool {
	if work == nil {
		return true
	}
	if !work.Continue() {
		return false
	}
	if additional <= out.Cap()-out.Len() {
		return true
	}
	previous := out.String()
	capacity := bufferCapacity(out.Cap(), out.Len()+additional)
	out.Reset()
	out.Grow(capacity)
	for start := 0; start < len(previous); start += 4096 {
		if !work.Continue() {
			return false
		}
		out.WriteString(previous[start:min(start+4096, len(previous))])
	}
	return true
}

func AppendString(out *strings.Builder, value string, work Work) bool {
	if work == nil {
		out.WriteString(value)
		return true
	}
	if len(value) == 0 {
		return ReserveString(out, 0, work)
	}
	for start := 0; start < len(value); start += 4096 {
		end := min(start+4096, len(value))
		if !ReserveString(out, end-start, work) {
			return false
		}
		out.WriteString(value[start:end])
	}
	return true
}

func AppendBytes(out *strings.Builder, value []byte, work Work) bool {
	if work == nil {
		out.Write(value)
		return true
	}
	if len(value) == 0 {
		return ReserveString(out, 0, work)
	}
	for start := 0; start < len(value); start += 4096 {
		end := min(start+4096, len(value))
		if !ReserveString(out, end-start, work) {
			return false
		}
		out.Write(value[start:end])
	}
	return true
}

func AppendByte(out *strings.Builder, value byte, work Work) bool {
	if work == nil {
		out.WriteByte(value)
		return true
	}
	if !ReserveString(out, 1, work) {
		return false
	}
	out.WriteByte(value)
	return true
}

func AppendRune(out *strings.Builder, value rune, work Work) bool {
	if work == nil {
		out.WriteRune(value)
		return true
	}
	if !ReserveString(out, utf8.UTFMax, work) {
		return false
	}
	out.WriteRune(value)
	return true
}

// AppendItems copies into explicitly reserved capacity. The checked path
// never asks append to relocate a growing prefix. Units contain at most 128
// slots and at most 4 KiB, except that an indivisible T can itself exceed that
// size. unsafe reads only type layout and slice addresses, never memory contents.
func AppendItems[T any](out, items []T, work Work) ([]T, bool) {
	if work == nil {
		return append(out, items...), true
	}
	if !work.Continue() {
		return nil, false
	}
	var element T
	unit := 128
	if size := unsafe.Sizeof(element); size > 0 {
		unit = max(1, min(unit, int(4096/size)))
	}
	needed := len(out) + len(items)
	if needed > cap(out) {
		grown := make([]T, len(out), bufferCapacity(cap(out), needed))
		for start := 0; start < len(out); start += unit {
			if !work.Continue() {
				return nil, false
			}
			copy(grown[start:min(start+unit, len(out))], out[start:min(start+unit, len(out))])
		}
		out = grown
	}
	previous := len(out)
	out = out[:needed]
	// Match append's memmove semantics even when items aliases unused capacity.
	backward := false
	if len(items) > 0 && unsafe.Sizeof(element) != 0 {
		destination := uintptr(unsafe.Pointer(&out[previous]))
		source := uintptr(unsafe.Pointer(unsafe.SliceData(items)))
		backward = destination > source && (destination-source)/unsafe.Sizeof(element) < uintptr(len(items))
	}
	for copied := 0; copied < len(items); copied += unit {
		if !work.Continue() {
			return nil, false
		}
		start, end := copied, min(copied+unit, len(items))
		if backward {
			end = len(items) - copied
			start = max(0, end-unit)
		}
		copy(out[previous+start:previous+end], items[start:end])
	}
	return out, true
}

func AppendItem[T any](out []T, item T, work Work) ([]T, bool) {
	items := [1]T{item}
	return AppendItems(out, items[:], work)
}
