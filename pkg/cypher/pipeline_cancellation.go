package cypher

import (
	"context"
	"hash/maphash"
	"sort"
	"strings"
)

const pipelineCancellationInterval = 128

type pipelineCancellationKey struct{}

// An expression's recursive readers share one probe. Context-free readers
// retain their nil-probe wrappers; a running expression carries its probe in
// the same statement context its row already uses for clocks and subqueries.
func withPipelineCancellation(ctx context.Context) context.Context {
	if ctx.Done() == nil {
		return ctx
	}
	if ctx.Value(pipelineCancellationKey{}) != nil {
		return ctx
	}
	return context.WithValue(ctx, pipelineCancellationKey{}, &pipelineCancellation{ctx: ctx})
}

func pipelineCancellationForContext(ctx context.Context) *pipelineCancellation {
	if probe, ok := ctx.Value(pipelineCancellationKey{}).(*pipelineCancellation); ok {
		return probe
	}
	return &pipelineCancellation{ctx: ctx}
}

func rowPipelineCancellation(values map[string]interface{}) *pipelineCancellation {
	if ctx := rowStatementContext(values); ctx != nil && ctx.Done() != nil {
		return pipelineCancellationForContext(ctx)
	}
	return nil
}

// Large leaves consume the same budget as nested elements. Each unit copies
// at most 4 KiB, so one sample bounds 512 KiB of unchecked byte work.
func appendPipelineString(out *strings.Builder, text string, probe *pipelineCancellation) bool {
	if probe == nil {
		out.WriteString(text)
		return true
	}
	for len(text) > 0 {
		if !probe.check() {
			return false
		}
		size := min(len(text), 4096)
		out.WriteString(text[:size])
		text = text[size:]
	}
	return probe.err == nil
}

func concatPipelineStrings(left, right string, probe *pipelineCancellation) string {
	if probe == nil {
		return left + right
	}
	if len(left) <= 4096 && len(right) <= 4096-len(left) {
		if !probe.check() {
			return ""
		}
		return left + right
	}
	var result strings.Builder
	if !appendPipelineString(&result, left, probe) || !appendPipelineString(&result, right, probe) {
		return ""
	}
	return result.String()
}

func appendPipelineValues(out, items []interface{}, probe *pipelineCancellation) []interface{} {
	if probe == nil {
		return append(out, items...)
	}
	for start := 0; start < len(items); start += 128 {
		if !probe.check() {
			return nil
		}
		out = append(out, items[start:min(start+128, len(items))]...)
	}
	return out
}

func joinPipelineStrings(parts []string, separator string, probe *pipelineCancellation) string {
	if probe == nil {
		return strings.Join(parts, separator)
	}
	if !probe.check() {
		return ""
	}
	if len(parts) == 0 {
		return ""
	}
	if len(parts) == 1 {
		return parts[0]
	}
	var result strings.Builder
	for index, part := range parts {
		if index > 0 && !appendPipelineString(&result, separator, probe) {
			return ""
		}
		if !appendPipelineString(&result, part, probe) {
			return ""
		}
	}
	return result.String()
}

func comparePipelineStrings(left, right string, probe *pipelineCancellation) int {
	for len(left) > 0 && len(right) > 0 {
		if probe != nil && !probe.check() {
			return 0
		}
		size := min(len(left), len(right), 4096)
		if order := strings.Compare(left[:size], right[:size]); order != 0 {
			return order
		}
		left, right = left[size:], right[size:]
	}
	return compareOrderedInts(len(left), len(right))
}

func hashPipelineString(text string, seed maphash.Seed, probe *pipelineCancellation) uint64 {
	if probe == nil || len(text) <= 4096 {
		return maphash.String(seed, text)
	}
	var hash maphash.Hash
	hash.SetSeed(seed)
	for len(text) > 0 {
		if !probe.check() {
			return 0
		}
		size := min(len(text), 4096)
		hash.WriteString(text[:size])
		text = text[size:]
	}
	return hash.Sum64()
}

func pipelineRunes(text string, probe *pipelineCancellation) []rune {
	if probe == nil {
		return []rune(text)
	}
	var result []rune
	for _, character := range text {
		if !probe.check() {
			return nil
		}
		result = append(result, character)
	}
	return result
}

func pipelineStringFromRunes(characters []rune, probe *pipelineCancellation) (string, error) {
	if probe == nil {
		return string(characters), nil
	}
	var result strings.Builder
	for _, character := range characters {
		if !probe.check() {
			return "", probe.err
		}
		result.WriteRune(character)
	}
	return result.String(), probe.err
}

type pipelineSortCancellation struct{}

// pipelineCancellation belongs to one synchronous stage (and one goroutine).
// The hot path is an inlinable decrement. The first unit and then every 128
// units consult the context; cancellation is latched and recorded in the
// statement's existing error slot, so a stopped stage cannot trigger fallback.
type pipelineCancellation struct {
	ctx       context.Context
	remaining int
	err       error
}

func (probe *pipelineCancellation) check() bool {
	probe.remaining--
	if probe.remaining > 0 {
		return true
	}
	return probe.poll()
}

//go:noinline
func (probe *pipelineCancellation) poll() bool {
	if probe.err != nil {
		return false
	}
	probe.err = probe.ctx.Err()
	if probe.err != nil {
		probe.remaining = 0
		recordExpressionFailure(probe.ctx, probe.err)
		return false
	}
	probe.remaining = pipelineCancellationInterval
	return true
}

// sort stops inside the comparator, rather than returning false comparisons
// and letting sort finish a large input. Only the private cancellation sentinel
// is recovered, within the sort call; unrelated panics retain their meaning.
func (probe *pipelineCancellation) sort(values interface{}, less func(int, int) bool, stable bool) (ok bool) {
	defer func() {
		if stopped := recover(); stopped != nil {
			if _, cancelled := stopped.(pipelineSortCancellation); !cancelled {
				panic(stopped)
			}
			ok = false
		}
	}()
	guarded := func(left, right int) bool {
		if !probe.check() {
			panic(pipelineSortCancellation{})
		}
		return less(left, right)
	}
	if stable {
		sort.SliceStable(values, guarded)
	} else {
		sort.Slice(values, guarded)
	}
	return probe.check()
}
