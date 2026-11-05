package security

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// referenceKeyedDigest reproduces the preserved framing independently of the
// implementation: one contiguous message holding the 8-byte big-endian length
// prefix and bytes of the namespace followed by every part, hashed in a single
// write. KeyedDigest hashes the same message in bounded chunks and must return
// the identical digest.
func referenceKeyedDigest(key []byte, namespace string, parts ...string) [32]byte {
	message := make([]byte, 0, 8+len(namespace))
	appendPart := func(value string) {
		var lenBuf [8]byte
		binary.BigEndian.PutUint64(lenBuf[:], uint64(len(value)))
		message = append(message, lenBuf[:]...)
		message = append(message, value...)
	}
	appendPart(namespace)
	for _, part := range parts {
		appendPart(part)
	}

	mac := hmac.New(sha256.New, key)
	if _, err := mac.Write(message); err != nil {
		panic(err)
	}
	var out [32]byte
	copy(out[:], mac.Sum(nil))
	return out
}

// scriptedCancelContext cancels a real context on a deterministic poll. Err
// and Done therefore describe the same cancellation, including inside a part.
type scriptedCancelContext struct {
	context.Context
	cancel            context.CancelFunc
	cancelAfterChecks int64
	checks            atomic.Int64
}

func newScriptedCancelContext(t *testing.T, checks int64) *scriptedCancelContext {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	return &scriptedCancelContext{Context: ctx, cancel: cancel, cancelAfterChecks: checks}
}
func (c *scriptedCancelContext) Err() error {
	if c.checks.Add(1) > c.cancelAfterChecks {
		c.cancel()
	}
	return c.Context.Err()
}

// allocatedBytesPerRun reports how many bytes, on average, fn allocates per
// call. It reads the process's cumulative allocation counter, so it measures
// allocation volume rather than peak memory: a digest that copies every chunk
// into a fresh buffer shows up here even when the peak stays flat.
func allocatedBytesPerRun(runs int, fn func()) uint64 {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < runs; i++ {
		fn()
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / uint64(runs)
}

func TestKeyedDigest_DeterministicWithinNamespace(t *testing.T) {
	ctx := context.Background()
	left, err := KeyedDigest(ctx, "auth.basic.header", "Basic YWxpY2U6cGFzcw==")
	if err != nil {
		t.Fatalf("KeyedDigest failed: %v", err)
	}
	right, err := KeyedDigest(ctx, "auth.basic.header", "Basic YWxpY2U6cGFzcw==")
	if err != nil {
		t.Fatalf("KeyedDigest failed: %v", err)
	}

	if left != right {
		t.Fatal("expected identical digests for identical namespace and input")
	}
}

func TestKeyedDigest_DomainSeparated(t *testing.T) {
	ctx := context.Background()
	authDigest, err := KeyedDigest(ctx, "auth.basic.credentials", "alice", "password123")
	if err != nil {
		t.Fatalf("KeyedDigest failed: %v", err)
	}
	schemaDigest, err := KeyedDigest(ctx, "storage.composite_key", "alice", "password123")
	if err != nil {
		t.Fatalf("KeyedDigest failed: %v", err)
	}

	if authDigest == schemaDigest {
		t.Fatal("expected different digests across namespaces")
	}
}

func TestKeyedDigest_DistinguishesPartBoundaries(t *testing.T) {
	ctx := context.Background()
	joined, err := KeyedDigest(ctx, "boundary", "ab", "c")
	if err != nil {
		t.Fatalf("KeyedDigest failed: %v", err)
	}
	split, err := KeyedDigest(ctx, "boundary", "a", "bc")
	if err != nil {
		t.Fatalf("KeyedDigest failed: %v", err)
	}

	if joined == split {
		t.Fatal("expected length-prefixed encoding to distinguish different part boundaries")
	}
}

func TestKeyedDigest_DistinguishesNamespacePartBoundary(t *testing.T) {
	ctx := context.Background()
	left, err := KeyedDigest(ctx, "ab", "c")
	if err != nil {
		t.Fatalf("KeyedDigest failed: %v", err)
	}
	right, err := KeyedDigest(ctx, "a", "bc")
	if err != nil {
		t.Fatalf("KeyedDigest failed: %v", err)
	}

	if left == right {
		t.Fatal("expected length-prefixed encoding to distinguish namespace from part boundary")
	}
}

func TestKeyedDigest_DistinguishesMissingAndEmptyPart(t *testing.T) {
	ctx := context.Background()
	absent, err := KeyedDigest(ctx, "empty.part")
	if err != nil {
		t.Fatalf("KeyedDigest failed: %v", err)
	}
	empty, err := KeyedDigest(ctx, "empty.part", "")
	if err != nil {
		t.Fatalf("KeyedDigest failed: %v", err)
	}

	if absent == empty {
		t.Fatal("expected length-prefixed encoding to distinguish no part from an empty part")
	}
}

func TestKeyedDigest_FramingMatchesExplicitMessage(t *testing.T) {
	// The explicitly laid out approved framing for namespace "ab" and parts ""
	// and "c": each value is its 8-byte big-endian length followed by its bytes,
	// namespace first, then the parts in order.
	message := []byte{
		0, 0, 0, 0, 0, 0, 0, 2, 'a', 'b',
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 1, 'c',
	}
	mac := hmac.New(sha256.New, processLocalDigestKey)
	if _, err := mac.Write(message); err != nil {
		t.Fatalf("reference HMAC failed: %v", err)
	}
	var want [32]byte
	copy(want[:], mac.Sum(nil))

	got, err := KeyedDigest(context.Background(), "ab", "", "c")
	if err != nil {
		t.Fatalf("KeyedDigest failed: %v", err)
	}
	if got != want {
		t.Fatalf("framing mismatch: got %x, want %x", got, want)
	}
}

func TestKeyedDigest_MatchesReferenceFraming(t *testing.T) {
	largePart := strings.Repeat("0123456789abcdef", (3*keyedDigestChunkSize)/16+7)

	cases := []struct {
		name      string
		namespace string
		parts     []string
	}{
		{name: "namespace only", namespace: "ns.none"},
		{name: "empty namespace and empty part", namespace: "", parts: []string{""}},
		{name: "empty parts around a value", namespace: "ns.empty", parts: []string{"", "value", ""}},
		{name: "part one byte below chunk boundary", namespace: "ns.chunk", parts: []string{strings.Repeat("a", keyedDigestChunkSize-1)}},
		{name: "part at chunk boundary", namespace: "ns.chunk", parts: []string{strings.Repeat("a", keyedDigestChunkSize)}},
		{name: "part one byte above chunk boundary", namespace: "ns.chunk", parts: []string{strings.Repeat("a", keyedDigestChunkSize+1)}},
		{name: "large part spanning chunks", namespace: "ns.large", parts: []string{largePart}},
		{name: "namespace spanning chunks", namespace: strings.Repeat("n", keyedDigestChunkSize+1), parts: []string{"value"}},
		{name: "multiple large parts", namespace: "ns.multi", parts: []string{largePart[:keyedDigestChunkSize+1], "", largePart}},
		{name: "large parts with shorter trailing part", namespace: "ns.multi", parts: []string{largePart[:2*keyedDigestChunkSize+7], "trailing", largePart[:5]}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := referenceKeyedDigest(processLocalDigestKey, tc.namespace, tc.parts...)

			got, err := KeyedDigest(context.Background(), tc.namespace, tc.parts...)
			if err != nil {
				t.Fatalf("KeyedDigest failed: %v", err)
			}
			if got != want {
				t.Fatalf("digest mismatch: got %x, want %x", got, want)
			}

			gotHex, err := KeyedDigestHex(context.Background(), tc.namespace, tc.parts...)
			if err != nil {
				t.Fatalf("KeyedDigestHex failed: %v", err)
			}
			if gotHex != hex.EncodeToString(want[:]) {
				t.Fatalf("hex digest mismatch: got %q, want %q", gotHex, hex.EncodeToString(want[:]))
			}
		})
	}
}

// TestKeyedDigest_SmallDigestDoesNotAllocateChunkBuffer holds the contract that
// the common small digest never pays for the bounded chunk buffer: it may only
// allocate far less than the buffer it would need if it streamed.
func TestKeyedDigest_SmallDigestDoesNotAllocateChunkBuffer(t *testing.T) {
	ctx := context.Background()
	const authHeader = "Basic YWxpY2U6cGFzc3dvcmQ="

	perDigest := allocatedBytesPerRun(200, func() {
		if _, err := KeyedDigest(ctx, "auth.basic.header", authHeader); err != nil {
			t.Fatalf("KeyedDigest failed: %v", err)
		}
	})
	t.Logf("small digest allocated %d bytes per digest (chunk buffer is %d bytes)", perDigest, keyedDigestChunkSize)

	if perDigest >= keyedDigestChunkSize {
		t.Fatalf("small digest allocated %d bytes per digest, which means it allocated the %d-byte chunk buffer", perDigest, keyedDigestChunkSize)
	}
}

// TestKeyedDigest_LargePartsReuseOneChunkBuffer holds the contract that a digest
// streams every chunk of every part through one bounded buffer: allocation does
// not grow with the number of chunks, and a shorter trailing part reuses the
// buffer the earlier large parts allocated.
func TestKeyedDigest_LargePartsReuseOneChunkBuffer(t *testing.T) {
	ctx := context.Background()
	largePart := strings.Repeat("0123456789abcdef", (64<<10)/16)
	trailingPart := strings.Repeat("t", 1000)
	megaPart := strings.Repeat("m", 1<<20)
	parts := []string{largePart, largePart, largePart, trailingPart}

	got, err := KeyedDigest(ctx, "ns.multi", parts...)
	if err != nil {
		t.Fatalf("KeyedDigest failed: %v", err)
	}
	if want := referenceKeyedDigest(processLocalDigestKey, "ns.multi", parts...); got != want {
		t.Fatalf("digest mismatch: got %x, want %x", got, want)
	}

	perDigest := allocatedBytesPerRun(20, func() {
		if _, err := KeyedDigest(ctx, "ns.multi", parts...); err != nil {
			t.Fatalf("KeyedDigest failed: %v", err)
		}
	})
	t.Logf("%d parts (%d large, 1 trailing) allocated %d bytes per digest", len(parts), 3, perDigest)

	// One bounded buffer per digest plus the HMAC state fits well under two
	// buffers; copying each chunk or part into its own buffer does not.
	if perDigest > 2*keyedDigestChunkSize {
		t.Fatalf("multi-part digest allocated %d bytes per digest, more than the %d-byte bound for one reused chunk buffer: chunks or parts are not sharing the buffer", perDigest, 2*keyedDigestChunkSize)
	}

	multiPartAllocs := testing.AllocsPerRun(20, func() {
		if _, err := KeyedDigest(ctx, "ns.multi", parts...); err != nil {
			t.Fatalf("KeyedDigest failed: %v", err)
		}
	})
	singlePartAllocs := testing.AllocsPerRun(20, func() {
		if _, err := KeyedDigest(ctx, "ns.multi", largePart); err != nil {
			t.Fatalf("KeyedDigest failed: %v", err)
		}
	})
	megaPartAllocs := testing.AllocsPerRun(20, func() {
		if _, err := KeyedDigest(ctx, "ns.multi", megaPart); err != nil {
			t.Fatalf("KeyedDigest failed: %v", err)
		}
	})
	t.Logf("allocations per digest: %v for %d parts, %v for 1 part, %v for one 1MiB part", multiPartAllocs, len(parts), singlePartAllocs, megaPartAllocs)

	// Allocation count must not follow the chunk count: the 1MiB part is 32
	// chunks and must not allocate more often than the multi-part digest.
	if megaPartAllocs > multiPartAllocs {
		t.Fatalf("a 1MiB part allocates %v times and the multi-part digest %v times: allocations grow with the number of chunks instead of reusing one buffer", megaPartAllocs, multiPartAllocs)
	}
	// At most one extra bounded allocation per part is allowed; more than that
	// means a part allocates once per chunk.
	if multiPartAllocs > singlePartAllocs+float64(len(parts)) {
		t.Fatalf("multi-part digest allocates %v times and a single-part digest %v times with %d parts: a part allocates per chunk instead of reusing one buffer", multiPartAllocs, singlePartAllocs, len(parts))
	}
}

func TestKeyedDigest_AlreadyCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	digest, err := KeyedDigest(ctx, "ns", "part")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if digest != ([32]byte{}) {
		t.Fatalf("expected zero digest on cancellation, got %x", digest)
	}

	hexDigest, err := KeyedDigestHex(ctx, "ns", "part")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled from KeyedDigestHex, got %v", err)
	}
	if hexDigest != "" {
		t.Fatalf("expected empty hex digest on cancellation, got %q", hexDigest)
	}
}

func TestKeyedDigest_ExpiredDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	_, err := KeyedDigest(ctx, "ns", "part")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

// TestKeyedDigest_CancelledBeforeChunkBufferIsAllocated holds the contract that
// the bounded chunk buffer is allocated lazily, after the context check that
// guards the chunk: a digest cancelled before its first part chunk allocates
// nothing like a buffer.
func TestKeyedDigest_CancelledBeforeChunkBufferIsAllocated(t *testing.T) {
	part := strings.Repeat("k", 8*keyedDigestChunkSize)
	// 1 entry, 2 namespace length prefix, 3 namespace chunk, 4 part length
	// prefix: cancellation lands before the first part chunk, so the digest
	// never reaches the allocation of the chunk buffer.
	// Every measured call must reach the intended mid-work cancellation. Reusing
	// an already canceled context would measure only the entry check after the
	// first iteration and could conceal an eagerly allocated chunk buffer.
	const runs = 20
	contexts := make([]*scriptedCancelContext, runs)
	for i := range contexts {
		contexts[i] = newScriptedCancelContext(t, 3)
	}
	run := 0
	perCancelledDigest := allocatedBytesPerRun(runs, func() {
		ctx := contexts[run]
		run++
		if _, err := KeyedDigest(ctx, "cancel.before-buffer", part); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	})
	for _, ctx := range contexts {
		if ctx.checks.Load() != 4 {
			t.Fatalf("allocation probe did not reach the intended cancellation phase: %d checks", ctx.checks.Load())
		}
		select {
		case <-ctx.Done():
		default:
			t.Fatal("allocation probe did not cancel a real context")
		}
	}
	t.Logf("cancelled digest allocated %d bytes per digest", perCancelledDigest)

	if perCancelledDigest >= keyedDigestChunkSize {
		t.Fatalf("digest cancelled before its first chunk allocated %d bytes, so the chunk buffer was allocated before the context check", perCancelledDigest)
	}
}

func TestKeyedDigest_CancelsMidHashThenRetriesHealthy(t *testing.T) {
	const partSize = 3 * keyedDigestChunkSize
	namespace := "cancel.mid-hash"
	part := strings.Repeat("k", partSize)

	// Err check order for this input: 1 entry, 2 namespace length prefix,
	// 3 namespace chunk, 4 part length prefix, 5 chunk 1, 6 chunk 2, 7 chunk 3.
	// Cancelling on check 7 stops the hash after two of the three part chunks,
	// which is only possible if the part is hashed in bounded chunks.
	ctx := newScriptedCancelContext(t, 6)

	digest, err := KeyedDigest(ctx, namespace, part)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled from a mid-hash cancellation, got %v", err)
	}
	if digest != ([32]byte{}) {
		t.Fatalf("expected zero digest on cancellation, got %x", digest)
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("mid-hash probe did not cancel its real context")
	}
	if ctx.checks.Load() != 7 {
		t.Fatalf("expected cancellation on the third part chunk (7 Err checks), got %d: hashing is not chunk-bounded", ctx.checks.Load())
	}

	retry, err := KeyedDigest(context.Background(), namespace, part)
	if err != nil {
		t.Fatalf("healthy retry after cancellation failed: %v", err)
	}
	want := referenceKeyedDigest(processLocalDigestKey, namespace, part)
	if retry != want {
		t.Fatalf("healthy retry digest mismatch: got %x, want %x", retry, want)
	}
}

func TestKeyedDigest_CancelsWithinNamespaceChunks(t *testing.T) {
	namespace := strings.Repeat("n", 3*keyedDigestChunkSize)

	// 1 entry, 2 namespace length prefix, 3 namespace chunk 1, 4 chunk 2,
	// 5 chunk 3: cancellation lands inside the namespace, after two chunks.
	ctx := newScriptedCancelContext(t, 4)

	if _, err := KeyedDigest(ctx, namespace, "part"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("namespace probe did not cancel its real context")
	}
	if ctx.checks.Load() != 5 {
		t.Fatalf("expected cancellation on the third namespace chunk (5 Err checks), got %d", ctx.checks.Load())
	}

	got, err := KeyedDigest(context.Background(), namespace, "part")
	if err != nil {
		t.Fatalf("healthy retry after cancellation failed: %v", err)
	}
	want := referenceKeyedDigest(processLocalDigestKey, namespace, "part")
	if got != want {
		t.Fatalf("healthy retry digest mismatch: got %x, want %x", got, want)
	}
}

func BenchmarkKeyedDigest(b *testing.B) {
	ctx := context.Background()
	const authHeader = "Basic YWxpY2U6cGFzc3dvcmQ="
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := KeyedDigest(ctx, "auth.basic.header", authHeader); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKeyedDigestLargePart(b *testing.B) {
	ctx := context.Background()
	part := strings.Repeat("0123456789abcdef", (1<<20)/16)
	b.SetBytes(int64(len(part)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := KeyedDigest(ctx, "storage.composite_key", part); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKeyedDigestMultipleLargeParts(b *testing.B) {
	ctx := context.Background()
	largePart := strings.Repeat("0123456789abcdef", (64<<10)/16)
	parts := []string{largePart, largePart, largePart, strings.Repeat("t", 1000)}
	b.SetBytes(int64(len(parts[0])*3 + len(parts[3])))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := KeyedDigest(ctx, "storage.composite_key", parts...); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKeyedDigestHex(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := KeyedDigestHex(ctx, "storage.composite_key", "Person:alice|string:email@example.com"); err != nil {
			b.Fatal(err)
		}
	}
}

func TestKeyedDigestCancellationBeforePublicationReturnsNoDigest(t *testing.T) {
	// Entry, namespace prefix/body, part prefix/body, then publication check.
	ctx := newScriptedCancelContext(t, 5)
	digest, err := KeyedDigest(ctx, "ns", "value")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected publication cancellation, got %v", err)
	}
	if digest != ([32]byte{}) {
		t.Fatalf("canceled finalization published digest %x", digest)
	}
	if ctx.checks.Load() != 6 {
		t.Fatalf("expected final publication check, got %d", ctx.checks.Load())
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("publication probe did not cancel a real context")
	}
	healthy, err := KeyedDigest(t.Context(), "ns", "value")
	if err != nil {
		t.Fatal(err)
	}
	if want := referenceKeyedDigest(processLocalDigestKey, "ns", "value"); healthy != want {
		t.Fatalf("healthy retry differs: %x != %x", healthy, want)
	}
}
