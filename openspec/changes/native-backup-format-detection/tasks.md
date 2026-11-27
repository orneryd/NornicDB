# Detect native backup framing before JSON restore (release reliability blocker)

- [x] Reproduce intermittent native restore parsing on the unmodified release head with repeated public tests.
- [x] Add a deterministic fail-first native-frame collision test and verify the pre-fix failure.
- [x] Verify a valid native backup with a JSON-punctuation length byte fails before the fix and restores after it.
- [x] Run focused and package checks on the release-base fix; 100 focused repetitions and the full `pkg/nornicdb` package pass.
- [ ] Open a dedicated release PR and propagate it into N152-343 before its final package gate.

Badger prefixes native backup frames with an eight-byte little-endian length.
The old detector interpreted its low byte as a JSON first token when it was
`{` or `[`, routing a valid native backup to the JSON parser.
