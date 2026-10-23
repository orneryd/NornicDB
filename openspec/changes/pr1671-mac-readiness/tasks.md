# PR 1671 Mac handoff readiness

Scope: complete the Mac make test gate, publish the pinned llama CPU prerequisite for Linux AMD64 and ARM64, finish the image-reference review correction, verify published consumers, and update PR readiness. Broad GPU construction and compatibility qualification remains on the release branch under the maintainer's selected gate.

- [x] Read PR handoff, reviews, conversation and repository instructions.
- [x] Verify the CUDA v0.7.0 tag exists; record its registry digest.
- [x] Build and push llama CPU v0.7.0 for AMD64 and ARM64 from the pinned source.
- [x] Verify the published CPU manifest and architectures; exported artifacts are checked by consumer compilation.
- [ ] Pin application prerequisites to published immutable CPU/CUDA references and verify consumers.
- [ ] Run make test on Mac; fix introduced regressions and classify existing failures.
- [ ] Publish exact-source results, image digests and remaining qualification limits; mark the PR ready.

Published CPU index: sha256:ec78fcac54c5f6d8f4cb7db4d41cae2eb856d348579001a41328bde839d2f028 (linux/amd64 and linux/arm64). Published CUDA manifest: sha256:89aa226654e16adcb794d1edc3cc90d945963e993d21d5f650b6d9638c2a98f7; config c8611f2a8da0865ad26426d955a37559f8c91cac9666ef5cd7cdd290551dc9c9 matches the handoff. The first Mac make test failed linking packages requiring the absent local native library; the pinned v0.7.0 Mac library has now been built. That initial run passed search (261.791s), but is not an overall pass.
