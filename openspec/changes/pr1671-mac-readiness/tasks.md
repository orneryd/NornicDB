# PR 1671 Mac handoff readiness

Scope: complete the Mac make test gate, publish the pinned llama CPU prerequisite for Linux AMD64 and ARM64, finish the image-reference review correction, verify published consumers, and update PR readiness. Broad GPU construction and compatibility qualification remains on the release branch under the maintainer's selected gate.

- [x] Read PR handoff, reviews, conversation and repository instructions.
- [x] Verify the CUDA v0.7.0 tag exists; record its registry digest.
- [ ] Build and push llama CPU v0.7.0 for AMD64 and ARM64 from the pinned source.
- [ ] Verify the published CPU manifest, architectures and exported artifacts.
- [ ] Pin application prerequisites to published immutable CPU/CUDA references and verify consumers.
- [ ] Run make test on Mac; fix introduced regressions and classify existing failures.
- [ ] Publish exact-source results, image digests and remaining qualification limits; mark the PR ready.
