# Reduce mutation-image amplification

- [x] Profile bounded 0/256-vector property-update preparation on current release.
- [x] Reject the unsafe all-images-sharing experiment using caller-mutation proof.
- [x] Retain required before-image copies; reuse private staged after-images.
- [x] Reproduce and fix diagnostic deletion-fact aliasing.
- [x] Check caller/diagnostic detachment and the existing stored-vector replay matrix.
- [x] Compare byte/allocation samples; no whole-commit throughput claim.
- [ ] Remove unchanged-vector WAL amplification while preserving exact recovery semantics (reports33/39).
- [ ] Reproduce/correct snapshot stalls and accepted large-value restore (reports32/31).
- [ ] Complete exact-head regular CI, race/census and combined release verification.

No wire, stored payload or key-format change in the current candidate. Remaining group9 AC are not resolved by this memory-only correction.
