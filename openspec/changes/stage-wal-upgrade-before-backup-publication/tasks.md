# Stage WAL upgrade before backup publication

- [x] Reproduce post-validation conversion failure publishing a backup before source replacement.
- [x] Stage immutable backup copy and all converted files before publishing backup.
- [x] Preserve interrupted-publication resume and strict corruption/torn-tail refusal.
- [x] Reproduce and correct named-return cleanup losing its allocated staging path.
- [x] Check repeated copy failure, source/marker byte equality and no staging residue.
- [x] Qualify near-limit historical JSON/gob startup and complete recovered data using the existing historical frame owner.
- [x] Parent focused tests2.398s and affected race controls36.033s pass before frame-helper reuse; rerun final source below.
- [x] Final-source focused/race31.732s after helper reuse; repeated-error assertion is checked again before push.
- [ ] Exact-head CI and remote duplicate review.
- [ ] Remote duplicate review and remaining historical async-store B05 investigation (not claimed fixed).
