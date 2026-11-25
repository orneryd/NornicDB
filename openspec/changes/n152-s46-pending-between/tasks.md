# S46 pending relationship MERGE lookup

- [x] Reproduce the 1,000/4,000/9,600-row constrained and unconstrained MERGE spike on release/v1.5.3.
- [x] Profile the public path and identify the directed-pair lookup owner.
- [x] Reuse transaction-staged edge mutation ownership to maintain one exact-pair index.
- [x] Verify CREATE, new and existing MERGE, non-key SET, duplicate rejection, endpoint/type changes, and lock-refresh images.
- [x] Run focused and affected package checks.
- [x] Publish a PR against release/v1.5.3 with exact head/base evidence.
