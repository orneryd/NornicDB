# Preserve UI result map keys

- [x] Reproduce the locked 6.2.0 driver's field loss with its real decoder and a real Bolt server; retain the server-side correct-value control.
- [x] Independently reproduce first-party conversion failures in the actual Vite-loaded API module even with driver 6.3.2, before accepting the correction.
- [x] Use the upstream driver fix and one shared own-entry conversion for the browser's Bolt and HTTP map walkers.
- [x] Check scalar, object-valued and nested special keys, empty/whitespace/constructor keys, unchanged object prototypes, and ordinary/safe-range integer conversion.
- [x] Parent macOS: all 35 UI package tests, production UI build and real Bolt/decoder controls pass.
- [x] Parent Linux Node 26.12.1: all 35 UI tests, production build and real Bolt/decoder controls pass. Initial Debian Node 20 lacks the existing suite TypeScript loader; that run failed and was repeated on the supported runtime.
- [x] Worker refresh on the current release base `e8cd436e5` at Node 26.12.1: the five regressions fail first — the locked 6.2.0 decoder and, with driver 6.3.2 installed and the API module unchanged, the first-party conversion — and all 38 UI package tests plus the production UI build pass after the correction.
- [ ] Current-head advisory duplicate analysis and CI; `scripts/pr_duplicates.py` stopped at the configured-endpoint/credential gate, so no graph comparison ran and no clean result is claimed.

## Scope and reuse review

The two API map walkers perform the same own-entry copying with different recursive value converters, so they now share `copyConvertedEntries`. The entity branches recurse through that map owner for Bolt properties; HTTP entity and point branches retain their already parsed property objects. The `parseCypherRows` column projection serves fixed internal administration queries and is not the arbitrary nested-map decoder. It remains a separate operation; generated property-edit parameters use controlled `pN` names. No server Packstream encoding or persisted format changes are needed.

The private Go/JavaScript reproduction harness is retained outside this PR. The committed regression exercises the actual API module through both transport entry paths, using the installed decoder for Bolt values and a transaction JSON response for HTTP. No global prototype mutation or downstream rendering exploit is claimed. Generated browser-review screenshots were archived and restored; no screenshot changes belong to this fix.

Full local release verification is deferred to CI by the maintainer's focused/package-check policy. A focused pass is not a full release-suite pass.
