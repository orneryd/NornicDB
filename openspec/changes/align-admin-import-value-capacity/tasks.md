# Align full-import capacity with the normal server

- [x] Reproduce accepted public-server136MiB value refusing import at128MiB after export/reopen.
- [x] Confirm the owner is the admin constructor’s balanced profile, not snapshot framing.
- [x] Select the existing normal-server profile for full imports only.
- [x] Preserve explicit low-memory and balanced embedded capacities; document the distinction and importer memory tradeoff.
- [x] Keep the default quoted/newline round-trip fixture small; place the multi-gigabyte136MiB qualification behind an explicit build tag.
- [x] Re-run public large export/import/reopen qualification on final source (11.302s; maxRSS2.76GB).
- [x] Run affected packages (CLI2.201s/import1.772s).
- [x] Complete vet and ordinary round-trip race controls (two repeats,2.928s).
- [ ] Publish candidate and run CI/duplicate review.
