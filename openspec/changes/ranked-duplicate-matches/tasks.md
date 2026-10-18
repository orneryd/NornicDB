- [x] Preserve the prior full-catalog scan's evidence and identify `ranked_then_id`
      as the cause of thousands of continuation requests per function.
- [x] Reproduce missing score-floor enforcement and ranked-completion rejection.
- [x] Use ranked continuation, a finite RRF floor of 0.01 and the existing 0.8
      code-similarity rule; preserve test/production separation and shared databases.
- [x] Verify later-page matches, floor boundaries, malformed scores, wrong modes,
      truncated/expired/repeated cursors and mandatory warnings on integrated source.
- [x] Install the runtime and repeat the real divergent-helper commit attempt
      against `nornicdbcode`, measuring analysis time and preserving HEAD/index.
- [ ] Publish evidence and source updates to #1449; report full-verification blockers.
- [ ] Resume #1406's ordered-publication performance tuning.
