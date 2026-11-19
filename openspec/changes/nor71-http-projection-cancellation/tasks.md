# TODO

- [x] DeepSeek recorded four fail-first HTTP cases and physical-scope failure on release base e8cd436e5.
- [x] DeepSeek published draft PR1866 before parent source review, as explicitly requested.
- [x] Parent reviewed all six changed files and shared admission kernel after publication.
- [x] Parent independently reran HTTP/scope regressions on macOS successfully.
- [ ] Parent independently verify the same focused checks on Linux.
- [ ] Run unchanged canonical duplicate checker over a staged reconstruction identical to published tree; retain tree identity proof.
- [ ] Record all reproduction sources and results inline in existing Plane descriptions.
- [ ] Let CI perform broader checks. No full local verification, race or stress run.
