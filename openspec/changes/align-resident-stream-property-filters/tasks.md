# Projected stream filter consistency

- [x] Reproduce the 36-node public scan failure on the current release; preserve cold and repeated scan results.
- [x] Make resident filtering reuse the canonical property projection owner.
- [x] Run affected projected-decoder, label-stream and filter controls and focused race checks.
- [ ] Publish the candidate against release/v1.5.0 and run exact-head CI.
