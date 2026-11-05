# Maintained dependency forks

NornicDB pins `github.com/orneryd/go-json`, based on upstream goccy/go-json
v0.11.2. A direct module requirement makes the dependency available to ordinary
Go embedding applications; no consumer-side replacement or vendoring is needed.
Upstream source, license and test populations are retained in the fork.

The maintainer-approved delta provides opt-in lossless string decoding and
corrects cyclic-map identity tracking. See go-json-lossless-decoding.md and the
reviewable patch. No hook rules or storage formats are changed by this dependency.
