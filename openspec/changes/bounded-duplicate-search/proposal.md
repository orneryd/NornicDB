# Bounded duplicate search

The maintainer replaced exhaustive cursor continuation with one high-limit
search request per candidate after live continuation repeatedly returned HTTP
410 despite a verified 15-minute TTL and a five-minute analysis deadline.
Use limit 1000, the server configured relevance filters, and enforce a finite
RRF floor of 0.01 locally. HTTP has no per-request threshold field; do not send
an ignored field or claim server support. Preserve mandatory duplicate blocking,
repo/database identity, test/production separation, and read-only analysis.
This explicitly accepts bounded recall instead of collection exhaustion.
