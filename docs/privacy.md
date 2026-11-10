# Documentation analytics

The public documentation site sends selected usage events to PostHog's US Cloud project for NornicDB. These help us understand which guides people read, whether semantic search is useful, and whether visitors explore the code or follow links to GitHub, releases, Docker, and the community.

Events include the documentation path and version, search scope, result count and semantic/fallback mode, code exploration, and outbound destination category. Search queries, URL query strings and fragments, form contents, source snippets, and graph data are excluded. Automatic click capture, session recording, error reporting, and person profiles are disabled. No database-instance telemetry is collected by this integration.

The browser SDK uses an in-memory random identifier, which resets on reload. It does not provide reliable returning-visitor or cross-session conversion counts. PostHog receives the network connection; the integration disables IP enrichment and geolocation properties. We do not claim that a network request is anonymous merely because names and email addresses are absent.

Use **Disable usage analytics** at the bottom of the site to opt out. That preference is stored locally in your browser. Do Not Track and Global Privacy Control also disable collection. Analytics run only on the public documentation hostname; local development and tests do not send events.

See [PostHog's privacy policy](https://posthog.com/privacy). Browser blockers may prevent collection, so analytics totals are an observed sample of traffic rather than a complete census.
