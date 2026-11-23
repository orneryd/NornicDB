# Documentation analytics

If you accept optional analytics, the public documentation site sends selected usage events to NornicDB's PostHog US Cloud project. These events help us understand which guides people read, how they use search, and which project links they follow.

Events include the documentation path and version, search scope, result count and semantic/fallback mode, code exploration, and outbound destination category. Search queries, URL query strings and fragments, form contents, source snippets, and graph data are excluded. Automatic click capture, session recording, error reporting, and person profiles are disabled. No database-instance telemetry is collected by this integration.

The browser SDK uses an in-memory random identifier, which resets on reload. It does not provide reliable returning-visitor or cross-session conversion counts. PostHog receives the network connection; the integration disables IP enrichment and geolocation properties. We do not claim that a network request is anonymous merely because names and email addresses are absent.

Analytics stays off until you select **Accept analytics**. You can accept, decline, or withdraw consent using **Analytics settings** at the bottom of any page. The site remembers your choice in browser storage. Do Not Track and Global Privacy Control keep analytics off. Collection runs only on the public documentation hostname.

See [PostHog's privacy policy](https://posthog.com/privacy). Browser blockers may prevent collection, so analytics totals are an observed sample of traffic rather than a complete census.

## Managed service inquiries

The **Managed services** link at the bottom of the documentation navigation opens the [inquiry page](./managed-services.md). It sends your name, email, organization (if provided), and message to our own HTTPS API, which stores them in a separate NornicDB database. Only authorized inquiry administrators can view submissions. We use these details to respond to your request; ask for deletion when we contact you. Do not include credentials or confidential data.

The documentation site counts form openings and successful submissions when analytics are enabled. These events contain no form fields. The private admin portal does not load analytics. Inquiry submission works when analytics are disabled.
