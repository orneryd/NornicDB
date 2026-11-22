# N152-112: document actual logout contract

Correct the false blacklist guarantee without introducing a new revocation architecture. The ticket explicitly permits correcting documentation; original verify-only, pooled reauthentication and revocation receipt designs remain open. Existing runtime behavior is unchanged.

Owners: server_auth.go handleLogout expires the cookie and audits; auth.go ValidateTokenWithFineGrainedProvider validates cached/signed JWTs and CurrentClaimsContext refreshes account/roles. Exp=0 means no expiry. External FGAC token validation is a separate owner and no universal provider-expiry guarantee is claimed.

Original ticket records independent macOS/Linux failure of the former documentation promise. Preserve that as evidence, not a now-passing runtime test. Source review finds no blacklist implementation. No broad authentication bypass or severity score is claimed.
