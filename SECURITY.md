# Security Policy

NornicDB is an actively maintained open-source database engine with a fast-moving main branch. This document explains how to report security issues responsibly and what kind of response to expect.

## Supported Versions

NornicDB does not currently maintain long-lived security branches for many historical versions.

| Version                                    | Status                           |
| ------------------------------------------ | -------------------------------- |
| Latest tagged release                      | Supported on a best-effort basis |
| `main`                                     | Actively maintained              |
| Older tags and untagged historical commits | Not supported                    |

Security fixes are made on `main` and shipped forward in subsequent releases. NornicDB does not maintain a backport-based security patch policy for older release lines at this point in time (may chnge subject to users)

## Reporting a Vulnerability

Please do **not** open a public GitHub issue, pull request, discussion, or Discord post for a suspected vulnerability.

Use GitHub private vulnerability reporting for this repository.

If you are unsure whether an issue is security-sensitive, report it privately first rather than opening a public issue.

When reporting, include:

- affected version, branch, or commit SHA
- deployment mode involved, if relevant: standalone, headless, embedded, clustered, Docker image, Heimdall-enabled, plugin-enabled
- reproduction steps or a minimal proof of concept
- impact assessment: confidentiality, integrity, availability, privilege escalation, remote code execution, data exposure, auth bypass, denial of service
- any relevant logs, panic output, stack traces, or malformed requests
- whether the issue is already known to be exploitable in default configuration or only under a specific setup

## Response Expectations

Current target response times are best-effort:

- initial triage acknowledgement: within 1-2 business days
- severity assessment and reproduction attempt: within 10 business days when enough detail is provided
- remediation timeline: depends on severity, exploitability, and release risk

For critical issues affecting remote attack surface, authentication boundaries, or data integrity, remediation is prioritized ahead of normal feature work.

## Disclosure Process

The normal process is:

1. confirm and reproduce the issue privately
2. assess severity and affected configurations
3. prepare and test a fix
4. publish the fix in a release or documented patch commit
5. disclose the issue publicly after a fix is available, when appropriate

Please avoid public disclosure until a fix or mitigation path is ready.

## Scope

Security-relevant reports include, for example:

- authentication or authorization bypass
- privilege escalation
- remote code execution
- arbitrary file read or write outside intended boundaries
- request smuggling, SSRF, CSRF, or injection vulnerabilities in exposed endpoints
- data corruption or integrity issues with security impact
- information disclosure across tenants, databases, sessions, or snapshots
- unsafe plugin, OAuth, webhook, or admin-surface behavior
- container, image, or dependency issues with meaningful exploitability in supported deployments

The following are generally out of scope unless they produce a clear security impact:

- missing hardening in local development setups
- self-inflicted misconfiguration without a product defect
- theoretical-only issues without a plausible exploit path
- denial of service that requires unrealistic local-only control already equivalent to admin access

## Hardening Notes

Some features in NornicDB are intentionally deployment-sensitive. Review configuration carefully when evaluating security posture, especially around:

- admin HTTP exposure
- authentication and RBAC configuration
- headless versus UI-enabled deployments
- plugin loading and execution
- Heimdall and model/tool integrations
- clustered, remote, or multi-database deployments

## Dependency and Supply Chain Issues

If the issue is caused by a third-party dependency, container base image, GitHub Action, or build-time component, please still report it through the same process and include the affected package, version, and advisory reference if known.
