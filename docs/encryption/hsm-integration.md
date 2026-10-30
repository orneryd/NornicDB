# HSM Integration Notes

This document describes how NornicDB maps CMEK/HSM integrations.

## FIPS and Responsibility Boundaries

NornicDB delegates KEK operations to configured providers (KMS/HSM wrappers).  
FIPS 140-2/3 posture depends on:

1. selected provider service (for example AWS KMS/Azure Key Vault HSM tiers),
2. endpoint selection (for example FIPS endpoints where applicable),
3. deployment controls and identity policy.

## AWS

Use KMS keys backed by your compliance-approved policy and endpoint requirements.

Recommended:

- dedicated CMK for NornicDB,
- least-privilege IAM role,
- CloudTrail enabled for all key operations.
- configure `encryption_rotation_enabled: true` so wrapped DEKs are periodically rewrapped against the active KMS key state.

## Azure

Use Key Vault with managed identity or service principal and restricted role assignments.

Recommended:

- soft delete + purge protection,
- centralized Activity Logs export.
- HSM-backed Key Vault tiers when your policy requires hardware-backed custody.

## GCP

Use Cloud KMS with a dedicated crypto key for NornicDB and service-account scope limited to encrypt/decrypt.

Recommended:

- Cloud Audit Logs enabled for KMS API activity,
- dedicated key ring per environment,
- credentials injected from a secrets manager or workload identity rather than committed files.

## Thales/CloudHSM

NornicDB currently integrates through provider wrappers in this branch.  
Direct PKCS#11/HSM socket provider support can be added as a dedicated `kms.KeyProvider`.

## Operational Boundary

NornicDB manages:

- wrapped DEK persistence,
- provider audit event emission,
- HMAC signing of local audit events,
- wrapped-DEK rotation scheduling policy.

The KMS/HSM manages:

- KEK custody,
- access control,
- hardware-backed assurances,
- provider-native audit trails and compliance posture.
