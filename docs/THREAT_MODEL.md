# Threat model

## Protected assets

User linkage tokens, event history, source contracts, publication manifests, Iceberg snapshots,
Kafka offsets and the active generation pointer are integrity- or confidentiality-sensitive.

## Controls

| Threat | Control |
|---|---|
| Raw identifiers leak into analytics | HMAC tokenization before the gateway contract |
| Credentials/card data enter bronze | Recursive prohibited-field rejection; redacted quarantine |
| Unknown producer bypasses ownership | Versioned 50-source registry and source allowlist |
| Event replay creates duplicate effect | Canonical identity fingerprint and offset checkpoint |
| Attacker publishes partial/stale data | Manifest hashes and DynamoDB conditional transaction |
| Public or plaintext lake access | S3 public-access block, TLS deny policy, KMS encryption |
| Over-privileged runtime | Separate EMR and publication roles with scoped data actions |
| Secret committed to source | Gitleaks, Bandit and dependency audit workflows |

## Residual risks before production

The reference infrastructure does not configure an enterprise identity provider, producer-side
certificate lifecycle, multi-account lake governance, cross-region recovery or a SIEM. A production
review must add those controls, validate KMS key policies, restrict network egress and run an
independent penetration/security assessment.
