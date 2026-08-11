# Operations and failure runbook

## Release sequence

1. Run quality, Spark, infrastructure and security workflows.
2. Build the Lambda package and upload Spark entrypoints to the artifact prefix.
3. Run `terraform plan`; a second engineer reviews IAM, data retention and replacement actions.
4. Apply infrastructure in stage, bootstrap Iceberg tables, then exercise the failure lab.
5. Register Airflow variables from Terraform outputs. Never place credentials in DAG variables.
6. Run one LIVE generation, validate its manifest and publish it by compare-and-swap.

## Alert triage

| Signal | First checks | Safe response |
|---|---|---|
| Kafka lag increasing | Producer rate, executor saturation, partition skew | Scale consumers; do not skip offsets |
| Contract quarantine spike | Source family, schema version, rejected field | Pause affected producer; preserve payload hashes |
| Watermark/late rate spike | Mobile/partner source, clock skew, outage window | Keep LIVE moving; schedule bounded replay |
| EMR generation failed | Driver log, source manifest hash, Iceberg commit conflict | Retry same immutable generation input |
| Publication conflict | Active pointer and requested parent | Mark stale candidate superseded; rebuild from active |
| Small-file growth | Write rate, partition cardinality, compaction age | Compact candidate/active tables without changing truth |

## Controlled replay

1. Identify OPEN corrections and the minimum affected event-time interval.
2. Create a REPLAY source manifest pinned to the current published frontier and exact bronze
   snapshots. A replay does not consume a new offset frontier.
3. Build all four candidate snapshots using the same versioned rules, or explicitly record a new
   rule hash.
4. Compare row conservation, correction counts, session deltas and aggregate deltas.
5. Publish only if the current active generation still equals the replay parent.
6. If the parent changed, discard the stale candidate and rebuild; never force the pointer.

## Rollback

Publication is metadata-only. Rollback creates a new generation whose four snapshot references
point to a previously validated bundle, then advances the pointer with the normal parent/version
checks. Directly editing the active item or individual table references is prohibited.

## Disaster recovery

- S3 versioning protects manifests and lake objects from accidental overwrite.
- DynamoDB point-in-time recovery protects generation state.
- Infrastructure code reconstructs regional services, but a real production deployment also needs
  tested cross-region replication, restored KMS access and documented RTO/RPO approval.
- A quarterly exercise should restore the active manifest, verify every snapshot and rebuild marts
  in an isolated account.

## Failure lab

`make refresh-evidence` injects a crash immediately after offset 777 is committed, resumes from
durable checkpoints, replays all arrivals, races a stale candidate and verifies LIVE/REPLAY
semantics. It refuses concurrent runs through an exclusive lock so two evidence processes cannot
corrupt the same temporary database.
