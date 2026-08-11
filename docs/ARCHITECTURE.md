# Architecture

## Problem statement

Engagement events arrive from web, mobile, backend, campaign and partner producers. Network
retries produce duplicates; mobile clients arrive late; producer upgrades violate schemas; and a
historical correction can change sessions and aggregates together. Publishing each table
independently would expose combinations that never belonged to the same computation.

PulseStream separates continuous ingestion from controlled publication. The stream is always
captured into immutable bronze. A bounded generation then reads exact offsets, creates compatible
candidate snapshots and publishes a single pointer only after the whole bundle validates.

## Data plane

1. Producers send a versioned gateway event. The boundary rejects secrets and replaces the source
   user identifier with an HMAC token.
2. MSK retains ordered records by partition. Spark Structured Streaming records valid events in
   Iceberg bronze and only hashes/redacted diagnostics in contract quarantine.
3. A source manifest fixes the topic, exact offset ranges and source snapshot identifiers.
4. The generation job classifies lateness against the prior partition watermark, gates event
   identity, and constructs event, session, aggregate and correction tables on one Iceberg branch.
5. The job emits a publication manifest binding code, rules, contract, source registry, source
   offsets and exact candidate snapshot IDs.
6. Lambda registers the candidate, then a DynamoDB transaction conditionally changes the active
   generation and marks the candidate published. A stale parent loses the race and remains
   invisible.
7. Redshift/dbt consumers resolve the active manifest and expose only its compatible snapshots.

## State ownership

| State | Authoritative owner | Recovery rule |
|---|---|---|
| Ordered event history | MSK + immutable S3/Iceberg bronze | Re-read the manifest ranges |
| Source frontier | Publication manifest | LIVE advances; REPLAY never rewinds |
| Deduplication identity | Curated Iceberg candidate | Deterministic first `(partition, offset)` wins |
| Event-time corrections | `correction_exception` snapshot | OPEN in LIVE, RESOLVED by published REPLAY |
| Visible data bundle | DynamoDB active pointer | Conditional parent and pointer version |
| Analytical marts | dbt/Redshift | Rebuild from active snapshots |

## Publication protocol

A generation is immutable after registration and transitions through
`BUILDING -> CANDIDATE -> PUBLISHED` or `SUPERSEDED`. Its manifest must contain exactly four
configured snapshot references:

- `curated_event`
- `session_version`
- `engagement_aggregate`
- `correction_exception`

The publish transaction checks candidate status and manifest hash, checks the pointer version and
parent generation, changes the active pointer, and marks the candidate published. There is no
period during which only some tables are active.

## Invariants

- Kafka partition offsets are contiguous within a LIVE input range.
- A replay is pinned to an already published frontier and cannot advance or rewind it.
- `event_id` plus canonical business payload has at most one accepted financial/analytical effect.
- Same identity and same payload is a duplicate; same identity and different payload is quarantine.
- Processing time never decides session truth; `(event_time, event_id)` is the deterministic order.
- Essential-only events remain in governed storage but do not contribute to behavioral sessions.
- A published manifest binds every semantic input needed to reproduce the generation.

## Production/runtime boundary

The local SQLite model is intentionally not the production engine. It is an executable oracle for
restart, event-time, replay and publication semantics. Production mappings are Kafka/MSK for
ordered offsets, Spark for distributed transforms, Iceberg for snapshot isolation, DynamoDB for
compare-and-swap publication, and Airflow/EMR Serverless for controlled execution.
