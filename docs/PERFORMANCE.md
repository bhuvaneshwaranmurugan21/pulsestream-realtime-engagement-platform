# Performance and scale model

## Design point

The architecture is modeled for 50+ logical producers, 10M+ events/day and sub-five-minute data
freshness. These are design targets, not measured production results.

At 10M events/day, the average rate is about 116 events/second. A 10x peak is about 1,160
events/second. With a conservative 2 KiB encoded event, that peak is roughly 2.3 MiB/second before
replication and storage compression. Twenty-four Kafka partitions give ample baseline parallelism;
partition count must still be validated against key skew and the actual peak envelope.

## Bound the expensive state

- Deduplication is identity-based and must use retained table state or a bounded state-store policy;
  an unbounded in-memory streaming map is rejected.
- Watermarks are tracked per partition and carried across generations. Restarting a job does not
  reset the late-data boundary.
- Session windows use tokenized user identity and deterministic event-time ordering.
- Iceberg writes target 384 MiB files. Compaction is triggered by file count and age, not blindly on
  every batch.
- Candidate branches isolate retries and concurrent builds from the published snapshots.

## Benchmark plan

A deployment is not performance-proven until the following stage test publishes its raw results:

| Test | Input | Acceptance evidence |
|---|---|---|
| Sustained ingest | 24 hours at expected peak | p50/p95/p99 end-to-end freshness and zero loss |
| Burst | 10x average for 30 minutes | lag recovery curve and executor saturation |
| Skew | 40% of events on one key family | partition lag distribution and mitigation |
| Late storm | 5% events older than watermark | LIVE stability and replay duration |
| Restart | kill driver/executors mid-commit | no offset gaps and no duplicate effects |
| Publication race | two candidates, same parent | exactly one active generation |
| Small files | seven days of realistic traffic | file-size histogram and compaction cost |

Record AWS service versions, Spark configuration, input generator SHA, cost, table statistics and
CloudWatch exports. Do not convert modeled targets into resume metrics until that evidence exists.
