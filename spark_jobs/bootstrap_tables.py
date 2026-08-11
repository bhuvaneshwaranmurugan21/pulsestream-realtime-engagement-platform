"""Create the Iceberg tables whose snapshots form one publication bundle."""

from __future__ import annotations

import argparse

DDL = {
    "bronze_event": """(
      kafka_topic string, kafka_partition int, kafka_offset bigint, kafka_timestamp timestamp,
      schema_version string, event_id string, source_id string, user_token string,
      event_type string, event_time timestamp, arrival_time timestamp, consent_state string,
      page_id string, campaign_id string
    ) USING iceberg PARTITIONED BY (days(event_time), bucket(32, source_id))
    TBLPROPERTIES ('format-version'='2','write.target-file-size-bytes'='536870912')""",
    "contract_quarantine": """(
      kafka_topic string, kafka_partition int, kafka_offset bigint, kafka_timestamp timestamp,
      payload_sha256 string, reason string
    ) USING iceberg PARTITIONED BY (days(kafka_timestamp))
    TBLPROPERTIES ('format-version'='2')""",
    "curated_event": """(
      kafka_topic string, kafka_partition int, kafka_offset bigint, kafka_timestamp timestamp,
      schema_version string, event_id string, source_id string, user_token string,
      event_type string, event_time timestamp, arrival_time timestamp, consent_state string,
      page_id string, campaign_id string, beyond_watermark boolean, identity_sha256 string
    ) USING iceberg PARTITIONED BY (days(event_time), bucket(32, source_id))
    TBLPROPERTIES ('format-version'='2')""",
    "session_version": """(
      session_id string, user_token string, started_at timestamp, ended_at timestamp,
      event_count bigint, first_event_id string, last_event_id string
    ) USING iceberg PARTITIONED BY (days(started_at), bucket(32, user_token))
    TBLPROPERTIES ('format-version'='2')""",
    "engagement_aggregate": """(
      event_date date, event_type string, event_count bigint, unique_users bigint,
      sessions_started_on_date bigint
    ) USING iceberg PARTITIONED BY (months(event_date))
    TBLPROPERTIES ('format-version'='2')""",
    "correction_exception": """(
      event_id string, identity_sha256 string, reason string, generation_mode string,
      status string, kafka_partition int, kafka_offset bigint
    ) USING iceberg PARTITIONED BY (status)
    TBLPROPERTIES ('format-version'='2')""",
}


def main() -> None:
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="glue")
    parser.add_argument("--database", default="pulsestream")
    arguments = parser.parse_args()
    spark = SparkSession.builder.appName("pulsestream-bootstrap").getOrCreate()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {arguments.catalog}.{arguments.database}")
    for name, body in DDL.items():
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {arguments.catalog}.{arguments.database}.{name} {body}"
        )


if __name__ == "__main__":
    main()
