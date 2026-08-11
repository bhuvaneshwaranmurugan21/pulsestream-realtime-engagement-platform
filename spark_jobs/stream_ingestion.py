"""Long-running Kafka-to-Iceberg contract and immutable-bronze stream."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from pulsestream.contracts import load_source_registry
from spark_jobs.transforms import gateway_contract_invalid


def event_schema() -> Any:
    from pyspark.sql.types import MapType, StringType, StructField, StructType

    fields = [
        StructField("schema_version", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("source_id", StringType(), True),
        StructField("user_token", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("arrival_time", StringType(), True),
        StructField("consent_state", StringType(), True),
        StructField("page_id", StringType(), True),
        StructField("campaign_id", StringType(), True),
    ]
    return StructType(fields), MapType(StringType(), StringType())


def decode_kafka(frame: Any, registered_sources: tuple[str, ...]) -> Any:
    from pyspark.sql import functions as function

    schema, map_schema = event_schema()
    decoded = (
        frame.selectExpr(
            "CAST(value AS STRING) AS raw_json",
            "topic AS kafka_topic",
            "partition AS kafka_partition",
            "offset AS kafka_offset",
            "timestamp AS kafka_timestamp",
        )
        .withColumn("raw_map", function.from_json("raw_json", map_schema))
        .withColumn("event", function.from_json("raw_json", schema))
        .select(
            "raw_json",
            "raw_map",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "event.*",
        )
        .withColumn("event_time", function.to_timestamp("event_time"))
        .withColumn("arrival_time", function.to_timestamp("arrival_time"))
    )
    return decoded.withColumn("contract_invalid", gateway_contract_invalid(registered_sources))


def write_microbatch(batch: Any, _batch_id: int, bronze_table: str, quarantine_table: str) -> None:
    from pyspark.sql import functions as function

    common = [
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "schema_version",
        "event_id",
        "source_id",
        "user_token",
        "event_type",
        "event_time",
        "arrival_time",
        "consent_state",
        "page_id",
        "campaign_id",
    ]
    batch.where("NOT contract_invalid").select(*common).writeTo(bronze_table).append()
    batch.where("contract_invalid").select(
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        function.sha2("raw_json", 256).alias("payload_sha256"),
        function.lit("gateway_contract_invalid").alias("reason"),
    ).writeTo(quarantine_table).append()


def main() -> None:
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--bronze-table", required=True)
    parser.add_argument("--quarantine-table", required=True)
    parser.add_argument("--sources", default="config/sources.json")
    args = parser.parse_args()
    registry = load_source_registry(Path(args.sources))
    spark = SparkSession.builder.appName("pulsestream-ingestion").getOrCreate()
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "true")
        .load()
    )
    decoded = decode_kafka(raw, tuple(sorted(registry)))
    query = (
        decoded.writeStream.foreachBatch(
            lambda batch, batch_id: write_microbatch(
                batch, batch_id, args.bronze_table, args.quarantine_table
            )
        )
        .option("checkpointLocation", args.checkpoint)
        .queryName("pulsestream-contract-bronze")
        .trigger(processingTime="30 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
