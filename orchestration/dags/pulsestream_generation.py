"""Airflow-controlled LIVE and REPLAY generation build; deployment supplies connections."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

with DAG(
    dag_id="pulsestream_generation",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "generation_id": Param(type="string", pattern=r"^[A-Za-z0-9._:-]{1,128}$"),
        "mode": Param(default="LIVE", enum=["LIVE", "REPLAY"]),
        "source_manifest": Param(type="string", pattern=r"^s3://"),
        "parent_publication_manifest": Param(default="", type="string"),
        "output_publication_manifest": Param(type="string", pattern=r"^s3://"),
    },
    tags=["streaming", "iceberg", "controlled-replay"],
) as dag:
    build = EmrServerlessStartJobOperator(
        task_id="build_candidate_bundle",
        application_id="{{ var.value.pulsestream_emr_application_id }}",
        execution_role_arn="{{ var.value.pulsestream_emr_execution_role }}",
        job_driver={
            "sparkSubmit": {
                "entryPoint": "{{ var.value.pulsestream_generation_entrypoint }}",
                "entryPointArguments": [
                    "--generation-id",
                    "{{ params.generation_id }}",
                    "--mode",
                    "{{ params.mode }}",
                    "--source-manifest",
                    "{{ params.source_manifest }}",
                    "--parent-publication-manifest",
                    "{{ params.parent_publication_manifest }}",
                    "--output-publication-manifest",
                    "{{ params.output_publication_manifest }}",
                    "--implementation-sha256",
                    "{{ var.value.pulsestream_implementation_sha256 }}",
                    "--bronze-table",
                    "glue.pulsestream.bronze_event",
                    "--curated-table",
                    "glue.pulsestream.curated_event",
                    "--session-table",
                    "glue.pulsestream.session_version",
                    "--aggregate-table",
                    "glue.pulsestream.engagement_aggregate",
                    "--correction-table",
                    "glue.pulsestream.correction_exception",
                ],
                "sparkSubmitParameters": (
                    "--conf spark.sql.extensions="
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
                ),
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": "{{ var.value.pulsestream_log_uri }}"}
            }
        },
        wait_for_completion=True,
    )
    register = LambdaInvokeFunctionOperator(
        task_id="register_candidate",
        function_name="{{ var.value.pulsestream_register_function }}",
        payload="""{
          "generation_id":"{{ params.generation_id }}",
          "mode":"{{ params.mode }}",
          "manifest_uri":"{{ params.output_publication_manifest }}"
        }""",
    )
    publish = LambdaInvokeFunctionOperator(
        task_id="compare_and_swap_publication",
        function_name="{{ var.value.pulsestream_publish_function }}",
        payload="""{
          "generation_id":"{{ params.generation_id }}"
        }""",
    )
    build >> register >> publish
