"""
DAG 5: Master Pipeline
End-to-end orchestration — triggers all pipeline DAGs in sequence.
This is the single DAG to run for a full pipeline execution.

Schedule: Daily at 11 PM
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import timedelta
from orchestration.dags.dag_config import (
    DEFAULT_ARGS, TAGS_MASTER
)

with DAG(
    dag_id="banking_00_master_pipeline",
    description="Master DAG - orchestrates full end-to-end pipeline",
    default_args=DEFAULT_ARGS,
    schedule="0 23 * * *",
    catchup=False,
    max_active_runs=1,
    tags=TAGS_MASTER,
    doc_md="""
    ## Master Pipeline DAG
    Orchestrates the complete Banking Data Platform pipeline.

    ### Pipeline Flow
```
    start
      ↓
    trigger_ingestion (DAG 1)
      ↓
    wait_for_ingestion
      ↓
    trigger_bronze_to_silver (DAG 2)
      ↓
    wait_for_silver
      ↓
    trigger_silver_to_gold (DAG 3)
      ↓
    wait_for_gold
      ↓
    end
```

    ### Schedule
    Runs daily at 11 PM — completes by 3 AM next day.
    """,
) as dag:

    start = EmptyOperator(task_id="start")

    # Trigger Ingestion DAG
    trigger_ingestion = TriggerDagRunOperator(
        task_id="trigger_ingestion",
        trigger_dag_id="banking_01_ingestion",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
    )

    # Trigger Bronze to Silver DAG
    trigger_bronze_silver = TriggerDagRunOperator(
        task_id="trigger_bronze_to_silver",
        trigger_dag_id="banking_02_bronze_to_silver",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
    )

    # Trigger Silver to Gold DAG
    trigger_silver_gold = TriggerDagRunOperator(
        task_id="trigger_silver_to_gold",
        trigger_dag_id="banking_03_silver_to_gold",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
    )

    end = EmptyOperator(task_id="end")

    # ── Pipeline Flow ─────────────────────────────────────────
    (
        start
        >> trigger_ingestion
        >> trigger_bronze_silver
        >> trigger_silver_gold
        >> end
    )