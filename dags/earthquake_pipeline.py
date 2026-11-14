"""Earthquake pipeline DAG.

Every 5 minutes:  ingest  ->  (Day 2) dbt_build  ->  (Day 4) publish_serving_copy

Only ONE task writes to the DuckDB file at a time, and max_active_runs=1 stops
two runs from overlapping. That is the single-writer rule from the architecture.
"""
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="earthquake_pipeline",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,  # do not "replay" missed 5-minute slots
    max_active_runs=1,  # never two runs at once (single writer)
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["earthquakes"],
)
def earthquake_pipeline():
    @task
    def ingest() -> dict:
        # Imported inside the task so DAG parsing stays fast.
        from include.ingest import run

        stats = run()
        print(f"ingest finished: {stats}")
        return stats

    ingest()

    # Day 2: add the dbt task and chain it:  ingest_task >> dbt_build
    # dbt_build = BashOperator(
    #     task_id="dbt_build",
    #     bash_command="cd /opt/airflow/dbt && $DBT_BIN build",
    # )


earthquake_pipeline()