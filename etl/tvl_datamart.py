from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta

"""

"""

@dag(
    schedule_interval="*/20 * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    concurrency=1,
    max_active_runs=1,
    tags=['venom', 'tokens', 'tvl', 'datamart']
)
def venom_tvl_datamart():
      tvl_history_entry = PostgresOperator(
        task_id="tvl_history_entry",
        postgres_conn_id="venom_db",
        sql=[
            """
            insert into public.tvl_history(build_time, pool, 
              token, reserve
            )

            select now(), id as pool, token, tvl_native as reserve
            from venomics.tvl_current
            """
        ]
    )


venom_tvl_datamart_dag = venom_tvl_datamart()