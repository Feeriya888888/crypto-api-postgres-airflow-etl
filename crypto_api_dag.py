import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pipeline import run_etl_batch

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


@dag(
    dag_id="crypto_api_to_dwh_daily",
    default_args=default_args,
    description="Daily idempotent ETL from CoinGecko REST API to PostgreSQL DWH",
    schedule_interval="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["portfolio", "api", "dwh", "crypto"],
)
def crypto_etl_dag():

    @task
    def load_and_transform(ds=None):
        db_uri = os.getenv(
            "DWH_CONN_URI",
            "postgresql+psycopg2://postgres:postgres@postgres:5432/crypto_dwh",
        )
        run_etl_batch(db_uri=db_uri, batch_date=ds)

    load_and_transform()


dag_instance = crypto_etl_dag()
