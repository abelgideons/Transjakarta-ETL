from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
from io import StringIO

# ----------------------------------------------------
# DAG Configuration
# ----------------------------------------------------
PROJECT_ID = ##### project id
LOCATION = "asia-southeast2"
SQL_PATH = "/opt/airflow/dags/sql"

default_args = {
    "owner": "abel",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ----------------------------------------------------
# Helper Function: format CSV in Python
# ----------------------------------------------------
# ----------------------------------------------
# CSV 1 - transaksi_bus_agg.csv
# ----------------------------------------------
def process_csv1(**context):
    hook = GCSHook()
    source_bucket = "transjakarta-bucket"
    destination_blob = f"formatted/transaksi_bus_agg_{context['ds_nodash']}.csv"

    # Download CSV raw
    raw_csv = hook.download(bucket_name=source_bucket, object_name="dummy_transaksi_bus.csv")
    df = pd.read_csv(StringIO(raw_csv.decode("utf-8")))

    # Filter pelanggan
    df = df[df["status_var"] == "S"]

    # Format tanggal
    df["transaction_date"] = pd.to_datetime(df["waktu_transaksi"]).dt.date

    # Grouping
    result = (
        df.groupby(["transaction_date", "card_type_var", "gate_in_boo"], as_index=False)
        .agg(total_pelanggan=("card_number_var", "nunique"), amount=("balance_after_int", "sum"))
    )

    # Upload hasil
    buffer = StringIO()
    result.to_csv(buffer, index=False)
    hook.upload(bucket_name=source_bucket, object_name=destination_blob, data=buffer.getvalue())


# ----------------------------------------------
# CSV 2 - route_transaction_agg.csv
# ----------------------------------------------
def process_csv2(**context):
    hook = GCSHook()
    source_bucket = "transjakarta-bucket"
    destination_blob = f"formatted/route_transaction_agg_{context['ds_nodash']}.csv"

    # Download 2 CSV: realisasi_bus & routes
    raw_bus = hook.download(bucket_name=source_bucket, object_name="dummy_realisasi_bus.csv")
    raw_routes = hook.download(bucket_name=source_bucket, object_name="dummy_routes.csv")
    raw_trx = hook.download(bucket_name=source_bucket, object_name="dummy_transaksi_bus.csv")

    rb = pd.read_csv(StringIO(raw_bus.decode("utf-8")))
    rr = pd.read_csv(StringIO(raw_routes.decode("utf-8")))
    trx = pd.read_csv(StringIO(raw_trx.decode("utf-8")))

    # --- format tanggal ---
    rb["tanggal_realisasi"] = pd.to_datetime(rb["tanggal_realisasi"], format="%m/%d/%Y", errors="coerce").dt.date

    # --- format bus_body_no ---
    def format_bus_body_no(x):
        if pd.isna(x):
            return None
        prefix, suffix = x[:3], x[3:]
        if len(suffix) == 2 or "_" in suffix:
            return f"{prefix}-{suffix.zfill(3).replace('_', '')}"
        return f"{prefix}-{suffix}"

    rb["bus_body_no"] = rb["bus_body_no"].apply(format_bus_body_no)

    # Join route_name
    route = rb.merge(rr, left_on="rute_realisasi", right_on="route_code", how="left")

    # --- Format trx ---
    trx = trx[trx["status_var"] == "S"]
    trx["transaction_date"] = pd.to_datetime(trx["waktu_transaksi"]).dt.date
    trx["no_body_var"] = trx["no_body_var"].apply(format_bus_body_no)

    # Join transaksi dengan route (on tanggal + bus body)
    merged = trx.merge(
        route,
        left_on=["transaction_date", "no_body_var"],
        right_on=["tanggal_realisasi", "bus_body_no"],
        how="left",
    )

    # Group by
    result = (
        merged.groupby(["transaction_date", "route_code", "route_name", "gate_in_boo"], as_index=False)
        .agg(total_pelanggan=("card_number_var", "nunique"), amount=("balance_after_int", "sum"))
    )

    # Upload hasil
    buffer = StringIO()
    result.to_csv(buffer, index=False)
    hook.upload(bucket_name=source_bucket, object_name=destination_blob, data=buffer.getvalue())


# ----------------------------------------------
# CSV 3 - fare_transaction_agg.csv
# ----------------------------------------------
def process_csv3(**context):
    hook = GCSHook()
    source_bucket = "transjakarta-bucket"
    destination_blob = f"formatted/fare_transaction_agg_{context['ds_nodash']}.csv"

    raw_csv = hook.download(bucket_name=source_bucket, object_name="dummy_transaksi_bus.csv")
    df = pd.read_csv(StringIO(raw_csv.decode("utf-8")))

    df = df[df["status_var"] == "S"]
    df["transaction_date"] = pd.to_datetime(df["waktu_transaksi"]).dt.date

    result = (
        df.groupby(["transaction_date", "fare_int", "gate_in_boo"], as_index=False)
        .agg(total_pelanggan=("card_number_var", "nunique"), amount=("fare_int", "sum"))
    )

    buffer = StringIO()
    result.to_csv(buffer, index=False)
    hook.upload(bucket_name=source_bucket, object_name=destination_blob, data=buffer.getvalue())

# ----------------------------------------------------
# Define DAG
# ----------------------------------------------------
with DAG(
    dag_id="tj_etl_pipeline",
    description="ETL pipeline for TJ data (GCS → BigQuery staging)",
    default_args=default_args,
    schedule_interval="0 7 * * *",  # run tiap jam 7 pagi
    start_date=days_ago(1),
    catchup=False,
    tags=["tj", "etl", "bq"],
) as dag:

    # Load SQL scripts from file
    with open(f"{SQL_PATH}/insert_stg_transaksi_halte.sql", "r") as f:
        query_table_1 = f.read()

    with open(f"{SQL_PATH}/insert_stg_transaksi_bus.sql", "r") as f:
        query_table_2 = f.read()

    # Task 1 - Insert & Deduplicate stg_transaksi_halte
    deduplicate_table_1 = BigQueryInsertJobOperator(
        task_id="insert_stg_transaksi_halte",
        configuration={
            "query": {
                "query": query_table_1,
                "useLegacySql": False,
            }
        },
        location=LOCATION,
    )

    # Task 2 - Insert & Deduplicate stg_transaksi_bus
    deduplicate_table_2 = BigQueryInsertJobOperator(
        task_id="insert_stg_transaksi_bus",
        configuration={
            "query": {
                "query": query_table_2,
                "useLegacySql": False,
            }
        },
        location=LOCATION,
    )

    # Task 3 - Format CSV file (Python)
    from airflow.operators.python import PythonOperator

    csv1 = PythonOperator(
        task_id="format_csv1",
        python_callable=process_csv1,
        provide_context=True,
    )

    csv2 = PythonOperator(
        task_id="format_csv2",
        python_callable=process_csv2,
        provide_context=True,
    )

    csv3 = PythonOperator(
        task_id="format_csv3",
        python_callable=process_csv3,
        provide_context=True,
    )

    # Dependencies
    deduplicate_table_1 >> deduplicate_table_2 >> csv1 >> csv2 >> csv3
